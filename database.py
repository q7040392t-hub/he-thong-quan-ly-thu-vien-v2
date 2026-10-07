import json
import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from bm25_service import rank_books

try:
    import mysql.connector
    from mysql.connector import IntegrityError as MySQLIntegrityError
    from mysql.connector import pooling as mysql_pooling
except ImportError:
    mysql = None
    mysql_pooling = None
    class MySQLIntegrityError(Exception):
        pass

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / '.env')

# Đọc cấu hình theo thứ tự:
# 1) Biến môi trường / file .env khi chạy local
# 2) Streamlit Secrets khi deploy trên Streamlit Cloud
# 3) Giá trị mặc định
def _config(name, default=''):
    value = os.getenv(name)
    if value is not None and str(value).strip() != '':
        return str(value)

    try:
        import streamlit as st
        if name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass

    return str(default)


# Streamlit Cloud mặc định dùng SQLite để không cố kết nối MySQL localhost.
# Khi chạy local, chỉ cần DB_ENGINE=mysql trong .env là vẫn dùng MySQL như cũ.
DB_ENGINE = _config('DB_ENGINE', 'sqlite').strip().lower()
DB_PATH = BASE_DIR / 'library.db'
SEED_BOOKS = BASE_DIR / 'books_seed.json'

MYSQL_HOST = _config('MYSQL_HOST', '127.0.0.1')
MYSQL_PORT = int(_config('MYSQL_PORT', '3306'))
MYSQL_USER = _config('MYSQL_USER', 'root')
MYSQL_PASSWORD = _config('MYSQL_PASSWORD', '')
MYSQL_DATABASE = _config('MYSQL_DATABASE', 'smartlibrary')

INTEGRITY_ERRORS = (sqlite3.IntegrityError, MySQLIntegrityError)
_MYSQL_POOL = None
_MYSQL_DB_READY = False
_MYSQL_LOCK = threading.Lock()

# Avoid doing the overdue scan repeatedly during one burst of UI reruns.
_OVERDUE_LOCK = threading.Lock()
_OVERDUE_LAST_REFRESH = 0.0
_OVERDUE_REFRESH_SECONDS = 30.0


class RowProxy(dict):
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class CursorProxy:
    def __init__(self, cursor=None, lastrowid=None):
        self._cursor = cursor
        self.lastrowid = lastrowid

    def fetchone(self):
        if self._cursor is None:
            return None
        row = self._cursor.fetchone()
        self._cursor.close()
        self._cursor = None
        return RowProxy(row) if row is not None else None

    def fetchall(self):
        if self._cursor is None:
            return []
        rows = self._cursor.fetchall()
        self._cursor.close()
        self._cursor = None
        return [RowProxy(r) for r in rows]


def _mysql_translate(sql):
    sql = sql.replace('INSERT OR IGNORE INTO', 'INSERT IGNORE INTO')
    sql = sql.replace(' COLLATE NOCASE', '')
    # `key` is a reserved word in MySQL; quote it only for the settings table queries.
    sql = re.sub(r'settings\s*\(\s*key\s*,\s*value\s*\)', 'settings(`key`,value)', sql, flags=re.IGNORECASE)
    sql = re.sub(r'WHERE\s+key\s*=', 'WHERE `key`=', sql, flags=re.IGNORECASE)
    conflict = re.search(
        r'ON\s+CONFLICT\(([^)]+)\)\s+DO\s+UPDATE\s+SET\s+(.+)$',
        sql, flags=re.IGNORECASE | re.DOTALL,
    )
    if conflict:
        assignments = conflict.group(2).strip()
        assignments = re.sub(
            r'excluded\.([A-Za-z_][A-Za-z0-9_]*)',
            r'VALUES(\1)', assignments, flags=re.IGNORECASE,
        )
        sql = sql[:conflict.start()] + 'ON DUPLICATE KEY UPDATE ' + assignments
    return sql.replace('?', '%s')


class MySQLConnectionProxy:
    def __init__(self, raw):
        self.raw = raw

    def execute(self, sql, params=()):
        cur = self.raw.cursor(dictionary=True)
        cur.execute(_mysql_translate(sql), tuple(params or ()))
        lastrowid = cur.lastrowid
        if cur.with_rows:
            return CursorProxy(cur, lastrowid)
        cur.close()
        return CursorProxy(None, lastrowid)

    def executescript(self, script):
        for statement in script.split(';'):
            statement = statement.strip()
            if statement:
                self.execute(statement)

    def commit(self): self.raw.commit()
    def rollback(self): self.raw.rollback()
    def close(self): self.raw.close()


def _safe_mysql_database_name():
    if not re.fullmatch(r'[A-Za-z0-9_]+', MYSQL_DATABASE):
        raise RuntimeError('MYSQL_DATABASE chỉ được chứa chữ, số và dấu gạch dưới.')
    return MYSQL_DATABASE


def _ensure_mysql_database():
    global _MYSQL_DB_READY
    if _MYSQL_DB_READY: return
    if mysql_pooling is None:
        raise RuntimeError('Chưa cài mysql-connector-python. Hãy chạy: python -m pip install -r requirements.txt')
    with _MYSQL_LOCK:
        if _MYSQL_DB_READY: return
        name = _safe_mysql_database_name()
        raw = mysql.connector.connect(
            host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
            password=MYSQL_PASSWORD, connection_timeout=8, autocommit=True,
        )
        try:
            cur = raw.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS `{name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            cur.close()
        finally:
            raw.close()
        _MYSQL_DB_READY = True


def _mysql_pool():
    global _MYSQL_POOL
    _ensure_mysql_database()
    if _MYSQL_POOL is None:
        with _MYSQL_LOCK:
            if _MYSQL_POOL is None:
                _MYSQL_POOL = mysql_pooling.MySQLConnectionPool(
                    pool_name='smartlibrary_pool', pool_size=6, pool_reset_session=True,
                    host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                    password=MYSQL_PASSWORD, database=_safe_mysql_database_name(),
                    charset='utf8mb4', collation='utf8mb4_unicode_ci',
                    autocommit=False, connection_timeout=8,
                )
    return _MYSQL_POOL


@contextmanager
def get_conn():
    if DB_ENGINE == 'mysql':
        raw = _mysql_pool().get_connection()
        conn = MySQLConnectionProxy(raw)
    else:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA busy_timeout = 5000')
    try:
        yield conn
        conn.commit()
    except Exception:
        try: conn.rollback()
        except Exception: pass
        raise
    finally:
        conn.close()


def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _today():
    return date.today().isoformat()


def _columns(conn, table):
    if DB_ENGINE == 'mysql':
        return [r['Field'] for r in conn.execute(f'SHOW COLUMNS FROM `{table}`').fetchall()]
    return [r[1] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()]


def _add_column(conn, table, definition):
    name = definition.split()[0]
    if name in _columns(conn, table):
        return
    if DB_ENGINE == 'mysql':
        mysql_defs = {
            'view_count': 'view_count INT NOT NULL DEFAULT 0',
            'isbn': 'isbn VARCHAR(100)',
            'publisher': 'publisher VARCHAR(255)',
            'member_code': 'member_code VARCHAR(50)',
            'card_type': "card_type VARCHAR(80) DEFAULT 'Sinh viên'",
            'card_expiry': 'card_expiry VARCHAR(20)',
            'address': 'address VARCHAR(500)',
            'max_books': 'max_books INT',
            'birth_date': 'birth_date VARCHAR(20)',
            'gender': 'gender VARCHAR(30)',
            'auto_locked': 'auto_locked INT NOT NULL DEFAULT 0',
            'locked_reason': 'locked_reason VARCHAR(500)',
            'renew_count': 'renew_count INT NOT NULL DEFAULT 0',
            'fine_status': "fine_status VARCHAR(30) NOT NULL DEFAULT 'none'",
            'handled_at': 'handled_at VARCHAR(32)',
            'note': 'note TEXT',
        }
        definition = mysql_defs.get(name, definition.replace(' INTEGER', ' INT'))
    conn.execute(f'ALTER TABLE {table} ADD COLUMN {definition}')


def _ensure_index(conn, table, index_name, columns):
    if DB_ENGINE == 'mysql':
        row = conn.execute(
            'SELECT COUNT(*) AS n FROM information_schema.statistics WHERE table_schema=? AND table_name=? AND index_name=?',
            (MYSQL_DATABASE, table, index_name)
        ).fetchone()
        if not row['n']:
            conn.execute(f'CREATE INDEX {index_name} ON {table}({columns})')
    else:
        conn.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON {table}({columns})')


def connection_info():
    return {
        'engine': DB_ENGINE,
        'host': MYSQL_HOST if DB_ENGINE == 'mysql' else 'local file',
        'port': MYSQL_PORT if DB_ENGINE == 'mysql' else '',
        'database': MYSQL_DATABASE if DB_ENGINE == 'mysql' else DB_PATH.name,
        'user': MYSQL_USER if DB_ENGINE == 'mysql' else '',
    }


def test_connection():
    try:
        with get_conn() as conn:
            row = conn.execute('SELECT 1 AS ok').fetchone()
        return bool(row and row['ok'] == 1), 'Kết nối database thành công.'
    except Exception as exc:
        return False, f'Kết nối database thất bại: {exc}'

def init_db():
    mysql_schema = """
    CREATE TABLE IF NOT EXISTS users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(100) NOT NULL UNIQUE,
        password_hash VARCHAR(255) NOT NULL,
        full_name VARCHAR(255) NOT NULL,
        email VARCHAR(255), phone VARCHAR(50),
        role VARCHAR(30) NOT NULL DEFAULT 'reader',
        status VARCHAR(30) NOT NULL DEFAULT 'active',
        created_at VARCHAR(32) NOT NULL,
        member_code VARCHAR(50), card_type VARCHAR(80) DEFAULT 'Sinh viên',
        card_expiry VARCHAR(20), address VARCHAR(500), max_books INT,
        birth_date VARCHAR(20), gender VARCHAR(30),
        auto_locked INT NOT NULL DEFAULT 0, locked_reason VARCHAR(500)
    );
    CREATE TABLE IF NOT EXISTS books (
        id VARCHAR(50) PRIMARY KEY,
        title VARCHAR(500) NOT NULL, author VARCHAR(255) NOT NULL,
        type VARCHAR(100) NOT NULL, category VARCHAR(255) NOT NULL,
        year INT, quantity INT NOT NULL DEFAULT 0, available INT NOT NULL DEFAULT 0,
        location VARCHAR(255), description TEXT, cover_image VARCHAR(500),
        created_at VARCHAR(32) NOT NULL, view_count INT NOT NULL DEFAULT 0,
        isbn VARCHAR(100), publisher VARCHAR(255)
    );
    CREATE TABLE IF NOT EXISTS loans (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL, book_id VARCHAR(50) NOT NULL,
        request_date VARCHAR(32) NOT NULL, approved_date VARCHAR(32), due_date VARCHAR(20), returned_date VARCHAR(32),
        status VARCHAR(40) NOT NULL DEFAULT 'pending', fine_amount INT NOT NULL DEFAULT 0,
        note TEXT, renew_count INT NOT NULL DEFAULT 0, fine_status VARCHAR(30) NOT NULL DEFAULT 'none',
        FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(book_id) REFERENCES books(id)
    );
    CREATE TABLE IF NOT EXISTS reservations (
        id INT AUTO_INCREMENT PRIMARY KEY, user_id INT NOT NULL, book_id VARCHAR(50) NOT NULL,
        created_at VARCHAR(32) NOT NULL, status VARCHAR(40) NOT NULL DEFAULT 'pending', handled_at VARCHAR(32), note TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(book_id) REFERENCES books(id)
    );
    CREATE TABLE IF NOT EXISTS notifications (
        id INT AUTO_INCREMENT PRIMARY KEY, user_id INT NOT NULL, title VARCHAR(255) NOT NULL,
        content TEXT NOT NULL, is_read INT NOT NULL DEFAULT 0, created_at VARCHAR(32) NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS settings (`key` VARCHAR(100) PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS fines (
        id INT AUTO_INCREMENT PRIMARY KEY, loan_id INT NOT NULL UNIQUE, user_id INT NOT NULL,
        amount INT NOT NULL DEFAULT 0, reason TEXT, status VARCHAR(30) NOT NULL DEFAULT 'unpaid',
        created_at VARCHAR(32) NOT NULL, paid_at VARCHAR(32), note TEXT,
        FOREIGN KEY(loan_id) REFERENCES loans(id), FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS activity_logs (
        id INT AUTO_INCREMENT PRIMARY KEY, user_id INT, action VARCHAR(255) NOT NULL,
        detail TEXT, created_at VARCHAR(32) NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id)
    );
    CREATE TABLE IF NOT EXISTS favorites (
        id INT AUTO_INCREMENT PRIMARY KEY, user_id INT NOT NULL, book_id VARCHAR(50) NOT NULL,
        created_at VARCHAR(32) NOT NULL, UNIQUE KEY uq_favorite_user_book(user_id,book_id),
        FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(book_id) REFERENCES books(id)
    );
    CREATE TABLE IF NOT EXISTS support_tickets (
        id INT AUTO_INCREMENT PRIMARY KEY, user_id INT NOT NULL, subject VARCHAR(255) NOT NULL,
        content TEXT NOT NULL, status VARCHAR(40) NOT NULL DEFAULT 'open', created_at VARCHAR(32) NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """
    sqlite_schema = """
    CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL, full_name TEXT NOT NULL, email TEXT, phone TEXT, role TEXT NOT NULL DEFAULT 'reader', status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS books (id TEXT PRIMARY KEY, title TEXT NOT NULL, author TEXT NOT NULL, type TEXT NOT NULL, category TEXT NOT NULL, year INTEGER, quantity INTEGER NOT NULL DEFAULT 0, available INTEGER NOT NULL DEFAULT 0, location TEXT, description TEXT, cover_image TEXT, created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS loans (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, book_id TEXT NOT NULL, request_date TEXT NOT NULL, approved_date TEXT, due_date TEXT, returned_date TEXT, status TEXT NOT NULL DEFAULT 'pending', fine_amount INTEGER NOT NULL DEFAULT 0, note TEXT, FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(book_id) REFERENCES books(id));
    CREATE TABLE IF NOT EXISTS reservations (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, book_id TEXT NOT NULL, created_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(book_id) REFERENCES books(id));
    CREATE TABLE IF NOT EXISTS notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, title TEXT NOT NULL, content TEXT NOT NULL, is_read INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id));
    CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS fines (id INTEGER PRIMARY KEY AUTOINCREMENT, loan_id INTEGER NOT NULL UNIQUE, user_id INTEGER NOT NULL, amount INTEGER NOT NULL DEFAULT 0, reason TEXT, status TEXT NOT NULL DEFAULT 'unpaid', created_at TEXT NOT NULL, paid_at TEXT, note TEXT, FOREIGN KEY(loan_id) REFERENCES loans(id), FOREIGN KEY(user_id) REFERENCES users(id));
    CREATE TABLE IF NOT EXISTS activity_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, action TEXT NOT NULL, detail TEXT, created_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id));
    CREATE TABLE IF NOT EXISTS favorites (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, book_id TEXT NOT NULL, created_at TEXT NOT NULL, UNIQUE(user_id,book_id), FOREIGN KEY(user_id) REFERENCES users(id), FOREIGN KEY(book_id) REFERENCES books(id));
    CREATE TABLE IF NOT EXISTS support_tickets (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, subject TEXT NOT NULL, content TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'open', created_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id));
    """
    with get_conn() as conn:
        conn.executescript(mysql_schema if DB_ENGINE == 'mysql' else sqlite_schema)
        for d in ['view_count INTEGER NOT NULL DEFAULT 0','isbn TEXT','publisher TEXT']:
            _add_column(conn,'books',d)
        for d in ['member_code TEXT',"card_type TEXT DEFAULT 'Sinh viên'",'card_expiry TEXT','address TEXT','max_books INTEGER','birth_date TEXT','gender TEXT','auto_locked INTEGER NOT NULL DEFAULT 0','locked_reason TEXT']:
            _add_column(conn,'users',d)
        for d in ['renew_count INTEGER NOT NULL DEFAULT 0',"fine_status TEXT NOT NULL DEFAULT 'none'"]:
            _add_column(conn,'loans',d)
        for d in ['handled_at TEXT','note TEXT']:
            _add_column(conn,'reservations',d)
        defaults={'library_name':'SmartLibrary AI','loan_days':'14','fine_per_day':'5000','max_books_per_reader':'5','max_renewals':'2','reservation_hold_days':'3','library_email':'library@ictu.edu.vn','library_phone':'0966320627','auto_lock_overdue_days':'30','lost_after_days':'60','lost_book_fee':'150000','rag_top_k':'5','rag_mode':'auto'}
        for k,v in defaults.items():
            conn.execute('INSERT OR IGNORE INTO settings(`key`,value) VALUES(?,?)',(k,v))
        rows=conn.execute('SELECT id,role,member_code,card_expiry FROM users').fetchall()
        for r in rows:
            prefix='AD' if r['role']=='admin' else 'TT' if r['role']=='librarian' else 'DG'
            code=r['member_code'] or f'{prefix}{r["id"]:04d}'
            expiry=r['card_expiry'] or (date.today()+timedelta(days=365)).isoformat()
            conn.execute('UPDATE users SET member_code=?,card_expiry=? WHERE id=?',(code,expiry,r['id']))
        indexes=[('books','idx_books_category','category'),('books','idx_books_available','available'),('users','idx_users_role_status','role,status'),('loans','idx_loans_user_status','user_id,status'),('loans','idx_loans_book_status','book_id,status'),('loans','idx_loans_status_due','status,due_date'),('reservations','idx_reservations_user_status','user_id,status'),('reservations','idx_reservations_book_status','book_id,status'),('fines','idx_fines_user_status','user_id,status'),('activity_logs','idx_activity_created','created_at'),('favorites','idx_favorites_user','user_id')]
        for table,name,cols in indexes: _ensure_index(conn,table,name,cols)

def seed_books():
    if not SEED_BOOKS.exists():
        return
    with get_conn() as conn:
        books = json.loads(SEED_BOOKS.read_text(encoding='utf-8'))
        existing = {r[0] for r in conn.execute('SELECT id FROM books').fetchall()}
        for b in books:
            if b['id'] in existing:
                continue
            qty = int(b.get('quantity', 0) or 0)
            avail = int(b.get('available', qty) or 0)
            conn.execute('''
                INSERT INTO books(id,title,author,type,category,year,quantity,available,
                    location,description,cover_image,created_at,view_count,isbn,publisher)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (
                b['id'], b['title'], b['author'], b.get('type','Sách'), b.get('category','Khác'),
                b.get('year'), qty, avail, b.get('location',''), b.get('description',''),
                b.get('cover_image',''), _now(), int(b.get('view_count',0) or 0),
                b.get('isbn',''), b.get('publisher','')
            ))


def log_activity(user_id, action, detail=''):
    with get_conn() as conn:
        conn.execute('INSERT INTO activity_logs(user_id,action,detail,created_at) VALUES(?,?,?,?)',
                     (user_id, action, detail, _now()))


def list_activity(limit=20):
    with get_conn() as conn:
        rows = conn.execute('''
            SELECT a.*,u.full_name,u.username
            FROM activity_logs a LEFT JOIN users u ON u.id=a.user_id
            ORDER BY a.id DESC LIMIT ?
        ''', (int(limit),)).fetchall()
        return [dict(r) for r in rows]


def get_setting(key, default=None):
    with get_conn() as conn:
        r = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
        return r['value'] if r else default


def set_setting(key, value):
    with get_conn() as conn:
        conn.execute('''
            INSERT INTO settings(key,value) VALUES(?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        ''', (key, str(value)))


# ---------------- USERS / READERS ----------------

def create_user(username, password_hash, full_name, email='', phone='', role='reader',
                card_type='Sinh viên', address='', max_books=None):
    with get_conn() as conn:
        try:
            cur = conn.execute('''
                INSERT INTO users(username,password_hash,full_name,email,phone,role,status,
                    created_at,card_type,card_expiry,address,max_books)
                VALUES(?,?,?,?,?,?,'active',?,?,?,?,?)
            ''', (
                username.strip(), password_hash, full_name.strip(), email.strip(), phone.strip(),
                role, _now(), card_type, (date.today()+timedelta(days=365)).isoformat(),
                address.strip(), max_books
            ))
            uid = cur.lastrowid
            prefix = 'AD' if role == 'admin' else 'TT' if role == 'librarian' else 'DG'
            conn.execute('UPDATE users SET member_code=? WHERE id=?', (f'{prefix}{uid:04d}', uid))
            return True, uid
        except INTEGRITY_ERRORS:
            return False, 'Tên đăng nhập đã tồn tại.'


def get_user_by_username(username):
    with get_conn() as conn:
        r = conn.execute('SELECT * FROM users WHERE username=?', (username.strip(),)).fetchone()
        return dict(r) if r else None


def get_user(user_id):
    with get_conn() as conn:
        r = conn.execute('SELECT * FROM users WHERE id=?', (user_id,)).fetchone()
        return dict(r) if r else None


def list_users(role=None, status=None, keyword=''):
    params, where = [], []
    if role and role != 'Tất cả':
        where.append('role=?'); params.append(role)
    if status and status != 'Tất cả':
        where.append('status=?'); params.append(status)
    if keyword:
        q=f'%{keyword}%'
        where.append('(username LIKE ? OR full_name LIKE ? OR email LIKE ? OR member_code LIKE ?)')
        params += [q,q,q,q]
    sql='SELECT * FROM users'
    if where: sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY id DESC'
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]


def update_profile(user_id, full_name, email, phone):
    with get_conn() as conn:
        conn.execute('UPDATE users SET full_name=?,email=?,phone=? WHERE id=?',
                     (full_name.strip(), email.strip(), phone.strip(), user_id))


def update_reader_admin(user_id, full_name, email, phone, card_type, card_expiry,
                        address, max_books, status):
    with get_conn() as conn:
        conn.execute('''
            UPDATE users SET full_name=?,email=?,phone=?,card_type=?,card_expiry=?,
            address=?,max_books=?,status=? WHERE id=?
        ''', (full_name.strip(), email.strip(), phone.strip(), card_type, card_expiry,
              address.strip(), max_books, status, user_id))


def update_password_hash(user_id, password_hash):
    with get_conn() as conn:
        conn.execute('UPDATE users SET password_hash=? WHERE id=?', (password_hash, user_id))


def set_user_status(user_id, status):
    with get_conn() as conn:
        conn.execute('UPDATE users SET status=? WHERE id=?', (status, user_id))


def set_user_role(user_id, role):
    with get_conn() as conn:
        conn.execute('UPDATE users SET role=? WHERE id=?', (role, user_id))


def delete_user(user_id):
    with get_conn() as conn:
        active = conn.execute('''
            SELECT COUNT(*) FROM loans WHERE user_id=?
            AND status IN ('pending','borrowed','overdue','return_requested','lost')
        ''', (user_id,)).fetchone()[0]
        if active:
            return False, 'Không thể xóa tài khoản đang có phiếu mượn hoạt động.'
        conn.execute('DELETE FROM notifications WHERE user_id=?', (user_id,))
        conn.execute('DELETE FROM reservations WHERE user_id=?', (user_id,))
        conn.execute('DELETE FROM activity_logs WHERE user_id=?', (user_id,))
        conn.execute('DELETE FROM users WHERE id=?', (user_id,))
        return True, 'Đã xóa tài khoản.'




def update_reader_profile(user_id, full_name, email, phone, birth_date='', gender='', address=''):
    with get_conn() as conn:
        conn.execute('''
            UPDATE users SET full_name=?,email=?,phone=?,birth_date=?,gender=?,address=?
            WHERE id=?
        ''', (full_name.strip(), email.strip(), phone.strip(), birth_date.strip(), gender.strip(), address.strip(), user_id))


def toggle_favorite(user_id, book_id):
    with get_conn() as conn:
        row=conn.execute('SELECT id FROM favorites WHERE user_id=? AND book_id=?',(user_id,book_id)).fetchone()
        if row:
            conn.execute('DELETE FROM favorites WHERE id=?',(row['id'],))
            return False
        conn.execute('INSERT INTO favorites(user_id,book_id,created_at) VALUES(?,?,?)',(user_id,book_id,_now()))
        return True


def is_favorite(user_id, book_id):
    with get_conn() as conn:
        return conn.execute('SELECT COUNT(*) FROM favorites WHERE user_id=? AND book_id=?',(user_id,book_id)).fetchone()[0] > 0


def list_favorites(user_id):
    with get_conn() as conn:
        rows=conn.execute('''
            SELECT b.*,f.created_at AS favorite_at
            FROM favorites f JOIN books b ON b.id=f.book_id
            WHERE f.user_id=? ORDER BY f.id DESC
        ''',(user_id,)).fetchall()
        return [dict(r) for r in rows]


def reader_transactions(user_id, limit=100):
    with get_conn() as conn:
        rows=conn.execute('''
            SELECT 'activity' AS kind, a.id, a.action AS title, a.detail AS detail,
                   a.created_at, 0 AS amount, '' AS status
            FROM activity_logs a WHERE a.user_id=?
            UNION ALL
            SELECT 'fine' AS kind, f.id, 'Tiền phạt' AS title,
                   COALESCE(f.reason,'') AS detail, f.created_at, f.amount AS amount, f.status
            FROM fines f WHERE f.user_id=?
            ORDER BY created_at DESC LIMIT ?
        ''',(user_id,user_id,int(limit))).fetchall()
        return [dict(r) for r in rows]


def add_support_ticket(user_id, subject, content):
    with get_conn() as conn:
        cur=conn.execute('''
            INSERT INTO support_tickets(user_id,subject,content,status,created_at)
            VALUES(?,?,?,'open',?)
        ''',(user_id,subject.strip(),content.strip(),_now()))
        return cur.lastrowid


def list_support_tickets(user_id):
    with get_conn() as conn:
        return [dict(r) for r in conn.execute('SELECT * FROM support_tickets WHERE user_id=? ORDER BY id DESC',(user_id,)).fetchall()]


def reader_achievement_stats(user_id):
    with get_conn() as conn:
        returned=conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status='returned'",(user_id,)).fetchone()[0]
        borrowed=conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status IN ('borrowed','overdue','return_requested')",(user_id,)).fetchone()[0]
        favorites=conn.execute('SELECT COUNT(*) FROM favorites WHERE user_id=?',(user_id,)).fetchone()[0]
        reservations=conn.execute('SELECT COUNT(*) FROM reservations WHERE user_id=?',(user_id,)).fetchone()[0]
        total_fine=conn.execute("SELECT COALESCE(SUM(amount),0) FROM fines WHERE user_id=?",(user_id,)).fetchone()[0]
        return {'returned':returned,'borrowed':borrowed,'favorites':favorites,'reservations':reservations,'total_fine':total_fine}


# ---------------- BOOKS ----------------

def list_books(keyword='', category='Tất cả', only_available=False, publisher='Tất cả'):
    """Filter books then rank keyword results with BM25.

    With an empty keyword this behaves like the old alphabetical listing.
    With a keyword, title/author/id/category/description are ranked by relevance.
    """
    params, where = [], []
    keyword = (keyword or '').strip()
    if category and category != 'Tất cả':
        where.append('category=?'); params.append(category)
    if publisher and publisher != 'Tất cả':
        where.append('publisher=?'); params.append(publisher)
    if only_available:
        where.append('available>0')
    sql='SELECT * FROM books'
    if where: sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY title COLLATE NOCASE'
    with get_conn() as conn:
        books=[dict(r) for r in conn.execute(sql, params).fetchall()]
    if keyword:
        ranked=rank_books(books, keyword)
        if not ranked:
            return []
        top=float(ranked[0].get('_bm25_score',0) or 0)
        # Keep strong/reasonable matches only. This avoids unrelated books appearing
        # just because they share one weak token with the query.
        threshold=max(0.8, top*0.30) if top>0 else 0.8
        filtered=[b for b in ranked if float(b.get('_bm25_score',0) or 0)>=threshold]
        return filtered or ranked[:5]
    return books


def search_books_bm25(keyword, top_k=8, category='Tất cả', only_available=False):
    books=list_books('', category, only_available)
    ranked=rank_books(books, keyword)
    if not ranked:
        return []
    top=float(ranked[0].get('_bm25_score',0) or 0)
    threshold=max(0.8, top*0.30) if top>0 else 0.8
    filtered=[b for b in ranked if float(b.get('_bm25_score',0) or 0)>=threshold]
    return (filtered or ranked[:5])[:int(top_k)]


def get_book(book_id):
    with get_conn() as conn:
        r=conn.execute('SELECT * FROM books WHERE id=?',(book_id,)).fetchone()
        return dict(r) if r else None


def list_categories():
    with get_conn() as conn:
        return [r['category'] for r in conn.execute("SELECT DISTINCT category FROM books WHERE COALESCE(category,'')<>'' ORDER BY category").fetchall()]


def list_publishers():
    with get_conn() as conn:
        return [r['publisher'] for r in conn.execute("SELECT DISTINCT publisher FROM books WHERE COALESCE(publisher,'')<>'' ORDER BY publisher").fetchall()]


def add_book(book):
    with get_conn() as conn:
        try:
            qty=int(book.get('quantity',0))
            conn.execute('''
                INSERT INTO books(id,title,author,type,category,year,quantity,available,location,
                    description,cover_image,created_at,isbn,publisher,view_count)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)
            ''', (book['id'],book['title'],book['author'],book.get('type','Sách'),book.get('category',''),
                  book.get('year'),qty,qty,book.get('location',''),book.get('description',''),
                  book.get('cover_image',''),_now(),book.get('isbn',''),book.get('publisher','')))
            return True,'Đã thêm sách.'
        except INTEGRITY_ERRORS:
            return False,'Mã sách đã tồn tại.'


def update_book(book_id, book):
    with get_conn() as conn:
        old=conn.execute('SELECT quantity,available FROM books WHERE id=?',(book_id,)).fetchone()
        if not old: return False,'Không tìm thấy sách.'
        borrowed=max(int(old['quantity'])-int(old['available']),0)
        qty=int(book.get('quantity',0)); available=max(qty-borrowed,0)
        conn.execute('''
            UPDATE books SET title=?,author=?,type=?,category=?,year=?,quantity=?,available=?,
            location=?,description=?,cover_image=?,isbn=?,publisher=? WHERE id=?
        ''', (book['title'],book['author'],book.get('type','Sách'),book.get('category',''),book.get('year'),
              qty,available,book.get('location',''),book.get('description',''),book.get('cover_image',''),
              book.get('isbn',''),book.get('publisher',''),book_id))
        return True,'Đã cập nhật sách.'


def delete_book(book_id):
    with get_conn() as conn:
        active=conn.execute("SELECT COUNT(*) FROM loans WHERE book_id=? AND status IN ('pending','borrowed','overdue','return_requested','lost')",(book_id,)).fetchone()[0]
        if active: return False,'Không thể xóa sách đang có phiếu mượn.'
        conn.execute('DELETE FROM reservations WHERE book_id=?',(book_id,))
        conn.execute('DELETE FROM books WHERE id=?',(book_id,))
        return True,'Đã xóa sách.'


def increment_book_view(book_id):
    with get_conn() as conn:
        conn.execute('UPDATE books SET view_count=COALESCE(view_count,0)+1 WHERE id=?',(book_id,))


def top_viewed_books(limit=3):
    with get_conn() as conn:
        return [dict(r) for r in conn.execute('SELECT * FROM books ORDER BY COALESCE(view_count,0) DESC,id DESC LIMIT ?',(int(limit),)).fetchall()]


def top_borrowed_books(limit=8):
    with get_conn() as conn:
        rows=conn.execute('''
            SELECT b.id,b.title,b.author,b.category,b.cover_image,b.available,b.quantity,
                   COUNT(l.id) AS borrow_count
            FROM books b LEFT JOIN loans l ON l.book_id=b.id
            GROUP BY b.id ORDER BY borrow_count DESC,COALESCE(b.view_count,0) DESC LIMIT ?
        ''',(int(limit),)).fetchall()
        return [dict(r) for r in rows]



def seed_demo_workflows():
    """Create a small idempotent demo dataset for management screens."""
    if _config('DEMO_DATA', 'true').strip().lower() not in ('1','true','yes','on'): return False
    with get_conn() as conn:
        if conn.execute('SELECT COUNT(*) AS n FROM loans').fetchone()['n']: return False
        users={r['username']:r for r in conn.execute("SELECT id,username FROM users WHERE username IN ('docgia','docgia2','docgia3')").fetchall()}
        if any(x not in users for x in ['docgia','docgia2','docgia3']): return False
        existing={r['id'] for r in conn.execute("SELECT id FROM books WHERE id IN ('B001','B003','B004','B010','B018')").fetchall()}
        ids=[x for x in ['B001','B003','B004','B010','B018'] if x in existing]
        if len(ids)<4: return False
        today=date.today(); now=_now(); u1,u2,u3=users['docgia']['id'],users['docgia2']['id'],users['docgia3']['id']
        conn.execute("INSERT INTO loans(user_id,book_id,request_date,status,renew_count,fine_status) VALUES(?,?,?,'pending',0,'none')",(u1,ids[0],now))
        conn.execute("INSERT INTO loans(user_id,book_id,request_date,approved_date,due_date,status,renew_count,fine_status) VALUES(?,?,?,?,?,'borrowed',0,'none')",(u2,ids[1],(today-timedelta(days=3)).isoformat(),(today-timedelta(days=3)).isoformat(),(today+timedelta(days=11)).isoformat()))
        conn.execute('UPDATE books SET available=CASE WHEN available>0 THEN available-1 ELSE 0 END WHERE id=?',(ids[1],))
        due=(today-timedelta(days=6)).isoformat()
        cur=conn.execute("INSERT INTO loans(user_id,book_id,request_date,approved_date,due_date,status,fine_amount,renew_count,fine_status) VALUES(?,?,?,?,?,'overdue',30000,0,'unpaid')",(u3,ids[2],(today-timedelta(days=20)).isoformat(),(today-timedelta(days=20)).isoformat(),due))
        conn.execute('UPDATE books SET available=CASE WHEN available>0 THEN available-1 ELSE 0 END WHERE id=?',(ids[2],))
        conn.execute("INSERT INTO fines(loan_id,user_id,amount,reason,status,created_at) VALUES(?,?,30000,?,'unpaid',?)",(cur.lastrowid,u3,'Trả quá hạn 6 ngày',now))
        conn.execute("INSERT INTO loans(user_id,book_id,request_date,approved_date,due_date,returned_date,status,fine_amount,renew_count,fine_status) VALUES(?,?,?,?,?,?,'returned',0,0,'none')",(u1,ids[3],(today-timedelta(days=30)).isoformat(),(today-timedelta(days=29)).isoformat(),(today-timedelta(days=15)).isoformat(),(today-timedelta(days=18)).isoformat()))
        reserve_book=ids[4] if len(ids)>4 else ids[0]
        conn.execute("INSERT INTO reservations(user_id,book_id,created_at,status,note) VALUES(?,?,?,'pending',?)",(u2,reserve_book,now,'Độc giả chờ sách sẵn sàng'))
        conn.execute("INSERT INTO activity_logs(user_id,action,detail,created_at) VALUES(?,?,?,?)",(u1,'Gửi yêu cầu mượn',f'Sách {ids[0]}',now))
        return True

# ---------------- NOTIFICATIONS ----------------

def _notify(conn,user_id,title,content):
    conn.execute('INSERT INTO notifications(user_id,title,content,is_read,created_at) VALUES(?,?,?,0,?)',(user_id,title,content,_now()))


def get_notifications(user_id,limit=50):
    with get_conn() as conn:
        return [dict(r) for r in conn.execute('SELECT * FROM notifications WHERE user_id=? ORDER BY id DESC LIMIT ?',(user_id,int(limit))).fetchall()]


def mark_notifications_read(user_id):
    with get_conn() as conn:
        conn.execute('UPDATE notifications SET is_read=1 WHERE user_id=?',(user_id,))


# ---------------- LOANS / RETURNS / FINES ----------------

def _maybe_auto_unlock(conn, user_id):
    u=conn.execute('SELECT status,auto_locked FROM users WHERE id=?',(user_id,)).fetchone()
    if not u or not int(u['auto_locked'] or 0):
        return
    overdue=conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status IN ('overdue','lost')",(user_id,)).fetchone()[0]
    unpaid=conn.execute("SELECT COUNT(*) FROM fines WHERE user_id=? AND status='unpaid'",(user_id,)).fetchone()[0]
    if overdue==0 and unpaid==0:
        conn.execute("UPDATE users SET status='active',auto_locked=0,locked_reason=NULL WHERE id=?",(user_id,))
        _notify(conn,user_id,'Tài khoản đã được mở lại','Bạn đã hoàn tất nghĩa vụ quá hạn/tiền phạt và có thể mượn sách trở lại.')


def _refresh_overdue_now():
    """Refresh overdue state, fines and automatic borrowing suspension."""
    fine_per_day=int(get_setting('fine_per_day',5000))
    lock_days=int(get_setting('auto_lock_overdue_days',30))
    with get_conn() as conn:
        rows=conn.execute("SELECT * FROM loans WHERE status IN ('borrowed','overdue') AND due_date IS NOT NULL").fetchall()
        for loan in rows:
            due=date.fromisoformat(loan['due_date']); late=max((date.today()-due).days,0)
            status='overdue' if late>0 else 'borrowed'; fine=late*fine_per_day
            fstatus='unpaid' if fine>0 else 'none'
            conn.execute('UPDATE loans SET status=?,fine_amount=?,fine_status=? WHERE id=?',(status,fine,fstatus,loan['id']))
            if fine>0:
                conn.execute("""
                    INSERT INTO fines(loan_id,user_id,amount,reason,status,created_at)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(loan_id) DO UPDATE SET amount=excluded.amount,reason=excluded.reason
                """,(loan['id'],loan['user_id'],fine,f'Trả quá hạn {late} ngày','unpaid',_now()))
            if late >= lock_days:
                u=conn.execute('SELECT status,auto_locked FROM users WHERE id=?',(loan['user_id'],)).fetchone()
                if u and u['status']=='active':
                    reason=f'Tự động khóa do quá hạn {late} ngày (phiếu #{loan["id"]})'
                    conn.execute("UPDATE users SET status='locked',auto_locked=1,locked_reason=? WHERE id=?",(reason,loan['user_id']))
                    _notify(conn,loan['user_id'],'Tạm khóa quyền mượn',f'Bạn có sách quá hạn {late} ngày. Hãy trả sách và thanh toán khoản phạt để được mở lại quyền mượn.')


def refresh_overdue(force=False):
    """Refresh overdue data at most once every 30 seconds per process.

    Streamlit reruns the script frequently; without throttling this scan used to
    execute again for many unrelated clicks and made the UI feel sluggish.
    """
    global _OVERDUE_LAST_REFRESH
    now = time.monotonic()
    if not force and (now - _OVERDUE_LAST_REFRESH) < _OVERDUE_REFRESH_SECONDS:
        return False
    with _OVERDUE_LOCK:
        now = time.monotonic()
        if not force and (now - _OVERDUE_LAST_REFRESH) < _OVERDUE_REFRESH_SECONDS:
            return False
        _refresh_overdue_now()
        _OVERDUE_LAST_REFRESH = time.monotonic()
        return True


def _reader_limit(conn,user_id):
    r=conn.execute('SELECT max_books FROM users WHERE id=?',(user_id,)).fetchone()
    if r and r['max_books'] is not None:
        return int(r['max_books'])
    setting=conn.execute("SELECT value FROM settings WHERE key='max_books_per_reader'").fetchone()
    return int(setting['value'] if setting else 5)


def reader_borrow_limit_status(user_id):
    refresh_overdue()
    with get_conn() as conn:
        limit=_reader_limit(conn,user_id)
        active=conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status IN ('pending','borrowed','overdue','return_requested','lost')",(user_id,)).fetchone()[0]
        u=conn.execute('SELECT status,locked_reason FROM users WHERE id=?',(user_id,)).fetchone()
        return {
            'limit':limit,
            'active':active,
            'remaining':max(limit-active,0),
            'status':u['status'] if u else 'unknown',
            'locked_reason':u['locked_reason'] if u else None,
        }


def request_borrow(user_id,book_id):
    refresh_overdue()
    with get_conn() as conn:
        u=conn.execute('SELECT * FROM users WHERE id=?',(user_id,)).fetchone()
        if not u or u['status']!='active':
            reason=(u['locked_reason'] if u and 'locked_reason' in u.keys() else '') or 'Tài khoản hiện không thể mượn sách.'
            return False,reason
        if u['card_expiry'] and u['card_expiry'] < _today(): return False,'Thẻ độc giả đã hết hạn.'
        if conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status='overdue'",(user_id,)).fetchone()[0]:
            return False,'Bạn đang có sách quá hạn.'
        if conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status='lost'",(user_id,)).fetchone()[0]:
            return False,'Bạn đang có sách được ghi nhận chưa trả/mất. Hãy liên hệ thư viện để xử lý.'
        if conn.execute("SELECT COUNT(*) FROM fines WHERE user_id=? AND status='unpaid'",(user_id,)).fetchone()[0]:
            return False,'Bạn đang có tiền phạt chưa thanh toán.'
        active=conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status IN ('pending','borrowed','overdue','return_requested','lost')",(user_id,)).fetchone()[0]
        if active >= _reader_limit(conn,user_id): return False,'Bạn đã đạt giới hạn số sách được mượn.'
        if conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND book_id=? AND status IN ('pending','borrowed','overdue','return_requested','lost')",(user_id,book_id)).fetchone()[0]:
            return False,'Bạn đã có yêu cầu/phiếu mượn cuốn này.'
        book=conn.execute('SELECT * FROM books WHERE id=?',(book_id,)).fetchone()
        if not book: return False,'Không tìm thấy sách.'
        if book['available']<=0: return False,'Sách đang hết. Hãy đặt trước.'
        cur=conn.execute("INSERT INTO loans(user_id,book_id,request_date,status,renew_count,fine_status) VALUES(?,?,?,'pending',0,'none')",(user_id,book_id,_now()))
        _notify(conn,user_id,'Đã gửi yêu cầu mượn',f'Phiếu #{cur.lastrowid} đang chờ duyệt.')
        return True,'Đã gửi yêu cầu mượn sách.'


def approve_loan(loan_id,actor_id=None):
    days=int(get_setting('loan_days',14))
    refresh_overdue()
    with get_conn() as conn:
        loan=conn.execute('SELECT * FROM loans WHERE id=?',(loan_id,)).fetchone()
        if not loan or loan['status']!='pending': return False,'Phiếu không còn ở trạng thái chờ.'
        u=conn.execute('SELECT * FROM users WHERE id=?',(loan['user_id'],)).fetchone()
        if not u or u['status']!='active': return False,'Độc giả đang bị khóa quyền mượn.'
        if conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND status IN ('overdue','lost')",(loan['user_id'],)).fetchone()[0]:
            return False,'Độc giả đang có sách quá hạn/chưa trả.'
        if conn.execute("SELECT COUNT(*) FROM fines WHERE user_id=? AND status='unpaid'",(loan['user_id'],)).fetchone()[0]:
            return False,'Độc giả đang có tiền phạt chưa thanh toán.'
        active=conn.execute("SELECT COUNT(*) FROM loans WHERE user_id=? AND id<>? AND status IN ('pending','borrowed','overdue','return_requested','lost')",(loan['user_id'],loan_id)).fetchone()[0]
        if active >= _reader_limit(conn,loan['user_id']): return False,'Độc giả đã đạt giới hạn số lượng mượn.'
        book=conn.execute('SELECT * FROM books WHERE id=?',(loan['book_id'],)).fetchone()
        if not book or book['available']<=0: return False,'Sách đã hết.'
        due=date.today()+timedelta(days=days)
        conn.execute("UPDATE loans SET status='borrowed',approved_date=?,due_date=? WHERE id=?",(_today(),due.isoformat(),loan_id))
        conn.execute('UPDATE books SET available=available-1 WHERE id=?',(loan['book_id'],))
        _notify(conn,loan['user_id'],'Yêu cầu mượn đã được duyệt',f'Hạn trả: {due.isoformat()}.')
    if actor_id: log_activity(actor_id,'Duyệt mượn',f'Phiếu #{loan_id}')
    return True,'Đã duyệt phiếu mượn.'


def reject_loan(loan_id,actor_id=None):
    with get_conn() as conn:
        loan=conn.execute('SELECT * FROM loans WHERE id=?',(loan_id,)).fetchone()
        if not loan or loan['status']!='pending': return False,'Phiếu không hợp lệ.'
        conn.execute("UPDATE loans SET status='rejected' WHERE id=?",(loan_id,))
        _notify(conn,loan['user_id'],'Yêu cầu mượn bị từ chối',f'Phiếu #{loan_id} đã bị từ chối.')
    if actor_id: log_activity(actor_id,'Từ chối mượn',f'Phiếu #{loan_id}')
    return True,'Đã từ chối phiếu mượn.'


def request_return(user_id,loan_id):
    with get_conn() as conn:
        loan=conn.execute('SELECT * FROM loans WHERE id=? AND user_id=?',(loan_id,user_id)).fetchone()
        if not loan or loan['status'] not in ('borrowed','overdue'): return False,'Phiếu không hợp lệ.'
        conn.execute("UPDATE loans SET status='return_requested' WHERE id=?",(loan_id,))
        return True,'Đã gửi yêu cầu trả.'


def confirm_return(loan_id,actor_id=None):
    fine_per_day=int(get_setting('fine_per_day',5000))
    with get_conn() as conn:
        loan=conn.execute('SELECT * FROM loans WHERE id=?',(loan_id,)).fetchone()
        if not loan or loan['status'] not in ('return_requested','borrowed','overdue'):
            return False,'Phiếu trả không hợp lệ.'
        due=date.fromisoformat(loan['due_date']) if loan['due_date'] else date.today()
        late=max((date.today()-due).days,0); fine=late*fine_per_day
        fstatus='unpaid' if fine>0 else 'none'
        conn.execute("UPDATE loans SET status='returned',returned_date=?,fine_amount=?,fine_status=? WHERE id=?",(_today(),fine,fstatus,loan_id))
        conn.execute('UPDATE books SET available=CASE WHEN available+1>quantity THEN quantity ELSE available+1 END WHERE id=?',(loan['book_id'],))
        if fine>0:
            conn.execute('''
                INSERT INTO fines(loan_id,user_id,amount,reason,status,created_at)
                VALUES(?,?,?,?,?,?)
                ON CONFLICT(loan_id) DO UPDATE SET amount=excluded.amount,reason=excluded.reason,status='unpaid'
            ''',(loan_id,loan['user_id'],fine,f'Trả quá hạn {late} ngày','unpaid',_now()))
        _notify(conn,loan['user_id'],'Đã xác nhận trả sách',f'Phí quá hạn: {fine:,} VNĐ.')
        first=conn.execute("SELECT * FROM reservations WHERE book_id=? AND status='pending' ORDER BY id LIMIT 1",(loan['book_id'],)).fetchone()
        if first:
            _notify(conn,first['user_id'],'Sách đặt trước đã sẵn sàng',f'Sách {loan["book_id"]} hiện đã có bản trống.')
        _maybe_auto_unlock(conn,loan['user_id'])
    if actor_id: log_activity(actor_id,'Xác nhận trả',f'Phiếu #{loan_id}; phạt {fine}')
    return True,f'Đã xác nhận trả. Phí: {fine:,} VNĐ.'



def mark_loan_lost(loan_id, actor_id=None, replacement_fee=None):
    """Administrative resolution for a book that was not returned for a long time."""
    refresh_overdue()
    replacement_fee=int(replacement_fee if replacement_fee is not None else get_setting('lost_book_fee',150000))
    with get_conn() as conn:
        loan=conn.execute('SELECT * FROM loans WHERE id=?',(loan_id,)).fetchone()
        if not loan or loan['status'] not in ('overdue','borrowed','return_requested'):
            return False,'Phiếu này không thể đánh dấu mất/chưa trả.'
        late=0
        if loan['due_date']:
            late=max((date.today()-date.fromisoformat(loan['due_date'])).days,0)
        threshold=int(get_setting('lost_after_days',60))
        if late < threshold:
            return False,f'Chưa đủ ngưỡng xử lý không trả/mất: mới quá hạn {late} ngày, yêu cầu tối thiểu {threshold} ngày.'
        overdue_fine=late*int(get_setting('fine_per_day',5000))
        total=overdue_fine+replacement_fee
        conn.execute("UPDATE loans SET status='lost',fine_amount=?,fine_status='unpaid',note=? WHERE id=?",
                     (total,f'Không trả/mất sách; phí thay thế {replacement_fee:,} VNĐ',loan_id))
        conn.execute("""
            INSERT INTO fines(loan_id,user_id,amount,reason,status,created_at)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(loan_id) DO UPDATE SET amount=excluded.amount,reason=excluded.reason,status='unpaid'
        """,(loan_id,loan['user_id'],total,f'Không trả/mất sách: quá hạn {late} ngày + phí thay thế {replacement_fee:,} VNĐ','unpaid',_now()))
        reason=f'Khóa do sách chưa trả/mất (phiếu #{loan_id})'
        conn.execute("UPDATE users SET status='locked',auto_locked=1,locked_reason=? WHERE id=?",(reason,loan['user_id']))
        _notify(conn,loan['user_id'],'Sách được ghi nhận chưa trả/mất',f'Phiếu #{loan_id} đã được xử lý. Tổng nghĩa vụ hiện tại: {total:,} VNĐ.')
    if actor_id: log_activity(actor_id,'Đánh dấu sách chưa trả/mất',f'Phiếu #{loan_id}; tổng {total}')
    return True,f'Đã ghi nhận chưa trả/mất sách. Tổng phí: {total:,} VNĐ.'

def renew_loan(user_id,loan_id):
    max_renewals=int(get_setting('max_renewals',2))
    days=int(get_setting('loan_days',14))
    with get_conn() as conn:
        loan=conn.execute('SELECT * FROM loans WHERE id=? AND user_id=?',(loan_id,user_id)).fetchone()
        if not loan or loan['status']!='borrowed': return False,'Phiếu hiện không thể gia hạn.'
        if int(loan['renew_count'] or 0)>=max_renewals: return False,'Đã đạt số lần gia hạn tối đa.'
        pending_res=conn.execute("SELECT COUNT(*) FROM reservations WHERE book_id=? AND status='pending'",(loan['book_id'],)).fetchone()[0]
        if pending_res: return False,'Sách đang có người đặt trước nên không thể gia hạn.'
        due=date.fromisoformat(loan['due_date'])+timedelta(days=days)
        conn.execute('UPDATE loans SET due_date=?,renew_count=renew_count+1 WHERE id=?',(due.isoformat(),loan_id))
        return True,f'Gia hạn thành công đến {due.isoformat()}.'


def list_loans(status=None,keyword=''):
    refresh_overdue(); params=[]; where=[]
    if status and status!='Tất cả': where.append('l.status=?'); params.append(status)
    if keyword:
        q=f'%{keyword}%'; where.append('(u.full_name LIKE ? OR u.username LIKE ? OR b.title LIKE ? OR b.id LIKE ?)'); params += [q,q,q,q]
    sql='''SELECT l.*,u.username,u.full_name,u.member_code,b.title,b.author,b.category FROM loans l JOIN users u ON u.id=l.user_id JOIN books b ON b.id=l.book_id'''
    if where: sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY l.id DESC'
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql,params).fetchall()]


def reader_loans(user_id):
    refresh_overdue()
    with get_conn() as conn:
        rows=conn.execute('''SELECT l.*,b.title,b.author,b.cover_image,b.category FROM loans l JOIN books b ON b.id=l.book_id WHERE l.user_id=? ORDER BY l.id DESC''',(user_id,)).fetchall()
        return [dict(r) for r in rows]


def list_fines(status='Tất cả'):
    refresh_overdue(); params=[]; where=''
    if status and status!='Tất cả': where='WHERE f.status=?'; params=[status]
    with get_conn() as conn:
        rows=conn.execute(f'''SELECT f.*,u.full_name,u.username,u.member_code,b.title,b.id AS book_id FROM fines f JOIN users u ON u.id=f.user_id JOIN loans l ON l.id=f.loan_id JOIN books b ON b.id=l.book_id {where} ORDER BY f.id DESC''',params).fetchall()
        return [dict(r) for r in rows]


def pay_fine(fine_id,actor_id=None):
    with get_conn() as conn:
        f=conn.execute('SELECT * FROM fines WHERE id=?',(fine_id,)).fetchone()
        if not f or f['status']=='paid': return False,'Khoản phạt không hợp lệ.'
        conn.execute("UPDATE fines SET status='paid',paid_at=? WHERE id=?",(_now(),fine_id))
        loan=conn.execute('SELECT status,note FROM loans WHERE id=?',(f['loan_id'],)).fetchone()
        if loan and loan['status']=='lost':
            note=(loan['note'] or '') + ' | Đã xử lý nghĩa vụ mất/chưa trả.'
            conn.execute("UPDATE loans SET status='lost_resolved',fine_status='paid',note=? WHERE id=?",(note,f['loan_id']))
        else:
            conn.execute("UPDATE loans SET fine_status='paid' WHERE id=?",(f['loan_id'],))
        _notify(conn,f['user_id'],'Đã thanh toán tiền phạt',f'Số tiền {f["amount"]:,} VNĐ đã được ghi nhận.')
        _maybe_auto_unlock(conn,f['user_id'])
    if actor_id: log_activity(actor_id,'Thu tiền phạt',f'Khoản phạt #{fine_id}')
    return True,'Đã ghi nhận thanh toán.'


# ---------------- RESERVATIONS ----------------

def reserve_book(user_id,book_id):
    with get_conn() as conn:
        if conn.execute("SELECT COUNT(*) FROM reservations WHERE user_id=? AND book_id=? AND status='pending'",(user_id,book_id)).fetchone()[0]:
            return False,'Bạn đã đặt trước sách này.'
        cur=conn.execute("INSERT INTO reservations(user_id,book_id,created_at,status) VALUES(?,?,?,'pending')",(user_id,book_id,_now()))
        _notify(conn,user_id,'Đã đặt trước sách',f'Phiếu đặt trước #{cur.lastrowid} đã được ghi nhận.')
        return True,'Đã đặt trước sách.'


def list_reservations(status='Tất cả',keyword=''):
    params=[]; where=[]
    if status and status!='Tất cả': where.append('r.status=?'); params.append(status)
    if keyword:
        q=f'%{keyword}%'; where.append('(u.full_name LIKE ? OR u.username LIKE ? OR b.title LIKE ? OR b.id LIKE ?)'); params += [q,q,q,q]
    sql='''SELECT r.*,u.full_name,u.username,u.member_code,b.title,b.author,b.available FROM reservations r JOIN users u ON u.id=r.user_id JOIN books b ON b.id=r.book_id'''
    if where: sql += ' WHERE ' + ' AND '.join(where)
    sql += ' ORDER BY r.id DESC'
    with get_conn() as conn:
        return [dict(r) for r in conn.execute(sql,params).fetchall()]


def reader_reservations(user_id):
    with get_conn() as conn:
        rows=conn.execute('''SELECT r.*,b.title,b.author,b.cover_image,b.available FROM reservations r JOIN books b ON b.id=r.book_id WHERE r.user_id=? ORDER BY r.id DESC''',(user_id,)).fetchall()
        return [dict(r) for r in rows]


def update_reservation(reservation_id,status,note='',actor_id=None):
    with get_conn() as conn:
        r=conn.execute('SELECT * FROM reservations WHERE id=?',(reservation_id,)).fetchone()
        if not r: return False,'Không tìm thấy phiếu đặt trước.'
        conn.execute('UPDATE reservations SET status=?,handled_at=?,note=? WHERE id=?',(status,_now(),note,reservation_id))
        _notify(conn,r['user_id'],'Cập nhật đặt trước',f'Phiếu #{reservation_id}: {status}. {note}'.strip())
    if actor_id: log_activity(actor_id,'Xử lý đặt trước',f'Phiếu #{reservation_id} -> {status}')
    return True,'Đã cập nhật đặt trước.'


# ---------------- STATS / REPORTS ----------------

def reader_stats(user_id):
    refresh_overdue()
    with get_conn() as conn:
        row=conn.execute('''SELECT
            SUM(CASE WHEN status='borrowed' THEN 1 ELSE 0 END) borrowed,
            SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) pending,
            SUM(CASE WHEN status='overdue' THEN 1 ELSE 0 END) overdue,
            SUM(CASE WHEN status='returned' THEN fine_amount ELSE 0 END) fines
            FROM loans WHERE user_id=?''',(user_id,)).fetchone()
        unpaid=conn.execute("SELECT COALESCE(SUM(amount),0) FROM fines WHERE user_id=? AND status='unpaid'",(user_id,)).fetchone()[0]
        return {'borrowed':row['borrowed'] or 0,'pending':row['pending'] or 0,'overdue':row['overdue'] or 0,'fines':unpaid or 0}


def admin_stats():
    refresh_overdue()
    with get_conn() as conn:
        stats={
            'titles':conn.execute('SELECT COUNT(*) FROM books').fetchone()[0],
            'copies':conn.execute('SELECT COALESCE(SUM(quantity),0) FROM books').fetchone()[0],
            'available':conn.execute('SELECT COALESCE(SUM(available),0) FROM books').fetchone()[0],
            'readers':conn.execute("SELECT COUNT(*) FROM users WHERE role='reader' AND status='active'").fetchone()[0],
            'borrowed':conn.execute("SELECT COUNT(*) FROM loans WHERE status IN ('borrowed','overdue','return_requested')").fetchone()[0],
            'overdue':conn.execute("SELECT COUNT(*) FROM loans WHERE status='overdue'").fetchone()[0],
            'pending':conn.execute("SELECT COUNT(*) FROM loans WHERE status='pending'").fetchone()[0],
            'reservations':conn.execute("SELECT COUNT(*) FROM reservations WHERE status='pending'").fetchone()[0],
            'unpaid_fines':conn.execute("SELECT COALESCE(SUM(amount),0) FROM fines WHERE status='unpaid'").fetchone()[0],
        }
        cats=[dict(r) for r in conn.execute('SELECT category,COUNT(*) titles,SUM(quantity) copies FROM books GROUP BY category ORDER BY titles DESC').fetchall()]
        months=[dict(r) for r in conn.execute("SELECT substr(request_date,1,7) month,COUNT(*) total FROM loans GROUP BY substr(request_date,1,7) ORDER BY month DESC LIMIT 12").fetchall()]
        return stats,cats,list(reversed(months))


def report_snapshot():
    refresh_overdue()
    with get_conn() as conn:
        books=[dict(r) for r in conn.execute('SELECT * FROM books ORDER BY id').fetchall()]
        users=[dict(r) for r in conn.execute('SELECT id,member_code,username,full_name,email,phone,role,status,card_type,card_expiry,address,created_at FROM users ORDER BY id').fetchall()]
        loans=[dict(r) for r in conn.execute('''SELECT l.*,u.member_code,u.full_name,b.title FROM loans l JOIN users u ON u.id=l.user_id JOIN books b ON b.id=l.book_id ORDER BY l.id DESC''').fetchall()]
        reservations=[dict(r) for r in conn.execute('''SELECT r.*,u.member_code,u.full_name,b.title FROM reservations r JOIN users u ON u.id=r.user_id JOIN books b ON b.id=r.book_id ORDER BY r.id DESC''').fetchall()]
        fines=[dict(r) for r in conn.execute('''SELECT f.*,u.member_code,u.full_name,b.title FROM fines f JOIN users u ON u.id=f.user_id JOIN loans l ON l.id=f.loan_id JOIN books b ON b.id=l.book_id ORDER BY f.id DESC''').fetchall()]
        return {'books':books,'users':users,'loans':loans,'reservations':reservations,'fines':fines}
