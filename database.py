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
