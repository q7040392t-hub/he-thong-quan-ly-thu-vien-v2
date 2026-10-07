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
