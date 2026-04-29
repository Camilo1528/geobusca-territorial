import json
from typing import Optional
from werkzeug.security import generate_password_hash, check_password_hash
from backend.database_web import get_conn, now_iso, with_db_retry
from backend.services.event_service import log_audit
from backend.config import MODULE_LABELS

@with_db_retry()
def get_all_users() -> list[dict]:
    """Retrieves all users from the database."""
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT id, email, full_name, role, created_at, must_change_password FROM users ORDER BY created_at ASC, id ASC'
        ).fetchall()
    return [dict(r) for r in rows]

def get_user_by_email(email: str) -> Optional[dict]:
    """Retrieves a user by their email address."""
    email = email.strip().lower()
    with get_conn() as conn:
        row = conn.execute('SELECT * FROM users WHERE email=?', (email,)).fetchone()
    return dict(row) if row else None

@with_db_retry()
def get_user_by_id(user_id: int) -> Optional[dict]:
    """Retrieves a user by their ID."""
    with get_conn() as conn:
        row = conn.execute('SELECT * FROM users WHERE id=?', (user_id,)).fetchone()
    return dict(row) if row else None

@with_db_retry()
def create_user(email: str, full_name: str, role: str, actor_id: int) -> int:
    """Creates a new user with a temporary password."""
    email = email.strip().lower()
    with get_conn() as conn:
        exists = conn.execute('SELECT 1 FROM users WHERE email=?', (email,)).fetchone()
        if exists:
            raise ValueError('El correo ya está en uso.')
            
        cursor = conn.execute(
            'INSERT INTO users (email, password_hash, full_name, role, created_at, must_change_password) VALUES (?, ?, ?, ?, ?, 1)',
            (email, generate_password_hash('temporal123'), full_name.strip(), role.strip().lower(), now_iso())
        )
        user_id = cursor.lastrowid
        
    log_audit(actor_id, 'create_user', 'user', user_id, details={'email': email, 'role': role})
    return user_id

@with_db_retry()
def update_user_role(target_id: int, new_role: str, actor_id: int) -> None:
    """Updates a user's role."""
    with get_conn() as conn:
        conn.execute('UPDATE users SET role=? WHERE id=?', (new_role.strip().lower(), target_id))
    log_audit(actor_id, 'change_role', 'user', target_id, details={'role': new_role})

@with_db_retry()
def get_user_module_permissions(user_id: int, role: str = '') -> dict:
    """Retrieves module permissions for a user, considering their role defaults."""
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT module_key, allowed FROM user_module_permissions WHERE user_id=?',
            (user_id,)).fetchall()
    perms = {row['module_key']: bool(row['allowed']) for row in rows}
    
    # If no specific permissions, use role defaults (this is a simplified version)
    if not perms:
        if role == 'admin':
            return {k: True for k in MODULE_LABELS}
        if role == 'revisor':
            return {k: k in {'sync_panel', 'review_datasets', 'daily_agenda'} for k in MODULE_LABELS}
    return perms

@with_db_retry()
def update_user_permissions(target_id: int, submitted_perms: dict, actor_id: int) -> None:
    """Updates a user's specific module permissions."""
    with get_conn() as conn:
        conn.execute('DELETE FROM user_module_permissions WHERE user_id=?', (target_id,))
        for module_key in MODULE_LABELS:
            allowed = 1 if submitted_perms.get(module_key) else 0
            conn.execute(
                'INSERT INTO user_module_permissions (user_id, module_key, allowed, updated_by, updated_at) VALUES (?, ?, ?, ?, ?)',
                (target_id, module_key, allowed, actor_id, now_iso()),
            )
    log_audit(actor_id, 'update_permissions', 'user', target_id, details={'modules': submitted_perms})

@with_db_retry()
def update_password(user_id: int, new_password: str) -> None:
    """Updates a user's password and clears the must_change_password flag."""
    hashed = generate_password_hash(new_password)
    with get_conn() as conn:
        conn.execute(
            'UPDATE users SET password_hash=?, must_change_password=0 WHERE id=?',
            (hashed, user_id)
        )
    log_audit(user_id, 'change_password', 'user', user_id)

def authenticate_user(email: str, password: str) -> Optional[dict]:
    """Authenticates a user by email and password."""
    user = get_user_by_email(email)
    if not user:
        return None
    if check_password_hash(user['password_hash'], password):
        return user
    return None
