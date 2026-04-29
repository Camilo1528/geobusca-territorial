import secrets
import sqlite3
from typing import Optional
from functools import wraps
from flask import request, jsonify
from backend.database_web import get_conn, now_iso, with_db_retry

@with_db_retry()
def create_api_key(user_id: int, key_name: str) -> str:
    """Generates and stores a new API key."""
    api_key = f"gb_{secrets.token_urlsafe(32)}"
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO api_keys (user_id, key_name, api_key, created_at) VALUES (?, ?, ?, ?)',
            (user_id, key_name.strip(), api_key, now_iso())
        )
    return api_key

@with_db_retry()
def validate_api_key(api_key: str) -> Optional[dict]:
    """Validates an API key and returns the associated user data."""
    with get_conn() as conn:
        row = conn.execute(
            '''
            SELECT ak.*, u.email, u.full_name, u.role 
            FROM api_keys ak
            JOIN users u ON ak.user_id = u.id
            WHERE ak.api_key = ? AND ak.is_active = 1
            ''',
            (api_key,)
        ).fetchone()
        
        if row:
            conn.execute('UPDATE api_keys SET last_used_at = ? WHERE id = ?', (now_iso(), row['id']))
            return dict(row)
    return None

def api_key_required(f):
    """Decorator to require a valid API key in the X-API-KEY header."""
    @wraps(f)
    def decorated(*args, **kwargs):
        api_key = request.headers.get('X-API-KEY')
        if not api_key:
            return jsonify({"error": "X-API-KEY header is missing"}), 401
        
        user_data = validate_api_key(api_key)
        if not user_data:
            return jsonify({"error": "Invalid or inactive API key"}), 403
            
        # Attach user data to request context if needed
        request.api_user = user_data
        return f(*args, **kwargs)
    return decorated
