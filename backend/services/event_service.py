import json
import sqlite3
from typing import Optional, List
from backend.database_web import get_conn, now_iso, with_db_retry

@with_db_retry()
def log_audit(user_id: Optional[int], action: str, entity_type: str, entity_id: object = '',
              dataset_id: Optional[int] = None, row_idx: Optional[int] = None, 
              details: Optional[dict] = None,
              old_data: Optional[dict] = None,
              new_data: Optional[dict] = None,
              conn: Optional[sqlite3.Connection] = None) -> None:
    
    final_details = details or {}
    
    # Calcular diff si se proporcionan ambos estados
    if old_data is not None and new_data is not None:
        diff = {}
        all_keys = set(old_data.keys()) | set(new_data.keys())
        for key in all_keys:
            old_val = old_data.get(key)
            new_val = new_data.get(key)
            if old_val != new_val:
                diff[key] = {"from": old_val, "to": new_val}
        if diff:
            final_details["_diff"] = diff

    sql = 'INSERT INTO audit_log (user_id, action, entity_type, entity_id, dataset_id, row_idx, details_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
    params = (user_id, action, entity_type, str(entity_id or ''), dataset_id,
              row_idx, json.dumps(final_details, ensure_ascii=False), now_iso())
    
    if conn is not None:
        conn.execute(sql, params)
    else:
        with get_conn() as new_conn:
            new_conn.execute(sql, params)

@with_db_retry()
def create_notification(user_id: int, category: str,
                        title: str, message: str, link: str = '',
                        conn: Optional[sqlite3.Connection] = None) -> None:
    if not user_id:
        return
    sql = 'INSERT INTO notifications (user_id, category, title, message, link, is_read, created_at) VALUES (?, ?, ?, ?, ?, 0, ?)'
    params = (user_id, str(category or 'info'), str(title or '').strip(), str(
                message or '').strip(), str(link or '').strip(), now_iso())
    
    if conn is not None:
        conn.execute(sql, params)
    else:
        with get_conn() as new_conn:
            new_conn.execute(sql, params)
