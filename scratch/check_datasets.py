from backend.database_web import get_conn
with get_conn() as conn:
    rows = conn.execute('SELECT id, user_id, original_filename FROM datasets').fetchall()
    for r in rows:
        print(f"ID: {r['id']}, User: {r['user_id']}, File: {r['original_filename']}")
