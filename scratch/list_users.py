import sqlite3
from pathlib import Path

db_path = Path('backend/geobusca_data/geobusca_saas.db')
if not db_path.exists():
    print(f"Database not found at {db_path}")
else:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    users = conn.execute("SELECT id, email, role FROM users").fetchall()
    for u in users:
        print(f"ID: {u['id']} | Email: {u['email']} | Role: {u['role']}")
    conn.close()
