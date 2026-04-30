import sqlite3
import os

db_path = 'geobusca_territorial.db'
if not os.path.exists(db_path):
    print(f"Database {db_path} not found.")
else:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    datasets = conn.execute('SELECT * FROM datasets').fetchall()
    print(f"Total datasets: {len(datasets)}")
    for d in datasets:
        print(f"ID: {d['id']}, User: {d['user_id']}, File: {d['original_filename']}")
    
    users = conn.execute('SELECT * FROM users').fetchall()
    print(f"\nTotal users: {len(users)}")
    for u in users:
        print(f"ID: {u['id']}, Email: {u['email']}, Role: {u['role']}")
    conn.close()
