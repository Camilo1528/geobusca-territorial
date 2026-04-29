import sqlite3
import os

db_path = 'backend/geobusca_data/geobusca_saas.db'
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT id, original_filename, stored_filename FROM datasets").fetchall()
if not rows:
    print("No datasets found in DB.")
else:
    for row in rows:
        print(f"ID: {row['id']}, Original: {row['original_filename']}, Stored: {row['stored_filename']}")
conn.close()
