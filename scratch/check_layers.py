import sqlite3
import os

db_path = 'backend/geobusca_data/geobusca.db'
if not os.path.exists(db_path):
    print(f"DB not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT id, display_name, layer_type, is_active FROM territorial_layers").fetchall()
if not rows:
    print("No layers found in DB.")
else:
    for row in rows:
        print(f"ID: {row['id']}, Name: {row['display_name']}, Type: {row['layer_type']}, Active: {row['is_active']}")
conn.close()
