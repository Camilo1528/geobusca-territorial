import sqlite3
import os

db_path = 'backend/geobusca_data/geobusca.db'
conn = sqlite3.connect(db_path)
try:
    conn.execute("ALTER TABLE system_settings ADD COLUMN brand_logo TEXT")
    print("Added brand_logo column.")
except sqlite3.OperationalError:
    print("Column brand_logo already exists.")
conn.close()
