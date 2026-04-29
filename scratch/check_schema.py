import sqlite3
import os

db_path = 'backend/geobusca_data/geobusca.db'
conn = sqlite3.connect(db_path)
cursor = conn.execute("PRAGMA table_info(system_settings)")
for row in cursor.fetchall():
    print(row)
conn.close()
