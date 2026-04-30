import sqlite3
import os

db_path = r"C:\tmp\geobusca_data\geobusca_saas.db"

if not os.path.exists(db_path):
    print(f"Error: No se encuentra la base de datos en {db_path}")
else:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        users = cursor.execute("SELECT email, role, full_name FROM users").fetchall()
        print("USUARIOS ENCONTRADOS:")
        for user in users:
            print(f"- Email: {user['email']} | Rol: {user['role']} | Nombre: {user['full_name']}")
    except Exception as e:
        print(f"Error al leer usuarios: {e}")
    finally:
        conn.close()
