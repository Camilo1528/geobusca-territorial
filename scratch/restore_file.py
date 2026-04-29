import shutil
import os

src = r"C:\Users\Camilo\Desktop\filtrado_47km_rionegro.csv"
dst = r"C:\proyecto geo\backend\geobusca_data\uploads\bcf8a9f81b1f7174_filtrado_47km_rionegro.csv"

if os.path.exists(src):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)
    print(f"File copied to {dst}")
else:
    print(f"Source file not found at {src}")
