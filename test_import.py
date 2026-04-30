import os
import sys
# Add current directory to path
sys.path.insert(0, os.getcwd())
try:
    from backend.app import app
    print("Import successful")
except Exception as e:
    import traceback
    traceback.print_exc()
