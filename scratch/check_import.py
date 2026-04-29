
import sys
import os

# Add the current directory to sys.path to allow importing backend
sys.path.append(os.getcwd())

try:
    from backend.services.visit_service import extract_row_coordinates
    print("Successfully imported extract_row_coordinates from visit_service")
except Exception as e:
    print(f"Failed to import: {e}")
    sys.exit(1)
