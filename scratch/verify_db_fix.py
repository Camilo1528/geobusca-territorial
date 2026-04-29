import os
import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / 'backend'))

from database_web import get_conn
from app import log_audit

def test_nested_write():
    print("Testing nested write...")
    try:
        with get_conn() as conn:
            print("Opened connection 1 and started transaction.")
            conn.execute("INSERT INTO audit_log (action, entity_type, created_at) VALUES (?, ?, ?)", 
                         ("test_nested_outer", "test", "2023-01-01T00:00:00Z"))
            
            print("Calling log_audit with conn...")
            log_audit(None, "test_nested_inner", "test", conn=conn)
            print("log_audit with conn successful.")
            
        print("Transaction committed.")
        
        # Test concurrent write (WAL + Timeout)
        print("\nTesting concurrent write (simulating another process/thread)...")
        with get_conn() as conn1:
            conn1.execute("INSERT INTO audit_log (action, entity_type, created_at) VALUES (?, ?, ?)", 
                          ("test_concurrent_1", "test", "2023-01-01T00:00:00Z"))
            
            print("Connection 1 has an open transaction. Trying to write with Connection 2...")
            # This should work in WAL mode (it will wait for conn1 to finish if it's a write lock, 
            # but in WAL mode, we might still hit BUSY if multiple writers)
            # Actually WAL allows 1 writer + N readers. Multiple writers still wait.
            # But with 30s timeout, it should wait.
            
            import threading
            def writer2():
                try:
                    print("Thread 2: Trying to log audit...")
                    log_audit(None, "test_concurrent_2", "test")
                    print("Thread 2: Success!")
                except Exception as e:
                    print(f"Thread 2: Failed! {e}")

            t = threading.Thread(target=writer2)
            t.start()
            
            import time
            time.sleep(2)
            print("Connection 1: Committing...")
        
        t.join()
        print("Verification complete.")

    except Exception as e:
        print(f"Verification failed: {e}")

if __name__ == "__main__":
    test_nested_write()
