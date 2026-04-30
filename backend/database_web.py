import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from werkzeug.security import generate_password_hash
BASE_DIR = Path(__file__).resolve().parent.parent
APP_DATA_DIR = BASE_DIR / 'geobusca_data'
UPLOAD_DIR = APP_DATA_DIR / 'uploads'
EXPORT_DIR = APP_DATA_DIR / 'exports'
TERRITORIAL_DIR = APP_DATA_DIR / 'territorial_layers'
DB_PATH = APP_DATA_DIR / 'geobusca_saas.db'

for path in (APP_DATA_DIR, UPLOAD_DIR, EXPORT_DIR, TERRITORIAL_DIR):
    path.mkdir(parents=True, exist_ok=True)


import time
from functools import wraps

def with_db_retry(max_retries: int = 5, base_delay: float = 0.1):
    """
    Decorator that retries a function if a 'database is locked' OperationalError occurs.
    Uses exponential backoff for the delay between retries.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            last_error = None
            for i in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e).lower():
                        last_error = e
                        # Exponential backoff: 0.1, 0.2, 0.4, 0.8, 1.6s
                        time.sleep(base_delay * (2 ** i))
                        continue
                    raise
            raise last_error
        return wrapper
    return decorator


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    # Performance and concurrency optimizations
    conn.execute('PRAGMA foreign_keys = ON')
    conn.execute('PRAGMA journal_mode = WAL')
    conn.execute('PRAGMA synchronous = NORMAL')
    conn.execute('PRAGMA busy_timeout = 30000')
    return conn


def _column_exists(conn: sqlite3.Connection, table_name: str,
                   column_name: str) -> bool:
    rows = conn.execute(f'PRAGMA table_info({table_name})').fetchall()
    return any(row['name'] == column_name for row in rows)


def init_db() -> None:
    with get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                full_name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'analyst',
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS datasets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                original_filename TEXT NOT NULL,
                stored_filename TEXT NOT NULL,
                provider TEXT NOT NULL,
                city TEXT NOT NULL,
                region TEXT NOT NULL,
                row_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                provider TEXT NOT NULL,
                total_rows INTEGER NOT NULL DEFAULT 0,
                ok_rows INTEGER NOT NULL DEFAULT 0,
                exportable_rows INTEGER NOT NULL DEFAULT 0,
                manual_rows INTEGER NOT NULL DEFAULT 0,
                duplicate_rows INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS manual_edits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                old_lat TEXT,
                old_lon TEXT,
                new_lat TEXT,
                new_lon TEXT,
                reason TEXT,
                edited_by INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(edited_by) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS geocode_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                normalized_address TEXT NOT NULL,
                city TEXT NOT NULL,
                region TEXT NOT NULL,
                provider TEXT NOT NULL,
                latitud REAL,
                longitud REAL,
                estado_geo TEXT NOT NULL,
                direccion_geocodificada TEXT,
                consulta_usada TEXT,
                geo_score INTEGER,
                geo_confianza TEXT,
                created_at TEXT NOT NULL,
                last_used_at TEXT NOT NULL,
                UNIQUE(normalized_address, city, region, provider)
            );

            CREATE TABLE IF NOT EXISTS territorial_layers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                display_name TEXT NOT NULL,
                layer_type TEXT NOT NULL,
                city TEXT NOT NULL,
                region TEXT NOT NULL,
                source TEXT,
                file_path TEXT NOT NULL,
                srid TEXT,
                feature_count INTEGER NOT NULL DEFAULT 0,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_geocode_cache_lookup
                ON geocode_cache(normalized_address, city, region, provider);

            CREATE INDEX IF NOT EXISTS idx_territorial_layers_lookup
                ON territorial_layers(city, region, layer_type, is_active);

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER,
                user_id INTEGER NOT NULL,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER NOT NULL DEFAULT 0,
                current_step TEXT,
                error_message TEXT,
                requested_payload TEXT,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                action TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                entity_id TEXT,
                dataset_id INTEGER,
                row_idx INTEGER,
                details_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS visit_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                visit_date TEXT,
                official_name TEXT,
                visit_status TEXT,
                visit_result TEXT,
                observations TEXT,
                updated_data_json TEXT,
                debt_status TEXT,
                debt_amount REAL,
                debt_reference TEXT,
                debt_source TEXT,
                created_by INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(created_by) REFERENCES users(id) ON DELETE SET NULL,
                UNIQUE(dataset_id, row_idx)
            );

            CREATE TABLE IF NOT EXISTS debt_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                row_idx INTEGER,
                identifier_value TEXT,
                debt_status TEXT,
                debt_amount REAL,
                debt_reference TEXT,
                source TEXT,
                checked_by INTEGER,
                checked_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(checked_by) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_user_status ON jobs(user_id, status, created_at);
            CREATE INDEX IF NOT EXISTS idx_audit_dataset_created ON audit_log(dataset_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_visit_dataset_row ON visit_records(dataset_id, row_idx);

            CREATE TABLE IF NOT EXISTS sync_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER,
                row_idx INTEGER,
                user_id INTEGER,
                action TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                device_label TEXT,
                server_updated_at TEXT,
                error_message TEXT,
                payload_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS visit_conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                user_id INTEGER,
                client_version TEXT,
                server_version TEXT,
                resolution_status TEXT NOT NULL DEFAULT 'pending',
                client_payload_json TEXT,
                server_payload_json TEXT,
                notes TEXT,
                resolved_by INTEGER,
                resolved_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL,
                FOREIGN KEY(resolved_by) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS visit_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dataset_id INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                visit_record_id INTEGER,
                stored_filename TEXT NOT NULL,
                original_filename TEXT,
                mime_type TEXT,
                file_size INTEGER NOT NULL DEFAULT 0,
                uploaded_by INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                FOREIGN KEY(visit_record_id) REFERENCES visit_records(id) ON DELETE CASCADE,
                FOREIGN KEY(uploaded_by) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_sync_events_user_created ON sync_events(user_id, created_at);
            CREATE INDEX IF NOT EXISTS idx_sync_events_dataset_row ON sync_events(dataset_id, row_idx, created_at);
            CREATE INDEX IF NOT EXISTS idx_visit_conflicts_dataset_row ON visit_conflicts(dataset_id, row_idx, created_at);
            CREATE INDEX IF NOT EXISTS idx_visit_attachments_lookup ON visit_attachments(dataset_id, row_idx, created_at);

            CREATE TABLE IF NOT EXISTS user_module_permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                module_key TEXT NOT NULL,
                allowed INTEGER NOT NULL DEFAULT 1,
                updated_by INTEGER,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, module_key),
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(updated_by) REFERENCES users(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_user_module_permissions_lookup ON user_module_permissions(user_id, module_key);


            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                category TEXT NOT NULL,
                title TEXT NOT NULL,
                message TEXT NOT NULL,
                link TEXT,
                is_read INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                read_at TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_notifications_user_created ON notifications(user_id, is_read, created_at);

            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                key_name TEXT NOT NULL,
                api_key TEXT NOT NULL UNIQUE,
                is_active INTEGER NOT NULL DEFAULT 1,
                last_used_at TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_api_keys_lookup ON api_keys(api_key, is_active);

            CREATE TABLE IF NOT EXISTS system_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1), -- Solo una fila
                brand_name TEXT NOT NULL DEFAULT 'GeoBusca Territorial',
                brand_subtitle TEXT NOT NULL DEFAULT 'Municipio de Rionegro',
                primary_color TEXT NOT NULL DEFAULT '#C1121F',
                logo_path TEXT,
                updated_at TEXT NOT NULL
            );

            INSERT OR IGNORE INTO system_settings (id, brand_name, brand_subtitle, primary_color, updated_at)
            VALUES (1, 'GeoBusca Territorial', 'Municipio de Rionegro', '#C1121F', datetime('now'));

            CREATE TABLE IF NOT EXISTS smtp_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                smtp_server TEXT,
                smtp_port INTEGER,
                smtp_user TEXT,
                smtp_password TEXT,
                smtp_from TEXT,
                use_tls INTEGER DEFAULT 1,
                updated_at TEXT NOT NULL
            );

            INSERT OR IGNORE INTO smtp_settings (id, updated_at) VALUES (1, datetime('now'));

            CREATE TABLE IF NOT EXISTS support_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                subject TEXT NOT NULL,
                message TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open', -- open, closed, pending
                admin_response TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS establishment_registry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                dataset_id INTEGER NOT NULL,
                row_idx INTEGER NOT NULL,
                nit TEXT NOT NULL,
                matricula TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(dataset_id) REFERENCES datasets(id) ON DELETE CASCADE,
                UNIQUE(user_id, nit, matricula)
            );

            CREATE INDEX IF NOT EXISTS idx_establishment_registry_lookup ON establishment_registry(user_id, nit, matricula);
            """
        )

        if not _column_exists(conn, 'runs', 'notes'):
            conn.execute("ALTER TABLE runs ADD COLUMN notes TEXT")

        visit_record_columns = {
            'approval_status': "TEXT NOT NULL DEFAULT 'draft'",
            'approval_notes': 'TEXT',
            'approval_reason_code': 'TEXT',
            'approval_reason_label': 'TEXT',
            'reviewed_by': 'INTEGER',
            'reviewed_at': 'TEXT',
            'assigned_to': 'INTEGER',
            'assigned_by': 'INTEGER',
            'assigned_at': 'TEXT',
            'assignment_status': "TEXT NOT NULL DEFAULT 'unassigned'",
            'territory_scope': 'TEXT',
            'agenda_date': 'TEXT',
            'agenda_order': 'INTEGER',
            'route_group': 'TEXT',
            'route_optimization_method': 'TEXT',
            'route_optimized_at': 'TEXT',
            'route_optimized_by': 'INTEGER',
            'route_distance_km': 'REAL',
            'route_estimated_minutes': 'INTEGER',
            'schedule_start_time': 'TEXT',
            'schedule_end_time': 'TEXT',
            'estimated_service_minutes': 'INTEGER',
            'route_vehicle': 'TEXT',
            'route_capacity': 'INTEGER',
            'completion_status': "TEXT NOT NULL DEFAULT 'pending'",
            'completion_notes': 'TEXT',
            'completion_reason_code': 'TEXT',
            'completion_reason_label': 'TEXT',
            'checkin_at': 'TEXT',
            'checkin_latitude': 'REAL',
            'checkin_longitude': 'REAL',
            'checkin_accuracy': 'REAL',
            'checkout_at': 'TEXT',
            'checkout_latitude': 'REAL',
            'checkout_longitude': 'REAL',
            'checkout_accuracy': 'REAL',
            'completed_at': 'TEXT',
            'completed_by': 'INTEGER',
            'alert_status': 'TEXT',
            'alert_message': 'TEXT',
            'last_alert_sent_at': 'TEXT',
            'evidence_required': 'INTEGER NOT NULL DEFAULT 0',
            'sla_status': 'TEXT',
            'sla_message': 'TEXT',
            'day_closed_at': 'TEXT',
            'day_closed_by': 'INTEGER',
            'distance_anomaly_meters': 'REAL',
            'has_gps_anomaly': 'INTEGER NOT NULL DEFAULT 0',
        }
        for column_name, definition in visit_record_columns.items():
            if not _column_exists(conn, 'visit_records', column_name):
                conn.execute(
                    f"ALTER TABLE visit_records ADD COLUMN {column_name} {definition}")
        
        if not _column_exists(conn, 'users', 'must_change_password'):
            conn.execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0")

        # Seed admin user if database is empty
        count = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()['c']
        if count == 0:
            admin_email = 'admin@rionegro.gov.co'
            admin_pass = generate_password_hash('admin123')
            conn.execute(
                "INSERT INTO users (email, password_hash, full_name, role, created_at, must_change_password) VALUES (?, ?, ?, ?, ?, ?)",
                (admin_email, admin_pass, 'Administrador del Sistema', 'admin', now_iso(), 1)
            )
