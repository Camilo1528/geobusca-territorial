import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from notifications import smtp_ready
from address_ai import auto_fix_address

import backend.services.media_service as media_service
import backend.services.visit_service as visit_service
import backend.services.report_service as report_service
import backend.services.job_service as job_service
import backend.services.dataset_service as dataset_service
import backend.services.territory_service as territory_service
import backend.services.geocoding_service as geocoding_service
import backend.services.routing_service as routing_service
import backend.services.admin_service as admin_service
import backend.services.agenda_service as agenda_service

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from territorial import (
    active_layer_options,
    allowed_layer_file,
    assign_territories_to_dataframe,
    build_layer_geojson_with_counts,
    persist_canonical_layer,
)
from quality import apply_quality_flags, deduplicate_suspicious_coordinates
from normalizer import AddressNormalizer
from models import GeocodeResult
from geocoder import BulkGeocoder
from notifications import (
    build_visit_email_html,
    build_visit_email_subject,
    build_visit_email_text,
    build_visit_whatsapp_url,
    send_html_email,
)
from institutional import (
    apply_visit_update,
    build_visit_queue,
    compare_dataset_frames,
    ensure_visit_columns,
    merge_debt_dataframe,
    summarize_visits,
)
from economic_activity import detect_activity_columns, enrich_economic_activity
from arcgis_rest import ArcGISRestError, RIONEGRO_LAYER_PRESETS, fetch_layer_geojson, write_geojson_temp
from database_web import EXPORT_DIR, TERRITORIAL_DIR, UPLOAD_DIR, get_conn, init_db, now_iso
from config import (
    ALCALDIA_LAT,
    ALCALDIA_LON,
    DEFAULT_CITY,
    DEFAULT_COUNTRY,
    DEFAULT_PROVIDER,
    DEFAULT_REGION,
    TERRITORIAL_LAYER_TYPES,
    ROUTING_PROFILE,
    ROUTING_PROVIDER,
    ROUTING_TIMEOUT_SECONDS,
    OSRM_BASE_URL,
    GRAPHHOPPER_BASE_URL,
    GRAPHHOPPER_API_KEY,
    ALERT_GRACE_MINUTES,
    ALERT_REPEAT_MINUTES,
    ROLE_LABELS,
    ROLE_RANK,
    MODULE_LABELS,
    ROLE_DEFAULT_MODULES,
    APPROVAL_REASON_OPTIONS,
    ALL_APPROVAL_REASON_CODES,
    COMPLETION_REASON_OPTIONS,
    EVIDENCE_REQUIRED_CAUSES,
    resolve_provider,
)
from analytics import build_review_queue, dataset_summary, to_excel_report, to_geojson, to_kml
# Standard libraries
import base64
import csv
import logging
import math
import binascii
import hashlib
import json
import mimetypes
import os  
import secrets
import sys
import sqlite3
import threading
from collections import Counter, defaultdict

from datetime import datetime, timedelta
import time
from io import BytesIO
from functools import wraps
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from flask import Flask, abort, flash, jsonify, make_response, redirect, render_template, request, send_file, session, url_for
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))


BASE_DIR = Path(__file__).resolve().parent
ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls'}
VISIT_MEDIA_DIR = ROOT_DIR / 'geobusca_data' / 'visit_media'
VISIT_MEDIA_DIR.mkdir(parents=True, exist_ok=True)

app = Flask(__name__, 
            template_folder=str(ROOT_DIR / 'frontend' / 'templates'),
            static_folder=str(ROOT_DIR / 'frontend' / 'static'))
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
_secret_key = os.getenv('FLASK_SECRET_KEY', '').strip()
if not _secret_key:
    if os.getenv('FLASK_DEBUG', '0') == '1':
        _secret_key = 'dev-insecure-change-me'
    else:
        raise RuntimeError(
            'FLASK_SECRET_KEY es obligatorio cuando FLASK_DEBUG no está activo.')
app.secret_key = _secret_key
app.config['MAX_CONTENT_LENGTH'] = 40 * 1024 * 1024
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Validar configuración SMTP al inicio
if not smtp_ready():
    logging.warning(
        "La configuración SMTP no está completa. El envío de emails estará deshabilitado.")
app.config['SESSION_COOKIE_SECURE'] = os.getenv(
    'FLASK_COOKIE_SECURE', '0') == '1'

init_db()

@app.template_filter('basename')
def basename_filter(s):
    return os.path.basename(s)

def ensure_admin_email():
    try:
        with get_conn() as conn:
            # Find an admin-level user
            row = conn.execute("SELECT id, email FROM users WHERE role IN ('admin', 'manager', 'gerente') LIMIT 1").fetchone()
            if row:
                target_email = 'camilo152893@gmail.com'
                if row['email'] != target_email:
                    conn.execute("UPDATE users SET email = ? WHERE id = ?", (target_email, row['id']))
                    logging.info(f"Admin email updated to {target_email} for 2FA.")
            
            # Also ensure SMTP settings are correct in DB if they exist
            conn.execute('''
                UPDATE smtp_settings 
                SET smtp_server='smtp.gmail.com', smtp_port=587, 
                    smtp_user='camilo152893@gmail.com', smtp_password='iucihbjuscpxajyg', 
                    smtp_from='camilo152893@gmail.com', use_tls=1 
                WHERE id=1
            ''')
            logging.info("SMTP settings updated in database.")
    except Exception as e:
        logging.error(f"Error ensuring admin email: {e}")

ensure_admin_email()

@app.route('/manifest.json')
def manifest_json():
    return send_file(str(ROOT_DIR / 'frontend' / 'static' / 'manifest.json'), mimetype='application/json')

@app.context_processor
def inject_system_settings():
    try:
        with get_conn() as conn:
            row = conn.execute('SELECT * FROM system_settings WHERE id = 1').fetchone()
            if row:
                return {'system_settings': dict(row), 'now_iso': now_iso, 'csrf_token': generate_csrf_token()}


    except:
        pass
    return {'system_settings': {
        'brand_name': 'GeoBusca Territorial',
        'brand_subtitle': 'Municipio de Rionegro',
        'primary_color': '#C1121F',
        'logo_path': None
    }, 'now_iso': now_iso, 'csrf_token': generate_csrf_token()}



@app.context_processor
def inject_i18n():
    return {
        'sla_labels': {
            'met': 'Cumplido',
            'breached': 'Incumplido',
            'active': 'Activo',
            'open': 'Pendiente',
            'no_schedule': 'Sin Agenda'
        },
        'completion_labels': {
            'pending': 'Pendiente',
            'in_progress': 'En Proceso',
            'completed': 'Completada',
            'no_effective': 'No Efectiva',
            'started': 'Iniciada'
        },
        'sync_labels': {
            'synced': 'Sincronizado',
            'pending': 'Pendiente',
            'error': 'Error'
        }
    }

# --- Service Aliases ---
create_job = job_service.create_job
update_job = job_service.update_job
get_job = job_service.get_job
run_pdf_job_wrapper = job_service.run_pdf_job_wrapper
JOB_THREADS = job_service.JOB_THREADS
ATTACHMENTS_DIR = VISIT_MEDIA_DIR / "attachments"
ATTACHMENTS_DIR.mkdir(parents=True, exist_ok=True)
# Role and module constants moved to config.py

# Workflow and alert constants moved to config.py
# Alert constants moved to config.py

# Replacement of local functions with service calls
def get_user_dataset(dataset_id: int, user_id: int):
    user = current_user()
    is_revisor = has_role(user, 'revisor') if user else False
    return dataset_service.get_user_dataset(dataset_id, user_id, is_revisor)

def load_dataset(dataset_id: int, user_id: int) -> pd.DataFrame:
    user = current_user()
    is_revisor = has_role(user, 'revisor') if user else False
    return dataset_service.load_dataset(dataset_id, user_id, is_revisor)

def save_processed_dataset(dataset_id: int, user_id: int, df: pd.DataFrame) -> Path:
    return dataset_service.save_processed_dataset(dataset_id, user_id, df)

def load_processed_dataset(dataset_id: int, user_id: int) -> pd.DataFrame:
    return dataset_service.load_processed_dataset(dataset_id, user_id)

def _load_any_dataset_version(dataset_id: int, user_id: int) -> pd.DataFrame:
    user = current_user()
    is_revisor = has_role(user, 'revisor') if user else False
    return dataset_service.load_any_dataset_version(dataset_id, user_id, is_revisor)

def detect_address_columns(df: pd.DataFrame) -> List[str]:
    return dataset_service.detect_address_columns(df)

def detect_coordinate_columns(df: pd.DataFrame) -> tuple[str, str] | tuple[None, None]:
    return dataset_service.detect_coordinate_columns(df)

def _ensure_visit_placeholder(dataset_id: int, row_idx: int) -> int:
    return visit_service.ensure_visit_placeholder(dataset_id, row_idx)

def _update_visit_workflow(dataset_id: int, row_idx: int, **updates) -> None:
    return visit_service.update_visit_workflow(dataset_id, row_idx, **updates)

def _visit_record_by_row(dataset_id: int, row_idx: int) -> Optional[dict]:
    return visit_service.get_visit_record_by_row(dataset_id, row_idx)

def _visit_records_for_dataset(dataset_id: int) -> List[dict]:
    return visit_service.get_visit_records_for_dataset(dataset_id)

def _persist_visit_record(dataset_id: int, row_idx: int, user_id: int, payload: dict) -> int:
    return visit_service.persist_visit_record(dataset_id, row_idx, user_id, payload)

def get_active_layers(city: str, region: str, layer_type: Optional[str] = None) -> List[dict]:
    return territory_service.get_active_layers(city, region, layer_type)

def _territory_label_from_row(row: dict) -> str:
    return territory_service.territory_label_from_row(row)

def _extract_row_coordinates(row: dict) -> tuple[Optional[float], Optional[float]]:
    return routing_service.extract_row_coordinates(row)

def _build_route_plan(rows: list[dict], start_lat: float = ALCALDIA_LAT, start_lon: float = ALCALDIA_LON) -> dict:
    return routing_service.build_route_plan(rows, start_lat, start_lon)

def load_geocode_cache_map(city: str, region: str, provider: str) -> Dict[str, dict]:
    return geocoding_service.load_geocode_cache_map(city, region, provider)

def save_geocode_cache_entry(normalized_address: str, city: str, region: str, provider: str, result: GeocodeResult, geo_score: Optional[int] = None, geo_confianza: str = '') -> None:
    return geocoding_service.save_geocode_cache_entry(normalized_address, city, region, provider, result, geo_score, geo_confianza)

def touch_geocode_cache_entry(normalized_address: str, city: str, region: str, provider: str) -> None:
    return geocoding_service.touch_geocode_cache_entry(normalized_address, city, region, provider)

def geocode_result_from_cache(row: dict, provider: str) -> GeocodeResult:
    return geocoding_service.geocode_result_from_cache(row, provider)

def _role_value(user: Optional[dict]) -> str:
    return str((user or {}).get("role") or "funcionario").strip().lower()


def role_label(role: str) -> str:
    return ROLE_LABELS.get(str(role or "").strip().lower(),
                           str(role or "Funcionario").title())


def has_role(user: Optional[dict], minimum_role: str) -> bool:
    return ROLE_RANK.get(_role_value(user), 0) >= ROLE_RANK.get(
        str(minimum_role or "funcionario").lower(), 0)


def _default_module_permissions_for_role(role: str) -> dict:
    role_key = str(role or 'funcionario').strip().lower()
    allowed = ROLE_DEFAULT_MODULES.get(
        role_key, ROLE_DEFAULT_MODULES['funcionario'])
    return {module: module in allowed for module in MODULE_LABELS}


def _get_user_module_permissions(user_id: int, role: str) -> dict:
    permissions = _default_module_permissions_for_role(role)
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT module_key, allowed FROM user_module_permissions WHERE user_id=?',
            (user_id,
             )).fetchall()
    for row in rows:
        permissions[str(row['module_key'])] = bool(row['allowed'])
    return permissions


def has_module_permission(user: Optional[dict], module_key: str) -> bool:
    if not user:
        return False
    module_key = str(module_key or '').strip()
    if not module_key:
        return True
    permissions = user.get('module_permissions')
    if not isinstance(permissions, dict):
        permissions = _get_user_module_permissions(
            int(user['id']), _role_value(user))
        user['module_permissions'] = permissions
    return bool(permissions.get(module_key, False))


def module_required(module_key: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                return redirect(url_for('auth.login'))
            if not has_module_permission(user, module_key):
                flash(
                    'No tienes permisos para acceder a este módulo.',
                    'warning')
                return redirect(url_for('dashboard.dashboard'))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def role_required(minimum_role: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                return redirect(url_for("auth.login"))
            if not has_role(user, minimum_role):
                flash(
                    "No tienes permisos para acceder a esta sección.",
                    "warning")
                return redirect(url_for("dashboard.dashboard"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


# _guess_extension_from_data_url is now in file_service


_save_data_url_image = media_service.save_data_url_image
_save_uploaded_visit_file = media_service.save_uploaded_visit_file
_signature_metadata = media_service.get_signature_metadata
_save_attachments_from_request = media_service.save_attachments_from_request
_save_attachments_from_dataurls = media_service.save_attachments_from_dataurls

def _visit_media_url(filename: str) -> str:
    filename = str(filename or '').strip()
    if not filename:
        return ''
    return url_for('visit_media', filename=filename)
_save_attachments_from_dataurls = media_service.save_attachments_from_dataurls


def _persist_visit_attachments(dataset_id: int, row_idx: int, user_id: int,
                               attachments: list[dict], visit_record_id: Optional[int] = None) -> None:
    if not attachments:
        return
    with get_conn() as conn:
        for item in attachments:
            conn.execute(
                'INSERT INTO visit_attachments (dataset_id, row_idx, visit_record_id, stored_filename, original_filename, mime_type, file_size, uploaded_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (dataset_id,
                 row_idx,
                 visit_record_id,
                 item['stored_filename'],
                    item.get('original_filename',
                             ''),
                    item.get('mime_type',
                             ''),
                    int(item.get('file_size') or 0),
                    user_id,
                    now_iso()),
            )


def _attachments_for_visit(dataset_id: int, row_idx: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT * FROM visit_attachments WHERE dataset_id=? AND row_idx=? ORDER BY created_at ASC, id ASC',
            (dataset_id,
             row_idx)).fetchall()
    out = []
    for row in rows:
        item = dict(row)
        item['url'] = _visit_media_url(item.get('stored_filename', ''))
        item['preview_type'] = _attachment_preview_type(item)
        out.append(item)
    return out


def _record_sync_event(dataset_id: int, row_idx: int, user_id: int, action: str, status: str, attempts: int = 0,
                       device_label: str = '', server_updated_at: str = '', error_message: str = '', payload: Optional[dict] = None) -> None:
    with get_conn() as conn:
        conn.execute(
            'INSERT INTO sync_events (dataset_id, row_idx, user_id, action, status, attempts, device_label, server_updated_at, error_message, payload_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (dataset_id,
             row_idx,
             user_id,
             action,
             status,
             int(attempts or 0),
                device_label,
                server_updated_at,
                error_message,
                json.dumps(payload or {},
                           ensure_ascii=False),
                now_iso(),
                now_iso()),
        )


def _record_visit_conflict(dataset_id: int, row_idx: int, user_id: int, client_version: str,
                           server_record: Optional[dict], client_payload: Optional[dict] = None, notes: str = '') -> int:
    with get_conn() as conn:
        cur = conn.execute(
            'INSERT INTO visit_conflicts (dataset_id, row_idx, user_id, client_version, server_version, resolution_status, client_payload_json, server_payload_json, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (
                dataset_id, row_idx, user_id, client_version, str(
                    (server_record or {}).get(
                        'updated_at', '') or ''), 'pending', json.dumps(
                    client_payload or {}, ensure_ascii=False), json.dumps(
                    server_record or {}, ensure_ascii=False), notes, now_iso()),
        )
        return int(cur.lastrowid)


def create_notification(user_id: int, category: str,
                        title: str, message: str, link: str = '',
                        conn: Optional[sqlite3.Connection] = None) -> None:
    from backend.services.event_service import create_notification as svc_notify
    svc_notify(user_id, category, title, message, link, conn=conn)


def unread_notification_count(user_id: int) -> int:
    if not user_id:
        return 0
    with get_conn() as conn:
        row = conn.execute(
            'SELECT COUNT(*) AS total FROM notifications WHERE user_id=? AND is_read=0',
            (user_id,
             )).fetchone()
    return int(row['total']) if row else 0


def recent_notifications(user_id: int, limit: int = 25) -> list[dict]:
    if not user_id:
        return []
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT * FROM notifications WHERE user_id=? ORDER BY created_at DESC, id DESC LIMIT ?',
            (user_id,
             limit)).fetchall()
    return [dict(r) for r in rows]


def _notify_roles(minimum_role: str, category: str, title: str,
                  message: str, link: str = '') -> None:
    with get_conn() as conn:
        rows = conn.execute('SELECT id, role FROM users').fetchall()
    for row in rows:
        if ROLE_RANK.get(str(row['role']).lower(), 0) >= ROLE_RANK.get(
                str(minimum_role or 'revisor').lower(), 0):
            create_notification(int(row['id']), category, title, message, link)


# Functions now handled by services or consolidated aliases


# Alias for jobs is at the top (get_job = job_service.get_job)


def _cache_dataset_for_job(
        dataset_id: int, user_id: int, df: pd.DataFrame) -> None:
    dataset_service.PROCESSED_DATASET_CACHE[dataset_service._cache_key(dataset_id, user_id)] = df.copy()


def run_process_job(job_id: int, dataset_id: int, user: dict, address_col: str,
                    provider: str, api_key: str, city: str, region: str, activity_col: str = '') -> None:
    # Use the standardized wrapper from job_service
    # We pass process_dataset and dataset_summary as callbacks to avoid circular imports in job_service
    job_service.run_process_job_wrapper(
        job_id, dataset_id, user, address_col, provider, api_key, city, region, activity_col,
        process_func=process_dataset,
        summary_func=dataset_summary
    )


def _match_previous_dataset(current_dataset: dict,
                            user_id: int) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            '''
            SELECT * FROM datasets
            WHERE user_id=? AND city=? AND region=? AND id <> ?
            ORDER BY created_at DESC, id DESC
            LIMIT 1
            ''',
            (user_id,
             current_dataset['city'],
             current_dataset['region'],
             current_dataset['id']),
        ).fetchone()
    return dict(row) if row else None


# Aliases for dataset operations
def _load_any_dataset_version(dataset_id: int, user_id: int) -> pd.DataFrame:
    return dataset_service.load_any_dataset_version(dataset_id, user_id)

def deduplicate_against_history(user_id: int, new_df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    return dataset_service.deduplicate_against_history(user_id, new_df)

def combine_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    """Concatenates two dataframes and resets the index to ensure stability."""
    combined = pd.concat([df1, df2], ignore_index=True)
    return combined


def _truthy(value: object) -> bool:
    return str(value or '').strip().lower() in {
        '1', 'true', 'si', 'sí', 'yes', 'y', 'on'}


def _check_visit_conflict(dataset_id: int, row_idx: int, client_version: str,
                          force_overwrite: bool = False) -> Optional[dict]:
    current = _visit_record_by_row(dataset_id, row_idx)
    if not current or force_overwrite:
        return None
    server_version = str(current.get('updated_at') or '').strip()
    client_version = str(client_version or '').strip()
    if not client_version:
        return None
    if server_version and server_version != client_version:
        return current
    return None


def _conflict_compare_rows(client_payload: dict,
                           server_payload: dict) -> list[dict]:
    compare_rows = []
    keys = sorted(set(client_payload) | set(server_payload))
    for key in keys:
        if key in {'server_updated_at'}:
            continue
        client_value = client_payload.get(key, '')
        server_value = server_payload.get(key, '')
        client_s = '' if client_value is None else str(client_value)
        server_s = '' if server_value is None else str(server_value)
        if client_s == server_s:
            continue
        compare_rows.append({'field': key, 'label': key.replace(
            '_', ' ').title(), 'client_value': client_s, 'server_value': server_s})
    return compare_rows


def _build_payload_from_field_choices(
        conflict: dict, field_choices: dict[str, str]) -> tuple[dict, list[str], list[str]]:
    client_payload = json.loads(conflict.get('client_payload_json') or '{}')
    server_payload = json.loads(conflict.get('server_payload_json') or '{}')
    merged = dict(server_payload)
    compare_rows = _conflict_compare_rows(client_payload, server_payload)
    changed_from_client = []
    kept_server = []
    for item in compare_rows:
        field = item['field']
        choice = str(field_choices.get(field, 'client')
                     ).strip().lower() or 'client'
        if choice == 'server':
            merged[field] = server_payload.get(field, '')
            kept_server.append(field)
        else:
            merged[field] = client_payload.get(field, '')
            changed_from_client.append(field)
    return merged, changed_from_client, kept_server


def _apply_resolved_conflict(conflict: dict, merged_payload: dict,
                             changed_from_client: list[str], resolver_user: dict) -> None:
    dataset_id = int(conflict['dataset_id'])
    row_idx = int(conflict['row_idx'])
    dataset = get_user_dataset(dataset_id, int(resolver_user['id']))
    if not dataset:
        raise ValueError('Dataset no encontrado para resolver conflicto')
    owner_id = int(dataset['user_id'])
    df = ensure_visit_columns(_load_any_dataset_version(dataset_id, owner_id))
    updated_df, result = apply_visit_update(
        df, row_idx=row_idx, payload=merged_payload)
    active_layers = get_active_layers(dataset['city'], dataset['region'])
    if active_layers:
        updated_df = assign_territories_to_dataframe(updated_df, active_layers)
    updated_df = apply_quality_flags(
        updated_df,
        current_city=dataset['city'],
        center_lat=ALCALDIA_LAT,
        center_lon=ALCALDIA_LON)
    save_processed_dataset(dataset_id, owner_id, updated_df)
    visit_record_id = _persist_visit_record(
        dataset_id, row_idx, int(
            resolver_user['id']), merged_payload)
    log_audit(int(resolver_user['id']),
              'apply_conflict_fields',
              'visit_conflict',
              conflict['id'],
              dataset_id=dataset_id,
              row_idx=row_idx,
              details={'client_fields_applied': changed_from_client,
                       'visit_record_id': visit_record_id,
                       'changed_fields': result.changed_fields})


def _attachment_preview_type(item: dict) -> str:
    mime = str(item.get('mime_type') or '').lower()
    if mime.startswith('image/'):
        return 'image'
    if 'pdf' in mime:
        return 'pdf'
    return 'file'


def _build_visit_pdf_bytes(dataset: dict, visit: dict) -> bytes:
    return report_service.build_visit_pdf_bytes(dataset, visit)

def _save_report_to_disk(pdf_bytes: bytes, filename: str) -> str:
    return report_service.save_report_to_disk(pdf_bytes, filename)



# Routing and Coordinate extraction handled by routing_service and visit_service

def _approval_reason_label(action: str, code: str) -> str:
    action = str(action or '').strip().lower()
    code = str(code or '').strip().lower()
    for option_code, option_label in APPROVAL_REASON_OPTIONS.get(action, []):
        if option_code == code:
            return option_label
    return ALL_APPROVAL_REASON_CODES.get(
        code, code.replace('_', ' ').title() if code else '')


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = math.sin(d_lat / 2) ** 2 + math.cos(math.radians(lat1)) * \
        math.cos(math.radians(lat2)) * math.sin(d_lon / 2) ** 2
    return r * 2 * math.asin(min(1.0, math.sqrt(a)))


def _route_provider_choice() -> str:
    return str(os.getenv('ROUTING_PROVIDER',
               ROUTING_PROVIDER or 'osrm')).strip().lower()


def _route_points_from_rows(rows: list[dict]) -> tuple[list[dict], list[dict]]:
    available = []
    missing = []
    for original_index, item in enumerate(rows):
        row = dict(item)
        lat, lon = _extract_row_coordinates(row)
        row['_route_lat'] = lat
        row['_route_lon'] = lon
        row['_route_original_index'] = original_index
        if lat is None or lon is None:
            missing.append(row)
        else:
            available.append(row)
    return available, missing


def _cleanup_route_rows(rows: list[dict]) -> list[dict]:
    cleaned = []
    for row in rows:
        item = dict(row)
        for key in ['_route_lat', '_route_lon',
                    '_route_original_index', '_route_step_km']:
            item.pop(key, None)
        cleaned.append(item)
    return cleaned


def _nearest_neighbor_order(
        rows: list[dict], start_lat: float, start_lon: float) -> tuple[list[dict], float]:
    available = [dict(item) for item in rows]
    ordered = []
    total_km = 0.0
    current_lat, current_lon = start_lat, start_lon
    while available:
        next_item = min(
            available,
            key=lambda item: _haversine_km(
                current_lat,
                current_lon,
                item['_route_lat'],
                item['_route_lon']))
        step = _haversine_km(
            current_lat,
            current_lon,
            next_item['_route_lat'],
            next_item['_route_lon'])
        total_km += step
        next_item['_route_step_km'] = round(step, 3)
        ordered.append(next_item)
        current_lat, current_lon = next_item['_route_lat'], next_item['_route_lon']
        available.remove(next_item)
    return ordered, round(total_km, 2)


def _build_osrm_trip_url(points: list[dict]) -> str:
    coords = ';'.join(
        f"{item['_route_lon']},{item['_route_lat']}" for item in points)
    profile = str(
        os.getenv(
            'ROUTING_PROFILE',
            ROUTING_PROFILE or 'driving')).strip().lower() or 'driving'
    return f"{OSRM_BASE_URL}/trip/v1/{profile}/{coords}"


def _fetch_osrm_trip_plan(points: list[dict]) -> Optional[dict]:
    if len(points) < 2:
        return None
    params = {
        'source': 'first',
        'roundtrip': 'false',
        'steps': 'false',
        'overview': 'full',
        'geometries': 'geojson',
    }
    try:
        response = requests.get(
            _build_osrm_trip_url(points),
            params=params,
            timeout=ROUTING_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None
    if str(payload.get('code')) != 'Ok' or not payload.get('trips'):
        return None
    trip = payload['trips'][0]
    waypoints = payload.get('waypoints') or []
    if not waypoints:
        return None
    ordered_indexes = [None] * len(points)
    for input_index, waypoint in enumerate(waypoints):
        waypoint_index = waypoint.get('waypoint_index')
        if isinstance(waypoint_index,
                      int) and 0 <= waypoint_index < len(points):
            ordered_indexes[waypoint_index] = input_index
    if any(index is None for index in ordered_indexes):
        return None
    ordered = [dict(points[input_idx]) for input_idx in ordered_indexes]
    distance_km = round(float(trip.get('distance') or 0) / 1000.0, 2)
    duration_minutes = int(
        round(
            float(
                trip.get('duration') or 0) /
            60.0)) if trip.get('duration') is not None else None
    return {
        'ordered': ordered,
        'total_km': distance_km,
        'total_minutes': duration_minutes,
        'geometry': trip.get('geometry'),
        'provider': 'osrm',
        'method': 'osrm_trip',
    }


def _fetch_osrm_route_for_order(points: list[dict]) -> Optional[dict]:
    if len(points) < 2:
        return None
    coords = ';'.join(
        f"{item['_route_lon']},{item['_route_lat']}" for item in points)
    profile = str(
        os.getenv(
            'ROUTING_PROFILE',
            ROUTING_PROFILE or 'driving')).strip().lower() or 'driving'
    url = f"{OSRM_BASE_URL}/route/v1/{profile}/{coords}"
    params = {'steps': 'false', 'overview': 'full', 'geometries': 'geojson'}
    try:
        response = requests.get(
            url, params=params, timeout=ROUTING_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None
    if str(payload.get('code')) != 'Ok' or not payload.get('routes'):
        return None
    route = payload['routes'][0]
    return {
        'total_km': round(float(route.get('distance') or 0) / 1000.0, 2),
        'total_minutes': int(round(float(route.get('duration') or 0) / 60.0)) if route.get('duration') is not None else None,
        'geometry': route.get('geometry'),
        'provider': 'osrm',
        'method': 'osrm_route',
    }


def _fetch_graphhopper_route_for_order(points: list[dict]) -> Optional[dict]:
    if len(points) < 2 or not GRAPHHOPPER_API_KEY:
        return None
    params = [
        ('profile', os.getenv('GRAPHHOPPER_PROFILE', 'car').strip() or 'car'),
        ('points_encoded', 'false'),
        ('instructions', 'false'),
        ('calc_points', 'true'),
    ]
    for item in points:
        params.append(('point', f"{item['_route_lat']},{item['_route_lon']}"))
    try:
        response = requests.get(
            f"{GRAPHHOPPER_BASE_URL}/route", params=params + [
                ('key', GRAPHHOPPER_API_KEY)], timeout=ROUTING_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except Exception:
        return None
    paths = payload.get('paths') or []
    if not paths:
        return None
    path = paths[0]
    points_geo = path.get('points') if isinstance(
        path.get('points'), dict) else None
    return {
        'total_km': round(float(path.get('distance') or 0) / 1000.0, 2),
        'total_minutes': int(round(float(path.get('time') or 0) / 60000.0)) if path.get('time') is not None else None,
        'geometry': points_geo,
        'provider': 'graphhopper',
        'method': 'graphhopper_route',
    }



# Moved to routing_service


def _optimize_route_rows(rows: list[dict], start_lat: float = ALCALDIA_LAT,
                         start_lon: float = ALCALDIA_LON) -> tuple[list[dict], float]:
    plan = _build_route_plan(rows, start_lat=start_lat, start_lon=start_lon)
    return plan['ordered'], float(plan.get('total_km') or 0.0)


def _bulk_assign_agenda_from_file(path: Path, fallback_dataset_id: int,
                                  actor_user: dict, staff_rows: list[dict]) -> tuple[int, list[str]]:
    df = now_df(path)
    df.columns = [str(col).strip() for col in df.columns]
    normalized = {col.lower(): col for col in df.columns}
    if 'row_idx' not in normalized:
        raise ValueError('El archivo debe incluir la columna row_idx.')
    users_by_id = {int(u['id']): dict(u) for u in staff_rows}
    users_by_email = {str((u.get('email') or '')).strip().lower(): dict(
        u) for u in staff_rows if str((u.get('email') or '')).strip()}
    applied = 0
    errors: list[str] = []
    for pos, row in df.iterrows():
        try:
            row_idx = int(row[normalized['row_idx']])
        except Exception:
            errors.append(f'Fila {pos + 2}: row_idx inválido.')
            continue
        dataset_id = fallback_dataset_id
        if 'dataset_id' in normalized:
            try:
                dataset_id = int(row[normalized['dataset_id']])
            except Exception:
                errors.append(f'Fila {pos + 2}: dataset_id inválido.')
                continue
        assigned_to = None
        for candidate in ['assigned_to', 'assigned_to_id',
                          'user_id', 'funcionario_id']:
            if candidate in normalized:
                raw = str(row[normalized[candidate]]).strip()
                if raw:
                    try:
                        assigned_to = int(float(raw))
                        break
                    except Exception:
                        pass
        if assigned_to is None:
            for candidate in ['assigned_to_email',
                              'funcionario_email', 'email']:
                if candidate in normalized:
                    email = str(row[normalized[candidate]]).strip().lower()
                    if email and email in users_by_email:
                        assigned_to = int(users_by_email[email]['id'])
                        break
        if assigned_to is None or assigned_to not in users_by_id:
            errors.append(f'Fila {pos + 2}: funcionario no identificado.')
            continue
        agenda_date = ''
        if 'agenda_date' in normalized:
            agenda_date = str(row[normalized['agenda_date']]).strip()
        elif 'fecha' in normalized:
            agenda_date = str(row[normalized['fecha']]).strip()
        route_group = ''
        if 'route_group' in normalized:
            route_group = str(row[normalized['route_group']]).strip()
        elif 'grupo_ruta' in normalized:
            route_group = str(row[normalized['grupo_ruta']]).strip()
        territory_scope = ''
        for candidate in ['territory_scope', 'territorio', 'territory']:
            if candidate in normalized:
                territory_scope = str(row[normalized[candidate]]).strip()
                break
        agenda_order = None
        for candidate in ['agenda_order', 'orden', 'orden_ruta']:
            if candidate in normalized:
                raw = str(row[normalized[candidate]]).strip()
                if raw:
                    try:
                        agenda_order = int(float(raw))
                    except Exception:
                        agenda_order = None
                break
        _update_visit_workflow(dataset_id, row_idx,
                               assigned_to=assigned_to,
                               assigned_by=actor_user['id'],
                               assigned_at=now_iso(),
                               assignment_status='scheduled' if agenda_date else 'assigned',
                               territory_scope=territory_scope,
                               agenda_date=agenda_date,
                               agenda_order=agenda_order,
                               route_group=route_group,
                               )
        applied += 1
    return applied, errors


def _agenda_template_response() -> object:
    import io
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(['dataset_id',
                     'row_idx',
                     'assigned_to_email',
                     'agenda_date',
                     'route_group',
                     'agenda_order',
                     'territory_scope'])
    writer.writerow([1, 0, 'funcionario@rionegro.gov.co',
                    '2026-01-15', 'Ruta Centro AM', 1, 'barrio / Centro'])
    writer.writerow([1, 1, 'funcionario@rionegro.gov.co',
                    '2026-01-15', 'Ruta Centro AM', 2, 'barrio / Centro'])
    response = make_response(buffer.getvalue())
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = 'attachment; filename=plantilla_agenda_masiva.csv'
    return response


def _multi_staff_template_response() -> object:
    import io
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(['assigned_to_email', 'capacity',
                    'shift_start', 'shift_end', 'vehicle_label'])
    writer.writerow(['funcionario1@rionegro.gov.co',
                    8, '08:00', '12:00', 'Moto 1'])
    writer.writerow(['funcionario2@rionegro.gov.co',
                    10, '08:30', '17:00', 'Moto 2'])
    writer.writerow(['funcionario3@rionegro.gov.co', 6,
                    '13:00', '17:00', 'Camioneta 1'])
    response = make_response(buffer.getvalue())
    response.headers['Content-Type'] = 'text/csv; charset=utf-8'
    response.headers['Content-Disposition'] = 'attachment; filename=plantilla_planificador_multi_staff.csv'
    return response


def _parse_hhmm(value: object, default_minutes: int = 480) -> int:
    raw = str(value or '').strip()
    if not raw:
        return int(default_minutes)
    try:
        hh, mm = raw.split(':', 1)
        return max(0, min(23, int(hh))) * 60 + max(0, min(59, int(mm)))
    except Exception:
        return int(default_minutes)


def _minutes_to_hhmm(total_minutes: Optional[int]) -> str:
    if total_minutes is None:
        return ''
    total = max(0, int(total_minutes))
    return f"{(total // 60) % 24:02d}:{total % 60:02d}"


def _parse_positive_int(value: object, default: int) -> int:
    try:
        parsed = int(float(str(value or '').strip()))
        return parsed if parsed > 0 else int(default)
    except Exception:
        return int(default)


def _split_total_minutes(
        total_minutes: Optional[int], stop_count: int) -> list[int]:
    if stop_count <= 1:
        return []
    total = int(total_minutes or 0)
    if total <= 0:
        return [0] * (stop_count - 1)
    base = total // (stop_count - 1)
    extra = total % (stop_count - 1)
    return [base + (1 if i < extra else 0) for i in range(stop_count - 1)]


def _build_schedule_for_plan(items: list[dict], *, shift_start: str, shift_end: str,
                             service_minutes: int, total_route_minutes: Optional[int]) -> tuple[list[dict], list[dict]]:
    scheduled, overflow = [], []
    if not items:
        return scheduled, overflow
    current = _parse_hhmm(shift_start, 8 * 60)
    limit = _parse_hhmm(shift_end, 17 * 60)
    if limit <= current:
        limit = current + 8 * 60
    legs = _split_total_minutes(total_route_minutes, len(items))
    for idx, item in enumerate(items):
        start_at = current
        end_at = start_at + max(1, int(service_minutes or 20))
        if end_at > limit:
            overflow.append(item)
            continue
        cloned = dict(item)
        cloned['schedule_start_time'] = _minutes_to_hhmm(start_at)
        cloned['schedule_end_time'] = _minutes_to_hhmm(end_at)
        cloned['estimated_service_minutes'] = int(service_minutes or 20)
        scheduled.append(cloned)
        if idx < len(legs):
            current = end_at + max(0, int(legs[idx]))
    return scheduled, overflow


def _parse_multi_staff_specs(
        raw_lines: str, staff_rows: list[dict], service_minutes: int = 20) -> tuple[list[dict], list[str]]:
    staff_by_email = {str(item.get('email', '')).strip(
    ).lower(): item for item in staff_rows}
    staff_by_id = {int(item['id']): item for item in staff_rows}
    specs, errors = [], []
    for line_no, raw in enumerate(str(raw_lines or '').splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        parts = [part.strip() for part in line.split('|')]
        if len(parts) < 4:
            errors.append(
                f'Línea {line_no}: usa formato email|capacidad|hora_inicio|hora_fin|vehiculo')
            continue
        identity = parts[0].lower()
        user_row = staff_by_id.get(int(identity)) if identity.isdigit() and int(
            identity) in staff_by_id else staff_by_email.get(identity)
        if not user_row:
            errors.append(
                f'Línea {line_no}: funcionario no encontrado ({
                    parts[0]})')
            continue
        specs.append({
            'user_id': int(user_row['id']),
            'user_name': user_row.get('full_name') or user_row.get('email'),
            'email': user_row.get('email', ''),
            'capacity': _parse_positive_int(parts[1], 8),
            'shift_start': parts[2] or '08:00',
            'shift_end': parts[3] or '17:00',
            'vehicle_label': parts[4] if len(parts) > 4 and parts[4] else f'Unidad {len(specs) + 1}',
            'service_minutes': int(service_minutes or 20),
        })
    return specs, errors


def _plan_multi_staff_assignments(
        selected_items: list[dict], planner_specs: list[dict], *, optimize_route: bool = True) -> dict:
    if not planner_specs:
        return {'plans': [], 'unassigned': list(selected_items), 'summary': {
            'assigned': 0, 'unassigned': len(selected_items), 'km': 0.0, 'minutes': 0}}
    ordered_items = _build_route_plan(selected_items).get(
        'ordered', list(selected_items)) if optimize_route else list(selected_items)
    queue = list(ordered_items)
    plans, unassigned = [], []
    total_km, total_minutes = 0.0, 0
    for spec in planner_specs:
        cap = max(1, int(spec.get('capacity') or 1))
        chunk = []
        while queue and len(chunk) < cap:
            chunk.append(queue.pop(0))
        if not chunk:
            plans.append({'spec': spec, 'items': [], 'route': {
                         'total_km': 0.0, 'total_minutes': 0, 'provider': 'none', 'method': 'none'}})
            continue
        route = _build_route_plan(chunk) if optimize_route else {
            'ordered': chunk,
            'total_km': 0.0,
            'total_minutes': 0,
            'provider': 'none',
            'method': 'manual'}
        scheduled, overflow = _build_schedule_for_plan(
            route.get(
                'ordered', chunk), shift_start=spec.get(
                'shift_start', '08:00'), shift_end=spec.get(
                'shift_end', '17:00'), service_minutes=int(
                    spec.get('service_minutes') or 20), total_route_minutes=route.get('total_minutes'))
        queue = overflow + queue
        plans.append({'spec': spec, 'items': scheduled, 'route': route})
        total_km += float(route.get('total_km') or 0.0)
        total_minutes += int(route.get('total_minutes') or 0)
    unassigned.extend(queue)
    return {'plans': plans, 'unassigned': unassigned, 'summary': {'assigned': sum(len(
        p['items']) for p in plans), 'unassigned': len(unassigned), 'km': round(total_km, 2), 'minutes': total_minutes}}


def _productivity_rows_for_date(target_date: str) -> dict:
    target_date = str(target_date or '').strip() or time.strftime('%Y-%m-%d')
    with get_conn() as conn:
        visit_rows = [dict(r) for r in conn.execute(
            'SELECT vr.*, u.full_name AS assigned_name, u.role AS assigned_role, d.user_id AS dataset_owner_id FROM visit_records vr LEFT JOIN users u ON u.id = vr.assigned_to LEFT JOIN datasets d ON d.id = vr.dataset_id WHERE COALESCE(vr.agenda_date, "")=? ORDER BY vr.assigned_to, vr.agenda_order, vr.id',
            (target_date,),
        ).fetchall()]
    by_staff = defaultdict(
        lambda: {
            'assigned': 0,
            'completed': 0,
            'no_efectiva': 0,
            'in_progress': 0,
            'pending': 0,
            'km': 0.0,
            'territories': Counter()})
    by_territory = defaultdict(
        lambda: {
            'assigned': 0,
            'completed': 0,
            'pending': 0})
    for item in visit_rows:
        ds_owner = int(item.get('dataset_owner_id') or 0)
        row_idx = int(item.get('row_idx') or 0)
        try:
            df = ensure_visit_columns(_load_any_dataset_version(
                int(item['dataset_id']), ds_owner))
            row_data = df.iloc[row_idx].to_dict(
            ) if 0 <= row_idx < len(df) else {}
        except Exception:
            row_data = {}
        territory = _territory_label_from_row(row_data) or str(
            item.get('territory_scope') or '').strip() or 'Sin territorio'
        assigned_name = item.get('assigned_name') or f"Funcionario {
            item.get('assigned_to') or 'N/A'}"
        staff_key = f"{item.get('assigned_to') or 0}:{assigned_name}"
        s = by_staff[staff_key]
        s['name'] = assigned_name
        s['role'] = role_label(item.get('assigned_role'))
        s['assigned'] += 1
        s['km'] += float(item.get('route_distance_km') or 0)
        s['territories'][territory] += 1
        comp = str(item.get('completion_status') or 'pending').strip().lower()
        if comp == 'completed':
            s['completed'] += 1
        elif comp in {'no_effective', 'cancelled'}:
            s['no_efectiva'] += 1
        elif comp in {'in_progress', 'started'}:
            s['in_progress'] += 1
        else:
            s['pending'] += 1
        t = by_territory[territory]
        t['territorio'] = territory
        t['assigned'] += 1
        if comp == 'completed':
            t['completed'] += 1
        else:
            t['pending'] += 1
    staff_rows = []
    for row in by_staff.values():
        compliance = round(
            (row['completed'] / row['assigned']) * 100.0,
            1) if row['assigned'] else 0.0
        staff_rows.append({'name': row['name'],
                           'role': row['role'],
                           'assigned': row['assigned'],
                           'completed': row['completed'],
                           'pending': row['pending'],
                           'in_progress': row['in_progress'],
                           'no_efectiva': row['no_efectiva'],
                           'compliance': compliance,
                           'territories': ', '.join(name for name,
                                                    _ in row['territories'].most_common(3)),
                           'km': round(row['km'],
                                       2)})
    staff_rows.sort(key=lambda x: (-x['completed'], x['pending'], x['name']))
    territory_rows = []
    for territory, row in sorted(by_territory.items(
    ), key=lambda kv: (-kv[1]['assigned'], kv[0]))[:25]:
        compliance = round(
            (row['completed'] / row['assigned']) * 100.0,
            1) if row['assigned'] else 0.0
        territory_rows.append({'territorio': territory,
                               'assigned': row['assigned'],
                               'completed': row['completed'],
                               'pending': row['pending'],
                               'compliance': compliance})
    return {'date': target_date, 'staff': staff_rows,
            'territories': territory_rows}


def _complete_visit_progress(dataset_id: int, row_idx: int, status: str, actor_user: dict,
                             notes: str = '', reason_code: str = '', gps: Optional[dict] = None) -> None:
    status = str(status or '').strip().lower()
    reason_code = str(reason_code or '').strip().lower()
    now_value = now_iso()
    gps = gps or {}
    updates = {
        'completion_status': status,
        'completion_notes': notes,
        'completion_reason_code': reason_code,
        'completion_reason_label': dict(COMPLETION_REASON_OPTIONS.get(status, [])).get(reason_code, reason_code.replace('_', ' ').title()) if reason_code else '',
        'completed_by': actor_user['id'],
    }
    if status in {'in_progress', 'started'}:
        updates['checkin_at'] = now_value
        updates['checkin_latitude'] = gps.get('latitude')
        updates['checkin_longitude'] = gps.get('longitude')
        updates['checkin_accuracy'] = gps.get('accuracy')
        updates['assignment_status'] = 'in_progress'
        updates['visit_status'] = 'EN_CURSO'
    elif status == 'completed':
        updates['checkout_at'] = now_value
        updates['checkout_latitude'] = gps.get('latitude')
        updates['checkout_longitude'] = gps.get('longitude')
        updates['checkout_accuracy'] = gps.get('accuracy')
        updates['completed_at'] = now_value
        updates['assignment_status'] = 'done'
        updates['visit_status'] = 'REALIZADA'
        if not reason_code:
            updates['completion_reason_code'] = 'gestion_exitosa'
            updates['completion_reason_label'] = 'Gestión exitosa'
    elif status in {'no_effective', 'cancelled'}:
        updates['checkout_at'] = now_value
        updates['checkout_latitude'] = gps.get('latitude')
        updates['checkout_longitude'] = gps.get('longitude')
        updates['checkout_accuracy'] = gps.get('accuracy')
        updates['assignment_status'] = 'done'
        updates['visit_status'] = 'NO_EFECTIVA'
        updates['evidence_required'] = 1 if reason_code in EVIDENCE_REQUIRED_CAUSES else 0
    else:
        updates['assignment_status'] = 'scheduled'
        updates['visit_status'] = 'PROGRAMADA'
    _update_visit_workflow(dataset_id, row_idx, **updates)


_agenda_alert_state = agenda_service.get_agenda_alert_state


_agenda_sla_state = agenda_service.get_agenda_sla_state


_last_position_from_item = visit_service.get_last_position


_create_day_close_summary = agenda_service.create_day_close_summary


_emit_agenda_alerts = agenda_service.emit_agenda_alerts


_agenda_route_payload = agenda_service.get_agenda_route_payload
_bulk_reassign_visits = admin_service.bulk_reassign_visits


def _workflow_status_badge(value: str) -> str:
    value = str(value or '').strip().lower()
    return {
        'draft': 'secondary',
        'submitted': 'warning',
        'under_review': 'info',
        'approved': 'success',
        'rejected': 'danger',
        'returned': 'dark',
    }.get(value, 'light')


def _assignment_status_badge(value: str) -> str:
    value = str(value or '').strip().lower()
    return {
        'unassigned': 'secondary',
        'assigned': 'primary',
        'scheduled': 'info',
        'in_progress': 'warning',
        'done': 'success',
        'cancelled': 'dark',
    }.get(value, 'light')



# Moved to territory_service


_ensure_visit_placeholder = visit_service.ensure_visit_placeholder


_update_visit_workflow = visit_service.update_visit_workflow


_approval_rows = admin_service.get_approval_rows


def _available_assignment_territory_values(
        df: pd.DataFrame, territory_type: str) -> list[str]:
    territory_type = str(territory_type or '').strip()
    if territory_type not in {'comuna', 'barrio', 'vereda',
                              'corregimiento'} or territory_type not in df.columns:
        return []
    return sorted({str(v).strip() for v in df[territory_type].fillna('').astype(str) if str(v).strip()})

_queue_rows_for_assignment = admin_service.get_queue_rows_for_assignment
_agenda_rows_for_user = agenda_service.get_agenda_rows_for_user
_territorial_indicators = admin_service.get_territorial_indicators


def _clean_display(value: object) -> str:
    if value is None:
        return ''
    if isinstance(value, float) and pd.isna(value):
        return ''
    return str(value).strip()


def _visit_context_from_row(dataset: dict, row_idx: int,
                            row_data: dict, visit_record: Optional[dict]) -> dict:
    context = {k: _clean_display(v) for k, v in row_data.items()}
    context['row_idx'] = row_idx
    for key in ['rvt_razon_social', 'rvt_nit_cc', 'rvt_direccion_establecimiento', 'rvt_direccion_cobro', 'rvt_municipio', 'rvt_departamento', 'rvt_municipio_cobro', 'rvt_departamento_cobro', 'rvt_telefono_movil', 'rvt_telefono_fijo', 'rvt_correo_electronico', 'rvt_sector_economico', 'rvt_fecha_inicio_actividades', 'rvt_codigo_ciiu_1', 'rvt_codigo_ciiu_2', 'rvt_descripcion_actividad', 'rvt_recibe_nombre', 'rvt_recibe_tipo_documento',
                'rvt_recibe_numero_documento', 'rvt_recibe_cargo', 'rvt_rep_legal_a_nombre', 'rvt_rep_legal_a_identificacion', 'rvt_rep_legal_a_correo', 'rvt_rep_legal_b_nombre', 'rvt_rep_legal_b_identificacion', 'rvt_rep_legal_b_correo', 'rvt_firma_recibe_nombre', 'rvt_firma_recibe_tipo_documento', 'rvt_firma_recibe_numero_documento', 'rvt_funcionario_firma_nombre', 'rvt_tipo_visita', 'rvt_codigo_establecimiento', 'rvt_avisos_tableros']:
        context.setdefault(key, '')
    context.setdefault('visita_fecha', '')
    context.setdefault('visita_hora', '')
    context.setdefault('visita_observaciones', '')
    context.setdefault('visita_funcionario', '')
    context.setdefault('visita_estado', '')
    context.setdefault('visit_latitude', '')
    context.setdefault('visit_longitude', '')
    context.setdefault('visit_gps_accuracy', '')
    context.setdefault('visit_device', '')
    context.setdefault('correo_envio_destino', '')
    context.setdefault('visit_photo_establecimiento', '')
    context.setdefault('visit_photo_documento', '')
    context.setdefault('visit_signature_receiver', '')
    context.setdefault('visit_signature_officer', '')
    context.setdefault('visit_signature_receiver_hash', '')
    context.setdefault('visit_signature_receiver_signed_at', '')
    context.setdefault('visit_signature_receiver_signer', '')
    context.setdefault('visit_signature_officer_hash', '')
    context.setdefault('visit_signature_officer_signed_at', '')
    context.setdefault('visit_signature_officer_signer', '')
    context.setdefault('deuda_estado', '')
    context.setdefault('deuda_monto', '')
    context.setdefault('deuda_referencia', '')
    context.setdefault('deuda_fuente', '')
    context.setdefault('server_updated_at', '')
    if visit_record:
        for key, value in visit_record.items():
            if value not in (None, ''):
                context[key] = _clean_display(value)
        context['server_updated_at'] = _clean_display(
            visit_record.get('updated_at'))
    context['dataset_id'] = dataset['id']
    context['dataset_name'] = dataset['original_filename']
    context['municipio'] = dataset.get(
        'city', 'Rionegro') if isinstance(
        dataset, dict) else 'Rionegro'
    context['departamento'] = dataset.get(
        'region', 'Antioquia') if isinstance(
        dataset, dict) else 'Antioquia'
    context['rvt_razon_social'] = context.get(
        'rvt_razon_social') or context.get('nom_establec', '')
    context['rvt_direccion_establecimiento'] = context.get(
        'rvt_direccion_establecimiento') or context.get('direccion', '')
    context['rvt_municipio'] = context.get('rvt_municipio') or dataset['city']
    context['rvt_departamento'] = context.get(
        'rvt_departamento') or dataset['region']
    context['territorio_principal_nombre'] = context.get(
        'territorio_principal_nombre') or context.get('barrio') or context.get('vereda') or ''
    context['visit_photo_establecimiento_url'] = _visit_media_url(
        context.get('visit_photo_establecimiento', ''))
    context['visit_photo_documento_url'] = _visit_media_url(
        context.get('visit_photo_documento', ''))
    context['attachments'] = _attachments_for_visit(
        dataset['id'], row_idx) if isinstance(
        dataset, dict) and dataset.get('id') else []
    return context


def _build_external_url(endpoint: str, **values) -> str:
    base_url = os.getenv('APP_BASE_URL', '').strip().rstrip('/')
    if base_url:
        return f"{base_url}{url_for(endpoint, **values)}"
    return url_for(endpoint, _external=True, **values)


def _recent_jobs_for_user(user_id: int, limit: int = 10) -> List[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT * FROM jobs WHERE user_id=? ORDER BY created_at DESC, id DESC LIMIT ?',
            (user_id,
             limit)).fetchall()
    return [dict(r) for r in rows]


def _cache_key(dataset_id: int, user_id: int) -> int:
    return (user_id * 10_000_000) + dataset_id


def now_df(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in {'.xlsx', '.xls'}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def generate_csrf_token() -> str:
    token = session.get('csrf_token')
    if not token:
        token = secrets.token_urlsafe(32)
        session['csrf_token'] = token
    return token


def validate_csrf() -> None:
    expected = session.get('csrf_token', '')
    provided = request.form.get(
        'csrf_token') or request.headers.get('X-CSRF-Token')
    if not expected or not provided or expected != provided:
        abort(400, description='CSRF token inválido')


def current_user() -> Optional[dict]:
    user_id = session.get('user_id')
    if not user_id:
        return None
    with get_conn() as conn:
        user = conn.execute(
            'SELECT * FROM users WHERE id=?', (user_id,)).fetchone()
    if not user:
        return None
    item = dict(user)
    item['module_permissions'] = _get_user_module_permissions(
        int(item['id']), item.get('role', 'funcionario'))
    return item


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user():
            return redirect(url_for('auth.login'))
        return fn(*args, **kwargs)

    return wrapper


def get_user_dataset(dataset_id: int, user_id: int):
    user = current_user()
    is_revisor = has_role(user, 'revisor') if user else False
    return dataset_service.get_user_dataset(dataset_id, user_id, is_revisor)


def dataframe_profile(df: pd.DataFrame) -> Dict[str, object]:
    address_cols = dataset_service.detect_address_columns(df)
    activity_cols = detect_activity_columns(df)
    preview_columns = []
    for col in list(df.columns)[:12]:
        series = df[col]
        preview_columns.append(
            {
                'column': str(col),
                'nulls': int(series.isna().sum()),
                'sample': '' if series.empty else str(series.iloc[0])[:120],
            }
        )
    return {
        'rows': int(len(df)),
        'columns': int(len(df.columns)),
        'address_candidates': address_cols[:5],
        'activity_candidates': activity_cols[:5],
        'preview_columns': preview_columns,
    }


def _normalize_cache_key(normalized_address: str, city: str,
                         region: str, provider: str) -> tuple[str, str, str, str]:
    return (
        str(normalized_address or '').strip().upper(),
        str(city or '').strip().upper(),
        str(region or '').strip().upper(),
        str(provider or '').strip().lower(),
    )


def load_geocode_cache_map(city: str, region: str,
                           provider: str) -> Dict[str, dict]:
    city_norm, region_norm, provider_norm = str(city).strip().upper(), str(
        region).strip().upper(), str(provider).strip().lower()
    with get_conn() as conn:
        rows = conn.execute(
            '''
            SELECT normalized_address, latitud, longitud, estado_geo, direccion_geocodificada,
                   consulta_usada, geo_score, geo_confianza
            FROM geocode_cache
            WHERE city=? AND region=? AND provider=?
            ''',
            (city_norm, region_norm, provider_norm),
        ).fetchall()
    out: Dict[str, dict] = {}
    for row in rows:
        out[row['normalized_address']] = dict(row)
    return out


def save_geocode_cache_entry(
    normalized_address: str,
    city: str,
    region: str,
    provider: str,
    result: GeocodeResult,
    geo_score: Optional[int] = None,
    geo_confianza: str = '',
) -> None:
    norm_addr, city_norm, region_norm, provider_norm = _normalize_cache_key(
        normalized_address, city, region, provider)
    if not norm_addr:
        return
    with get_conn() as conn:
        conn.execute(
            '''
            INSERT INTO geocode_cache (
                normalized_address, city, region, provider, latitud, longitud, estado_geo,
                direccion_geocodificada, consulta_usada, geo_score, geo_confianza, created_at, last_used_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(normalized_address, city, region, provider) DO UPDATE SET
                latitud=excluded.latitud,
                longitud=excluded.longitud,
                estado_geo=excluded.estado_geo,
                direccion_geocodificada=excluded.direccion_geocodificada,
                consulta_usada=excluded.consulta_usada,
                geo_score=excluded.geo_score,
                geo_confianza=excluded.geo_confianza,
                last_used_at=excluded.last_used_at
            ''',
            (
                norm_addr,
                city_norm,
                region_norm,
                provider_norm,
                result.latitud,
                result.longitud,
                result.estado_geo,
                result.direccion_geocodificada,
                result.consulta_usada,
                geo_score,
                geo_confianza,
                now_iso(),
                now_iso(),
            ),
        )


def touch_geocode_cache_entry(
        normalized_address: str, city: str, region: str, provider: str) -> None:
    norm_addr, city_norm, region_norm, provider_norm = _normalize_cache_key(
        normalized_address, city, region, provider)
    with get_conn() as conn:
        conn.execute(
            'UPDATE geocode_cache SET last_used_at=? WHERE normalized_address=? AND city=? AND region=? AND provider=?',
            (now_iso(), norm_addr, city_norm, region_norm, provider_norm),
        )


def geocode_result_from_cache(row: dict, provider: str) -> GeocodeResult:
    return GeocodeResult(
        latitud=row.get('latitud'),
        longitud=row.get('longitud'),
        direccion_geocodificada=row.get('direccion_geocodificada') or '',
        estado_geo=row.get('estado_geo') or 'NO_ENCONTRADO',
        proveedor=provider,
        consulta_usada=row.get('consulta_usada') or '',
    )


def get_active_layers(city: str, region: str,
                      layer_type: Optional[str] = None) -> List[dict]:
    query = 'SELECT * FROM territorial_layers WHERE is_active=1 AND UPPER(city)=UPPER(?) AND UPPER(region)=UPPER(?)'
    params: List[object] = [city, region]
    if layer_type:
        query += ' AND layer_type=?'
        params.append(layer_type)
    query += ' ORDER BY layer_type, id DESC'
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def register_territorial_layer(
    user_id: int,
    display_name: str,
    layer_type: str,
    city: str,
    region: str,
    source: str,
    stored_meta: Dict[str, object],
) -> None:
    with get_conn() as conn:
        conn.execute(
            '''
            INSERT INTO territorial_layers (user_id, display_name, layer_type, city, region, source, file_path, srid, feature_count, is_active, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ''',
            (user_id,
             display_name,
             layer_type,
             city,
             region,
             source,
             stored_meta['file_path'],
             'EPSG:4326',
             stored_meta['feature_count'],
             now_iso()),
        )


def import_arcgis_layer(
    user_id: int,
    display_name: str,
    layer_type: str,
    city: str,
    region: str,
    source: str,
    service_url: str,
    layer_id: str = '',
    where: str = '1=1',
) -> Dict[str, object]:
    payload, metadata = fetch_layer_geojson(
        service_url=service_url, layer_id=layer_id, where=where)
    temp_path = write_geojson_temp(
        payload,
        TERRITORIAL_DIR,
        f"{display_name}_{layer_type}_{
            secrets.token_hex(4)}")
    try:
        stored_meta = persist_canonical_layer(
            temp_path, display_name, layer_type)
    finally:
        temp_path.unlink(missing_ok=True)
    register_territorial_layer(
        user_id=user_id,
        display_name=display_name,
        layer_type=layer_type,
        city=city,
        region=region,
        source=source,
        stored_meta=stored_meta,
    )
    stored_meta['layer_name'] = metadata.get('name', display_name)
    stored_meta['max_record_count'] = metadata.get('max_record_count', 0)
    return stored_meta


def process_dataset(
    df: pd.DataFrame,
    address_col: str,
    provider: str,
    api_key: str,
    city: str,
    region: str,
    activity_col: str = '',
    job_id: Optional[int] = None
) -> pd.DataFrame:
    work = df.copy()
    work['row_idx'] = range(len(work))
    if address_col not in work.columns:
        raise ValueError(f"La columna '{address_col}' no existe")

    # Ensure geocoding columns exist and are of object type to avoid dtype issues
    for col in ['latitud', 'longitud', 'estado_geo', 'proveedor', 'error_geo', 'direccion_geocodificada', 'fuente_resultado']:
        if col not in work.columns:
            work[col] = ''
        work[col] = work[col].astype(object)

    resolved_provider, _ = resolve_provider(provider, api_key)
    geocoder = BulkGeocoder(
        provider=resolved_provider,
        api_key=api_key,
        user_agent='geobusca_saas')
    persistent_cache = load_geocode_cache_map(city, region, resolved_provider)
    in_run_cache: Dict[str, GeocodeResult] = {}
    records: List[dict] = []

    total_rows = len(work)
    last_progress_update = 0

    for idx, row in work.iterrows():
        # Update progress every 1% or at least every 50 rows
        if job_id and (idx - last_progress_update >= total_rows // 100 or idx - last_progress_update >= 50):
            from backend.services.job_service import update_job
            progress = 15 + int((idx / total_rows) * 60) # Map 0-100% to 15-75% range
            update_job(job_id, progress=progress, current_step=f'geocodificando: {idx}/{total_rows}')
            last_progress_update = idx

        # Skip if already has coordinates (resumability)
        if pd.notna(row.get('latitud')) and pd.notna(row.get('longitud')) and str(row.get('estado_geo', '')) in ('OK', 'MANUAL', 'EDITADO_MANUALMENTE'):
            continue

        raw_address = row.get(address_col)
        raw_text = '' if pd.isna(raw_address) else str(raw_address)
        fixed = auto_fix_address(raw_text, city, region, DEFAULT_COUNTRY)
        corrected = fixed.get('corrected', '').strip()
        candidate_info = AddressNormalizer.build_candidates(
            corrected or raw_text, city, region, DEFAULT_COUNTRY)
        normalized_address = candidate_info.get(
            'direccion_geocodable') or corrected or raw_text
        candidates = candidate_info.get('candidatos', []) or [
            corrected or raw_text]
        source = 'api'

        cached_row = persistent_cache.get(
            str(normalized_address).strip().upper())
        
        result = None
        if normalized_address in in_run_cache:
            result = in_run_cache[normalized_address]
            source = 'cache_lote'
        elif cached_row and cached_row.get('latitud') is not None and cached_row.get('longitud') is not None:
            result = geocode_result_from_cache(cached_row, resolved_provider)
            touch_geocode_cache_entry(
                normalized_address, city, region, resolved_provider)
            in_run_cache[normalized_address] = result
            source = 'cache_historico'
        else:
            try:
                result = geocoder.geocode_with_fallbacks(candidates)
                # Save to persistent cache immediately (persistence/crash-safety)
                if result.estado_geo == 'OK':
                    save_geocode_cache_entry(
                        normalized_address=normalized_address,
                        city=city,
                        region=region,
                        provider=resolved_provider,
                        result=result,
                        geo_score=result.geo_score,
                        geo_confianza=result.geo_confianza
                    )
            except Exception as exc:
                result = GeocodeResult(
                    estado_geo='ERROR',
                    error_geo=str(exc),
                    proveedor=resolved_provider,
                    consulta_usada=candidates[0] if candidates else normalized_address,
                )
            in_run_cache[normalized_address] = result

        if result:
            work.at[idx, 'direccion_geocodable'] = normalized_address
            work.at[idx, 'ai_fix_reason'] = fixed.get('reason', 'heuristic_cleanup')
            work.at[idx, 'direccion_corregida_ai'] = corrected
            work.at[idx, 'latitud'] = result.latitud
            work.at[idx, 'longitud'] = result.longitud
            work.at[idx, 'estado_geo'] = result.estado_geo
            work.at[idx, 'proveedor'] = result.proveedor or resolved_provider
            work.at[idx, 'consulta_usada'] = result.consulta_usada or (candidates[0] if candidates else normalized_address)
            work.at[idx, 'error_geo'] = result.error_geo
            work.at[idx, 'direccion_geocodificada'] = result.direccion_geocodificada
            work.at[idx, 'fuente_resultado'] = source

    work = enrich_economic_activity(work, explicit_column=activity_col or None)
    active_layers = get_active_layers(city, region)
    if active_layers:
        work = assign_territories_to_dataframe(work, active_layers)
    else:
        for col in ['comuna', 'codigo_comuna', 'barrio', 'codigo_barrio', 'corregimiento', 'codigo_corregimiento', 'vereda', 'codigo_vereda',
                    'territorio_principal_tipo', 'territorio_principal_nombre', 'zona_rionegro', 'fuera_municipio', 'territorial_match_score']:
            if col not in work.columns:
                work[col] = ''

    work = apply_quality_flags(
        work,
        current_city=city,
        center_lat=ALCALDIA_LAT,
        center_lon=ALCALDIA_LON)
    work = deduplicate_suspicious_coordinates(work)
    work['geo_exportable_strict'] = work['geo_exportable_strict'].fillna(False)
    work['selected_for_export'] = work['geo_exportable_strict'] | work['estado_geo'].eq(
        'EDITADO_MANUALMENTE')

    work = ensure_visit_columns(work)
    work['visita_requerida'] = work.get('visita_requerida', False)
    for _, row in work.iterrows():
        if not str(row.get('direccion_geocodable', '')).strip():
            continue
        save_geocode_cache_entry(
            normalized_address=str(row.get('direccion_geocodable', '')),
            city=city,
            region=region,
            provider=resolved_provider,
            result=GeocodeResult(
                latitud=row.get('latitud'),
                longitud=row.get('longitud'),
                direccion_geocodificada=str(
                    row.get('direccion_geocodificada', '') or ''),
                estado_geo=str(row.get('estado_geo', '') or 'NO_ENCONTRADO'),
                proveedor=str(row.get('proveedor', '') or resolved_provider),
                consulta_usada=str(row.get('consulta_usada', '') or ''),
            ),
            geo_score=int(
                row['geo_score']) if pd.notna(
                row.get('geo_score')) else None,
            geo_confianza=str(row.get('geo_confianza', '') or ''),
        )
    work['visita_requerida'] = work['visita_requerida'].fillna(False).astype(bool) | work.get(
        'geo_anomalia', pd.Series(
            index=work.index, dtype='object')).fillna('NO').astype(str).str.upper().eq('SI')
    return work

def enrich_dataset_from_cache(df: pd.DataFrame, city: str, region: str) -> pd.DataFrame:
    """Fast enrichment from historical cache only, no API calls."""
    work = df.copy()
    # Ensure columns exist
    for col in ['latitud', 'longitud', 'estado_geo', 'geo_confianza', 'direccion_geocodable']:
        if col not in work.columns:
            work[col] = None
        work[col] = work[col].astype(object)
    
    # Try to find address column if not present
    from backend.services.dataset_service import detect_address_columns
    address_cols = detect_address_columns(work)
    if not address_cols:
        return work
    address_col = address_cols[0]
    
    # Load ALL cache for this city/region to memory for speed
    with get_conn() as conn:
        cache_data = conn.execute(
            'SELECT * FROM geocode_cache WHERE UPPER(city)=UPPER(?) AND UPPER(region)=UPPER(?)',
            (city, region)
        ).fetchall()
    
    cache_map = {str(row['normalized_address']).strip().upper(): row for row in cache_data}
    
    # Optimized vectorized mapping
    norm_addresses = work[address_col].fillna('').astype(str).str.strip().str.upper()
    
    # Create mapping series for each column we want to enrich
    for col in ['latitud', 'longitud', 'estado_geo', 'geo_confianza']:
        mapper = {addr: row[col] for addr, row in cache_map.items()}
        work[col] = norm_addresses.map(mapper).combine_first(work[col])
    
    return work


def reverse_geocode_dataset(
        df: pd.DataFrame, provider: str, api_key: str) -> pd.DataFrame:
    work = df.copy()
    lat_col, lon_col = detect_coordinate_columns(work)
    if not lat_col or not lon_col:
        raise ValueError(
            'No se encontraron columnas de latitud y longitud en el dataset.')

    resolved_provider, _ = resolve_provider(provider, api_key)
    geocoder = BulkGeocoder(
        provider=resolved_provider,
        api_key=api_key,
        user_agent='geobusca_saas')
    if 'direccion_inversa' not in work.columns:
        work['direccion_inversa'] = ''
    if 'direccion_inversa_estado' not in work.columns:
        work['direccion_inversa_estado'] = ''

    for idx, row in work.iterrows():
        lat = row.get(lat_col)
        lon = row.get(lon_col)
        if pd.isna(lat) or pd.isna(lon):
            work.at[idx, 'direccion_inversa_estado'] = 'SIN_COORDENADAS'
            continue
        try:
            address = geocoder.reverse_geocode(float(lat), float(lon))
            work.at[idx, 'direccion_inversa'] = address
            work.at[idx, 'direccion_inversa_estado'] = 'OK' if address else 'VACIO'
            if 'direccion_geocodificada' in work.columns and not str(
                    work.at[idx, 'direccion_geocodificada'] or '').strip() and address:
                work.at[idx, 'direccion_geocodificada'] = address
        except Exception as exc:
            work.at[idx, 'direccion_inversa_estado'] = f'ERROR: {exc}'
    return work


def import_cache_from_dataframe(
        df: pd.DataFrame, city: str, region: str, provider: str) -> int:
    if df.empty:
        return 0
    lat_col, lon_col = detect_coordinate_columns(df)
    if not lat_col or not lon_col:
        raise ValueError(
            'El archivo histórico debe tener columnas de latitud y longitud.')

    address_candidates = [
        'direccion_geocodable',
        'direccion_geocodificada',
        'direccion',
        'address',
        'dirección']
    lowered = {str(col).strip().lower(): str(col) for col in df.columns}
    address_col = next(
        (lowered[name] for name in address_candidates if name in lowered),
        None)
    if not address_col:
        suggested = detect_address_columns(df)
        address_col = suggested[0] if suggested else None
    if not address_col:
        raise ValueError(
            'No se encontró una columna de dirección para importar la cache.')

    imported = 0
    resolved_provider, _ = resolve_provider(provider, '')
    for _, row in df.iterrows():
        if pd.isna(row.get(lat_col)) or pd.isna(row.get(lon_col)):
            continue
        address = str(row.get(address_col, '') or '').strip()
        if not address:
            continue
        try:
            lat = float(row.get(lat_col))
            lon = float(row.get(lon_col))
        except Exception:
            continue
        save_geocode_cache_entry(
            normalized_address=address,
            city=city,
            region=region,
            provider=resolved_provider,
            result=GeocodeResult(
                latitud=lat,
                longitud=lon,
                direccion_geocodificada=str(
                    row.get(
                        'direccion_geocodificada',
                        '') or address),
                estado_geo=str(row.get('estado_geo', '') or 'CRUCE_HISTORICO'),
                proveedor=resolved_provider,
                consulta_usada=str(row.get('consulta_usada', '') or address),
            ),
            geo_score=int(
                row['geo_score']) if 'geo_score' in row and pd.notna(
                row.get('geo_score')) else None,
            geo_confianza=str(row.get('geo_confianza', '') or ''),
        )
        imported += 1
    return imported


@app.context_processor
def inject_globals():
    user = current_user()
    return {
        'current_user': user,
        'csrf_token': generate_csrf_token(),
        'current_role_label': role_label(user.get('role', '')) if user else '',
        'current_module_permissions': user.get('module_permissions', {}) if user else {},
        'module_labels': MODULE_LABELS,
        'unread_notifications': unread_notification_count(int(user['id'])) if user else 0,
    }

@app.before_request
def check_password_change_required():
    if request.endpoint in ('static', 'auth.login', 'auth.logout', 'auth.change_password'):
        return
    user = current_user()
    if user and user.get('must_change_password'):
        flash('Por razones de seguridad, debes cambiar tu contraseña inicial antes de continuar.', 'warning')
        return redirect(url_for('auth.change_password'))










@app.route('/health')
def health_check():
    return jsonify({'status': 'ok'})


@app.route('/manifest.webmanifest')
def web_manifest():
    payload = {
        'name': 'GeoBusca Campo Rionegro',
        'short_name': 'GeoBusca Campo',
        'start_url': url_for('dashboard.dashboard'),
        'display': 'standalone',
        'background_color': '#f6f8fb',
        'theme_color': '#0d6efd',
        'description': 'Captura de visitas tributarias y gestion territorial en tablet',
    }
    response = jsonify(payload)
    response.headers['Content-Type'] = 'application/manifest+json'
    return response


@app.route('/service-worker.js')
def service_worker():
    js = """
const CACHE = 'geobusca-campo-v11';
const ASSETS = ['/', '/manifest.webmanifest'];
self.addEventListener('install', (event) => {
  event.waitUntil(caches.open(CACHE).then(cache => cache.addAll(ASSETS)).catch(() => null));
  self.skipWaiting();
});
self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});
self.addEventListener('fetch', (event) => {
  const req = event.request;
  if (req.method !== 'GET') return;
  event.respondWith(
    fetch(req).then((response) => {
      const copy = response.clone();
      caches.open(CACHE).then(cache => cache.put(req, copy)).catch(() => null);
      return response;
    }).catch(() => caches.match(req).then(r => r || caches.match('/')))
  );
});
"""
    response = make_response(js)
    response.headers['Content-Type'] = 'application/javascript; charset=utf-8'
    return response


@app.route('/notifications')
@login_required
@module_required('notifications')
def notifications_view():
    user = current_user()
    items = recent_notifications(int(user['id']), limit=100)
    return render_template('notifications.html', notifications=items)


@app.route('/notifications/<int:notification_id>/read', methods=['POST'])
@login_required
@module_required('notifications')
def notification_mark_read(notification_id: int):
    validate_csrf()
    user = current_user()
    with get_conn() as conn:
        conn.execute(
            'UPDATE notifications SET is_read=1, read_at=? WHERE id=? AND user_id=?',
            (now_iso(),
             notification_id,
             user['id']))
    return redirect(request.form.get('next') or url_for('notifications_view'))


from backend.routes.auth import auth_bp
from backend.routes.dashboard import dashboard_bp
from backend.routes.layers import layers_bp
from backend.routes.datasets import datasets_bp
from backend.routes.visits import visits_bp
from backend.routes.admin import admin_bp
from backend.routes.api import api_bp

app.register_blueprint(auth_bp)
app.register_blueprint(dashboard_bp)
app.register_blueprint(layers_bp)
app.register_blueprint(datasets_bp)
app.register_blueprint(visits_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(api_bp)

if __name__ == '__main__':
    app.run(
        debug=os.getenv('FLASK_DEBUG', '0') == '1',
        host=os.getenv('FLASK_HOST', '127.0.0.1'),
        port=int(os.getenv('FLASK_PORT', '8000')),
    )
