import json
import sqlite3
from typing import List, Dict, Optional, Tuple
from backend.database_web import get_conn, now_iso, with_db_retry
from backend.services.event_service import create_notification, log_audit
from geopy.distance import geodesic

@with_db_retry()
def ensure_visit_placeholder(dataset_id: int, row_idx: int) -> int:
    with get_conn() as conn:
        row = conn.execute(
            'SELECT id FROM visit_records WHERE dataset_id=? AND row_idx=?',
            (dataset_id, row_idx)).fetchone()
        if row:
            return int(row['id'])
        conn.execute(
            "INSERT INTO visit_records (dataset_id, row_idx, visit_date, official_name, visit_status, visit_result, observations, updated_data_json, debt_status, debt_amount, debt_reference, debt_source, created_by, created_at, updated_at, approval_status, assignment_status) VALUES (?, ?, '', '', '', '', '', '{}', '', NULL, '', '', NULL, ?, ?, 'draft', 'unassigned')",
            (dataset_id, row_idx, now_iso(), now_iso()),
        )
        new_row = conn.execute(
            'SELECT id FROM visit_records WHERE dataset_id=? AND row_idx=?',
            (dataset_id, row_idx)).fetchone()
        return int(new_row['id']) if new_row else 0

@with_db_retry()
def update_visit_workflow(dataset_id: int, row_idx: int, **updates) -> None:
    if not updates:
        return
    ensure_visit_placeholder(dataset_id, row_idx)
    allowed = {
        'approval_status', 'approval_notes', 'approval_reason_code', 'approval_reason_label', 'reviewed_by', 'reviewed_at',
        'assigned_to', 'assigned_by', 'assigned_at', 'assignment_status',
        'territory_scope', 'agenda_date', 'agenda_order', 'route_group',
        'route_optimization_method', 'route_optimized_at', 'route_optimized_by', 'route_distance_km', 'route_estimated_minutes',
        'schedule_start_time', 'schedule_end_time', 'estimated_service_minutes', 'route_vehicle', 'route_capacity',
        'completion_status', 'completion_notes', 'checkin_at', 'checkout_at', 'completed_at', 'completed_by',
        'alert_status', 'alert_message', 'last_alert_sent_at', 'evidence_required',
        'sla_status', 'sla_message', 'day_closed_at', 'day_closed_by',
        'updated_at', 'visit_status', 'official_name',
        'distance_anomaly_meters', 'has_gps_anomaly',
        'checkin_latitude', 'checkin_longitude', 'checkin_accuracy',
        'checkout_latitude', 'checkout_longitude', 'checkout_accuracy'
    }
    parts = []
    params = []
    for key, value in updates.items():
        if key not in allowed:
            continue
        parts.append(f"{key}=?")
        params.append(value)
    if not parts:
        return
    if 'updated_at' not in updates:
        parts.append('updated_at=?')
        params.append(now_iso())
    params.extend([dataset_id, row_idx])
    with get_conn() as conn:
        conn.execute(
            f"UPDATE visit_records SET {', '.join(parts)} WHERE dataset_id=? AND row_idx=?",
            tuple(params))

def get_visit_record_by_row(dataset_id: int, row_idx: int) -> Optional[dict]:
    with get_conn() as conn:
        row = conn.execute(
            'SELECT * FROM visit_records WHERE dataset_id=? AND row_idx=? ORDER BY updated_at DESC, id DESC LIMIT 1',
            (dataset_id, row_idx)).fetchone()
    if not row:
        return None
    item = dict(row)
    try:
        extra = json.loads(item.get('updated_data_json') or '{}')
    except Exception:
        extra = {}
    item.update(extra)
    item['visita_hora'] = extra.get('visita_hora', '')
    return item

def get_visit_records_for_dataset(dataset_id: int) -> list[dict]:
    with get_conn() as conn:
        rows = conn.execute(
            'SELECT * FROM visit_records WHERE dataset_id=? ORDER BY updated_at DESC, id DESC',
            (dataset_id,)).fetchall()
    records = []
    for row in rows:
        item = dict(row)
        try:
            extra = json.loads(item.get('updated_data_json') or '{}')
        except Exception:
            extra = {}
        item.update(extra)
        item['visita_hora'] = extra.get('visita_hora', '')
        item['rvt_tipo_visita'] = extra.get('rvt_tipo_visita', item.get('visit_result', ''))
        item['rvt_recibe_nombre'] = extra.get('rvt_recibe_nombre', '')
        item['rvt_codigo_establecimiento'] = extra.get('rvt_codigo_establecimiento', '')
        item['debt_label'] = item.get('debt_status', '')
        if item.get('debt_amount') not in (None, ''):
            item['debt_label'] = f"{item['debt_status']} · {item['debt_amount']}" if item.get('debt_status') else str(item['debt_amount'])
        records.append(item)
    return records

@with_db_retry()
def persist_visit_record(dataset_id: int, row_idx: int, user_id: int, payload: dict) -> int:
    approval_status = str(payload.get('approval_status') or 'submitted').strip().lower() or 'submitted'
    assignment_status = str(payload.get('assignment_status') or 'unassigned').strip().lower() or 'unassigned'
    with get_conn() as conn:
        existing = conn.execute(
            'SELECT * FROM visit_records WHERE dataset_id=? AND row_idx=?',
            (dataset_id, row_idx)).fetchone()
        
        # Detección de Conflictos (Last Write Wins con validación)
        client_ts = payload.get('_client_updated_at')
        if existing and client_ts:
            server_ts = existing['updated_at']
            if client_ts < server_ts:
                # El cliente tiene datos viejos, registrar conflicto
                conn.execute(
                    '''
                    INSERT INTO visit_conflicts (dataset_id, row_idx, user_id, client_version, server_version, client_payload_json, server_payload_json, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''',
                    (dataset_id, row_idx, user_id, client_ts, server_ts, json.dumps(payload), json.dumps(dict(existing)), now_iso())
                )
                # Opcional: Notificar al revisor
                create_notification(existing['created_by'] or user_id, 'warning', 'Conflicto de Sincronización', f'Se detectó un conflicto en el registro {row_idx} del dataset {dataset_id}.')
                # Por ahora, permitimos que el servidor mantenga la versión más reciente (no sobreescribimos)
                return int(existing['id'])

        existing_item = dict(existing) if existing else {}
        conn.execute(
            '''
            INSERT INTO visit_records (dataset_id, row_idx, visit_date, official_name, visit_status, visit_result, observations, updated_data_json, debt_status, debt_amount, debt_reference, debt_source, created_by, created_at, updated_at, approval_status, approval_notes, approval_reason_code, approval_reason_label, reviewed_by, reviewed_at, assigned_to, assigned_by, assigned_at, assignment_status, territory_scope, agenda_date, agenda_order, route_group, route_optimization_method, route_optimized_at, route_optimized_by, route_distance_km, route_estimated_minutes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(dataset_id, row_idx) DO UPDATE SET
                visit_date=excluded.visit_date,
                official_name=excluded.official_name,
                visit_status=excluded.visit_status,
                visit_result=excluded.visit_result,
                observations=excluded.observations,
                updated_data_json=excluded.updated_data_json,
                debt_status=excluded.debt_status,
                debt_amount=excluded.debt_amount,
                debt_reference=excluded.debt_reference,
                debt_source=excluded.debt_source,
                updated_at=excluded.updated_at,
                approval_status=CASE WHEN visit_records.approval_status IN ('approved','rejected') THEN visit_records.approval_status ELSE excluded.approval_status END,
                approval_notes=COALESCE(excluded.approval_notes, visit_records.approval_notes),
                approval_reason_code=COALESCE(excluded.approval_reason_code, visit_records.approval_reason_code),
                approval_reason_label=COALESCE(excluded.approval_reason_label, visit_records.approval_reason_label),
                assignment_status=CASE WHEN excluded.assignment_status='unassigned' THEN visit_records.assignment_status ELSE excluded.assignment_status END,
                route_optimization_method=COALESCE(excluded.route_optimization_method, visit_records.route_optimization_method),
                route_optimized_at=COALESCE(excluded.route_optimized_at, visit_records.route_optimized_at),
                route_optimized_by=COALESCE(excluded.route_optimized_by, visit_records.route_optimized_by),
                route_distance_km=COALESCE(excluded.route_distance_km, visit_records.route_distance_km),
                route_estimated_minutes=COALESCE(excluded.route_estimated_minutes, visit_records.route_estimated_minutes)
            ''',
            (
                dataset_id, row_idx, payload.get('visita_fecha', ''), payload.get('visita_funcionario', ''),
                payload.get('visita_estado', ''), payload.get('visita_resultado', ''), payload.get('visita_observaciones', ''),
                json.dumps({**payload}, ensure_ascii=False), payload.get('deuda_estado', ''),
                float(payload.get('deuda_monto')) if str(payload.get('deuda_monto', '')).strip() else None,
                payload.get('deuda_referencia', ''), payload.get('deuda_fuente', ''), user_id, now_iso(), now_iso(),
                approval_status, payload.get('approval_notes', existing_item.get('approval_notes', '')),
                payload.get('approval_reason_code', existing_item.get('approval_reason_code', '')),
                payload.get('approval_reason_label', existing_item.get('approval_reason_label', '')),
                payload.get('reviewed_by', existing_item.get('reviewed_by')), payload.get('reviewed_at', existing_item.get('reviewed_at')),
                payload.get('assigned_to', existing_item.get('assigned_to')), payload.get('assigned_by', existing_item.get('assigned_by')),
                payload.get('assigned_at', existing_item.get('assigned_at')), assignment_status,
                payload.get('territory_scope', existing_item.get('territory_scope', '')),
                payload.get('agenda_date', existing_item.get('agenda_date', '')),
                payload.get('agenda_order', existing_item.get('agenda_order')),
                payload.get('route_group', existing_item.get('route_group', '')),
                payload.get('route_optimization_method', existing_item.get('route_optimization_method', '')),
                payload.get('route_optimized_at', existing_item.get('route_optimized_at')),
                payload.get('route_optimized_by', existing_item.get('route_optimized_by')),
                payload.get('route_distance_km', existing_item.get('route_distance_km')),
                payload.get('route_estimated_minutes', existing_item.get('route_estimated_minutes')),
            ),
        )
        conn.execute(
            'INSERT INTO debt_snapshots (dataset_id, row_idx, identifier_value, debt_status, debt_amount, debt_reference, source, checked_by, checked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (dataset_id, row_idx, str(row_idx), payload.get('deuda_estado', ''), 
             float(payload.get('deuda_monto')) if str(payload.get('deuda_monto', '')).strip() else None, 
             payload.get('deuda_referencia', ''), payload.get('deuda_fuente', ''), user_id, now_iso()),
        )
        row = conn.execute('SELECT id FROM visit_records WHERE dataset_id=? AND row_idx=?', (dataset_id, row_idx)).fetchone()
        
        # Calcular anomalía de GPS si hay Check-in y coordenadas de destino
        _recalculate_gps_anomaly(dataset_id, row_idx)
        
        return int(row['id']) if row else 0

def _recalculate_gps_anomaly(dataset_id: int, row_idx: int):
    """Checks if the check-in distance from target is an anomaly."""
    with get_conn() as conn:
        record = conn.execute('SELECT * FROM visit_records WHERE dataset_id=? AND row_idx=?', (dataset_id, row_idx)).fetchone()
        if not record or not record['checkin_latitude'] or not record['checkin_longitude']:
            return
            
        # Get target coords from dataset (cached or reloaded)
        # For simplicity, we search in updated_data_json first
        try:
            data = json.loads(record['updated_data_json'] or '{}')
        except:
            data = {}
            
        target_lat = data.get('latitud')
        target_lon = data.get('longitud')
        
        if target_lat and target_lon:
            try:
                dist = geodesic((record['checkin_latitude'], record['checkin_longitude']), (target_lat, target_lon)).meters
                has_anomaly = 1 if dist > 300 else 0
                conn.execute(
                    'UPDATE visit_records SET distance_anomaly_meters = ?, has_gps_anomaly = ? WHERE id = ?',
                    (round(dist, 2), has_anomaly, record['id'])
                )
            except:
                pass

def extract_row_coordinates(row_data: dict) -> tuple[Optional[float], Optional[float]]:
    """Extracts latitude and longitude from a data row using fuzzy matching."""
    lat_candidates = ['latitud', 'latitude', 'lat', 'y', 'visit_latitude']
    lon_candidates = ['longitud', 'longitude', 'lng', 'lon', 'x', 'visit_longitude']
    
    lowered = {str(k).lower(): v for k, v in row_data.items()}
    lat = next((lowered[c] for c in lat_candidates if c in lowered), None)
    lon = next((lowered[c] for c in lon_candidates if c in lowered), None)
    
    try:
        if lat not in (None, '') and lon not in (None, ''):
            return float(lat), float(lon)
    except (ValueError, TypeError):
        pass
    return None, None

def get_last_position(item: dict) -> Optional[dict]:
    """Determines the best available position and timestamp for a visit item."""
    candidates = [
        ('checkout', item.get('checkout_latitude'), item.get('checkout_longitude'), item.get('checkout_at')),
        ('checkin', item.get('checkin_latitude'), item.get('checkin_longitude'), item.get('checkin_at')),
        ('visit', item.get('visit_latitude'), item.get('visit_longitude'), item.get('updated_at')),
    ]
    for source, lat, lon, ts in candidates:
        try:
            if lat not in (None, '') and lon not in (None, ''):
                return {
                    'latitude': float(lat),
                    'longitude': float(lon),
                    'timestamp': str(ts or ''),
                    'source': source
                }
        except (ValueError, TypeError):
            continue
    return None
