import json
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from flask import url_for
from backend.database_web import get_conn, now_iso, with_db_retry
from backend.services.event_service import create_notification, log_audit
from backend.services.visit_service import update_visit_workflow, get_visit_record_by_row, get_last_position
from backend.services.dataset_service import load_any_dataset_version
from backend.institutional import ensure_visit_columns
from backend.services.routing_service import build_route_plan
from backend.config import DEFAULT_CITY, DEFAULT_REGION, ALERT_GRACE_MINUTES, ALERT_REPEAT_MINUTES

def get_agenda_alert_state(item: dict, now_dt: Optional[datetime] = None) -> dict:
    """Calculates if an agenda item is delayed."""
    now_dt = now_dt or datetime.now()
    agenda_date = str(item.get('agenda_date') or '').strip()
    start_time = str(item.get('schedule_start_time') or '').strip()
    completion = str(item.get('completion_status') or 'pending').strip().lower()
    
    if not agenda_date or not start_time:
        return {'status': 'ok', 'message': ''}
    
    try:
        start_dt = datetime.fromisoformat(f"{agenda_date}T{start_time}:00")
    except Exception:
        return {'status': 'ok', 'message': ''}
        
    if completion in {'completed', 'no_effective', 'cancelled'}:
        return {'status': 'ok', 'message': ''}
        
    grace = timedelta(minutes=ALERT_GRACE_MINUTES)
    if now_dt <= start_dt + grace:
        return {'status': 'ok', 'message': ''}
        
    mins = int((now_dt - start_dt).total_seconds() // 60)
    if completion in {'in_progress', 'started'}:
        return {'status': 'ok', 'message': ''} # Already started
        
    return {'status': 'late', 'message': f'Visita pendiente con atraso de {mins} min'}

def get_agenda_sla_state(item: dict, now_dt: Optional[datetime] = None) -> dict:
    """Calculates SLA compliance for an agenda item."""
    now_dt = now_dt or datetime.now()
    agenda_date = str(item.get('agenda_date') or '').strip()
    start_time = str(item.get('schedule_start_time') or '').strip()
    end_time = str(item.get('schedule_end_time') or '').strip()
    completion = str(item.get('completion_status') or 'pending').strip().lower()
    
    if not agenda_date or not start_time or not end_time:
        return {'status': 'no_schedule', 'message': ''}
        
    try:
        start_dt = datetime.fromisoformat(f"{agenda_date}T{start_time}:00")
        end_dt = datetime.fromisoformat(f"{agenda_date}T{end_time}:00")
    except Exception:
        return {'status': 'no_schedule', 'message': ''}
        
    grace = timedelta(minutes=ALERT_GRACE_MINUTES)
    if completion in {'completed', 'no_effective', 'cancelled'}:
        raw = str(item.get('checkout_at') or item.get('completed_at') or '').strip()
        try:
            actual = datetime.fromisoformat(raw) if raw else end_dt
        except Exception:
            actual = end_dt
        if actual <= end_dt + grace:
            return {'status': 'met', 'message': 'Cumplida dentro del SLA'}
        mins = int((actual - end_dt).total_seconds() // 60)
        return {'status': 'breached', 'message': f'SLA vencido por {mins} min'}
        
    if completion in {'in_progress', 'started'}:
        if now_dt <= end_dt + grace:
            return {'status': 'active', 'message': 'En curso dentro del SLA'}
        mins = int((now_dt - end_dt).total_seconds() // 60)
        return {'status': 'breached', 'message': f'En curso fuera del SLA por {mins} min'}
        
    if now_dt < start_dt:
        return {'status': 'planned', 'message': 'Aún no inicia la ventana'}
    if now_dt <= end_dt + grace:
        mins = int((end_dt - now_dt).total_seconds() // 60)
        return {'status': 'active', 'message': f'Faltan {mins} min para vencimiento'}
    
    mins = int((now_dt - end_dt).total_seconds() // 60)
    return {'status': 'breached', 'message': f'Pendiente fuera del SLA por {mins} min'}

@with_db_retry()
def emit_agenda_alerts(items: list[dict]) -> None:
    """Checks for delays and sends notifications."""
    now_dt = datetime.now()
    for item in items:
        state = get_agenda_alert_state(item, now_dt)
        dataset_id = int(item.get('dataset_id') or 0)
        row_idx = int(item.get('row_idx') or 0)
        assigned_to = int(item.get('assigned_to') or 0)
        
        if not dataset_id or not row_idx:
            continue
            
        update_visit_workflow(
            dataset_id, row_idx, 
            alert_status=state['status'], 
            alert_message=state['message']
        )
        
        if state['status'] != 'late' or not assigned_to:
            continue
            
        last_sent = str(item.get('alert_last_sent_at') or '').strip()
        should_send = True
        if last_sent:
            try:
                last_dt = datetime.fromisoformat(last_sent)
                should_send = (now_dt - last_dt) >= timedelta(minutes=ALERT_REPEAT_MINUTES)
            except Exception:
                should_send = True
                
        if should_send:
            create_notification(
                assigned_to, 'agenda_alert', 'Atraso en agenda', state['message'],
                url_for('admin.daily_agenda', user_id=assigned_to, agenda_date=item.get('agenda_date') or time.strftime('%Y-%m-%d'))
            )
            update_visit_workflow(dataset_id, row_idx, alert_last_sent_at=now_iso())

@with_db_retry()
def create_day_close_summary(target_user: dict, target_date: str, items: list[dict], actor_user: dict) -> dict:
    """Generates a summary of a day's activity and closes it."""
    metrics = {
        'total': len(items),
        'completed': sum(1 for item in items if str(item.get('completion_status') or '').lower() == 'completed'),
        'no_effective': sum(1 for item in items if str(item.get('completion_status') or '').lower() in {'no_effective', 'cancelled'}),
        'in_progress': sum(1 for item in items if str(item.get('completion_status') or '').lower() in {'in_progress', 'started'}),
        'pending': sum(1 for item in items if str(item.get('completion_status') or 'pending').lower() in {'pending', ''}),
        'alerts': sum(1 for item in items if str(item.get('alert_status') or '') == 'late'),
        'sla_met': sum(1 for item in items if str(item.get('sla_status') or '') == 'met'),
        'sla_breached': sum(1 for item in items if str(item.get('sla_status') or '') == 'breached'),
    }
    summary = {
        'date': target_date,
        'user_id': int(target_user.get('id') or 0),
        'user_name': target_user.get('full_name') or target_user.get('email') or 'Funcionario',
        'closed_by': actor_user.get('full_name') or actor_user.get('email') or 'Sistema',
        'closed_at': now_iso(),
        'metrics': metrics,
    }
    
    target_id = int(target_user.get('id') or actor_user['id'])
    create_notification(
        target_id, 'day_close', 'Resumen de cierre de jornada',
        f"{summary['user_name']}: {metrics['completed']} completadas, {metrics['no_effective']} no efectivas, {metrics['pending']} pendientes, SLA vencido {metrics['sla_breached']}.",
        url_for('admin.daily_agenda', user_id=target_id, agenda_date=target_date)
    )
    
    log_audit(actor_user['id'], 'close_day_summary', 'agenda', f"{summary['user_id']}:{target_date}", details=summary)
    return summary

def get_agenda_rows_for_user(user_id: int, target_date: str = '', actor_user: dict = None) -> list[dict]:
    """Retrieves all agenda items for a user on a given date."""
    target_date = str(target_date or '').strip()
    query = (
        'SELECT vr.*, d.original_filename, d.city, d.region, d.user_id AS dataset_owner_id '
        'FROM visit_records vr '
        'LEFT JOIN datasets d ON d.id = vr.dataset_id '
        'WHERE vr.assigned_to=? '
    )
    params = [user_id]
    if target_date:
        query += 'AND COALESCE(vr.agenda_date, "")=? '
        params.append(target_date)
    query += 'ORDER BY COALESCE(vr.agenda_date, "9999-12-31"), COALESCE(vr.route_group, ""), COALESCE(vr.agenda_order, 999999), vr.dataset_id, vr.row_idx'
    
    with get_conn() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        
    items = []
    for row in rows:
        item = dict(row)
        try:
            owner_id = int(item.get('dataset_owner_id') or user_id)
            row_df = ensure_visit_columns(load_any_dataset_version(int(item['dataset_id']), owner_id))
            if 0 <= int(item['row_idx']) < len(row_df):
                row_data = row_df.iloc[int(item['row_idx'])].to_dict()
            else:
                row_data = {}
        except Exception:
            row_data = {}
            
        visit = get_visit_record_by_row(int(item['dataset_id']), int(item['row_idx'])) or {}
        
        # Build context (simplified version of _visit_context_from_row)
        context = {str(k): str(v) if v is not None else '' for k, v in row_data.items()}
        context['row_idx'] = int(item['row_idx'])
        context['dataset_id'] = item['dataset_id']
        context['dataset_name'] = item.get('original_filename', '')
        
        # Overlay visit record fields
        for key, value in item.items():
            if value not in (None, ''):
                context[key] = str(value)
        
        items.append(context)
    return items

def get_agenda_route_payload(items: list[dict]) -> dict:
    """Builds a GeoJSON payload for a route based on agenda items."""
    plan = build_route_plan(items)
    features = []
    
    for pos, item in enumerate(plan['ordered'], start=1):
        # We need coordinates. In a real scenario, we'd extract them from item or row_data
        lat = item.get('visit_latitude') or item.get('latitud')
        lon = item.get('visit_longitude') or item.get('longitud')
        
        try:
            if lat and lon:
                features.append({
                    'type': 'Feature',
                    'geometry': {'type': 'Point', 'coordinates': [float(lon), float(lat)]},
                    'properties': {
                        'order': pos,
                        'dataset_id': item.get('dataset_id'),
                        'row_idx': item.get('row_idx'),
                        'name': item.get('rvt_razon_social') or item.get('nom_establec') or 'Sin nombre',
                        'address': item.get('rvt_direccion_establecimiento') or item.get('direccion') or '',
                        'territory': item.get('territorio_principal_nombre') or item.get('barrio') or '',
                    }
                })
        except (ValueError, TypeError):
            continue
            
    return {
        'ok': True,
        'provider': plan.get('provider'),
        'method': plan.get('method'),
        'total_km': plan.get('total_km'),
        'total_minutes': plan.get('total_minutes'),
        'missing_count': plan.get('missing_count'),
        'route_geometry': plan.get('geometry'),
        'stops_geojson': {'type': 'FeatureCollection', 'features': features},
    }
