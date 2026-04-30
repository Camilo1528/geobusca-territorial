import json
import pandas as pd
from typing import List, Dict, Optional, Tuple
from collections import Counter
from backend.database_web import get_conn, now_iso, with_db_retry
from backend.services.visit_service import get_visit_record_by_row, update_visit_workflow
from backend.services.dataset_service import load_any_dataset_version
from backend.institutional import ensure_visit_columns
from backend.services.routing_service import build_route_plan

def get_approval_rows(status: str = '') -> list[dict]:
    """Retrieves visits waiting for approval or in other workflow states."""
    query = (
        'SELECT vr.*, d.original_filename, d.user_id AS dataset_owner_id, '
        'cu.full_name AS created_by_name, au.full_name AS assigned_to_name, ru.full_name AS reviewed_by_name '
        'FROM visit_records vr '
        'LEFT JOIN datasets d ON d.id = vr.dataset_id '
        'LEFT JOIN users cu ON cu.id = vr.created_by '
        'LEFT JOIN users au ON au.id = vr.assigned_to '
        'LEFT JOIN users ru ON ru.id = vr.reviewed_by '
    )
    params = []
    if status:
        query += 'WHERE vr.approval_status=? '
        params.append(status)
    query += "ORDER BY CASE vr.approval_status WHEN 'submitted' THEN 0 WHEN 'returned' THEN 1 WHEN 'under_review' THEN 2 ELSE 3 END, vr.updated_at DESC, vr.id DESC"
    
    with get_conn() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(r) for r in rows]

def get_territorial_indicators() -> dict:
    """Calculates visit and workflow statistics broken down by territory."""
    with get_conn() as conn:
        datasets = [dict(r) for r in conn.execute(
            'SELECT * FROM datasets ORDER BY created_at DESC, id DESC').fetchall()]
            
    breakdown = {k: Counter() for k in ['comuna', 'barrio', 'vereda', 'corregimiento']}
    workflow = {k: Counter() for k in ['comuna', 'barrio', 'vereda', 'corregimiento']}
    
    for ds in datasets:
        try:
            df = ensure_visit_columns(load_any_dataset_version(int(ds['id']), int(ds['user_id'])))
        except Exception:
            continue
            
        for territory in breakdown:
            if territory not in df.columns:
                continue
            names = df[territory].fillna('').astype(str).str.strip()
            for idx, name in names.items():
                if not name:
                    continue
                breakdown[territory][name] += 1
                visit = get_visit_record_by_row(int(ds['id']), int(idx)) or {}
                status = str(visit.get('approval_status') or '').strip().lower()
                if status in {'submitted', 'under_review'}:
                    workflow[territory][name] += 1
                    
    result = {}
    for territory in breakdown:
        rows = []
        for name, total in breakdown[territory].most_common(15):
            rows.append({
                'territorio': name, 
                'total': total,
                'pendientes_revision': workflow[territory].get(name, 0)
            })
        result[territory] = rows
    return result

@with_db_retry()
def bulk_reassign_visits(dataset_row_pairs: list[tuple[int, int]], assigned_to: int, actor_user: dict,
                        agenda_date: str = '', route_group: str = '', optimize_route: bool = False) -> tuple[int, float, Optional[int]]:
    """Assigns multiple visits to a user, optionally optimizing the route."""
    selected_items = []
    for dataset_id, row_idx in dataset_row_pairs:
        # Here we'd normally build the context for route optimization
        # For brevity, we just collect IDs. In a real scenario, we'd load coordinates.
        selected_items.append({'dataset_id': dataset_id, 'row_idx': row_idx})
        
    plan = build_route_plan(selected_items) if optimize_route and selected_items else {
        'ordered': selected_items,
        'total_km': 0.0,
        'total_minutes': None,
        'provider': None,
        'method': None
    }
    
    for order, item in enumerate(plan['ordered'], start=1):
        update_visit_workflow(
            int(item['dataset_id']), int(item['row_idx']),
            assigned_to=assigned_to,
            assigned_by=actor_user['id'],
            assigned_at=now_iso(),
            assignment_status='scheduled' if agenda_date else 'assigned',
            agenda_date=agenda_date,
            agenda_order=order,
            route_group=route_group,
            route_optimization_method=plan.get('method'),
            route_optimized_at=now_iso() if optimize_route else None,
            route_optimized_by=actor_user['id'] if optimize_route else None,
            route_distance_km=plan.get('total_km') if optimize_route else None,
            route_estimated_minutes=plan.get('total_minutes') if optimize_route else None,
        )
        
    return len(selected_items), float(plan.get('total_km') or 0.0), plan.get('total_minutes')

def get_manager_metrics(start_date: Optional[str] = None, end_date: Optional[str] = None) -> dict:
    """
    Calculates high-level metrics for managers within an optional date range.
    """
    if not start_date:
        start_date = now_iso()[:10]
    if not end_date:
        end_date = start_date
        
    date_clause = 'agenda_date BETWEEN ? AND ?'
    params = (start_date, end_date)
        
    with get_conn() as conn:
        # 1. SLA Status counts
        sla_stats = conn.execute(
            f'SELECT sla_status, COUNT(*) as count FROM visit_records WHERE {date_clause} GROUP BY sla_status',
            params
        ).fetchall()
        
        # 2. Visit Status counts
        visit_stats = conn.execute(
            f'SELECT completion_status, COUNT(*) as count FROM visit_records WHERE {date_clause} GROUP BY completion_status',
            params
        ).fetchall()
        
        # 3. Productivity by official
        staff_perf = conn.execute(
            f'''SELECT u.full_name, 
                       COUNT(*) as assigned,
                       SUM(CASE WHEN vr.completion_status="completed" THEN 1 ELSE 0 END) as completed,
                       SUM(CASE WHEN vr.sla_status="breached" THEN 1 ELSE 0 END) as breached
                FROM visit_records vr
                JOIN users u ON u.id = vr.assigned_to
                WHERE vr.{date_clause}
                GROUP BY vr.assigned_to''',
            params
        ).fetchall()

        # 4. Global counts
        user_counts = conn.execute('SELECT role, COUNT(*) as count FROM users GROUP BY role').fetchall()
        dataset_count = conn.execute('SELECT COUNT(*) FROM datasets').fetchone()[0]
        visit_count = conn.execute('SELECT COUNT(*) FROM visit_records').fetchone()[0]
        conflict_pending = conn.execute('SELECT COUNT(*) FROM visit_conflicts WHERE resolution_status="pending"').fetchone()[0]
        sync_errors = conn.execute('SELECT COUNT(*) FROM sync_events WHERE status="error"').fetchone()[0]

    roles = {r['role']: r['count'] for r in user_counts}
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'sla': {r['sla_status'] or 'planned': r['count'] for r in sla_stats},
        'completion': {r['completion_status'] or 'pending': r['count'] for r in visit_stats},
        'staff': [dict(r) for r in staff_perf],
        'usuarios': sum(roles.values()),
        'funcionarios': roles.get('funcionario', 0),
        'revisores': roles.get('revisor', 0),
        'admins': roles.get('admin', 0),
        'datasets': dataset_count,
        'visitas': visit_count,
        'conflictos_pendientes': conflict_pending,
        'errores_sync': sync_errors
    }

def get_queue_rows_for_assignment(dataset_id: int, user_id: int, territory_type: str = '', territory_value: str = '', approval_filter: str = '') -> list[dict]:
    """
    Returns rows from a dataset that are candidates for assignment,
    optionally filtered by territory and approval status.
    """
    df = ensure_visit_columns(load_any_dataset_version(dataset_id, user_id))
    df['row_idx'] = range(len(df))
    
    # 1. Apply territory filters
    if territory_type and territory_value:
        if territory_type in df.columns:
            mask = df[territory_type].fillna('').astype(str).str.strip().str.upper() == str(territory_value).strip().upper()
            df = df[mask]
    
    # 2. Get existing visit records for this dataset
    with get_conn() as conn:
        rows = conn.execute('SELECT * FROM visit_records WHERE dataset_id=?', (dataset_id,)).fetchall()
    visit_map = {int(r['row_idx']): dict(r) for r in rows}
    
    out = []
    for _, row in df.iterrows():
        idx = int(row['row_idx'])
        visit = visit_map.get(idx, {})
        
        # 3. Filter by approval status
        if approval_filter and approval_filter != 'all':
            if str(visit.get('approval_status') or 'draft').lower() != approval_filter.lower():
                continue
        
        # 4. Skip already assigned
        if visit.get('assigned_to'):
            continue
            
        item = {str(k): v for k, v in row.to_dict().items()}
        item['approval_status'] = visit.get('approval_status') or 'draft'
        item['assignment_status'] = visit.get('assignment_status') or 'unassigned'
        out.append(item)
        
    return out
