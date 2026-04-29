import json
from backend.database_web import get_conn, now_iso, with_db_retry
from backend.notifications import send_email

@with_db_retry()
def check_and_notify_sla_breaches():
    """
    Checks for visits that have breached SLA and notifies assigned supervisors.
    """
    query = '''
        SELECT vr.*, u.full_name as staff_name, u.email as staff_email, 
               ru.full_name as supervisor_name, ru.email as supervisor_email
        FROM visit_records vr
        JOIN users u ON u.id = vr.assigned_to
        JOIN users ru ON ru.id = vr.assigned_by
        WHERE vr.sla_status = 'breached' 
          AND vr.alert_status = 'late'
          AND vr.completion_status IN ('pending', 'in_progress')
          AND (vr.last_alert_sent_at IS NULL OR datetime(vr.last_alert_sent_at) < datetime('now', '-4 hours'))
    '''
    
    with get_conn() as conn:
        rows = conn.execute(query).fetchall()
        
    notifications_sent = 0
    for row in rows:
        visit = dict(row)
        
        # Notify Supervisor
        if visit['supervisor_email']:
            subject = f"⚠️ ALERTA SLA: Visita Retrasada - {visit['staff_name']}"
            body = f"""
            Hola {visit['supervisor_name']},
            
            Se ha detectado un incumplimiento de SLA crítico para el funcionario {visit['staff_name']}.
            
            Detalles de la Visita:
            - Dataset ID: {visit['dataset_id']}
            - Fila ID: {visit['row_idx']}
            - Fecha Agenda: {visit['agenda_date']}
            - Territorio: {visit['territorio_scope'] or 'No especificado'}
            
            Por favor, revisa el tablero de supervisión para tomar acciones.
            
            ---
            GeoBusca Territorial Rionegro
            """
            
            try:
                # Assuming send_email is available and configured
                send_email(visit['supervisor_email'], subject, body)
                
                # Update last alert sent
                with get_conn() as conn:
                    conn.execute(
                        'UPDATE visit_records SET last_alert_sent_at = ? WHERE dataset_id = ? AND row_idx = ?',
                        (now_iso(), visit['dataset_id'], visit['row_idx'])
                    )
                notifications_sent += 1
            except Exception as e:
                print(f"Error sending SLA alert: {e}")
                
    return notifications_sent
