from flask import Blueprint, request, flash, redirect, url_for, render_template, jsonify, abort, make_response, send_file
import os
import json
import secrets
import time
from io import BytesIO
from pathlib import Path
from collections import Counter
from werkzeug.utils import secure_filename
from backend.database_web import get_conn, UPLOAD_DIR, DB_PATH
from backend.config import DEFAULT_CITY, DEFAULT_REGION

import backend.app as app_main
import backend.services.user_service as user_service
import backend.services.admin_service as admin_service
import backend.services.agenda_service as agenda_service
import backend.services.visit_service as visit_service
import backend.services.dataset_service as dataset_service
import backend.services.event_service as event_service
import backend.services.api_service as api_service
import backend.services.alert_service as alert_service

admin_bp = Blueprint("admin", __name__)

login_required = app_main.login_required
module_required = app_main.module_required
role_required = app_main.role_required
current_user = app_main.current_user
validate_csrf = app_main.validate_csrf
now_iso = app_main.now_iso
log_audit = event_service.log_audit
create_notification = event_service.create_notification
role_label = app_main.role_label

from backend.config import ROLE_LABELS, MODULE_LABELS, EVIDENCE_REQUIRED_CAUSES, APPROVAL_REASON_OPTIONS, COMPLETION_REASON_OPTIONS, TERRITORIAL_LAYER_TYPES

@admin_bp.route('/admin/users', methods=['GET', 'POST'])
@login_required
@role_required('admin')
@module_required('admin_users')
def admin_users():
    user = current_user()
    if request.method == 'POST':
        validate_csrf()
        target_id = int(request.form.get('user_id', '0') or 0)
        action = str(request.form.get('action', 'role')).strip().lower() or 'role'
                
        if action == 'create':
            email = request.form.get('email', '').strip().lower()
            full_name = request.form.get('full_name', '').strip()
            role = str(request.form.get('role', 'funcionario')).strip().lower()
            if not email or not full_name:
                flash('Email y nombre son requeridos.', 'danger')
                return redirect(url_for('admin.admin_users'))
            
            try:
                user_service.create_user(email, full_name, role, user['id'])
                flash(f'Usuario {email} creado. Contraseña temporal: temporal123', 'success')
            except ValueError as e:
                flash(str(e), 'danger')
            return redirect(url_for('admin.admin_users'))
            
        if action == 'permissions':
            submitted = {key[7:]: True for key in request.form.keys() if key.startswith('perm___')}
            user_service.update_user_permissions(target_id, submitted, user['id'])
            flash('Permisos por módulo actualizados.', 'success')
            return redirect(url_for('admin.admin_users'))

        new_role = str(request.form.get('role', 'funcionario')).strip().lower()
        if new_role not in ROLE_LABELS:
            flash('Rol inválido.', 'warning')
            return redirect(url_for('admin.admin_users'))
            
        user_service.update_user_role(target_id, new_role, user['id'])
        flash('Rol actualizado correctamente.', 'success')
        return redirect(url_for('admin.admin_users'))
        
    users = user_service.get_all_users()
    for item in users:
        item['module_permissions'] = user_service.get_user_module_permissions(int(item['id']), item.get('role', 'funcionario'))
        
    return render_template('admin_users.html', users=users, role_labels=ROLE_LABELS, module_labels=MODULE_LABELS)


@admin_bp.route('/admin/sync-panel')
@login_required
@role_required('revisor')
@module_required('sync_panel')
def sync_panel():
    selected_user = request.args.get('user_id', '').strip()
    params = []
    extra_sql = ''
    if selected_user.isdigit():
        extra_sql = ' WHERE se.user_id=? '
        params.append(int(selected_user))
    query = (
        'SELECT se.*, u.full_name, u.email, u.role '
        'FROM sync_events se '
        'LEFT JOIN users u ON u.id = se.user_id '
        f'{extra_sql}'
        'ORDER BY se.created_at DESC, se.id DESC LIMIT 400'
    )
    with get_conn() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        user_rows = conn.execute(
            'SELECT id, full_name, email, role FROM users ORDER BY full_name, email').fetchall()
    events = [dict(r) for r in rows]
    grouped = {}
    for item in events:
        key = item.get('full_name') or item.get('email') or 'Sin usuario'
        bucket = grouped.setdefault(key,
                                    {'label': key,
                                     'user_id': item.get('user_id'),
                                        'role': role_label(item.get('role',
                                                                    '')),
                                        'total': 0,
                                        'synced': 0,
                                        'conflict': 0,
                                        'error': 0,
                                        'queued': 0,
                                        'syncing': 0,
                                        'items': []})
        bucket['total'] += 1
        bucket[item['status']] = bucket.get(item['status'], 0) + 1
        bucket['items'].append(item)
    return render_template('sync_panel.html', grouped=list(grouped.values(
    )), events=events, users=[dict(r) for r in user_rows], selected_user=selected_user)


@admin_bp.route('/admin/api-keys', methods=['GET', 'POST'])
@login_required
@role_required('admin')
@module_required('admin_users')
def admin_api_keys():
    user = current_user()
    if request.method == 'POST':
        validate_csrf()
        action = request.form.get('action')
        
        if action == 'create':
            key_name = request.form.get('key_name', '').strip()
            if not key_name:
                flash('El nombre de la llave es requerido.', 'danger')
            else:
                new_key = api_service.create_api_key(user['id'], key_name)
                flash(f'Nueva llave API creada: {new_key}. ¡Guárdala ahora, no se volverá a mostrar completa!', 'success')
                log_audit(user['id'], 'create_api_key', 'api_key', key_name)
        
        elif action == 'revoke':
            key_id = request.form.get('key_id')
            with get_conn() as conn:
                conn.execute('UPDATE api_keys SET is_active = 0 WHERE id = ?', (key_id,))
            flash('Llave API revocada.', 'warning')
            log_audit(user['id'], 'revoke_api_key', 'api_key', key_id)
            
        return redirect(url_for('admin.admin_api_keys'))

    with get_conn() as conn:
        rows = conn.execute(
            '''
            SELECT ak.*, u.full_name as creator_name 
            FROM api_keys ak
            JOIN users u ON ak.user_id = u.id
            ORDER BY ak.created_at DESC
            '''
        ).fetchall()
    keys = [dict(r) for r in rows]
    return render_template('admin_api_keys.html', keys=keys)


@admin_bp.route('/admin/conflicts')
@login_required
@role_required('revisor')
@module_required('conflicts')
def conflict_log():
    query = (
        'SELECT vc.*, u.full_name AS actor_name, ru.full_name AS resolved_name '
        'FROM visit_conflicts vc '
        'LEFT JOIN users u ON u.id = vc.user_id '
        'LEFT JOIN users ru ON ru.id = vc.resolved_by '
        'ORDER BY vc.created_at DESC, vc.id DESC LIMIT 300'
    )
    with get_conn() as conn:
        rows = conn.execute(query).fetchall()
    conflicts = [dict(r) for r in rows]
    return render_template('conflict_log.html', conflicts=conflicts)


@admin_bp.route('/admin/audit')
@login_required
@role_required('admin')
@module_required('admin_users')
def audit_log_view():
    user_id = request.args.get('user_id')
    dataset_id = request.args.get('dataset_id')
    
    query = (
        'SELECT al.*, u.full_name, u.email '
        'FROM audit_log al '
        'LEFT JOIN users u ON u.id = al.user_id '
    )
    where_parts = []
    params = []
    
    if user_id:
        where_parts.append('al.user_id = ?')
        params.append(user_id)
    if dataset_id:
        where_parts.append('al.dataset_id = ?')
        params.append(dataset_id)
        
    if where_parts:
        query += ' WHERE ' + ' AND '.join(where_parts)
        
    query += ' ORDER BY al.created_at DESC LIMIT 500'
    
    with get_conn() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
        users = conn.execute('SELECT id, full_name, email FROM users ORDER BY full_name').fetchall()
        datasets = conn.execute('SELECT id, original_filename FROM datasets ORDER BY created_at DESC').fetchall()
        
    logs = []
    for r in rows:
        item = dict(r)
        try:
            item['details'] = json.loads(item['details_json'] or '{}')
        except:
            item['details'] = {}
        logs.append(item)
        
    return render_template('admin_audit.html', 
                           logs=logs, 
                           users=[dict(u) for u in users], 
                           datasets=[dict(d) for d in datasets],
                           selected_user=user_id,
                           selected_dataset=dataset_id)


@admin_bp.route('/admin/quality')
@login_required
@role_required('revisor')
@module_required('manager_dashboard')
def operational_quality():
    with get_conn() as conn:
        rows = conn.execute(
            '''
            SELECT vr.*, u.full_name as official_name, d.original_filename
            FROM visit_records vr
            JOIN users u ON u.id = vr.assigned_to
            JOIN datasets d ON d.id = vr.dataset_id
            WHERE vr.has_gps_anomaly = 1
            ORDER BY vr.distance_anomaly_meters DESC
            '''
        ).fetchall()
    anomalies = [dict(r) for r in rows]
    return render_template('admin_quality.html', anomalies=anomalies)


@admin_bp.route('/admin/settings', methods=['GET', 'POST'])
@login_required
@role_required('admin')
@module_required('admin_users')
def admin_settings():
    if request.method == 'POST':
        validate_csrf()
        brand_name = request.form.get('brand_name', 'GeoBusca Territorial').strip()
        brand_subtitle = request.form.get('brand_subtitle', 'Municipio de Rionegro').strip()
        primary_color = request.form.get('primary_color', '#C1121F').strip()
        
        # Logo upload
        brand_logo = None
        logo_file = request.files.get('brand_logo')
        if logo_file and logo_file.filename:
            ext = Path(logo_file.filename).suffix.lower()
            if ext in ('.svg', '.webp'):
                filename = f"logo_{secrets.token_hex(4)}{ext}"
                logo_path = Path(app_main.app.static_folder) / 'img' / filename
                logo_file.save(logo_path)
                brand_logo = filename
            else:
                flash('Solo se permiten archivos SVG o WebP para el logo.', 'danger')

        with get_conn() as conn:
            # Safety check/migration for column
            try:
                conn.execute("ALTER TABLE system_settings ADD COLUMN brand_logo TEXT")
            except:
                pass
                
            if brand_logo:
                conn.execute(
                    '''
                    UPDATE system_settings 
                    SET brand_name=?, brand_subtitle=?, primary_color=?, brand_logo=?, updated_at=datetime('now')
                    WHERE id=1
                    ''',
                    (brand_name, brand_subtitle, primary_color, brand_logo)
                )
            else:
                conn.execute(
                    '''
                    UPDATE system_settings 
                    SET brand_name=?, brand_subtitle=?, primary_color=?, updated_at=datetime('now')
                    WHERE id=1
                    ''',
                    (brand_name, brand_subtitle, primary_color)
                )
        flash('Configuración de sistema actualizada con éxito.', 'success')
        log_audit(current_user()['id'], 'update_system_settings', 'system', 1)
        return redirect(url_for('admin.admin_settings'))
        
    with get_conn() as conn:
        settings = conn.execute('SELECT * FROM system_settings WHERE id=1').fetchone()
        
    return render_template('admin_settings.html', settings=dict(settings))


@admin_bp.route('/admin/layers', methods=['GET', 'POST'])
@login_required
@role_required('admin')
@module_required('admin_users')
def admin_layers():
    from backend.services.territory_service import register_territorial_layer, get_active_layers, import_arcgis_layer, get_all_territorial_layers
    from backend.territorial import persist_canonical_layer, allowed_layer_file
    
    if request.method == 'POST':
        validate_csrf()
        action = request.form.get('action')
        
        if action == 'upload':
            file = request.files.get('layer_file')
            display_name = request.form.get('display_name', '').strip()
            layer_type = request.form.get('layer_type', 'barrio').strip()
            
            if file and allowed_layer_file(file.filename):
                filename = secure_filename(file.filename)
                temp_path = UPLOAD_DIR / f"layer_{secrets.token_hex(4)}_{filename}"
                file.save(temp_path)
                try:
                    stored_meta = persist_canonical_layer(temp_path, display_name, layer_type)
                    register_territorial_layer(
                        user_id=current_user()['id'],
                        display_name=display_name,
                        layer_type=layer_type,
                        city=DEFAULT_CITY,
                        region=DEFAULT_REGION,
                        source='Local Upload',
                        stored_meta=stored_meta
                    )
                    flash(f'Capa "{display_name}" cargada con {stored_meta["feature_count"]} polígonos.', 'success')
                except Exception as e:
                    flash(f'Error procesando capa: {str(e)}', 'danger')
                finally:
                    temp_path.unlink(missing_ok=True)
            else:
                flash('Archivo no válido. Use GeoJSON, SHP o GPKG.', 'warning')
        
        elif action == 'arcgis':
            service_url = request.form.get('service_url', '').strip()
            display_name = request.form.get('display_name', '').strip()
            layer_type = request.form.get('layer_type', 'barrio').strip()
            
            if service_url:
                try:
                    import_arcgis_layer(
                        user_id=current_user()['id'],
                        display_name=display_name,
                        layer_type=layer_type,
                        city=DEFAULT_CITY,
                        region=DEFAULT_REGION,
                        source='ArcGIS REST',
                        service_url=service_url
                    )
                    flash(f'Servicio ArcGIS "{display_name}" conectado con éxito.', 'success')
                except Exception as e:
                    flash(f'Error conectando ArcGIS: {str(e)}', 'danger')
        
        elif action == 'toggle':
            layer_id = request.form.get('layer_id')
            with get_conn() as conn:
                conn.execute('UPDATE territorial_layers SET is_active = NOT is_active WHERE id = ?', (layer_id,))
            flash('Estado de capa actualizado.', 'info')
            
        return redirect(url_for('admin.admin_layers'))

    layers = get_all_territorial_layers(DEFAULT_CITY, DEFAULT_REGION)
    return render_template('admin_layers.html', layers=layers, layer_types=TERRITORIAL_LAYER_TYPES)


@admin_bp.route('/admin/support', methods=['GET', 'POST'])
@login_required
@role_required('admin')
@module_required('admin_users')
def admin_support():
    if request.method == 'POST':
        validate_csrf()
        ticket_id = request.form.get('ticket_id')
        response = request.form.get('response', '').strip()
        status = request.form.get('status', 'pending')
        
        with get_conn() as conn:
            conn.execute(
                'UPDATE support_messages SET admin_response=?, status=?, updated_at=datetime("now") WHERE id=?',
                (response, status, ticket_id)
            )
        flash('Ticket actualizado.', 'success')
        return redirect(url_for('admin.admin_support'))
        
    with get_conn() as conn:
        rows = conn.execute(
            '''
            SELECT sm.*, u.full_name, u.email 
            FROM support_messages sm
            JOIN users u ON u.id = sm.user_id
            ORDER BY sm.created_at DESC
            '''
        ).fetchall()
    tickets = [dict(r) for r in rows]
    return render_template('admin_support.html', tickets=tickets)


@admin_bp.route('/support', methods=['GET', 'POST'])
@login_required
def support_center():
    user = current_user()
    if request.method == 'POST':
        validate_csrf()
        subject = request.form.get('subject', '').strip()
        message = request.form.get('message', '').strip()
        
        if not subject or not message:
            flash('Asunto y mensaje son requeridos.', 'warning')
        else:
            with get_conn() as conn:
                conn.execute(
                    'INSERT INTO support_messages (user_id, subject, message, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
                    (user['id'], subject, message, now_iso(), now_iso())
                )
            flash('Tu mensaje ha sido enviado al equipo de soporte. Te notificaremos la respuesta.', 'success')
            return redirect(url_for('admin.support_center'))
            
    with get_conn() as conn:
        rows = conn.execute('SELECT * FROM support_messages WHERE user_id=? ORDER BY created_at DESC', (user['id'],)).fetchall()
    my_tickets = [dict(r) for r in rows]
    return render_template('support.html', tickets=my_tickets)


@admin_bp.route('/admin/smtp', methods=['GET', 'POST'])
@login_required
@role_required('admin')
@module_required('admin_users')
def admin_smtp():
    if request.method == 'POST':
        validate_csrf()
        server = request.form.get('smtp_server', '').strip()
        port = int(request.form.get('smtp_port', '587') or 587)
        user = request.form.get('smtp_user', '').strip()
        password = request.form.get('smtp_password', '').strip()
        sender = request.form.get('smtp_from', '').strip()
        tls = 1 if request.form.get('use_tls') == 'on' else 0
        
        with get_conn() as conn:
            conn.execute(
                '''
                UPDATE smtp_settings 
                SET smtp_server=?, smtp_port=?, smtp_user=?, smtp_password=?, smtp_from=?, use_tls=?, updated_at=datetime('now')
                WHERE id=1
                ''',
                (server, port, user, password, sender, tls)
            )
        flash('Configuración SMTP guardada. El sistema la usará para alertas y notificaciones.', 'success')
        return redirect(url_for('admin.admin_smtp'))
        
    with get_conn() as conn:
        settings = conn.execute('SELECT * FROM smtp_settings WHERE id=1').fetchone()
    return render_template('admin_smtp.html', settings=dict(settings))


@admin_bp.route('/admin/health')
@login_required
@role_required('admin')
@module_required('admin_users')
def server_health():
    import shutil
    import platform
    import time
    
    # Disk Usage
    total, used, free = shutil.disk_usage("/")
    disk = {
        'total': f"{total / (1024**3):.1f} GB",
        'used': f"{used / (1024**3):.1f} GB",
        'free': f"{free / (1024**3):.1f} GB",
        'percent': round((used / total) * 100, 1)
    }
    
    # DB Health
    db_size = os.path.getsize(DB_PATH)
    with get_conn() as conn:
        conn.execute("PRAGMA integrity_check") # Just to make sure
        user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        visit_count = conn.execute("SELECT COUNT(*) FROM visit_records").fetchone()[0]
        dataset_count = conn.execute("SELECT COUNT(*) FROM datasets").fetchone()[0]
        
    # System Info
    sys_info = {
        'os': platform.system(),
        'version': platform.version(),
        'python': platform.python_version(),
        'uptime': "Consultar Logs"
    }
    
    return render_template('admin_health.html', 
                           disk=disk, 
                           db_size=f"{db_size / (1024**2):.1f} MB",
                           metrics={'users': user_count, 'visits': visit_count, 'datasets': dataset_count},
                           sys_info=sys_info)


@admin_bp.route('/admin/conflicts/<int:conflict_id>')
@login_required
@role_required('revisor')
@module_required('conflicts')
def conflict_detail(conflict_id: int):
    query = (
        'SELECT vc.*, u.full_name AS actor_name, ru.full_name AS resolved_name '
        'FROM visit_conflicts vc '
        'LEFT JOIN users u ON u.id = vc.user_id '
        'LEFT JOIN users ru ON ru.id = vc.resolved_by '
        'WHERE vc.id=?'
    )
    with get_conn() as conn:
        row = conn.execute(query, (conflict_id,)).fetchone()
    if not row:
        flash('Conflicto no encontrado.', 'warning')
        return redirect(url_for('admin.conflict_log'))
    conflict = dict(row)
    client_payload = json.loads(conflict.get('client_payload_json') or '{}')
    server_payload = json.loads(conflict.get('server_payload_json') or '{}')
    compare_rows = []
    ignored = {'csrf_token'}
    for key in sorted(set(client_payload.keys()) | set(server_payload.keys())):
        if key in ignored:
            continue
        client_s = '' if client_payload.get(
            key) is None else str(client_payload.get(key))
        server_s = '' if server_payload.get(
            key) is None else str(server_payload.get(key))
        if client_s == server_s:
            continue
        compare_rows.append({'field': key, 'label': key.replace(
            '_', ' ').title(), 'client_value': client_s, 'server_value': server_s})
    return render_template('conflict_detail.html', conflict=conflict, compare_rows=compare_rows,
                           client_payload=client_payload, server_payload=server_payload)


@admin_bp.route('/admin/conflicts/<int:conflict_id>/apply_fields', methods=['POST'])
@login_required
@role_required('revisor')
@module_required('conflicts')
def apply_conflict_fields(conflict_id: int):
    validate_csrf()
    user = current_user()
    with get_conn() as conn:
        row = conn.execute(
            'SELECT * FROM visit_conflicts WHERE id=?',
            (conflict_id,
             )).fetchone()
    if not row:
        flash('Conflicto no encontrado.', 'danger')
        return redirect(url_for('admin.conflict_log'))
    conflict = dict(row)
    field_choices = {}
    for key in request.form:
        if key.startswith('pick__'):
            field_choices[key.split('pick__', 1)[1]
                          ] = request.form.get(key, 'client')
    merged_payload, changed_from_client, kept_server = _build_payload_from_field_choices(
        conflict, field_choices)
    notes = str(request.form.get('notes', '')).strip()
    try:
        _apply_resolved_conflict(
            conflict,
            merged_payload,
            changed_from_client,
            user)
        with get_conn() as conn:
            conn.execute(
                'UPDATE visit_conflicts SET resolution_status=?, notes=?, resolved_by=?, resolved_at=? WHERE id=?',
                ('merged',
                 notes or f'Campos cliente: {
                     len(changed_from_client)}, servidor: {
                     len(kept_server)}',
                    user['id'],
                    now_iso(),
                    conflict_id))
        conflict_owner = int(conflict.get('user_id') or 0)
        if conflict_owner:
            create_notification(
                conflict_owner,
                'conflict_resolved',
                'Conflicto resuelto campo a campo',
                f'Se resolvió el conflicto del dataset {
                    conflict["dataset_id"]}, fila {
                    conflict["row_idx"]}.',
                url_for(
                    'visit_print_view',
                    dataset_id=conflict['dataset_id'],
                    row_idx=conflict['row_idx']))
        flash('Conflicto aplicado campo a campo.', 'success')
    except Exception as exc:
        flash(
            f'No se pudo aplicar la resolución campo a campo: {exc}',
            'danger')
    return redirect(url_for('admin.conflict_detail', conflict_id=conflict_id))


@admin_bp.route('/admin/conflicts/<int:conflict_id>/resolve', methods=['POST'])
@login_required
@role_required('revisor')
@module_required('conflicts')
def resolve_conflict(conflict_id: int):
    validate_csrf()
    user = current_user()
    resolution = str(
        request.form.get(
            'resolution_status',
            'resolved')).strip().lower() or 'resolved'
    notes = str(request.form.get('notes', '')).strip()
    with get_conn() as conn:
        conn.execute(
            'UPDATE visit_conflicts SET resolution_status=?, notes=?, resolved_by=?, resolved_at=? WHERE id=?',
            (resolution,
             notes,
             user['id'],
                now_iso(),
                conflict_id))
    log_audit(
        user['id'],
        'resolve_conflict',
        'visit_conflict',
        conflict_id,
        details={
            'resolution': resolution})
    flash('Conflicto actualizado.', 'success')
    if request.form.get('back_to') == 'detail':
        return redirect(url_for('admin.conflict_detail', conflict_id=conflict_id))
    return redirect(url_for('admin.conflict_log'))


@admin_bp.route('/admin/assignments/template')
@login_required
@role_required('revisor')
@module_required('visit_assignments')
def visit_assignments_template():
    return _agenda_template_response()


@admin_bp.route('/admin/assignments/multi-template')
@login_required
@role_required('revisor')
@module_required('visit_assignments')
def visit_assignments_multi_template():
    return _multi_staff_template_response()


@admin_bp.route('/api/agenda/action', methods=['POST'])
@login_required
@module_required('daily_agenda')
def api_agenda_action():
    user = current_user()
    data = request.get_json(silent=True) or {}
    action = str(data.get('action') or '').strip().lower()
    dataset_id = int(data.get('dataset_id') or 0)
    row_idx = int(data.get('row_idx') or 0)
    completion_status = str(data.get('completion_status')
                            or 'pending').strip().lower()
    completion_notes = str(data.get('completion_notes') or '').strip()
    reason_code = str(data.get('completion_reason_code') or '').strip().lower()
    gps = {
        'latitude': data.get('gps_latitude') or None,
        'longitude': data.get('gps_longitude') or None,
        'accuracy': data.get('gps_accuracy') or None,
    }
    if action in {'checkin', 'checkout'} and (
            not gps['latitude'] or not gps['longitude']):
        return jsonify({'ok': False, 'error': 'GPS obligatorio'}), 400
    if action == 'checkin':
        _complete_visit_progress(
            dataset_id,
            row_idx,
            'in_progress',
            user,
            completion_notes or 'Inicio de visita',
            reason_code,
            gps)
        log_audit(
            user['id'],
            'checkin_visit',
            'visit_record',
            f'{dataset_id}:{row_idx}',
            dataset_id=dataset_id,
            row_idx=row_idx,
            details=gps)
        return jsonify({'ok': True, 'message': 'Check-in registrado'})
    if action == 'checkout':
        _complete_visit_progress(
            dataset_id,
            row_idx,
            'completed',
            user,
            completion_notes or 'Cierre de visita',
            reason_code or 'gestion_exitosa',
            gps)
        log_audit(
            user['id'],
            'checkout_visit',
            'visit_record',
            f'{dataset_id}:{row_idx}',
            dataset_id=dataset_id,
            row_idx=row_idx,
            details=gps)
        return jsonify({'ok': True, 'message': 'Check-out registrado'})
    if action == 'progress':
        if completion_status in {'no_effective',
                                 'cancelled'} and not completion_notes:
            return jsonify(
                {'ok': False, 'error': 'Observación obligatoria para visita no efectiva'}), 400
        if completion_status in {'no_effective',
                                 'cancelled'} and not reason_code:
            return jsonify({'ok': False, 'error': 'Causal obligatoria'}), 400
        if reason_code in EVIDENCE_REQUIRED_CAUSES and not _attachments_for_visit(
                dataset_id, row_idx):
            return jsonify(
                {'ok': False, 'error': 'La causal exige evidencia previa'}), 400
        _complete_visit_progress(
            dataset_id,
            row_idx,
            completion_status,
            user,
            completion_notes,
            reason_code,
            gps if gps['latitude'] and gps['longitude'] else None)
        log_audit(
            user['id'],
            'update_agenda_progress',
            'visit_record',
            f'{dataset_id}:{row_idx}',
            dataset_id=dataset_id,
            row_idx=row_idx,
            details={
                'completion_status': completion_status,
                'notes': completion_notes,
                'reason_code': reason_code})
        return jsonify({'ok': True, 'message': 'Estado actualizado'})
    return jsonify({'ok': False, 'error': 'Acción no soportada'}), 400


@admin_bp.route('/agenda/close_day', methods=['POST'])
@login_required
@module_required('daily_agenda')
def close_day():
    user = current_user()
    validate_csrf()
    target_user_id = int(request.form.get('user_id', user['id']) or user['id'])
    target_date = str(request.form.get('agenda_date', '')
                      ).strip() or time.strftime('%Y-%m-%d')
    with get_conn() as conn:
        target_user = conn.execute(
            'SELECT id, full_name, email, role FROM users WHERE id=?',
            (target_user_id,
             )).fetchone()
    items = _agenda_rows_for_user(target_user_id, target_date)
    for item in items:
        sla = _agenda_sla_state(item)
        _update_visit_workflow(int(item['dataset_id']),
                               int(item['row_idx']),
                               sla_status=sla['status'],
                               sla_message=sla['message'],
                               day_closed_at=now_iso(),
                               day_closed_by=user['id'])
    summary = _create_day_close_summary(
        dict(target_user) if target_user else {
            'id': target_user_id,
            'full_name': 'Funcionario'},
        target_date,
        items,
        user)
    flash(
        f"Cierre de jornada generado: {
            summary['metrics']['completed']} completadas, {
            summary['metrics']['pending']} pendientes, SLA vencido {
                summary['metrics']['sla_breached']}.",
        'success')
    return redirect(
        url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))


@admin_bp.route('/api/supervision/live_data')
@login_required
@role_required('revisor')
@module_required('manager_dashboard')
def supervision_live_data():
    target_date = str(request.args.get('agenda_date', '')
                      ).strip() or time.strftime('%Y-%m-%d')
    with get_conn() as conn:
        users = [dict(r) for r in conn.execute(
            "SELECT id, full_name, email, role FROM users WHERE role IN ('funcionario','analyst','revisor','admin') ORDER BY full_name, email").fetchall()]
    staff_rows, incident_rows = [], []
    territory_counter = Counter()
    staff_positions = []
    for u in users:
        items = _agenda_rows_for_user(int(u['id']), target_date)
        if not items:
            continue
        _emit_agenda_alerts(items)
        items = _agenda_rows_for_user(int(u['id']), target_date)
        alerts = completed = in_progress = pending = 0
        last_activity = ''
        last_pos = None
        for item in items:
            state = _agenda_alert_state(item)
            sla = _agenda_sla_state(item)
            if state['status'] == 'late':
                alerts += 1
                incident_rows.append(
                    {
                        'user_name': u.get('full_name') or u.get('email'),
                        'territory': item.get('territorio_principal_nombre') or item.get('barrio') or item.get('vereda') or item.get('comuna') or item.get('corregimiento') or 'Sin territorio',
                        'name': item.get('rvt_razon_social') or item.get('nom_establec') or 'Sin nombre',
                        'message': state['message'],
                        'dataset_id': item.get('dataset_id'),
                        'row_idx': item.get('row_idx')})
            st = str(item.get('completion_status') or 'pending').lower()
            if st == 'completed':
                completed += 1
            elif st in {'in_progress', 'started'}:
                in_progress += 1
            else:
                pending += 1
            last_activity = max([v for v in [last_activity, str(item.get('updated_at') or ''), str(
                item.get('checkin_at') or ''), str(item.get('checkout_at') or '')] if v], default='')
            territory = item.get('territorio_principal_nombre') or item.get('barrio') or item.get(
                'vereda') or item.get('comuna') or item.get('corregimiento') or ''
            if territory:
                territory_counter[territory] += 1
            pos = _last_position_from_item(item)
            if pos and (not last_pos or str(pos.get('timestamp') or '')
                        > str(last_pos.get('timestamp') or '')):
                last_pos = {
                    **pos,
                    'territory': territory,
                    'item_name': item.get('rvt_razon_social') or item.get('nom_establec') or 'Sin nombre',
                    'sla_status': sla['status']}
        staff_rows.append({'user_id': u['id'],
                           'name': u.get('full_name') or u.get('email'),
                           'total': len(items),
                           'completed': completed,
                           'in_progress': in_progress,
                           'pending': pending,
                           'alerts': alerts,
                           'last_activity': last_activity})
        if last_pos:
            staff_positions.append({'user_id': u['id'],
                                    'name': u.get('full_name') or u.get('email'),
                                    'latitude': last_pos['latitude'],
                                    'longitude': last_pos['longitude'],
                                    'timestamp': last_pos['timestamp'],
                                    'source': last_pos['source'],
                                    'territory': last_pos['territory'],
                                    'item_name': last_pos['item_name'],
                                    'sla_status': last_pos['sla_status']})
    staff_rows.sort(key=lambda x: (-x['alerts'], x['pending'], x['name']))
    territory_rows = [{'territorio': name, 'visitas': count}
                      for name, count in territory_counter.most_common(15)]
    metrics = {
        'funcionarios_activos': len(staff_rows), 'visitas_en_alerta': sum(
            r['alerts'] for r in staff_rows), 'visitas_en_curso': sum(
            r['in_progress'] for r in staff_rows), 'visitas_cumplidas': sum(
                r['completed'] for r in staff_rows)}
    return jsonify({'ok': True, 'agenda_date': target_date, 'staff_rows': staff_rows,
                   'incident_rows': incident_rows[:100], 'territory_rows': territory_rows, 'metrics': metrics, 'positions': staff_positions})


@admin_bp.route('/agenda/route_data')
@login_required
@module_required('daily_agenda')
def agenda_route_data():
    user = current_user()
    target_user_id = int(user['id'])
    if has_role(user, 'revisor') and str(
            request.args.get('user_id', '')).isdigit():
        target_user_id = int(request.args.get('user_id'))
    target_date = str(request.args.get('agenda_date', '')
                      ).strip() or time.strftime('%Y-%m-%d')
    items = _agenda_rows_for_user(target_user_id, target_date)
    return jsonify(_agenda_route_payload(items))


@admin_bp.route('/admin/approvals', methods=['GET', 'POST'])
@login_required
@role_required('revisor')
@module_required('approval_queue')
def approval_queue():
    user = current_user()
    if request.method == 'POST':
        validate_csrf()
        dataset_id = int(request.form.get('dataset_id', '0') or 0)
        row_idx = int(request.form.get('row_idx', '0') or 0)
        action = str(request.form.get('action', 'approve')).strip().lower()
        notes = str(request.form.get('notes', '')).strip()
        reason_code = str(request.form.get('reason_code', '')).strip().lower()
        status_map = {
            'approve': 'approved',
            'reject': 'rejected',
            'return': 'returned',
            'review': 'under_review'}
        status = status_map.get(action, 'under_review')
        if action in {'return', 'reject'} and not notes:
            flash(
                'Debes diligenciar una observación para devolver o rechazar.',
                'warning')
            return redirect(
                url_for('admin.approval_queue', status=request.args.get('status', 'submitted')))
        if action in {'return', 'reject'} and not reason_code:
            flash(
                'Debes seleccionar una causal estructurada para devolver o rechazar.',
                'warning')
            return redirect(
                url_for('admin.approval_queue', status=request.args.get('status', 'submitted')))
        reason_label = _approval_reason_label(action, reason_code)
        _update_visit_workflow(
            dataset_id,
            row_idx,
            approval_status=status,
            approval_notes=notes,
            approval_reason_code=reason_code,
            approval_reason_label=reason_label,
            reviewed_by=user['id'],
            reviewed_at=now_iso())
        with get_conn() as conn:
            row = conn.execute(
                'SELECT created_by, assigned_to FROM visit_records WHERE dataset_id=? AND row_idx=?',
                (dataset_id,
                 row_idx)).fetchone()
        recipients = {
            int(v) for v in [
                row['created_by'] if row else None,
                row['assigned_to'] if row else None] if v}
        reason_text = f' Causal: {reason_label}.' if reason_label else ''
        for recipient in recipients:
            create_notification(
                recipient,
                'approval_workflow',
                'Visita revisada',
                f'La visita dataset {dataset_id} fila {row_idx} quedó en estado {status}.{reason_text}',
                url_for(
                    'visit_print_view',
                    dataset_id=dataset_id,
                    row_idx=row_idx))
        log_audit(
            user['id'],
            'approval_action',
            'visit_record',
            f'{dataset_id}:{row_idx}',
            dataset_id=dataset_id,
            row_idx=row_idx,
            details={
                'status': status,
                'notes': notes,
                'reason_code': reason_code,
                'reason_label': reason_label})
        flash('Flujo de aprobación actualizado.', 'success')
        return redirect(
            url_for('admin.approval_queue', status=request.args.get('status', 'submitted')))

    status = str(request.args.get('status', 'submitted')).strip().lower()
    rows = _approval_rows('' if status == 'all' else status)
    for item in rows:
        item['approval_badge'] = _workflow_status_badge(
            item.get('approval_status', ''))
        item['assignment_badge'] = _assignment_status_badge(
            item.get('assignment_status', ''))
    return render_template('approval_queue.html', items=rows,
                           selected_status=status, approval_reason_options=APPROVAL_REASON_OPTIONS)


@admin_bp.route('/admin/assignments', methods=['GET', 'POST'])
@login_required
@role_required('revisor')
@module_required('visit_assignments')
def visit_assignments():
    user = current_user()
    with get_conn() as conn:
        dataset_rows = conn.execute(
            'SELECT d.*, u.full_name AS owner_name FROM datasets d LEFT JOIN users u ON u.id=d.user_id ORDER BY d.created_at DESC, d.id DESC').fetchall()
        staff_rows = conn.execute(
            "SELECT id, full_name, email, role FROM users WHERE role IN ('funcionario','analyst','revisor','admin') ORDER BY full_name, email").fetchall()
    datasets = [dict(r) for r in dataset_rows]
    staff = [dict(r) for r in staff_rows]
    selected_dataset = int(
        request.values.get(
            'dataset_id', str(
                datasets[0]['id'] if datasets else 0)) or 0)
    territory_type = str(request.values.get('territory_type', '')).strip()
    territory_value = str(request.values.get('territory_value', '')).strip()
    approval_filter = str(
        request.values.get(
            'approval_status',
            '')).strip().lower()
    available_values = []
    preview_rows = []
    owner_id = 0
    if selected_dataset:
        ds = next(
            (d for d in datasets if int(
                d['id']) == selected_dataset),
            None)
        owner_id = int(ds['user_id']) if ds else 0
        try:
            df = ensure_visit_columns(
                _load_any_dataset_version(
                    selected_dataset, owner_id))
            available_values = _available_assignment_territory_values(
                df, territory_type)
            preview_rows = _queue_rows_for_assignment(
                selected_dataset, owner_id, territory_type, territory_value, approval_filter)
        except Exception:
            preview_rows = []
    if request.method == 'POST':
        validate_csrf()
        form_action = str(
            request.form.get(
                'form_action',
                'assign')).strip().lower() or 'assign'
        if form_action == 'bulk_upload':
            upload = request.files.get('agenda_file')
            if not upload or not upload.filename:
                flash(
                    'Debes seleccionar un archivo CSV o Excel para la carga masiva.',
                    'warning')
                return redirect(url_for('admin.visit_assignments', dataset_id=selected_dataset,
                                territory_type=territory_type, territory_value=territory_value, approval_status=approval_filter))
            ext = Path(upload.filename).suffix.lower()
            if ext not in ALLOWED_EXTENSIONS:
                flash(
                    'Formato de agenda no soportado. Usa CSV o Excel.',
                    'warning')
                return redirect(url_for('admin.visit_assignments', dataset_id=selected_dataset,
                                territory_type=territory_type, territory_value=territory_value, approval_status=approval_filter))
            temp_path = UPLOAD_DIR / \
                f'agenda_{
                    secrets.token_hex(8)}_{
                    secure_filename(
                        upload.filename)}'
            upload.save(temp_path)
            try:
                applied, errors = _bulk_assign_agenda_from_file(
                    temp_path, selected_dataset, user, staff)
                if applied:
                    log_audit(
                        user['id'],
                        'bulk_assign_visits',
                        'dataset',
                        selected_dataset,
                        dataset_id=selected_dataset,
                        details={
                            'applied': applied,
                            'error_count': len(errors)})
                if errors:
                    flash(
                        f'Agenda cargada con {applied} asignaciones y {len(errors)} observaciones. Primeras: ' + ' | '.join(errors[:3]), 'warning')
                else:
                    flash(
                        f'Se cargaron {applied} asignaciones de agenda.',
                        'success')
            finally:
                temp_path.unlink(missing_ok=True)
            return redirect(url_for('admin.visit_assignments', dataset_id=selected_dataset,
                            territory_type=territory_type, territory_value=territory_value, approval_status=approval_filter))

        agenda_date = str(request.form.get('agenda_date', '')).strip()
        route_group = str(request.form.get('route_group', '')).strip()
        optimize_route = request.form.get('optimize_route') == '1'
        territory_scope = ' / '.join([v for v in [territory_type,
                                     territory_value] if v]).strip(' /')
        selected_rows = [int(v) for v in request.form.getlist(
            'row_idx') if str(v).isdigit()]
        if not selected_rows and preview_rows:
            selected_rows = [int(item['row_idx']) for item in preview_rows]
        selected_items = [
            item for item in preview_rows if int(
                item['row_idx']) in selected_rows]
        if form_action == 'multi_plan':
            planner_specs, planner_errors = _parse_multi_staff_specs(
                request.form.get(
                    'planner_lines', ''), staff, _parse_positive_int(
                    request.form.get(
                        'service_minutes', '20'), 20))
            if planner_errors:
                flash(' | '.join(planner_errors[:4]), 'warning')
                return redirect(url_for('admin.visit_assignments', dataset_id=selected_dataset,
                                territory_type=territory_type, territory_value=territory_value, approval_status=approval_filter))
            plan_result = _plan_multi_staff_assignments(
                selected_items, planner_specs, optimize_route=optimize_route)
            for idx_plan, block in enumerate(plan_result['plans'], start=1):
                spec = block['spec']
                route = block.get('route', {})
                for order, item in enumerate(block.get('items', []), start=1):
                    _update_visit_workflow(selected_dataset, int(item['row_idx']),
                                           assigned_to=spec['user_id'], assigned_by=user['id'], assigned_at=now_iso(
                    ),
                        assignment_status='scheduled' if agenda_date else 'assigned', territory_scope=territory_scope,
                        agenda_date=agenda_date, agenda_order=order, route_group=route_group or f'Multi ruta {idx_plan}',
                        route_optimization_method=route.get(
                        'method') if optimize_route else None,
                        route_optimized_at=now_iso() if optimize_route else None,
                        route_optimized_by=user['id'] if optimize_route else None,
                        route_distance_km=route.get(
                            'total_km') if optimize_route else None,
                        route_estimated_minutes=route.get(
                            'total_minutes') if optimize_route else None,
                        schedule_start_time=item.get('schedule_start_time', ''), schedule_end_time=item.get('schedule_end_time', ''),
                        estimated_service_minutes=item.get('estimated_service_minutes', 20), route_vehicle=spec.get('vehicle_label', ''), route_capacity=spec.get('capacity'))
                if block.get('items'):
                    create_notification(
                        spec['user_id'],
                        'assignment',
                        'Ruta multi-funcionario asignada',
                        f'Se asignaron {
                            len(
                                block.get(
                                    "items",
                                    []))} visitas para {
                            agenda_date or "sin fecha"} en {
                            spec.get("vehicle_label") or spec.get("user_name")}.',
                        url_for(
                            'admin.daily_agenda',
                            user_id=spec['user_id'],
                            agenda_date=agenda_date or time.strftime('%Y-%m-%d')))
            if plan_result['unassigned']:
                flash(
                    f"Plan multi-funcionario aplicado. Quedaron {
                        len(
                            plan_result['unassigned'])} visitas sin asignar por capacidad/horario.",
                    'warning')
            else:
                flash(
                    f"Plan multi-funcionario aplicado a {
                        plan_result['summary']['assigned']} visitas.",
                    'success')
            log_audit(
                user['id'],
                'multi_staff_plan',
                'dataset',
                selected_dataset,
                dataset_id=selected_dataset,
                details={
                    'agenda_date': agenda_date,
                    'route_group': route_group,
                    'planner_specs': planner_specs,
                    'summary': plan_result['summary'],
                    'unassigned_rows': [
                        int(
                            item['row_idx']) for item in plan_result['unassigned']]})
            return redirect(url_for('admin.visit_assignments', dataset_id=selected_dataset,
                            territory_type=territory_type, territory_value=territory_value, approval_status=approval_filter))

        assigned_to = int(request.form.get('assigned_to', '0') or 0)
        plan = _build_route_plan(selected_items) if optimize_route and selected_items else {
            'ordered': selected_items,
            'total_km': 0.0,
            'total_minutes': None,
            'provider': None,
            'method': None}
        total_km = float(plan.get('total_km') or 0.0)
        total_minutes = plan.get('total_minutes')
        route_method = plan.get('method') if optimize_route else None
        schedule_rows, overflow_rows = _build_schedule_for_plan(
            plan['ordered'], shift_start=str(
                request.form.get(
                    'shift_start', '08:00') or '08:00'), shift_end=str(
                request.form.get(
                    'shift_end', '17:00') or '17:00'), service_minutes=_parse_positive_int(
                        request.form.get(
                            'service_minutes', '20'), 20), total_route_minutes=total_minutes)
        for order, item in enumerate(schedule_rows, start=1):
            _update_visit_workflow(selected_dataset, int(item['row_idx']),
                                   assigned_to=assigned_to, assigned_by=user['id'], assigned_at=now_iso(
            ),
                assignment_status='assigned' if not agenda_date else 'scheduled', territory_scope=territory_scope,
                agenda_date=agenda_date, agenda_order=order, route_group=route_group,
                route_optimization_method=route_method, route_optimized_at=now_iso(
            ) if optimize_route else None,
                route_optimized_by=user['id'] if optimize_route else None, route_distance_km=total_km if optimize_route else None,
                route_estimated_minutes=total_minutes if optimize_route else None, schedule_start_time=item.get('schedule_start_time', ''),
                schedule_end_time=item.get('schedule_end_time', ''), estimated_service_minutes=item.get('estimated_service_minutes', 20), route_capacity=len(schedule_rows))
        if assigned_to and selected_rows:
            extra = f' Ruta optimizada: {total_km} km.' if optimize_route and total_km else ''
            cap_note = f' Se dejaron {
                len(overflow_rows)} fuera de horario/capacidad.' if overflow_rows else ''
            create_notification(
                assigned_to,
                'assignment',
                'Nuevas visitas asignadas',
                f'Se asignaron {
                    len(schedule_rows)} visitas para {
                    agenda_date or "sin fecha"}.{extra}{cap_note}',
                url_for(
                    'admin.daily_agenda',
                    user_id=assigned_to,
                    agenda_date=agenda_date or time.strftime('%Y-%m-%d')))
        log_audit(user['id'],
                  'assign_visits',
                  'dataset',
                  selected_dataset,
                  dataset_id=selected_dataset,
                  details={'assigned_to': assigned_to,
                           'rows': selected_rows,
                           'scheduled_rows': [int(item['row_idx']) for item in schedule_rows],
                           'overflow_rows': [int(item['row_idx']) for item in overflow_rows],
                           'agenda_date': agenda_date,
                           'territory_scope': territory_scope,
                           'optimize_route': optimize_route,
                           'route_distance_km': total_km,
                           'route_method': route_method,
                           'route_minutes': total_minutes,
                           'shift_start': request.form.get('shift_start',
                                                           '08:00'),
                           'shift_end': request.form.get('shift_end',
                                                         '17:00'),
                           'service_minutes': request.form.get('service_minutes',
                                                               '20')})
        flash(
            f'Se asignaron {
                len(schedule_rows)} visitas.' + (
                f' Ruta optimizada: {total_km} km.' if optimize_route and total_km else '') + (
                f' Quedaron {
                    len(overflow_rows)} fuera de horario/capacidad.' if overflow_rows else ''),
            'success')
        return redirect(url_for('admin.visit_assignments', dataset_id=selected_dataset,
                        territory_type=territory_type, territory_value=territory_value, approval_status=approval_filter))
    return render_template('visit_assignments.html', datasets=datasets, staff=staff, selected_dataset=selected_dataset, territory_type=territory_type,
                           territory_value=territory_value, approval_filter=approval_filter, available_values=available_values, preview_rows=preview_rows)


@admin_bp.route('/agenda', methods=['GET', 'POST'])
@login_required
@module_required('daily_agenda')
def daily_agenda():
    user = current_user()
    target_user_id = int(user['id'])
    if has_role(user, 'revisor') and str(
            request.args.get('user_id', '')).isdigit():
        target_user_id = int(request.args.get('user_id'))
    target_date = str(request.args.get('agenda_date', '')
                      ).strip() or time.strftime('%Y-%m-%d')
    with get_conn() as conn:
        target_user = conn.execute(
            'SELECT id, full_name, email, role FROM users WHERE id=?',
            (target_user_id,
             )).fetchone()
        all_users = conn.execute(
            "SELECT id, full_name, email, role FROM users WHERE role IN ('funcionario','analyst','revisor','admin') ORDER BY full_name, email").fetchall()
    if request.method == 'POST':
        validate_csrf()
        action = str(request.form.get('action', 'optimize')
                     ).strip().lower() or 'optimize'
        if action == 'optimize':
            items_now = _agenda_rows_for_user(target_user_id, target_date)
            plan = _build_route_plan(items_now)
            optimized_items = plan['ordered']
            total_km = float(plan.get('total_km') or 0.0)
            total_minutes = plan.get('total_minutes')
            route_method = plan.get('method')
            scheduled_items, _overflow = _build_schedule_for_plan(
                optimized_items, shift_start='08:00', shift_end='17:00', service_minutes=20, total_route_minutes=total_minutes)
            schedule_lookup = {int(item['row_idx']): item for item in scheduled_items}
            for order, item in enumerate(optimized_items, start=1):
                sch = schedule_lookup.get(int(item['row_idx']), {})
                _update_visit_workflow(int(item['dataset_id']), int(item['row_idx']),
                                       agenda_order=order,
                                       route_optimization_method=route_method,
                                       route_optimized_at=now_iso(),
                                       route_optimized_by=user['id'],
                                       route_distance_km=total_km,
                                       route_estimated_minutes=total_minutes,
                                       schedule_start_time=sch.get(
                                           'schedule_start_time', ''),
                                       schedule_end_time=sch.get(
                                           'schedule_end_time', ''),
                                       estimated_service_minutes=sch.get(
                                           'estimated_service_minutes', 20),
                                       )
            log_audit(
                user['id'],
                'optimize_daily_route',
                'agenda',
                f'{target_user_id}:{target_date}',
                details={
                    'target_user_id': target_user_id,
                    'agenda_date': target_date,
                    'total_km': total_km,
                    'items': len(optimized_items),
                    'route_method': route_method,
                    'route_minutes': total_minutes})
            flash(
                f'Ruta optimizada para {
                    len(optimized_items)} visitas. Distancia estimada: {total_km} km.',
                'success')
            return redirect(
                url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))
        if action in {'progress', 'checkin', 'checkout'}:
            dataset_id = int(request.form.get('dataset_id', '0') or 0)
            row_idx = int(request.form.get('row_idx', '0') or 0)
            completion_status = str(
                request.form.get(
                    'completion_status',
                    'pending')).strip().lower()
            completion_notes = str(
                request.form.get(
                    'completion_notes',
                    '')).strip()
            reason_code = str(
                request.form.get(
                    'completion_reason_code',
                    '')).strip().lower()
            gps = {
                'latitude': request.form.get('gps_latitude') or None,
                'longitude': request.form.get('gps_longitude') or None,
                'accuracy': request.form.get('gps_accuracy') or None,
            }
            if action in {'checkin', 'checkout'} and (
                    not gps['latitude'] or not gps['longitude']):
                flash('El check-in/check-out exige GPS obligatorio.', 'danger')
                return redirect(
                    url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))
            if action == 'checkin':
                _complete_visit_progress(
                    dataset_id,
                    row_idx,
                    'in_progress',
                    user,
                    completion_notes or 'Inicio de visita',
                    reason_code,
                    gps)
                log_audit(
                    user['id'],
                    'checkin_visit',
                    'visit_record',
                    f'{dataset_id}:{row_idx}',
                    dataset_id=dataset_id,
                    row_idx=row_idx,
                    details=gps)
                flash('Check-in registrado con GPS.', 'success')
                return redirect(
                    url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))
            if action == 'checkout':
                _complete_visit_progress(
                    dataset_id,
                    row_idx,
                    'completed',
                    user,
                    completion_notes or 'Cierre de visita',
                    reason_code or 'gestion_exitosa',
                    gps)
                log_audit(
                    user['id'],
                    'checkout_visit',
                    'visit_record',
                    f'{dataset_id}:{row_idx}',
                    dataset_id=dataset_id,
                    row_idx=row_idx,
                    details=gps)
                flash('Check-out registrado con GPS.', 'success')
                return redirect(
                    url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))
            if completion_status in {'no_effective',
                                     'cancelled'} and not completion_notes:
                flash(
                    'Debes escribir una observación obligatoria al marcar una visita no efectiva.',
                    'danger')
                return redirect(
                    url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))
            if completion_status in {'no_effective',
                                     'cancelled'} and not reason_code:
                flash(
                    'Debes seleccionar una causal estructurada para la visita no efectiva.',
                    'danger')
                return redirect(
                    url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))
            if reason_code in EVIDENCE_REQUIRED_CAUSES and not _attachments_for_visit(
                    dataset_id, row_idx):
                flash(
                    'La causal seleccionada exige evidencia fotográfica o anexo. Abre la visita y adjunta evidencia antes de cerrarla.',
                    'danger')
                return redirect(
                    url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))
            _complete_visit_progress(
                dataset_id,
                row_idx,
                completion_status,
                user,
                completion_notes,
                reason_code,
                gps if gps['latitude'] and gps['longitude'] else None)
            log_audit(
                user['id'],
                'update_agenda_progress',
                'visit_record',
                f'{dataset_id}:{row_idx}',
                dataset_id=dataset_id,
                row_idx=row_idx,
                details={
                    'completion_status': completion_status,
                    'notes': completion_notes,
                    'reason_code': reason_code})
            flash('Estado de cumplimiento actualizado.', 'success')
            return redirect(
                url_for('admin.daily_agenda', user_id=target_user_id, agenda_date=target_date))

    items = _agenda_rows_for_user(target_user_id, target_date)
    _emit_agenda_alerts(items)
    items = _agenda_rows_for_user(target_user_id, target_date)
    route_plan = _agenda_route_payload(items) if items else {
        'total_km': 0.0,
        'total_minutes': None,
        'provider': 'none',
        'method': 'none',
        'missing_count': 0}
    route_km = float(route_plan.get('total_km') or 0.0)
    order_lookup = {
        int(
            feature['properties']['row_idx']): int(
            feature['properties']['order']) for feature in route_plan.get(
                'stops_geojson',
                {}).get(
                    'features',
            [])}
    for item in items:
        item['optimized_order'] = order_lookup.get(int(item['row_idx']), '')
        state = _agenda_alert_state(item)
        item['live_alert_status'] = state['status']
        item['live_alert_message'] = state['message']
        sla = _agenda_sla_state(item)
        item['sla_status'] = sla['status']
        item['sla_message'] = sla['message']
        _update_visit_workflow(int(item['dataset_id']),
                               int(item['row_idx']),
                               alert_status=state['status'],
                               alert_message=state['message'],
                               sla_status=sla['status'],
                               sla_message=sla['message'])
    metrics = {
        'total': len(items),
        'pendientes': sum(1 for item in items if str(item.get('completion_status') or 'pending').strip().lower() in {'pending', ''}),
        'con_deuda': sum(1 for item in items if str(item.get('deuda_estado') or '').strip().upper() in {'ADEUDA', 'MOROSO', 'PENDIENTE'}),
        'aprobadas': sum(1 for item in items if str(item.get('approval_status') or '').strip().lower() == 'approved'),
        'cumplidas': sum(1 for item in items if str(item.get('completion_status') or '').strip().lower() == 'completed'),
        'no_efectivas': sum(1 for item in items if str(item.get('completion_status') or '').strip().lower() in {'no_effective', 'cancelled'}),
        'en_alerta': sum(1 for item in items if str(item.get('live_alert_status') or '') == 'late'),
        'sla_ok': sum(1 for item in items if str(item.get('sla_status') or '') == 'met'),
        'sla_vencido': sum(1 for item in items if str(item.get('sla_status') or '') == 'breached'),
        'ruta_km': round(sum(float(item.get('route_distance_km') or 0) for item in items) or route_km, 2),
        'ruta_minutos': route_plan.get('total_minutes') or sum(int(item.get('route_estimated_minutes') or 0) for item in items) or None,
        'proveedor_ruta': route_plan.get('provider') or 'none',
        'territorios': len({str(item.get('territorio_principal_nombre') or item.get('barrio') or item.get('vereda') or item.get('comuna') or item.get('corregimiento') or '').strip() for item in items if str(item.get('territorio_principal_nombre') or item.get('barrio') or item.get('vereda') or item.get('comuna') or item.get('corregimiento') or '').strip()}),
    }
    return render_template('daily_agenda.html', items=items, metrics=metrics, target_user=dict(target_user) if target_user else None, all_users=[
                           dict(r) for r in all_users], selected_user_id=target_user_id, agenda_date=target_date, completion_reason_options=COMPLETION_REASON_OPTIONS)


@admin_bp.route('/supervision/live')
@login_required
@role_required('revisor')
@module_required('manager_dashboard')
def supervision_live():
    target_date = str(request.args.get('agenda_date', '')
                      ).strip() or time.strftime('%Y-%m-%d')
    with get_conn() as conn:
        users = [dict(r) for r in conn.execute(
            "SELECT id, full_name, email, role FROM users WHERE role IN ('funcionario','analyst','revisor','admin') ORDER BY full_name, email").fetchall()]
    staff_rows = []
    incident_rows = []
    territory_counter = Counter()
    for u in users:
        items = _agenda_rows_for_user(int(u['id']), target_date)
        if not items:
            continue
        _emit_agenda_alerts(items)
        items = _agenda_rows_for_user(int(u['id']), target_date)
        alerts = 0
        completed = 0
        in_progress = 0
        pending = 0
        last_activity = ''
        for item in items:
            state = _agenda_alert_state(item)
            if state['status'] == 'late':
                alerts += 1
                incident_rows.append(
                    {
                        'user_name': u.get('full_name') or u.get('email'),
                        'territory': item.get('territorio_principal_nombre') or item.get('barrio') or item.get('vereda') or item.get('comuna') or item.get('corregimiento') or 'Sin territorio',
                        'name': item.get('rvt_razon_social') or item.get('nom_establec') or 'Sin nombre',
                        'message': state['message'],
                        'dataset_id': item.get('dataset_id'),
                        'row_idx': item.get('row_idx')})
            st = str(item.get('completion_status') or 'pending').lower()
            if st == 'completed':
                completed += 1
            elif st in {'in_progress', 'started'}:
                in_progress += 1
            else:
                pending += 1
            last_activity = max([v for v in [last_activity, str(item.get('updated_at') or ''), str(
                item.get('checkin_at') or ''), str(item.get('checkout_at') or '')] if v], default='')
            territory = item.get('territorio_principal_nombre') or item.get('barrio') or item.get(
                'vereda') or item.get('comuna') or item.get('corregimiento') or ''
            if territory:
                territory_counter[territory] += 1
        staff_rows.append({'user_id': u['id'],
                           'name': u.get('full_name') or u.get('email'),
                           'total': len(items),
                           'completed': completed,
                           'in_progress': in_progress,
                           'pending': pending,
                           'alerts': alerts,
                           'last_activity': last_activity})
    staff_rows.sort(key=lambda x: (-x['alerts'], x['pending'], x['name']))
    territory_rows = [{'territorio': name, 'visitas': count}
                      for name, count in territory_counter.most_common(15)]
    metrics = {
        'funcionarios_activos': len(staff_rows), 'visitas_en_alerta': sum(
            r['alerts'] for r in staff_rows), 'visitas_en_curso': sum(
            r['in_progress'] for r in staff_rows), 'visitas_cumplidas': sum(
                r['completed'] for r in staff_rows)}
    return render_template('supervision_live.html', agenda_date=target_date, staff_rows=staff_rows,
                           incident_rows=incident_rows[:100], territory_rows=territory_rows, metrics=metrics)


# Eliminada la versión duplicada de manager_dashboard que estaba aquí


@admin_bp.route('/staff/panel', methods=['GET', 'POST'])
@login_required
@module_required('staff_panel')
def staff_panel():
    user = current_user()
    selected_user_id = str(request.values.get('user_id', user['id'])).strip()
    target_user_id = int(user['id'])
    if has_role(user, 'revisor') and selected_user_id.isdigit():
        target_user_id = int(selected_user_id)
    if request.method == 'POST':
        validate_csrf()
        if not has_role(user, 'revisor'):
            flash('No tienes permisos para reasignar visitas.', 'warning')
            return redirect(url_for('admin.staff_panel', user_id=target_user_id))
        target_assigned_to = int(request.form.get('assigned_to', '0') or 0)
        agenda_date = str(request.form.get('agenda_date', '')).strip()
        route_group = str(request.form.get('route_group', '')).strip()
        optimize_route = request.form.get('optimize_route') == '1'
        selected_pairs = []
        for raw in request.form.getlist('selected_visit'):
            if ':' not in raw:
                continue
            ds_raw, row_raw = raw.split(':', 1)
            if ds_raw.isdigit() and row_raw.isdigit():
                selected_pairs.append((int(ds_raw), int(row_raw)))
        applied, total_km, total_minutes = _bulk_reassign_visits(
            selected_pairs, target_assigned_to, user, agenda_date=agenda_date, route_group=route_group, optimize_route=optimize_route) if selected_pairs and target_assigned_to else (
            0, 0.0, None)
        if target_assigned_to and applied:
            create_notification(
                target_assigned_to,
                'assignment',
                'Visitas reasignadas',
                f'Se reasignaron {applied} visitas desde el tablero operativo.',
                url_for(
                    'daily_agenda',
                    user_id=target_assigned_to,
                    agenda_date=agenda_date or time.strftime('%Y-%m-%d')))
            log_audit(
                user['id'],
                'bulk_reassign_from_staff_panel',
                'user',
                target_assigned_to,
                details={
                    'rows': selected_pairs,
                    'agenda_date': agenda_date,
                    'route_group': route_group,
                    'optimize_route': optimize_route,
                    'total_km': total_km,
                    'total_minutes': total_minutes})
            flash(f'Se reasignaron {applied} visitas.' +
                  (f' Ruta estimada: {total_km} km.' if total_km else ''), 'success')
        else:
            flash('Selecciona visitas y un funcionario destino.', 'warning')
        return redirect(url_for('admin.staff_panel', user_id=target_user_id))
    with get_conn() as conn:
        target_user = conn.execute(
            'SELECT id, full_name, email, role FROM users WHERE id=?',
            (target_user_id,
             )).fetchone()
        all_users = conn.execute(
            'SELECT id, full_name, email, role FROM users ORDER BY full_name, email').fetchall()
        datasets = conn.execute(
            'SELECT * FROM datasets WHERE user_id=? ORDER BY created_at DESC, id DESC',
            (target_user_id,
             )).fetchall()
        recent_visits = conn.execute(
            'SELECT * FROM visit_records WHERE created_by=? ORDER BY updated_at DESC, id DESC LIMIT 20',
            (target_user_id,
             )).fetchall()
        sync_counts = conn.execute(
            'SELECT status, COUNT(*) AS total FROM sync_events WHERE user_id=? GROUP BY status',
            (target_user_id,
             )).fetchall()
        conflict_counts = conn.execute(
            'SELECT resolution_status, COUNT(*) AS total FROM visit_conflicts WHERE user_id=? GROUP BY resolution_status',
            (target_user_id,
             )).fetchall()
    metrics = {
        'datasets': len(datasets),
        'pendientes_visita': 0,
        'realizadas': 0,
        'adeudan': 0,
        'sincronizados': 0,
        'errores_sync': 0,
        'conflictos_pendientes': 0}
    pending_rows = []
    for ds in datasets:
        try:
            df = ensure_visit_columns(
                _load_any_dataset_version(
                    ds['id'], target_user_id))
        except Exception:
            continue
        summary = summarize_visits(df)
        metrics['pendientes_visita'] += int(
            summary.get('visitas_pendientes', 0))
        metrics['realizadas'] += int(summary.get('visitas_realizadas', 0))
        metrics['adeudan'] += int(summary.get('adeudan', 0))
        for item in build_visit_queue(df, only_pending=True, limit=12):
            pending_rows.append(
                {'dataset_id': ds['id'], 'dataset_name': ds['original_filename'], **item})
    for row in sync_counts:
        status = str(row['status'])
        if status == 'synced':
            metrics['sincronizados'] += int(row['total'])
        if status == 'error':
            metrics['errores_sync'] += int(row['total'])
    for row in conflict_counts:
        if str(row['resolution_status']) == 'pending':
            metrics['conflictos_pendientes'] += int(row['total'])
    return render_template('staff_panel.html', metrics=metrics, target_user=dict(target_user) if target_user else None, pending_rows=pending_rows[:30], recent_visits=[
                           dict(r) for r in recent_visits], all_users=[dict(r) for r in all_users], selected_user_id=target_user_id)


@admin_bp.route('/admin/manager-dashboard')
@login_required
@role_required('revisor')
@module_required('manager_dashboard')
def manager_dashboard():
    start_date = request.args.get('start_date') or now_iso()[:10]
    end_date = request.args.get('end_date') or start_date
    metrics = admin_service.get_manager_metrics(start_date, end_date)
    return render_template('dashboard_gerencial.html', metrics=metrics)


@admin_bp.route('/admin/backup')
@login_required
@role_required('admin')
@module_required('admin_users')
def admin_backup():
    if not DB_PATH.exists():
        flash('Base de datos no encontrada.', 'danger')
        return redirect(url_for('admin.admin_users'))
        
    log_audit(current_user()['id'], 'download_backup', 'system', 'database')
    
    filename = f"backup_geobusca_{now_iso()[:10]}_{secrets.token_hex(4)}.db"
    return send_file(DB_PATH, as_attachment=True, download_name=filename)


@admin_bp.route('/admin/trigger-alerts', methods=['POST'])
@login_required
@role_required('admin')
@module_required('admin_users')
def trigger_alerts():
    validate_csrf()
    count = alert_service.check_and_notify_sla_breaches()
    if count > 0:
        flash(f'Se enviaron {count} alertas de SLA a los supervisores.', 'success')
    else:
        flash('No se encontraron nuevos incumplimientos de SLA que requieran alerta.', 'info')
    return redirect(request.referrer or url_for('admin.manager_dashboard'))


@admin_bp.route('/admin/export-dashboard-pdf')
@login_required
@role_required('revisor')
@module_required('manager_dashboard')
def export_dashboard_pdf():
    from reportlab.lib.pagesizes import letter
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    
    start_date = request.args.get('start_date') or now_iso()[:10]
    end_date = request.args.get('end_date') or start_date
    metrics = admin_service.get_manager_metrics(start_date, end_date)
    
    buffer = BytesIO()
    p = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    
    # Header
    p.setFont("Helvetica-Bold", 16)
    p.drawString(1*inch, height - 1*inch, "Reporte Gerencial de Operaciones")
    p.setFont("Helvetica", 10)
    p.drawString(1*inch, height - 1.25*inch, f"Periodo: {start_date} a {end_date}")
    p.drawString(1*inch, height - 1.4*inch, f"Generado el: {now_iso()}")
    
    # SLA Metrics
    y = height - 2*inch
    p.setFont("Helvetica-Bold", 12)
    p.drawString(1*inch, y, "Métricas de SLA")
    y -= 0.3*inch
    p.setFont("Helvetica", 10)
    for k, v in metrics['sla'].items():
        p.drawString(1.2*inch, y, f"- {k.capitalize()}: {v}")
        y -= 0.2*inch
        
    # Completion Metrics
    y -= 0.2*inch
    p.setFont("Helvetica-Bold", 12)
    p.drawString(1*inch, y, "Estado de Visitas")
    y -= 0.3*inch
    p.setFont("Helvetica", 10)
    for k, v in metrics['completion'].items():
        p.drawString(1.2*inch, y, f"- {k.capitalize()}: {v}")
        y -= 0.2*inch
        
    # Staff Productivity
    y -= 0.4*inch
    p.setFont("Helvetica-Bold", 12)
    p.drawString(1*inch, y, "Productividad por Funcionario")
    y -= 0.3*inch
    
    # Table Header
    p.setFont("Helvetica-Bold", 10)
    p.drawString(1*inch, y, "Funcionario")
    p.drawString(3.5*inch, y, "Asignadas")
    p.drawString(4.5*inch, y, "Completadas")
    p.drawString(5.5*inch, y, "SLA Breached")
    y -= 0.2*inch
    p.line(1*inch, y+0.05*inch, 6.5*inch, y+0.05*inch)
    
    p.setFont("Helvetica", 9)
    for s in metrics['staff']:
        if y < 1*inch: # New page if needed
            p.showPage()
            y = height - 1*inch
            p.setFont("Helvetica", 9)
            
        p.drawString(1*inch, y, s['full_name'][:30])
        p.drawString(3.5*inch, y, str(s['assigned']))
        p.drawString(4.5*inch, y, str(s['completed']))
        p.drawString(5.5*inch, y, str(s['breached']))
        y -= 0.2*inch

    p.showPage()
    p.save()
    
    buffer.seek(0)
    filename = f"reporte_gerencial_{start_date}_{end_date}.pdf"
    return send_file(buffer, as_attachment=True, download_name=filename, mimetype='application/pdf')
