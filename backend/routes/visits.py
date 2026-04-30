from flask import Blueprint, request, flash, redirect, url_for, render_template, jsonify, abort, make_response, send_file
import os
import json
import secrets
from io import BytesIO
from pathlib import Path
from werkzeug.utils import secure_filename
from backend.database_web import get_conn, UPLOAD_DIR
from backend.config import DEFAULT_CITY, DEFAULT_REGION
import backend.app as app_main
import backend.services.dataset_service as dataset_service
import backend.services.visit_service as visit_service
import backend.services.territory_service as territory_service
import backend.services.event_service as event_service

visits_bp = Blueprint("visits", __name__)

@visits_bp.route('/visits/all')
@app_main.login_required
def visits_view_all():
    return redirect(url_for('dashboard.dashboard'))


# Reemplazamos los decoradores y funciones importándolos de app_main o servicios
login_required = app_main.login_required
module_required = app_main.module_required
role_required = app_main.role_required
current_user = app_main.current_user
get_user_dataset = dataset_service.get_user_dataset
ensure_visit_columns = app_main.ensure_visit_columns
VISIT_MEDIA_DIR = app_main.VISIT_MEDIA_DIR
ALLOWED_EXTENSIONS = app_main.ALLOWED_EXTENSIONS
_load_any_dataset_version = dataset_service.load_any_dataset_version
build_visit_queue = app_main.build_visit_queue
summarize_visits = app_main.summarize_visits
_visit_records_for_dataset = visit_service.get_visit_records_for_dataset
validate_csrf = app_main.validate_csrf
_truthy = app_main._truthy
_save_uploaded_visit_file = app_main._save_uploaded_visit_file
_save_data_url_image = app_main._save_data_url_image
_signature_metadata = app_main._signature_metadata
_save_attachments_from_request = app_main._save_attachments_from_request
_visit_media_url = app_main._visit_media_url
_check_visit_conflict = app_main._check_visit_conflict
_record_visit_conflict = app_main._record_visit_conflict
_record_sync_event = app_main._record_sync_event
create_notification = event_service.create_notification
_notify_roles = app_main._notify_roles
apply_visit_update = app_main.apply_visit_update
get_active_layers = territory_service.get_active_layers
assign_territories_to_dataframe = territory_service.apply_territory_assignment
apply_quality_flags = app_main.apply_quality_flags
ALCALDIA_LAT = app_main.ALCALDIA_LAT
ALCALDIA_LON = app_main.ALCALDIA_LON
save_processed_dataset = dataset_service.save_processed_dataset
_persist_visit_record = visit_service.persist_visit_record
_persist_visit_attachments = app_main._persist_visit_attachments
now_iso = app_main.now_iso
_visit_record_by_row = visit_service.get_visit_record_by_row
_visit_context_from_row = app_main._visit_context_from_row
_build_external_url = app_main._build_external_url
build_visit_email_subject = app_main.build_visit_email_subject
build_visit_email_html = app_main.build_visit_email_html
build_visit_email_text = app_main.build_visit_email_text
send_html_email = app_main.send_html_email
log_audit = event_service.log_audit
_save_attachments_from_dataurls = app_main._save_attachments_from_dataurls
build_visit_whatsapp_url = app_main.build_visit_whatsapp_url
_attachments_for_visit = app_main._attachments_for_visit
_build_visit_pdf_bytes = app_main._build_visit_pdf_bytes
create_job = app_main.create_job
run_pdf_job_wrapper = app_main.run_pdf_job_wrapper
start_background_job = app_main.job_service.start_background_job

@visits_bp.route('/visits/<int:dataset_id>')
@login_required
@module_required('visits')
def visits_view(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        df = ensure_visit_columns(
            _load_any_dataset_version(
                dataset_id, user['id']))
    except FileNotFoundError:
        flash('Dataset no disponible', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    q = request.args.get('q', '')
    only_pending = request.args.get('only_pending', '0') == '1'
    queue = build_visit_queue(df, q=q, only_pending=only_pending, limit=500)
    visit_summary = summarize_visits(df)
    return render_template('visits.html', dataset=dataset, queue=queue, visit_summary=visit_summary,
                           visit_records=_visit_records_for_dataset(dataset_id), only_pending=only_pending, q=q, user=user)


@visits_bp.route('/visits/<int:dataset_id>/save', methods=['POST'])
@login_required
@module_required('visits')
def save_visit(dataset_id: int):
    validate_csrf()
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        row_idx = int(request.form.get('row_idx', '-1'))
        df = ensure_visit_columns(
            _load_any_dataset_version(
                dataset_id, user['id']))
    except Exception as exc:
        flash(f'No se pudo cargar el dataset: {exc}', 'danger')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    visita_fecha = request.form.get('visita_fecha', '')
    client_version = request.form.get('server_updated_at', '')
    force_overwrite = _truthy(request.form.get('force_overwrite', ''))
    payload = {
        'visita_estado': request.form.get('visita_estado', '') or 'REALIZADA',
        'visita_fecha': visita_fecha,
        'visita_hora': request.form.get('visita_hora', ''),
        'visita_funcionario': request.form.get('visita_funcionario', '') or request.form.get('rvt_funcionario_firma_nombre', ''),
        'visita_resultado': request.form.get('rvt_tipo_visita', ''),
        'visita_observaciones': request.form.get('visita_observaciones', ''),
        'rvt_tipo_visita': request.form.get('rvt_tipo_visita', ''),
        'rvt_codigo_establecimiento': request.form.get('rvt_codigo_establecimiento', ''),
        'rvt_recibe_nombre': request.form.get('rvt_recibe_nombre', ''),
        'rvt_recibe_tipo_documento': request.form.get('rvt_recibe_tipo_documento', ''),
        'rvt_recibe_numero_documento': request.form.get('rvt_recibe_numero_documento', ''),
        'rvt_recibe_cargo': request.form.get('rvt_recibe_cargo', ''),
        'rvt_razon_social': request.form.get('rvt_razon_social', ''),
        'rvt_nit_cc': request.form.get('rvt_nit_cc', ''),
        'rvt_avisos_tableros': request.form.get('rvt_avisos_tableros', ''),
        'rvt_direccion_establecimiento': request.form.get('rvt_direccion_establecimiento', ''),
        'rvt_direccion_cobro': request.form.get('rvt_direccion_cobro', ''),
        'rvt_municipio': request.form.get('rvt_municipio', ''),
        'rvt_departamento': request.form.get('rvt_departamento', ''),
        'rvt_municipio_cobro': request.form.get('rvt_municipio_cobro', ''),
        'rvt_departamento_cobro': request.form.get('rvt_departamento_cobro', ''),
        'rvt_telefono_movil': request.form.get('rvt_telefono_movil', ''),
        'rvt_telefono_fijo': request.form.get('rvt_telefono_fijo', ''),
        'rvt_correo_electronico': request.form.get('rvt_correo_electronico', ''),
        'rvt_sector_economico': request.form.get('rvt_sector_economico', ''),
        'rvt_fecha_inicio_actividades': request.form.get('rvt_fecha_inicio_actividades', ''),
        'rvt_codigo_ciiu_1': request.form.get('rvt_codigo_ciiu_1', ''),
        'rvt_codigo_ciiu_2': request.form.get('rvt_codigo_ciiu_2', ''),
        'rvt_descripcion_actividad': request.form.get('rvt_descripcion_actividad', ''),
        'rvt_rep_legal_a_nombre': request.form.get('rvt_rep_legal_a_nombre', ''),
        'rvt_rep_legal_a_identificacion': request.form.get('rvt_rep_legal_a_identificacion', ''),
        'rvt_rep_legal_a_correo': request.form.get('rvt_rep_legal_a_correo', ''),
        'rvt_rep_legal_b_nombre': request.form.get('rvt_rep_legal_b_nombre', ''),
        'rvt_rep_legal_b_identificacion': request.form.get('rvt_rep_legal_b_identificacion', ''),
        'rvt_rep_legal_b_correo': request.form.get('rvt_rep_legal_b_correo', ''),
        'rvt_firma_recibe_nombre': request.form.get('rvt_firma_recibe_nombre', ''),
        'rvt_firma_recibe_tipo_documento': request.form.get('rvt_firma_recibe_tipo_documento', ''),
        'rvt_firma_recibe_numero_documento': request.form.get('rvt_firma_recibe_numero_documento', ''),
        'rvt_funcionario_firma_nombre': request.form.get('rvt_funcionario_firma_nombre', ''),
        'visit_latitude': request.form.get('visit_latitude', ''),
        'visit_longitude': request.form.get('visit_longitude', ''),
        'visit_gps_accuracy': request.form.get('visit_gps_accuracy', ''),
        'visit_device': request.form.get('visit_device', ''),
        'visit_user_agent': request.form.get('visit_user_agent', ''),
        'correo_envio_destino': request.form.get('correo_envio_destino', ''),
        'visit_signature_receiver': request.form.get('visit_signature_receiver', ''),
        'visit_signature_receiver_stats': request.form.get('visit_signature_receiver_stats', ''),
        'visit_signature_officer': request.form.get('visit_signature_officer', ''),
        'visit_signature_officer_stats': request.form.get('visit_signature_officer_stats', ''),
        'deuda_estado': request.form.get('deuda_estado', ''),
        'deuda_monto': request.form.get('deuda_monto', ''),
        'deuda_referencia': request.form.get('deuda_referencia', ''),
        'deuda_fuente': request.form.get('deuda_fuente', 'visita_funcionario'),
        'deuda_fecha_revision': visita_fecha or now_iso()[:10],
    }
    photo_est = _save_uploaded_visit_file(
        request.files.get('visit_photo_establecimiento_file'),
        dataset_id,
        row_idx,
        'establecimiento')
    if not photo_est:
        photo_est = _save_data_url_image(
            request.form.get(
                'visit_photo_establecimiento_dataurl',
                ''),
            dataset_id,
            row_idx,
            'establecimiento')
    photo_doc = _save_uploaded_visit_file(request.files.get(
        'visit_photo_documento_file'), dataset_id, row_idx, 'documento')
    if not photo_doc:
        photo_doc = _save_data_url_image(
            request.form.get(
                'visit_photo_documento_dataurl',
                ''),
            dataset_id,
            row_idx,
            'documento')
    sig_receiver_meta = _signature_metadata(
        payload.get(
            'visit_signature_receiver', ''), payload.get(
            'rvt_firma_recibe_nombre', ''), payload, payload.get(
                'visit_signature_receiver_stats', ''))
    sig_officer_meta = _signature_metadata(
        payload.get(
            'visit_signature_officer', ''), payload.get(
            'rvt_funcionario_firma_nombre', '') or payload.get(
                'visita_funcionario', ''), payload, payload.get(
                    'visit_signature_officer_stats', ''))
    sig_receiver_file = _save_data_url_image(
        payload.get(
            'visit_signature_receiver',
            ''),
        dataset_id,
        row_idx,
        'firma_recibe')
    sig_officer_file = _save_data_url_image(
        payload.get(
            'visit_signature_officer',
            ''),
        dataset_id,
        row_idx,
        'firma_funcionario')
    attachment_items = _save_attachments_from_request(request.files.getlist(
        'visit_attachment_files'), dataset_id, row_idx, user['id'])
    if photo_est:
        payload['visit_photo_establecimiento'] = photo_est
    if photo_doc:
        payload['visit_photo_documento'] = photo_doc
    if sig_receiver_file:
        payload['visit_signature_receiver'] = _visit_media_url(
            sig_receiver_file)
        payload['visit_signature_receiver_hash'] = sig_receiver_meta.get(
            'hash', '')
        payload['visit_signature_receiver_signed_at'] = sig_receiver_meta.get(
            'signed_at', '')
        payload['visit_signature_receiver_signer'] = sig_receiver_meta.get(
            'signer', '')
        payload['visit_signature_receiver_stats'] = sig_receiver_meta.get(
            'stats', '')
    if sig_officer_file:
        payload['visit_signature_officer'] = _visit_media_url(sig_officer_file)
        payload['visit_signature_officer_hash'] = sig_officer_meta.get(
            'hash', '')
        payload['visit_signature_officer_signed_at'] = sig_officer_meta.get(
            'signed_at', '')
        payload['visit_signature_officer_signer'] = sig_officer_meta.get(
            'signer', '')
        payload['visit_signature_officer_stats'] = sig_officer_meta.get(
            'stats', '')
    try:
        conflict = _check_visit_conflict(
            dataset_id,
            row_idx,
            client_version,
            force_overwrite=force_overwrite)
        if conflict:
            conflict_id = _record_visit_conflict(
                dataset_id,
                row_idx,
                user['id'],
                client_version,
                conflict,
                payload,
                notes='save_visit_conflict')
            _record_sync_event(
                dataset_id,
                row_idx,
                user['id'],
                'web_save',
                'conflict',
                attempts=1,
                device_label=payload.get(
                    'visit_device',
                    ''),
                server_updated_at=str(
                    conflict.get(
                        'updated_at',
                        '') or ''),
                error_message='Conflicto detectado',
                payload={
                    'conflict_id': conflict_id})
            create_notification(
                int(
                    user['id']),
                'conflict',
                'Conflicto de visita detectado',
                f'Se detectó un conflicto en dataset {dataset_id}, fila {row_idx}.',
                url_for(
                    'conflict_detail',
                    conflict_id=conflict_id))
            _notify_roles(
                'revisor',
                'conflict',
                'Nuevo conflicto pendiente',
                f'Conflicto pendiente en dataset {dataset_id}, fila {row_idx}.',
                url_for(
                    'conflict_detail',
                    conflict_id=conflict_id))
            flash(
                'Conflicto detectado: otra versión de la visita fue guardada antes de esta. Revisa la última versión o usa sobrescribir desde la tablet.',
                'warning')
            return redirect(url_for(
                'visit_print_view', dataset_id=dataset_id, row_idx=row_idx, conflict='1'))
        # --- SOPORTE PARA REGISTROS NUEVOS (RVT DESDE CERO) ---
        is_new_entry = (row_idx >= len(df))
        
        if is_new_entry:
            # Crear una fila vacía con el esquema correcto
            new_row = {col: '' for col in df.columns}
            # Combinar con los datos del payload
            for key, val in payload.items():
                if key in new_row: new_row[key] = val
            
            # Anexar al dataframe
            updated_df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            result = type('obj', (object,), {'changed_fields': list(payload.keys())})
            row_idx = len(updated_df) - 1 # Actualizar el índice al nuevo elemento
        else:
            updated_df, result = apply_visit_update(df, row_idx=row_idx, payload=payload)
            
        active_layers = get_active_layers(dataset['city'], dataset['region'])
        if active_layers:
            updated_df = assign_territories_to_dataframe(
                updated_df, active_layers)
        updated_df = apply_quality_flags(
            updated_df,
            current_city=dataset['city'],
            center_lat=ALCALDIA_LAT,
            center_lon=ALCALDIA_LON)
        save_processed_dataset(dataset_id, user['id'], updated_df)
        visit_record_id = _persist_visit_record(
            dataset_id, row_idx, user['id'], payload)
        _persist_visit_attachments(
            dataset_id,
            row_idx,
            user['id'],
            attachment_items,
            visit_record_id=visit_record_id)
        _record_sync_event(
            dataset_id,
            row_idx,
            user['id'],
            'web_save',
            'synced',
            attempts=1,
            device_label=payload.get(
                'visit_device',
                ''),
            server_updated_at=now_iso(),
            payload={
                'changed_fields': result.changed_fields,
                'attachments': len(attachment_items)})
        _notify_roles(
            'revisor',
            'approval_workflow',
            'Visita enviada a revisión',
            f'Se registró una visita pendiente de revisión en dataset {dataset_id}, fila {row_idx}.',
            url_for('admin.approval_queue'))
        visit_record = _visit_record_by_row(dataset_id, row_idx)
        row_data = updated_df.iloc[row_idx].to_dict()
        visit_context = _visit_context_from_row(
            dict(dataset), row_idx, row_data, visit_record)
        print_url = _build_external_url(
            'visits.visit_print_view',
            dataset_id=dataset_id,
            row_idx=row_idx)
            
        # --- LÓGICA DE CORREO AUTOMÁTICO CON PDF ADJUNTO ---
        email_to = (payload.get('rvt_correo_electronico', '') 
                    or payload.get('correo_envio_destino', '')
                    or user.get('email') or '').strip()
                    
        email_sent = False
        email_error = ''
        
        tipo_visita = payload.get('rvt_tipo_visita', '').strip()
        is_new_establishment = (tipo_visita == 'Nuevo Establecimiento' or is_new_entry)
        
        if is_new_establishment and email_to:
            subject = build_visit_email_subject(visit_context)
            html_body = build_visit_email_html(visit_context, print_url)
            text_body = build_visit_email_text(visit_context, print_url)
            
            # Generar el PDF para adjuntar
            try:
                # Asegurar que los anexos estén en el contexto para el PDF
                visit_context['attachments'] = _attachments_for_visit(dataset_id, row_idx)
                pdf_bytes = _build_visit_pdf_bytes(dict(dataset), visit_context)
                pdf_filename = f"RVT_{row_idx}_{payload.get('rvt_razon_social', 'establecimiento')}.pdf"
                pdf_filename = secure_filename(pdf_filename)
                
                email_sent, email_error = send_html_email(
                    email_to, subject, html_body, text_body,
                    attachment_data=pdf_bytes,
                    attachment_filename=pdf_filename
                )
            except Exception as pdf_exc:
                email_error = f"Error generando PDF: {pdf_exc}"
                # Fallback: enviar sin adjunto si falla el PDF
                email_sent, email_error = send_html_email(email_to, subject, html_body, text_body)

            log_audit(
                user['id'],
                'visit_email_attempt',
                'visit_record',
                f'{dataset_id}:{row_idx}',
                dataset_id=dataset_id,
                row_idx=row_idx,
                details={
                    'email_to': email_to,
                    'sent': email_sent,
                    'error': email_error,
                    'has_pdf': True})
        elif email_to and not is_new_entry:
            logging.info(f"Saltando correo automático para fila {row_idx} (no es nuevo establecimiento)")
        
        log_audit(
            user['id'],
            'visit_update',
            'visit_record',
            f'{dataset_id}:{row_idx}',
            dataset_id=dataset_id,
            row_idx=row_idx,
            details={
                'changed_fields': result.changed_fields,
                'email_to': email_to,
                'email_sent': email_sent})
        flash('Visita y validación de deuda guardadas.', 'success')
        if email_to and email_sent:
            flash(f'Se envió una copia al correo {email_to}.', 'info')
        elif email_to and not email_sent:
            flash(
                f'No se pudo enviar el correo automático: {
                    email_error or "SMTP no configurado"}',
                'warning')
        return redirect(url_for('visits.visit_print_view', dataset_id=dataset_id, row_idx=row_idx,
                        saved='1', emailed='1' if email_sent else '0', email_to=email_to if email_sent else ''))
    except Exception as exc:
        flash(f'No se pudo guardar la visita: {exc}', 'danger')
    return redirect(url_for('visits.visits_view', dataset_id=dataset_id))


def _normalize_sync_payload(data: dict, user: dict) -> dict:
    visita_fecha = data.get('visita_fecha', '')
    payload = {
        'visita_estado': data.get('visita_estado', '') or 'REALIZADA',
        'visita_fecha': visita_fecha,
        'visita_hora': data.get('visita_hora', ''),
        'visita_funcionario': data.get('visita_funcionario', '') or data.get('rvt_funcionario_firma_nombre', '') or user.get('full_name', ''),
        'visita_resultado': data.get('rvt_tipo_visita', ''),
        'visita_observaciones': data.get('visita_observaciones', ''),
        'rvt_tipo_visita': data.get('rvt_tipo_visita', ''),
        'rvt_codigo_establecimiento': data.get('rvt_codigo_establecimiento', ''),
        'rvt_recibe_nombre': data.get('rvt_recibe_nombre', ''),
        'rvt_recibe_tipo_documento': data.get('rvt_recibe_tipo_documento', ''),
        'rvt_recibe_numero_documento': data.get('rvt_recibe_numero_documento', ''),
        'rvt_recibe_cargo': data.get('rvt_recibe_cargo', ''),
        'rvt_razon_social': data.get('rvt_razon_social', ''),
        'rvt_nit_cc': data.get('rvt_nit_cc', ''),
        'rvt_avisos_tableros': data.get('rvt_avisos_tableros', ''),
        'rvt_direccion_establecimiento': data.get('rvt_direccion_establecimiento', ''),
        'rvt_direccion_cobro': data.get('rvt_direccion_cobro', ''),
        'rvt_municipio': data.get('rvt_municipio', ''),
        'rvt_departamento': data.get('rvt_departamento', ''),
        'rvt_municipio_cobro': data.get('rvt_municipio_cobro', ''),
        'rvt_departamento_cobro': data.get('rvt_departamento_cobro', ''),
        'rvt_telefono_movil': data.get('rvt_telefono_movil', ''),
        'rvt_telefono_fijo': data.get('rvt_telefono_fijo', ''),
        'rvt_correo_electronico': data.get('rvt_correo_electronico', ''),
        'rvt_sector_economico': data.get('rvt_sector_economico', ''),
        'rvt_fecha_inicio_actividades': data.get('rvt_fecha_inicio_actividades', ''),
        'rvt_codigo_ciiu_1': data.get('rvt_codigo_ciiu_1', ''),
        'rvt_codigo_ciiu_2': data.get('rvt_codigo_ciiu_2', ''),
        'rvt_descripcion_actividad': data.get('rvt_descripcion_actividad', ''),
        'rvt_rep_legal_a_nombre': data.get('rvt_rep_legal_a_nombre', ''),
        'rvt_rep_legal_a_identificacion': data.get('rvt_rep_legal_a_identificacion', ''),
        'rvt_rep_legal_a_correo': data.get('rvt_rep_legal_a_correo', ''),
        'rvt_rep_legal_b_nombre': data.get('rvt_rep_legal_b_nombre', ''),
        'rvt_rep_legal_b_identificacion': data.get('rvt_rep_legal_b_identificacion', ''),
        'rvt_rep_legal_b_correo': data.get('rvt_rep_legal_b_correo', ''),
        'rvt_firma_recibe_nombre': data.get('rvt_firma_recibe_nombre', ''),
        'rvt_firma_recibe_tipo_documento': data.get('rvt_firma_recibe_tipo_documento', ''),
        'rvt_firma_recibe_numero_documento': data.get('rvt_firma_recibe_numero_documento', ''),
        'rvt_funcionario_firma_nombre': data.get('rvt_funcionario_firma_nombre', '') or user.get('full_name', ''),
        'visit_latitude': data.get('visit_latitude', ''),
        'visit_longitude': data.get('visit_longitude', ''),
        'visit_gps_accuracy': data.get('visit_gps_accuracy', ''),
        'visit_device': data.get('visit_device', ''),
        'visit_user_agent': data.get('visit_user_agent', ''),
        'correo_envio_destino': data.get('correo_envio_destino', ''),
        'visit_signature_receiver': data.get('visit_signature_receiver', ''),
        'visit_signature_receiver_stats': data.get('visit_signature_receiver_stats', ''),
        'visit_signature_officer': data.get('visit_signature_officer', ''),
        'visit_signature_officer_stats': data.get('visit_signature_officer_stats', ''),
        'deuda_estado': data.get('deuda_estado', ''),
        'deuda_monto': data.get('deuda_monto', ''),
        'deuda_referencia': data.get('deuda_referencia', ''),
        'deuda_fuente': data.get('deuda_fuente', 'visita_funcionario'),
        'deuda_fecha_revision': visita_fecha or now_iso()[:10],
    }
    return payload


@visits_bp.route('/api/visits/<int:dataset_id>/sync_save', methods=['POST'])
@login_required
def api_visit_sync_save(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    data = request.get_json(silent=True) or {}
    try:
        row_idx = int(data.get('row_idx', -1))
        df = ensure_visit_columns(
            _load_any_dataset_version(
                dataset_id, user['id']))
        client_version = data.get('server_updated_at', '')
        force_overwrite = _truthy(data.get('force_overwrite', ''))
        conflict = _check_visit_conflict(
            dataset_id,
            row_idx,
            client_version,
            force_overwrite=force_overwrite)
        if conflict:
            conflict_id = _record_visit_conflict(
                dataset_id,
                row_idx,
                user['id'],
                client_version,
                conflict,
                data,
                notes='api_sync_conflict')
            _record_sync_event(
                dataset_id, row_idx, user['id'], 'offline_sync', 'conflict', attempts=int(
                    data.get('attempts') or 0), device_label=str(
                    data.get('visit_device') or ''), server_updated_at=str(
                    conflict.get(
                        'updated_at', '') or ''), error_message='Conflicto detectado', payload={
                        'conflict_id': conflict_id})
            create_notification(
                int(
                    user['id']),
                'conflict',
                'Conflicto al sincronizar visita',
                f'Se detectó un conflicto en dataset {dataset_id}, fila {row_idx}.',
                url_for(
                    'conflict_detail',
                    conflict_id=conflict_id))
            _notify_roles(
                'revisor',
                'conflict',
                'Nuevo conflicto pendiente',
                f'Conflicto pendiente en dataset {dataset_id}, fila {row_idx}.',
                url_for(
                    'conflict_detail',
                    conflict_id=conflict_id))
            return jsonify({'ok': False, 'error': 'conflict', 'conflict': True, 'server_updated_at': conflict.get(
                'updated_at', ''), 'server_record': conflict, 'conflict_id': conflict_id}), 409
        payload = _normalize_sync_payload(data, user)
        photo_est = _save_data_url_image(
            data.get(
                'visit_photo_establecimiento_dataurl',
                ''),
            dataset_id,
            row_idx,
            'establecimiento')
        photo_doc = _save_data_url_image(
            data.get(
                'visit_photo_documento_dataurl',
                ''),
            dataset_id,
            row_idx,
            'documento')
        attachment_items = _save_attachments_from_dataurls(
            data.get('visit_attachments_dataurls') or [], dataset_id, row_idx, user['id'])
        sig_receiver_meta = _signature_metadata(
            payload.get(
                'visit_signature_receiver', ''), payload.get(
                'rvt_firma_recibe_nombre', ''), payload, payload.get(
                'visit_signature_receiver_stats', ''))
        sig_officer_meta = _signature_metadata(
            payload.get(
                'visit_signature_officer', ''), payload.get(
                'rvt_funcionario_firma_nombre', '') or payload.get(
                'visita_funcionario', ''), payload, payload.get(
                    'visit_signature_officer_stats', ''))
        sig_receiver_file = _save_data_url_image(
            payload.get(
                'visit_signature_receiver',
                ''),
            dataset_id,
            row_idx,
            'firma_recibe')
        sig_officer_file = _save_data_url_image(
            payload.get(
                'visit_signature_officer',
                ''),
            dataset_id,
            row_idx,
            'firma_funcionario')
        if photo_est:
            payload['visit_photo_establecimiento'] = photo_est
        if photo_doc:
            payload['visit_photo_documento'] = photo_doc
        if sig_receiver_file:
            payload['visit_signature_receiver'] = _visit_media_url(
                sig_receiver_file)
            payload['visit_signature_receiver_hash'] = sig_receiver_meta.get(
                'hash', '')
            payload['visit_signature_receiver_signed_at'] = sig_receiver_meta.get(
                'signed_at', '')
            payload['visit_signature_receiver_signer'] = sig_receiver_meta.get(
                'signer', '')
        payload['visit_signature_receiver_stats'] = sig_receiver_meta.get(
            'stats', '')
        if sig_officer_file:
            payload['visit_signature_officer'] = _visit_media_url(
                sig_officer_file)
            payload['visit_signature_officer_hash'] = sig_officer_meta.get(
                'hash', '')
            payload['visit_signature_officer_signed_at'] = sig_officer_meta.get(
                'signed_at', '')
            payload['visit_signature_officer_signer'] = sig_officer_meta.get(
                'signer', '')
        payload['visit_signature_officer_stats'] = sig_officer_meta.get(
            'stats', '')
        # --- SOPORTE PARA REGISTROS NUEVOS (RVT DESDE CERO EN SYNC) ---
        is_new_entry = (row_idx >= len(df))
        
        if is_new_entry:
            # Crear una fila vacía con el esquema correcto
            new_row = {col: '' for col in df.columns}
            # Combinar con los datos del payload
            for key, val in payload.items():
                if key in new_row: new_row[key] = val
            
            # Anexar al dataframe
            updated_df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            result = type('obj', (object,), {'changed_fields': list(payload.keys())})
            row_idx = len(updated_df) - 1 # Actualizar el índice al nuevo elemento
        else:
            updated_df, result = apply_visit_update(df, row_idx=row_idx, payload=payload)
        active_layers = get_active_layers(dataset['city'], dataset['region'])
        if active_layers:
            updated_df = assign_territories_to_dataframe(
                updated_df, active_layers)
        updated_df = apply_quality_flags(
            updated_df,
            current_city=dataset['city'],
            center_lat=ALCALDIA_LAT,
            center_lon=ALCALDIA_LON)
        save_processed_dataset(dataset_id, user['id'], updated_df)
        visit_record_id = _persist_visit_record(
            dataset_id, row_idx, user['id'], payload)
        _persist_visit_attachments(
            dataset_id,
            row_idx,
            user['id'],
            attachment_items,
            visit_record_id=visit_record_id)
        _record_sync_event(
            dataset_id, row_idx, user['id'], 'offline_sync', 'synced', attempts=int(
                data.get('attempts') or 1), device_label=payload.get(
                'visit_device', ''), server_updated_at=now_iso(), payload={
                'changed_fields': result.changed_fields, 'attachments': len(attachment_items)})
        _notify_roles(
            'revisor',
            'approval_workflow',
            'Visita sincronizada pendiente de revisión',
            f'Se sincronizó una visita pendiente de revisión en dataset {dataset_id}, fila {row_idx}.',
            url_for('admin.approval_queue'))
        visit_record = _visit_record_by_row(dataset_id, row_idx)
        row_data = updated_df.iloc[row_idx].to_dict()
        visit_context = _visit_context_from_row(
            dict(dataset), row_idx, row_data, visit_record)
        print_url = _build_external_url(
            'visits.visit_print_view',
            dataset_id=dataset_id,
            row_idx=row_idx)
            
        # --- LÓGICA DE CORREO AUTOMÁTICO CON PDF ADJUNTO (SOLO PARA NUEVOS) ---
        email_to = (payload.get('rvt_correo_electronico', '') 
                    or payload.get('correo_envio_destino', '')
                    or user.get('email') or '').strip()
        
        email_sent = False
        email_error = ''
        
        tipo_visita = payload.get('rvt_tipo_visita', '').strip()
        is_new_establishment = (tipo_visita == 'Nuevo Establecimiento' or is_new_entry)
        
        if is_new_establishment and email_to:
            subject = build_visit_email_subject(visit_context)
            html_body = build_visit_email_html(visit_context, print_url)
            text_body = build_visit_email_text(visit_context, print_url)
            
            try:
                # Adjuntar PDF
                visit_context['attachments'] = _attachments_for_visit(dataset_id, row_idx)
                pdf_bytes = _build_visit_pdf_bytes(dict(dataset), visit_context)
                pdf_filename = f"RVT_{row_idx}_{payload.get('rvt_razon_social', 'establecimiento')}.pdf"
                pdf_filename = secure_filename(pdf_filename)
                
                email_sent, email_error = send_html_email(
                    email_to, subject, html_body, text_body,
                    attachment_data=pdf_bytes,
                    attachment_filename=pdf_filename
                )
            except Exception as pdf_exc:
                email_error = f"Error PDF Sync: {pdf_exc}"
                email_sent, email_error = send_html_email(email_to, subject, html_body, text_body)
        elif email_to and not is_new_entry:
            logging.info(f"Saltando correo sync para fila {row_idx} (no es nuevo)")
        log_audit(
            user['id'],
            'visit_sync_update',
            'visit_record',
            f'{dataset_id}:{row_idx}',
            dataset_id=dataset_id,
            row_idx=row_idx,
            details={
                'changed_fields': result.changed_fields,
                'email_to': email_to,
                'email_sent': email_sent})
        return jsonify({'ok': True, 'row_idx': row_idx, 'print_url': url_for('visits.visit_print_view', dataset_id=dataset_id, row_idx=row_idx),
                       'email_sent': email_sent, 'email_error': email_error, 'server_updated_at': (_visit_record_by_row(dataset_id, row_idx) or {}).get('updated_at', '')})
    except Exception as exc:
        return jsonify({'ok': False, 'error': str(exc)}), 400


@visits_bp.route('/visits/<int:dataset_id>/print/<int:row_idx>')
@login_required
def visit_print_view(dataset_id: int, row_idx: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        df = ensure_visit_columns(
            _load_any_dataset_version(
                dataset_id, user['id']))
    except Exception as exc:
        flash(f'No se pudo cargar el dataset: {exc}', 'danger')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    if row_idx < 0 or row_idx >= len(df):
        flash('Fila no válida para imprimir', 'danger')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    visit_record = _visit_record_by_row(dataset_id, row_idx)
    if not visit_record:
        flash('No existe un RVT guardado para esta fila', 'warning')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    row_data = df.iloc[row_idx].to_dict()
    visit_context = _visit_context_from_row(
        dict(dataset), row_idx, row_data, visit_record)
    print_url = _build_external_url(
        'visit_print_view',
        dataset_id=dataset_id,
        row_idx=row_idx)
    whatsapp_url = build_visit_whatsapp_url(visit_context, print_url)
    return render_template('visit_print.html', dataset=dataset, visit=visit_context, whatsapp_url=whatsapp_url, saved=request.args.get(
        'saved') == '1', emailed=request.args.get('emailed') == '1', emailed_to=request.args.get('email_to', ''))


@visits_bp.route('/visits/<int:dataset_id>/print/<int:row_idx>/download.html')
@login_required
def visit_print_download(dataset_id: int, row_idx: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    df = ensure_visit_columns(
        _load_any_dataset_version(
            dataset_id, user['id']))
    if row_idx < 0 or row_idx >= len(df):
        flash('Fila no válida para descargar', 'danger')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    visit_record = _visit_record_by_row(dataset_id, row_idx)
    if not visit_record:
        flash('No existe un RVT guardado para esta fila', 'warning')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    row_data = df.iloc[row_idx].to_dict()
    visit_context = _visit_context_from_row(
        dict(dataset), row_idx, row_data, visit_record)
    whatsapp_url = build_visit_whatsapp_url(
        visit_context,
        _build_external_url(
            'visit_print_view',
            dataset_id=dataset_id,
            row_idx=row_idx))
    html = render_template(
        'visit_print.html',
        dataset=dataset,
        visit=visit_context,
        whatsapp_url=whatsapp_url,
        saved=False,
        emailed=False,
        emailed_to='')
    response = make_response(html)
    response.headers['Content-Type'] = 'text/html; charset=utf-8'
    response.headers[
        'Content-Disposition'] = f'attachment; filename=RVT_dataset_{dataset_id}_fila_{row_idx}.html'
    return response


@visits_bp.route('/visits/<int:dataset_id>/print/<int:row_idx>/download.pdf')
@login_required
def visit_print_pdf(dataset_id: int, row_idx: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    df = ensure_visit_columns(
        _load_any_dataset_version(
            dataset_id, user['id']))
    if row_idx < 0 or row_idx >= len(df):
        flash('Fila no válida para PDF', 'danger')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    visit_record = _visit_record_by_row(dataset_id, row_idx)
    if not visit_record:
        flash('No existe un RVT guardado para esta fila', 'warning')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    row_data = df.iloc[row_idx].to_dict()
    visit_context = _visit_context_from_row(
        dict(dataset), row_idx, row_data, visit_record)
    visit_context['attachments'] = _attachments_for_visit(dataset_id, row_idx)
    
    is_async = request.args.get('async') == '1'
    if is_async:
        job_id = create_job(user['id'], dataset_id, 'GENERATE_PDF', {
            'row_idx': row_idx,
            'filename': dataset.get('original_filename')
        })
        start_background_job(job_id, run_pdf_job_wrapper, (job_id, user['id'], dataset_id, row_idx))
        flash('Generación de PDF iniciada en segundo plano. Recibirás una notificación cuando esté listo.', 'info')
        return redirect(url_for('visits.visit_print_view', dataset_id=dataset_id, row_idx=row_idx))

    pdf_bytes = _build_visit_pdf_bytes(dict(dataset), visit_context)
    return send_file(BytesIO(pdf_bytes), mimetype='application/pdf', as_attachment=True,
                     download_name=f'RVT_dataset_{dataset_id}_fila_{row_idx}.pdf')


@visits_bp.route('/visit_media/<path:filename>')
@login_required
def visit_media(filename: str):
    safe_name = Path(filename).name
    path = VISIT_MEDIA_DIR / safe_name
    if not path.exists():
        abort(404)
    return send_file(path)
