from flask import Blueprint, request, flash, redirect, url_for, jsonify, render_template, send_file
import os  
import secrets
from pathlib import Path
from werkzeug.utils import secure_filename
from backend.database_web import get_conn, UPLOAD_DIR, EXPORT_DIR
from backend.config import DEFAULT_CITY, DEFAULT_REGION, DEFAULT_PROVIDER
import threading
import json
import logging

import backend.app as app_main
import backend.services.dataset_service as dataset_service
import backend.services.event_service as event_service
import backend.services.territory_service as territory_service
import backend.services.job_service as job_service

login_required = app_main.login_required
module_required = app_main.module_required
validate_csrf = app_main.validate_csrf
current_user = app_main.current_user
get_user_dataset = dataset_service.get_user_dataset
load_dataset = dataset_service.load_dataset
now_df = dataset_service.now_df
ensure_visit_columns = app_main.ensure_visit_columns
_load_any_dataset_version = dataset_service.load_any_dataset_version
merge_debt_dataframe = app_main.merge_debt_dataframe
save_processed_dataset = dataset_service.save_processed_dataset
log_audit = event_service.log_audit
_match_previous_dataset = app_main._match_previous_dataset
compare_dataset_frames = app_main.compare_dataset_frames
dataset_summary = app_main.dataset_summary
summarize_visits = app_main.summarize_visits
build_visit_queue = app_main.build_visit_queue
processed_path_for = dataset_service.processed_path_for
load_processed_dataset = dataset_service.load_processed_dataset
to_geojson = app_main.to_geojson
to_kml = app_main.to_kml
to_excel_report = app_main.to_excel_report
ALLOWED_EXTENSIONS = app_main.ALLOWED_EXTENSIONS
dataframe_profile = app_main.dataframe_profile
deduplicate_against_history = app_main.deduplicate_against_history
validate_dataset_schema = dataset_service.validate_dataset_schema
run_process_job = app_main.run_process_job
create_job = job_service.create_job
get_job = job_service.get_job
_recent_jobs_for_user = app_main._recent_jobs_for_user
detect_address_columns = dataset_service.detect_address_columns
get_active_layers = territory_service.get_active_layers
process_dataset = app_main.process_dataset
reverse_geocode_dataset = app_main.reverse_geocode_dataset
now_iso = app_main.now_iso
JOB_THREADS = app_main.JOB_THREADS

datasets_bp = Blueprint('datasets', __name__)

@datasets_bp.route('/upload', methods=['POST'])
def upload():
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))

    file = request.files.get('file')
    provider = request.form.get('provider', os.getenv('GEOCODING_PROVIDER', 'nominatim')).strip()
    city = request.form.get('city', DEFAULT_CITY).strip() or DEFAULT_CITY
    region = request.form.get('region', DEFAULT_REGION).strip() or DEFAULT_REGION
    
    if not file or not file.filename:
        flash('Debes seleccionar un archivo CSV o Excel', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        flash('Formato no soportado. Usa CSV o Excel.', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    stored = f"{secrets.token_hex(8)}_{secure_filename(file.filename)}"
    path = UPLOAD_DIR / stored
    file.save(path)

    try:
        loaded = now_df(path)
        
        loaded, dropped = app_main.deduplicate_against_history(user['id'], loaded)
        
        if dropped > 0:
            if len(loaded) == 0:
                path.unlink(missing_ok=True)
                flash('Todas las filas del archivo ya existen en tus datasets anteriores. No se subió nada.', 'warning')
                return redirect(url_for('dashboard.dashboard'))
                
            stored = f"{secrets.token_hex(8)}_deduped.csv"
            path.unlink(missing_ok=True)
            path = UPLOAD_DIR / stored
            loaded.to_csv(path, index=False, encoding='utf-8-sig')
            
            flash(f'Se descartaron {dropped} filas porque ya existían en el historial (misma Cédula y Matrícula).', 'info')
            
        row_count = len(loaded)
        profile = dataframe_profile(loaded)
        schema_report = validate_dataset_schema(loaded)
        
        if not schema_report['is_valid']:
            flash(f"Aviso: El dataset tiene problemas de integridad. Faltan columnas: {', '.join(schema_report['missing_columns'])}", 'warning')
        
        with get_conn() as conn:
            potential_dupes = conn.execute(
                'SELECT id, stored_filename FROM datasets WHERE user_id=? AND original_filename=? AND row_count=?',
                (user['id'], file.filename, row_count)
            ).fetchall()
            
        for dupe in potential_dupes:
            dupe_path = UPLOAD_DIR / dupe['stored_filename']
            if dupe_path.exists():
                try:
                    dupe_df = now_df(dupe_path)
                    if loaded.equals(dupe_df):
                        path.unlink(missing_ok=True)
                        flash('Este archivo ya fue cargado previamente y es idéntico a un dataset existente. No se guardaron duplicados.', 'info')
                        return redirect(url_for('datasets.review', dataset_id=dupe['id']))
                except Exception:
                    pass
    except Exception as exc:
        path.unlink(missing_ok=True)
        flash(f'No se pudo leer el archivo: {exc}', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    # Automatizar Cruce Territorial si hay capas activas
    active_layers = get_active_layers(city, region)
    if active_layers:
        loaded = app_main.assign_territories_to_dataframe(loaded, active_layers)
        # Guardar el archivo enriquecido
        stored = f"{secrets.token_hex(8)}_enriched.csv"
        path.unlink(missing_ok=True)
        path = UPLOAD_DIR / stored
        loaded.to_csv(path, index=False, encoding='utf-8-sig')
        flash(f'Enriquecimiento territorial automático aplicado usando {len(active_layers)} capas.', 'info')

    # --- LÓGICA DE DATASET MAESTRO ÚNICO ---
    with get_conn() as conn:
        master = conn.execute(
            'SELECT id, stored_filename, row_count FROM datasets WHERE original_filename = "MAESTRO_UNIFICADO" AND user_id = ?',
            (user['id'],)
        ).fetchone()

    if master:
        dataset_id = master['id']
        master_path = UPLOAD_DIR / master['stored_filename']
        
        # Cargar maestro actual
        if master_path.exists():
            master_df = now_df(master_path)
            # Combinar y eliminar duplicados internos
            combined = pd.concat([master_df, loaded], ignore_index=True)
            # Detectar columnas de nit/matricula para deduplicar el maestro
            cedula_col = dataset_service.find_col(combined, ['cedula', 'cédula', 'nit', 'cc', 'identificacion', 'identificación', 'documento'])
            matricula_col = dataset_service.find_col(combined, ['matricula', 'matrícula', 'mat', 'placa'])
            
            if cedula_col and matricula_col:
                combined.drop_duplicates(subset=[cedula_col, matricula_col], keep='first', inplace=True)
            
            new_row_count = len(combined)
            combined.to_csv(master_path, index=False, encoding='utf-8-sig')
            
            with get_conn() as conn:
                conn.execute(
                    'UPDATE datasets SET row_count=?, created_at=? WHERE id=?',
                    (new_row_count, now_iso(), dataset_id)
                )
            flash(f"Datos anexados al Maestro Unificado. Nuevo total: {new_row_count} registros.", 'success')
        else:
            # Si el registro existe pero el archivo no (raro), lo creamos de nuevo
            loaded.to_csv(master_path, index=False, encoding='utf-8-sig')
            flash("Archivo maestro restaurado y actualizado.", 'info')
    else:
        # Crear el Maestro por primera vez
        stored = f"master_{user['id']}_{secrets.token_hex(4)}.csv"
        path = UPLOAD_DIR / stored
        loaded.to_csv(path, index=False, encoding='utf-8-sig')
        
        with get_conn() as conn:
            cur = conn.execute(
                'INSERT INTO datasets (user_id, original_filename, stored_filename, provider, city, region, row_count, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (user['id'], "MAESTRO_UNIFICADO", stored, provider, city, region, len(loaded), now_iso()),
            )
            dataset_id = cur.lastrowid
        flash(f"Dataset Maestro Unificado creado con {len(loaded)} registros.", 'success')

    # Limpiar archivos temporales
    if 'temp_path' in locals() and temp_path.exists():
        temp_path.unlink(missing_ok=True)
    
    # Indexar para búsquedas rápidas
    dataset_service.register_dataset_establishments(dataset_id, user['id'], loaded)
    
    # Limpiar caches
    dataset_service.RAW_DATASET_CACHE.pop(dataset_service._cache_key(dataset_id, user['id']), None)
    dataset_service.PROCESSED_DATASET_CACHE.pop(dataset_service._cache_key(dataset_id, user['id']), None)
    
    return redirect(url_for('datasets.review', dataset_id=dataset_id))


@datasets_bp.route('/review/<int:dataset_id>')
def review(dataset_id: int):
    # auth simulation
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
        
    try:
        df = load_dataset(dataset_id, user['id'])
    except FileNotFoundError as e:
        flash(str(e), 'danger')
        return redirect(url_for('dashboard.dashboard'))

    columns = list(df.columns)
    suggested_address_cols = detect_address_columns(df)
    profile = dataframe_profile(df)
    schema_report = validate_dataset_schema(df)
    layers = get_active_layers(dataset['city'], dataset['region'])
    
    return render_template(
        'review.html',
        dataset=dataset,
        columns=columns,
        suggested_address_cols=suggested_address_cols,
        profile=profile,
        schema_report=schema_report,
        layers=layers,
    )


@datasets_bp.route('/process/<int:dataset_id>', methods=['POST'])
def process(dataset_id: int):
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404

    payload = request.get_json(force=True) or {}
    address_col = payload.get('address_col')
    api_key = payload.get('api_key', '')
    if not api_key and dataset['provider'] == 'google': api_key = os.getenv('GOOGLE_GEOCODING_API_KEY', '')
    elif not api_key and dataset['provider'] == 'locationiq': api_key = os.getenv('LOCATIONIQ_API_KEY', '')
    
    activity_col = payload.get('activity_col', '')
    if not address_col: return jsonify({'ok': False, 'error': 'Debes seleccionar la columna de dirección'}), 400

    df = load_dataset(dataset_id, user['id'])
    processed = process_dataset(
        df, address_col, dataset['provider'], api_key, dataset['city'], dataset['region'], activity_col=activity_col
    )
    out_path = save_processed_dataset(dataset_id, user['id'], processed)
    summary = dataset_summary(processed)

    with get_conn() as conn:
        run_cur = conn.execute(
            'INSERT INTO runs (dataset_id, user_id, status, provider, total_rows, ok_rows, exportable_rows, manual_rows, duplicate_rows, notes, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (
                dataset_id, user['id'], 'DONE', dataset['provider'], int(len(processed)),
                int((processed['estado_geo'] == 'OK').sum()),
                int(processed['selected_for_export'].fillna(False).sum()),
                int((processed['estado_geo'] == 'EDITADO_MANUALMENTE').sum()),
                int((processed['coord_duplicate_drop'].fillna(False).sum()) if 'coord_duplicate_drop' in processed.columns else 0),
                json.dumps({'active_layers': [layer['display_name'] for layer in get_active_layers(dataset['city'], dataset['region'])]}, ensure_ascii=False),
                now_iso(),
            ),
        )
        run_id = run_cur.lastrowid
        
    log_audit(user['id'], 'process_sync', 'dataset', dataset_id, dataset_id=dataset_id, details={'run_id': run_id, 'summary': summary})
    return jsonify({
        'ok': True, 'run_id': run_id,
        'preview': processed.head(200).fillna('').to_dict(orient='records'),
        'export_path': out_path.name, 'summary': summary,
    })


@datasets_bp.route('/process_async/<int:dataset_id>', methods=['POST'])
def process_async(dataset_id: int):
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    
    payload = request.get_json(force=True) or {}
    address_col = payload.get('address_col')
    api_key = payload.get('api_key', '')
    if not api_key and dataset['provider'] == 'google': api_key = os.getenv('GOOGLE_GEOCODING_API_KEY', '')
    elif not api_key and dataset['provider'] == 'locationiq': api_key = os.getenv('LOCATIONIQ_API_KEY', '')
    
    activity_col = payload.get('activity_col', '')
    if not address_col: return jsonify({'ok': False, 'error': 'Debes seleccionar la columna de dirección'}), 400
    
    job_id = create_job(user['id'], dataset_id, 'process_dataset', payload={'address_col': address_col, 'activity_col': activity_col})
    
    job_service.start_background_job(
        job_id=job_id,
        target=run_process_job,
        args=(
            job_id, dataset_id, user,
            address_col, dataset['provider'],
            api_key, dataset['city'], dataset['region'], activity_col
        )
    )
    
    log_audit(user['id'], 'process_async_started', 'dataset', dataset_id, dataset_id=dataset_id, details={'job_id': job_id})
    return jsonify({'ok': True, 'job_id': job_id})

@datasets_bp.route('/jobs')
def jobs_view():
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    return render_template('jobs.html', jobs=_recent_jobs_for_user(user['id'], limit=50))

@datasets_bp.route('/jobs/<int:job_id>/status')
def job_status(job_id: int):
    user = current_user()
    if not user: return jsonify({'ok': False, 'error': 'No autorizado'}), 401
    
    job = get_job(job_id, user['id'])
    if not job: return jsonify({'ok': False, 'error': 'Job no encontrado'}), 404
    return jsonify({'ok': True, 'job': job})

@datasets_bp.route('/cache/import', methods=['POST'])
def import_cache():
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    file = request.files.get('cache_file')
    city = request.form.get('city', DEFAULT_CITY).strip() or DEFAULT_CITY
    region = request.form.get('region', DEFAULT_REGION).strip() or DEFAULT_REGION
    provider = request.form.get('provider', DEFAULT_PROVIDER).strip() or DEFAULT_PROVIDER

    if not file or not file.filename:
        flash('Debes seleccionar un archivo histórico para importar.', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        flash('Formato no soportado para la cache. Usa CSV o Excel.', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    temp_path = UPLOAD_DIR / f'cache_{secrets.token_hex(8)}_{secure_filename(file.filename)}'
    file.save(temp_path)
    try:
        df = now_df(temp_path)
        from backend.app import import_cache_from_dataframe
        imported = import_cache_from_dataframe(df, city=city, region=region, provider=provider)
    except Exception as exc:
        flash(f'No se pudo importar la cache histórica: {exc}', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    finally:
        temp_path.unlink(missing_ok=True)

    flash(f'Cache histórica importada correctamente. Filas útiles: {imported}.', 'success')
    return redirect(url_for('dashboard.dashboard'))


@datasets_bp.route('/dataset/<int:dataset_id>/append', methods=['POST'])
def append_dataset(dataset_id: int):
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    file = request.files.get('file')
    if not file or not file.filename:
        flash('Debes seleccionar un archivo para anexar.', 'danger')
        return redirect(url_for('datasets.review', dataset_id=dataset_id))

    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        flash('Formato no soportado. Usa CSV o Excel.', 'danger')
        return redirect(url_for('datasets.review', dataset_id=dataset_id))

    temp_path = UPLOAD_DIR / f'append_{secrets.token_hex(8)}_{secure_filename(file.filename)}'
    file.save(temp_path)
    try:
        base_df = load_dataset(dataset_id, user['id'])
        new_df = now_df(temp_path)
        from backend.app import combine_dataframes
        merged = combine_dataframes(base_df, new_df)
        merged_filename = f'merged_{user["id"]}_{dataset_id}.csv'
        merged_path = UPLOAD_DIR / merged_filename
        merged.to_csv(merged_path, index=False, encoding='utf-8-sig')
        original_name = str(dataset['original_filename']) + ' + ' + file.filename
        
        with get_conn() as conn:
            conn.execute(
                'UPDATE datasets SET stored_filename=?, original_filename=?, row_count=? WHERE id=? AND user_id=?',
                (merged_filename, original_name, int(len(merged)), dataset_id, user['id']),
            )
        dataset_service.RAW_DATASET_CACHE.pop(dataset_service._cache_key(dataset_id, user['id']), None)
        dataset_service.PROCESSED_DATASET_CACHE.pop(dataset_service._cache_key(dataset_id, user['id']), None)
        processed_path_for(dataset_id, user['id']).unlink(missing_ok=True)
    except Exception as exc:
        flash(f'No se pudo anexar el archivo: {exc}', 'danger')
        return redirect(url_for('datasets.review', dataset_id=dataset_id))
    finally:
        temp_path.unlink(missing_ok=True)

    flash(f'Se anexaron {len(new_df)} filas al dataset. Nuevo total: {len(merged)}.', 'success')
    return redirect(url_for('datasets.review', dataset_id=dataset_id))


@datasets_bp.route('/reverse/<int:dataset_id>', methods=['POST'])
def reverse_geocode_view(dataset_id: int):
    validate_csrf()
    user = current_user()
    if not user: return jsonify({'ok': False, 'error': 'No autorizado'}), 401
    
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404

    payload = request.get_json(force=True) or {}
    api_key = payload.get('api_key', '')
    if not api_key and dataset['provider'] == 'google': api_key = os.getenv('GOOGLE_GEOCODING_API_KEY', '')
    elif not api_key and dataset['provider'] == 'locationiq': api_key = os.getenv('LOCATIONIQ_API_KEY', '')
    
    try: df = load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError: df = load_dataset(dataset_id, user['id'])

    try:
        updated = reverse_geocode_dataset(df, dataset['provider'], api_key)
        save_processed_dataset(dataset_id, user['id'], updated)
    except Exception as exc:
        log_audit(user['id'], 'reverse_geocode_failed', 'dataset', dataset_id, details={'error': str(exc)})
        return jsonify({'ok': False, 'error': str(exc)}), 400

    return jsonify({'ok': True, 'preview': updated.head(200).fillna('').to_dict(orient='records'), 'summary': dataset_summary(updated)})


@datasets_bp.route('/dataset/<int:dataset_id>/data')
def dataset_data(dataset_id: int):
    user = current_user()
    if not user: return jsonify({'ok': False, 'error': 'No autorizado'}), 401
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    try: df = load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError: df = load_dataset(dataset_id, user['id'])
    return jsonify(df.fillna('').to_dict(orient='records'))


@datasets_bp.route('/dataset/<int:dataset_id>/summary')
def dataset_summary_view(dataset_id: int):
    user = current_user()
    if not user: return jsonify({'ok': False, 'error': 'No autorizado'}), 401
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    try: df = load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError: return jsonify({'ok': False, 'error': 'Primero procesa el archivo'}), 400
    return jsonify({'ok': True, 'summary': dataset_summary(df)})


@datasets_bp.route('/dataset/<int:dataset_id>/review_queue')
def review_queue(dataset_id: int):
    user = current_user()
    if not user: return jsonify({'ok': False, 'error': 'No autorizado'}), 401
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    try: df = load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError: return jsonify({'ok': False, 'error': 'Primero procesa el archivo'}), 400

    only_anomalies = request.args.get('only_anomalies', '0') == '1'
    min_score = request.args.get('min_score')
    min_score_value = int(min_score) if str(min_score or '').isdigit() else None
    q = request.args.get('q', '')
    limit = min(int(request.args.get('limit', '300')), 1000)

    from backend.app import build_review_queue
    rows = build_review_queue(df, only_anomalies=only_anomalies, min_score=min_score_value, text_query=q, limit=limit)
    return jsonify({'ok': True, 'rows': rows, 'count': len(rows)})


@datasets_bp.route('/dataset/<int:dataset_id>/batch_edit', methods=['POST'])
def batch_edit(dataset_id: int):
    validate_csrf()
    user = current_user()
    if not user: return jsonify({'ok': False, 'error': 'No autorizado'}), 401
    
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404

    payload = request.get_json(force=True) or {}
    rows = payload.get('rows', [])
    reason = payload.get('reason', 'batch_review')
    if not isinstance(rows, list): return jsonify({'ok': False, 'error': 'Formato inválido para rows'}), 400

    try: df = load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError: return jsonify({'ok': False, 'error': 'Primero procesa el archivo'}), 400

    # Ensure columns exist and are of object type to avoid dtype issues
    for col in ['latitud', 'longitud', 'estado_geo', 'fuente_resultado']:
        if col not in df.columns:
            df[col] = ''
        df[col] = df[col].astype(object)

    from backend.app import now_iso, assign_territories_to_dataframe, ALCALDIA_LAT, ALCALDIA_LON, apply_quality_flags, deduplicate_suspicious_coordinates
    for item in rows:
        idx = int(item['row_idx'])
        if idx < 0 or idx >= len(df): continue
        try:
            new_lat = float(item.get('latitud')) if item.get('latitud') not in (None, '') else None
            new_lon = float(item.get('longitud')) if item.get('longitud') not in (None, '') else None
        except (TypeError, ValueError):
            return jsonify({'ok': False, 'error': f'Coordenadas inválidas en row_idx={idx}'}), 400

        old_lat = df.at[idx, 'latitud'] if 'latitud' in df.columns else ''
        old_lon = df.at[idx, 'longitud'] if 'longitud' in df.columns else ''
        df.at[idx, 'latitud'] = new_lat
        df.at[idx, 'longitud'] = new_lon
        df.at[idx, 'estado_geo'] = 'EDITADO_MANUALMENTE'
        df.at[idx, 'fuente_resultado'] = 'manual'
        with get_conn() as conn:
            conn.execute(
                'INSERT INTO manual_edits (dataset_id, row_idx, old_lat, old_lon, new_lat, new_lon, reason, edited_by, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (dataset_id, idx, str(old_lat), str(old_lon), str(new_lat), str(new_lon), reason, user['id'], now_iso()),
            )

    active_layers = get_active_layers(dataset['city'], dataset['region'])
    if active_layers: df = assign_territories_to_dataframe(df, active_layers)
    df = apply_quality_flags(df, current_city=dataset['city'], center_lat=ALCALDIA_LAT, center_lon=ALCALDIA_LON)
    df = deduplicate_suspicious_coordinates(df)
    df['selected_for_export'] = df['geo_exportable_strict'].fillna(False) | df['estado_geo'].eq('EDITADO_MANUALMENTE')
    save_processed_dataset(dataset_id, user['id'], df)
    log_audit(user['id'], 'batch_edit', 'dataset', dataset_id, dataset_id=dataset_id, details={'rows': len(rows), 'reason': reason})
    return jsonify({'ok': True, 'summary': dataset_summary(df)})


@datasets_bp.route('/dataset/<int:dataset_id>/suggest_territories', methods=['POST'])
@login_required
def suggest_territories(dataset_id: int):
    validate_csrf()
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset: return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    
    try: df = load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError: df = load_dataset(dataset_id, user['id'])
    
    active_layers = get_active_layers(dataset['city'], dataset['region'])
    if not active_layers:
        return jsonify({'ok': False, 'error': 'No hay capas territoriales activas para esta ciudad.'}), 400
        
    # Enriquecer usando shapely y STRtree (ya implementado en territory_service/territorial)
    from backend.services.territory_service import apply_territory_assignment
    updated = apply_territory_assignment(df, dataset['city'], dataset['region'])
    
    save_processed_dataset(dataset_id, user['id'], updated)
    log_audit(user['id'], 'suggest_territories', 'dataset', dataset_id, dataset_id=dataset_id, details={'layers': len(active_layers)})
    
    return jsonify({
        'ok': True, 
        'summary': dataset_summary(updated),
        'preview': updated.head(200).fillna('').to_dict(orient='records')
    })


@datasets_bp.route('/dataset/<int:dataset_id>/import_debts', methods=['POST'])
@login_required
@module_required('debts')
def import_debts(dataset_id: int):
    validate_csrf()
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    file = request.files.get('debt_file')
    if not file or not file.filename:
        flash('Debes seleccionar un archivo de cartera o deuda.', 'danger')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    ext = Path(file.filename).suffix.lower()
    if ext not in ALLOWED_EXTENSIONS:
        flash(
            'Formato no soportado para el archivo de deuda. Usa CSV o Excel.',
            'danger')
        return redirect(url_for('visits.visits_view', dataset_id=dataset_id))
    temp_path = UPLOAD_DIR / \
        f'debt_{secrets.token_hex(8)}_{secure_filename(file.filename)}'
    file.save(temp_path)
    try:
        debt_df = now_df(temp_path)
        df = ensure_visit_columns(
            _load_any_dataset_version(
                dataset_id, user['id']))
        updated_df, applied = merge_debt_dataframe(
            df, debt_df, source=request.form.get(
                'source', 'archivo_deuda'))
        save_processed_dataset(dataset_id, user['id'], updated_df)
        log_audit(
            user['id'],
            'import_debts',
            'dataset',
            dataset_id,
            dataset_id=dataset_id,
            details={
                'applied': applied,
                'source_file': file.filename})
        flash(
            f'Se aplicaron {applied} validaciones de deuda al dataset.',
            'success')
    except Exception as exc:
        flash(f'No se pudo importar el archivo de deudas: {exc}', 'danger')
    finally:
        temp_path.unlink(missing_ok=True)
    return redirect(url_for('visits.visits_view', dataset_id=dataset_id))


@datasets_bp.route('/history/<int:dataset_id>')
@login_required
@module_required('history')
def history_view(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    previous = _match_previous_dataset(dict(dataset), user['id'])
    comparison = None
    if previous:
        current_df = ensure_visit_columns(
            _load_any_dataset_version(
                dataset_id, user['id']))
        previous_df = ensure_visit_columns(
            _load_any_dataset_version(
                previous['id'], user['id']))
        comparison = compare_dataset_frames(current_df, previous_df)
    return render_template('history.html', dataset=dataset,
                           previous=previous, comparison=comparison)


@datasets_bp.route('/api/datasets/<int:dataset_id>/summary')
@login_required
def api_dataset_summary(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    df = ensure_visit_columns(
        _load_any_dataset_version(
            dataset_id, user['id']))
    return jsonify({'ok': True, 'dataset_id': dataset_id,
                   'summary': dataset_summary(df)})


@datasets_bp.route('/api/datasets/<int:dataset_id>/territories')
@login_required
def api_dataset_territories(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    df = ensure_visit_columns(
        _load_any_dataset_version(
            dataset_id, user['id']))
    summary = dataset_summary(df)
    return jsonify({'ok': True, 'dataset_id': dataset_id,
                   'territories': summary.get('territorial_breakdown', {})})


@datasets_bp.route('/api/datasets/<int:dataset_id>/visits')
@login_required
def api_dataset_visits(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    df = ensure_visit_columns(
        _load_any_dataset_version(
            dataset_id, user['id']))
    return jsonify({'ok': True, 'dataset_id': dataset_id, 'visit_summary': summarize_visits(
        df), 'rows': build_visit_queue(df, limit=1000)})


@datasets_bp.route('/api/datasets/<int:dataset_id>/history')
@login_required
def api_dataset_history(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    previous = _match_previous_dataset(dict(dataset), user['id'])
    if not previous:
        return jsonify({'ok': True, 'comparison': None})
    current_df = ensure_visit_columns(
        _load_any_dataset_version(
            dataset_id, user['id']))
    previous_df = ensure_visit_columns(
        _load_any_dataset_version(
            previous['id'], user['id']))
    return jsonify({'ok': True, 'comparison': compare_dataset_frames(
        current_df, previous_df), 'previous_dataset': previous})


@datasets_bp.route('/export/<int:dataset_id>.csv')
@login_required
@module_required('exports')
def export_csv(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        df = load_processed_dataset(dataset_id, user['id'])
        return send_file(processed_path_for(dataset_id, user['id']),
                         as_attachment=True,
                         download_name=f'dataset_{dataset_id}_processed.csv')
    except FileNotFoundError as e:
        flash(str(e), 'warning')
        return redirect(url_for('datasets.review', dataset_id=dataset_id))


@datasets_bp.route('/export/<int:dataset_id>.geojson')
@login_required
@module_required('exports')
def export_geojson(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        df = load_processed_dataset(dataset_id, user['id'])
        out_path = EXPORT_DIR / f'dataset_{user["id"]}_{dataset_id}.geojson'
        out_path.write_text(
            json.dumps(
                to_geojson(df),
                indent=2,
                ensure_ascii=False),
            encoding='utf-8')
        return send_file(out_path, as_attachment=True, download_name=out_path.name)
    except FileNotFoundError as e:
        flash(str(e), 'warning')
        return redirect(url_for('datasets.review', dataset_id=dataset_id))


@datasets_bp.route('/export/<int:dataset_id>.kml')
@login_required
@module_required('exports')
def export_kml(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        df = load_processed_dataset(dataset_id, user['id'])
        out_path = EXPORT_DIR / f'dataset_{user["id"]}_{dataset_id}.kml'
        out_path.write_text(to_kml(df), encoding='utf-8')
        return send_file(out_path, as_attachment=True, download_name=out_path.name)
    except FileNotFoundError as e:
        flash(str(e), 'warning')
        return redirect(url_for('datasets.review', dataset_id=dataset_id))


@datasets_bp.route('/export/<int:dataset_id>.xlsx')
@login_required
@module_required('exports')
def export_xlsx(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        df = load_processed_dataset(dataset_id, user['id'])
        out_path = EXPORT_DIR / f'dataset_{user["id"]}_{dataset_id}.xlsx'
        to_excel_report(df, out_path)
        return send_file(out_path, as_attachment=True, download_name=out_path.name)
    except FileNotFoundError as e:
        flash(str(e), 'warning')
        return redirect(url_for('datasets.review', dataset_id=dataset_id))


@datasets_bp.route('/dataset/<int:dataset_id>/delete', methods=['POST'])
def delete_dataset(dataset_id: int):
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    # Borrado físico
    try:
        # 1. Archivo original subido
        stored_path = UPLOAD_DIR / str(dataset['stored_filename'])
        if stored_path.exists():
            stored_path.unlink(missing_ok=True)
            
        # 2. Archivo procesado (si existe)
        processed_path = processed_path_for(dataset_id, user['id'])
        if processed_path.exists():
            processed_path.unlink(missing_ok=True)
            
    except Exception as e:
        logging.error(f"Error borrando archivos físicos del dataset {dataset_id}: {e}")

    # Borrado lógico (Base de Datos)
    with get_conn() as conn:
        # Logueamos ANTES de borrar y SIN vincular al ID físico para que el log persista (evitar cascade delete)
        log_audit(user['id'], 'delete_dataset', 'dataset', dataset_id, conn=conn)
        conn.execute('DELETE FROM datasets WHERE id=?', (dataset_id,))

    flash(f'Dataset {dataset_id} eliminado correctamente.', 'success')
    return redirect(url_for('dashboard.dashboard'))


@datasets_bp.route('/delete_all', methods=['POST'])
@login_required
def delete_all_datasets():
    validate_csrf()
    user = current_user()
    with get_conn() as conn:
        datasets = conn.execute('SELECT id, stored_filename FROM datasets WHERE user_id=?', (user['id'],)).fetchall()
        for ds in datasets:
            try:
                # 1. Archivo original
                stored_path = UPLOAD_DIR / str(ds['stored_filename'])
                stored_path.unlink(missing_ok=True)
                
                # 2. Archivo procesado
                proc_path = processed_path_for(ds['id'], user['id'])
                proc_path.unlink(missing_ok=True)
            except Exception as e:
                logging.error(f"Error borrando archivos en delete_all para dataset {ds['id']}: {e}")
        
        conn.execute('DELETE FROM datasets WHERE user_id=?', (user['id'],))
        log_audit(user['id'], 'delete_all_datasets', 'user', user['id'], conn=conn)

    flash('Todos tus datasets han sido eliminados.', 'success')
    return redirect(url_for('dashboard.dashboard'))

@datasets_bp.route('/dataset/<int:dataset_id>/bulk_media', methods=['POST'])
@login_required
def bulk_media_upload(dataset_id: int):
    validate_csrf()
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
        
    zip_file = request.files.get('zip_file')
    mapping_type = request.form.get('mapping_type', 'row_idx')
    
    if not zip_file:
        return jsonify({'ok': False, 'error': 'Debes subir un archivo ZIP'}), 400
        
    df = _load_any_dataset_version(dataset_id, user['id'])
    
    import backend.services.media_service as media_service
    results = media_service.process_bulk_zip_media(zip_file, dataset_id, user['id'], mapping_type, df)
    
    for row_idx, item in results.get('items', []):
        _persist_visit_attachments(dataset_id, row_idx, user['id'], [item])
            
    log_audit(user['id'], 'bulk_media_upload', 'dataset', dataset_id, dataset_id=dataset_id, details={'mapped': results.get('mapped', 0), 'total': results.get('total', 0)})
    
    return jsonify({
        'ok': True,
        'results': {
            'total': results.get('total', 0),
            'mapped': results.get('mapped', 0),
            'errors': results.get('errors', [])
        }
    })
