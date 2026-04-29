from flask import Blueprint, render_template, redirect, url_for, flash, jsonify, request
from backend.database_web import get_conn
from backend.config import DEFAULT_CITY, DEFAULT_REGION, TERRITORIAL_LAYER_TYPES
from backend.arcgis_rest import RIONEGRO_LAYER_PRESETS
from backend.territorial import active_layer_options, build_layer_geojson_with_counts

import backend.app as app_main
import backend.services.dataset_service as dataset_service
import backend.services.territory_service as territory_service

login_required = app_main.login_required
module_required = app_main.module_required
current_user = app_main.current_user
get_active_layers = territory_service.get_active_layers
_recent_jobs_for_user = app_main._recent_jobs_for_user
get_user_dataset = dataset_service.get_user_dataset
load_processed_dataset = dataset_service.load_processed_dataset
load_any_dataset_version = dataset_service.load_any_dataset_version
enrich_dataset_from_cache = app_main.enrich_dataset_from_cache
from backend.territorial import assign_territories_to_dataframe
ALCALDIA_LAT = app_main.ALCALDIA_LAT
ALCALDIA_LON = app_main.ALCALDIA_LON

dashboard_bp = Blueprint('dashboard', __name__)

@dashboard_bp.route('/')
def index():
    return redirect(url_for('dashboard.dashboard') if current_user()
                    else url_for('auth.login'))

@dashboard_bp.route('/dashboard')
def dashboard():
    user = current_user()
    if not user:
        return redirect(url_for('auth.login'))
        
    with get_conn() as conn:
        datasets = conn.execute(
            '''
            SELECT d.*, r.ok_rows, r.exportable_rows, r.manual_rows, r.duplicate_rows, r.created_at AS run_created_at
            FROM datasets d
            LEFT JOIN runs r ON r.id = (
                SELECT id FROM runs WHERE dataset_id=d.id ORDER BY id DESC LIMIT 1
            )
            WHERE d.user_id=?
            ORDER BY d.id DESC
            ''',
            (user['id'],),
        ).fetchall()
        aggregates = conn.execute(
            '''
            SELECT COUNT(*) AS datasets,
                   COALESCE(SUM(row_count), 0) AS total_rows,
                   COALESCE(SUM(CASE WHEN ok_rows IS NOT NULL THEN ok_rows ELSE 0 END), 0) AS ok_rows,
                   COALESCE(SUM(CASE WHEN exportable_rows IS NOT NULL THEN exportable_rows ELSE 0 END), 0) AS exportable_rows,
                   COALESCE(SUM(CASE WHEN manual_rows IS NOT NULL THEN manual_rows ELSE 0 END), 0) AS manual_rows,
                   COALESCE(SUM(CASE WHEN duplicate_rows IS NOT NULL THEN duplicate_rows ELSE 0 END), 0) AS duplicate_rows
            FROM datasets d
            LEFT JOIN runs r ON r.id = (
                SELECT id FROM runs WHERE dataset_id=d.id ORDER BY id DESC LIMIT 1
            )
            WHERE d.user_id=?
            ''',
            (user['id'],),
        ).fetchone()

        cache_rows = conn.execute(
            'SELECT COUNT(*) AS total FROM geocode_cache').fetchone()['total']
        layers = conn.execute(
            'SELECT * FROM territorial_layers WHERE UPPER(city)=UPPER(?) AND UPPER(region)=UPPER(?) ORDER BY layer_type, id DESC',
            (DEFAULT_CITY, DEFAULT_REGION),
        ).fetchall()
        
    layer_rows = [dict(row) for row in layers]
    recent_jobs = _recent_jobs_for_user(user['id'], limit=8)
    
    return render_template(
        'dashboard.html',
        datasets=datasets,
        default_city=DEFAULT_CITY,
        default_region=DEFAULT_REGION,
        aggregates=dict(aggregates) if aggregates else {},
        territorial_layers=layer_rows,
        layer_types=TERRITORIAL_LAYER_TYPES,
        active_layer_options=active_layer_options(layer_rows),
        rionegro_presets=RIONEGRO_LAYER_PRESETS,
        cache_rows=cache_rows,
        recent_jobs=recent_jobs,
    )

@dashboard_bp.route('/map/<int:dataset_id>')
@login_required
@module_required('map')
def map_view(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        flash('Dataset no encontrado', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    
    # Allow viewing even if not processed for "Live Map" support
    is_processed = True
    try:
        load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError:
        is_processed = False
        flash('Visualizando datos en tiempo real (procesamiento en curso o pendiente)', 'info')

    return render_template('map.html', dataset=dataset, alcaldia_lat=ALCALDIA_LAT,
                           alcaldia_lon=ALCALDIA_LON, layer_types=TERRITORIAL_LAYER_TYPES,
                           is_live=not is_processed)


@dashboard_bp.route('/map_data/<int:dataset_id>')
@login_required
@module_required('map')
def map_data(dataset_id: int):
    try:
        user = current_user()
        dataset = get_user_dataset(dataset_id, user['id'])
        if not dataset:
            return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
        
        df = load_any_dataset_version(dataset_id, user['id'])
        
        if 'latitud' not in df.columns or df['latitud'].isnull().all():
            df = enrich_dataset_from_cache(df, dataset['city'], dataset['region'])
        
        try:
            layers = get_active_layers(dataset['city'], dataset['region'])
            if layers:
                df = assign_territories_to_dataframe(df, layers)
        except Exception as se:
            print(f"Spatial enrichment error: {se}")

        if 'selected_for_export' in df.columns:
            df = df[df['selected_for_export'].fillna(False)]
        df = df.dropna(subset=['latitud', 'longitud']).copy()

        filters = {
            'confidence': request.args.get('confidence', '').strip().upper(),
            'status': request.args.get('status', '').strip().upper(),
            'q': request.args.get('q', '').strip().lower(),
            'comuna': request.args.get('comuna', '').strip(),
            'barrio': request.args.get('barrio', '').strip(),
            'corregimiento': request.args.get('corregimiento', '').strip(),
            'vereda': request.args.get('vereda', '').strip(),
            'zona_rionegro': request.args.get('zona_rionegro', '').strip().upper(),
            'categoria_economica': request.args.get('categoria_economica', '').strip().upper(),
            'perimeter_only': request.args.get('perimeter_only') == 'true',
        }
        
        # Filtering logic...
        if filters['confidence'] and 'geo_confianza' in df.columns:
            df = df[df['geo_confianza'].fillna('').astype(str).str.upper() == filters['confidence']]
        if filters['status'] and 'estado_geo' in df.columns:
            df = df[df['estado_geo'].fillna('').astype(str).str.upper() == filters['status']]
        if filters['perimeter_only'] and 'fuera_municipio' in df.columns:
            df = df[df['fuera_municipio'].fillna('').astype(str).str.upper() == 'NO']
        
        # (Rest of filters omitted for brevity in chunk but I'll ensure they are there)
        if filters['comuna'] and 'comuna' in df.columns:
            df = df[df['comuna'].fillna('').astype(str).str.contains(filters['comuna'], case=False)]
        if filters['barrio'] and 'barrio' in df.columns:
            df = df[df['barrio'].fillna('').astype(str) == filters['barrio']]
        if filters['corregimiento'] and 'corregimiento' in df.columns:
            df = df[df['corregimiento'].fillna('').astype(str) == filters['corregimiento']]
        if filters['vereda'] and 'vereda' in df.columns:
            df = df[df['vereda'].fillna('').astype(str) == filters['vereda']]
        if filters['zona_rionegro'] and 'zona_rionegro' in df.columns:
            df = df[df['zona_rionegro'].fillna('').astype(str).str.upper() == filters['zona_rionegro']]
        if filters['categoria_economica'] and 'categoria_economica' in df.columns:
            df = df[df['categoria_economica'].fillna('').astype(str).str.upper() == filters['categoria_economica']]
        
        if filters['q']:
            mask = df.astype('string').apply(lambda col: col.str.lower().str.contains(filters['q'], regex=False, na=False)).any(axis=1)
            df = df[mask]

        cols = [
            'row_idx', 'nom_establec', 'direccion_geocodable', 'direccion', 'latitud', 'longitud',
            'estado_geo', 'geo_score', 'geo_confianza', 'geo_reason_short', 'anomalias_geo',
            'proveedor', 'consulta_usada', 'error_geo', 'ai_fix_reason', 'fuente_resultado',
            'coord_duplicate_drop', 'comuna', 'barrio', 'corregimiento', 'vereda', 'zona_rionegro',
            'categoria_economica', 'subcategoria_economica', 'territorio_principal_nombre',
            'visita_estado', 'visita_fecha', 'visita_funcionario', 'deuda_estado', 'deuda_monto'
        ]
        cols = [c for c in cols if c in df.columns]
        
        return jsonify({
            'ok': True,
            'points': df[cols].fillna('').to_dict(orient='records')
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'ok': False, 'error': str(e), 'traceback': traceback.format_exc()}), 500

@dashboard_bp.route('/map_layer_data/<int:dataset_id>')
@login_required
@module_required('map')
def map_layer_data(dataset_id: int):
    user = current_user()
    dataset = get_user_dataset(dataset_id, user['id'])
    if not dataset:
        return jsonify({'ok': False, 'error': 'Dataset no encontrado'}), 404
    try:
        df = load_processed_dataset(dataset_id, user['id'])
    except FileNotFoundError:
        return jsonify(
            {'ok': False, 'error': 'Primero procesa el archivo'}), 400

    layer_type = request.args.get('layer_type', 'barrio').strip().lower()
    if layer_type not in TERRITORIAL_LAYER_TYPES:
        return jsonify({'ok': False, 'error': 'Tipo de capa inválido'}), 400
    layers = get_active_layers(
        dataset['city'],
        dataset['region'],
        layer_type=layer_type)
    if not layers:
        return jsonify({'ok': True, 'feature_collection': {
                       'type': 'FeatureCollection', 'features': []}, 'message': 'No hay capa activa para ese tipo'})
    feature_collection = build_layer_geojson_with_counts(layers[0], df)
    return jsonify(
        {'ok': True, 'feature_collection': feature_collection, 'layer': layers[0]})

@dashboard_bp.route('/dashboard/map_layers')
def dashboard_map_layers():
    if not current_user():
        return redirect(url_for('auth.login'))
        
    for layer_type in ['barrio', 'zona_urbana', 'vereda', 'comuna']:
        layers = get_active_layers('Rionegro', 'Antioquia', layer_type=layer_type)
        if layers:
            feature_collection = build_layer_geojson_with_counts(layers[0], None)
            return jsonify({'ok': True, 'feature_collection': feature_collection})
    
    return jsonify({'ok': True, 'feature_collection': None})


@dashboard_bp.route('/dashboard/map_points')
@login_required
def dashboard_map_points():
    user = current_user()
    with get_conn() as conn:
        datasets = conn.execute('SELECT id, city, region FROM datasets WHERE user_id=?', (user['id'],)).fetchall()
    
    all_points = []
    for ds in datasets:
        try:
            df = load_processed_dataset(ds['id'], user['id'])
            # Filter validated points within Rionegro
            if 'estado_geo' in df.columns:
                mask = (df['estado_geo'].fillna('').astype(str).str.upper() == 'OK')
                if 'fuera_municipio' in df.columns:
                    mask &= (df['fuera_municipio'].fillna('').astype(str).str.upper() == 'NO')
                
                subset = df[mask].copy()
                if not subset.empty:
                    # Only keep essential columns for performance
                    cols = ['latitud', 'longitud', 'nom_establec', 'direccion', 'geo_confianza', 'barrio', 'vereda']
                    cols = [c for c in cols if c in subset.columns]
                    all_points.extend(subset[cols].fillna('').to_dict(orient='records'))
        except FileNotFoundError:
            continue
            
    return jsonify({'ok': True, 'points': all_points})
