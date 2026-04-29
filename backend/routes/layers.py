from flask import Blueprint, request, flash, redirect, url_for
from backend.database_web import get_conn, TERRITORIAL_DIR
from backend.config import DEFAULT_CITY, DEFAULT_REGION, TERRITORIAL_LAYER_TYPES
from backend.territorial import allowed_layer_file, persist_canonical_layer
from backend.arcgis_rest import ArcGISRestError, RIONEGRO_LAYER_PRESETS
from werkzeug.utils import secure_filename
from pathlib import Path
import secrets

import backend.app as app_main
import backend.services.territory_service as territory_service

login_required = app_main.login_required
module_required = app_main.module_required
validate_csrf = app_main.validate_csrf
current_user = app_main.current_user
register_territorial_layer = territory_service.register_territorial_layer
import_arcgis_layer = territory_service.import_arcgis_layer

layers_bp = Blueprint('layers', __name__)

@layers_bp.route('/layers/upload', methods=['POST'])
def upload_layer():
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    file = request.files.get('layer_file')
    layer_type = request.form.get('layer_type', '').strip().lower()
    city = request.form.get('city', DEFAULT_CITY).strip() or DEFAULT_CITY
    region = request.form.get('region', DEFAULT_REGION).strip() or DEFAULT_REGION
    display_name = request.form.get('display_name', '').strip()
    source = request.form.get('source', 'Carga manual').strip()

    if layer_type not in TERRITORIAL_LAYER_TYPES:
        flash('Tipo de capa inválido.', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    if not file or not file.filename:
        flash('Debes seleccionar una capa territorial.', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    if not allowed_layer_file(file.filename):
        flash('Formato de capa no soportado. Usa GeoJSON, SHP, GPKG o ZIP.', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    original_ext = Path(file.filename).suffix.lower()
    tmp_path = TERRITORIAL_DIR / f"tmp_{secrets.token_hex(8)}{original_ext}"
    file.save(tmp_path)
    display_name = display_name or Path(file.filename).stem
    try:
        stored_meta = persist_canonical_layer(tmp_path, display_name, layer_type)
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        flash(f'No se pudo procesar la capa territorial: {exc}', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    finally:
        tmp_path.unlink(missing_ok=True)

    register_territorial_layer(user['id'], display_name, layer_type, city, region, source, stored_meta)
    flash(f'Capa territorial cargada: {display_name} ({stored_meta["feature_count"]} elementos).', 'success')
    return redirect(url_for('dashboard.dashboard'))


@layers_bp.route('/layers/import_arcgis', methods=['POST'])
def import_layer_arcgis():
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    layer_type = request.form.get('layer_type', '').strip().lower()
    city = request.form.get('city', DEFAULT_CITY).strip() or DEFAULT_CITY
    region = request.form.get('region', DEFAULT_REGION).strip() or DEFAULT_REGION
    display_name = request.form.get('display_name', '').strip()
    source = request.form.get('source', '').strip() or 'ArcGIS REST'
    service_url = request.form.get('service_url', '').strip()
    layer_id = request.form.get('layer_id', '').strip()
    where = request.form.get('where', '1=1').strip() or '1=1'

    if layer_type not in TERRITORIAL_LAYER_TYPES:
        flash('Tipo de capa inválido para importación ArcGIS.', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    if not service_url:
        flash('Debes indicar la URL ArcGIS REST de la capa o del servicio.', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    display_name = display_name or f'{layer_type.replace("_", " ").title()} ArcGIS'
    
    try:
        stored_meta = import_arcgis_layer(
            user_id=user['id'],
            display_name=display_name,
            layer_type=layer_type,
            city=city,
            region=region,
            source=source or service_url,
            service_url=service_url,
            layer_id=layer_id,
            where=where,
        )
    except ArcGISRestError as exc:
        flash(f'No se pudo importar la capa ArcGIS: {exc}', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    except Exception as exc:
        flash(f'No se pudo procesar la capa ArcGIS: {exc}', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    flash(f"Capa ArcGIS importada: {display_name} ({stored_meta['feature_count']} elementos).", 'success')
    return redirect(url_for('dashboard.dashboard'))


@layers_bp.route('/layers/import_rionegro_preset', methods=['POST'])
def import_rionegro_preset():
    validate_csrf()
    user = current_user()
    if not user: return redirect(url_for('auth.login'))
    
    preset_key = request.form.get('preset_key', '').strip()
    preset = RIONEGRO_LAYER_PRESETS.get(preset_key)
    if not preset:
        flash('Preset ArcGIS no encontrado.', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    try:
        stored_meta = import_arcgis_layer(
            user_id=user['id'],
            display_name=preset['display_name'],
            layer_type=preset['layer_type'],
            city=DEFAULT_CITY,
            region=DEFAULT_REGION,
            source=preset['source'],
            service_url=preset['service_url'],
            where='1=1',
        )
    except ArcGISRestError as exc:
        flash(f'No se pudo importar el preset oficial: {exc}', 'danger')
        return redirect(url_for('dashboard.dashboard'))
    except Exception as exc:
        flash(f'No se pudo procesar el preset oficial: {exc}', 'danger')
        return redirect(url_for('dashboard.dashboard'))

    flash(f"Preset oficial importado: {preset['display_name']} ({stored_meta['feature_count']} elementos).", 'success')
    return redirect(url_for('dashboard.dashboard'))


@layers_bp.route('/layers/<int:layer_id>/toggle', methods=['POST'])
def toggle_layer(layer_id: int):
    validate_csrf()
    with get_conn() as conn:
        row = conn.execute('SELECT * FROM territorial_layers WHERE id=?', (layer_id,)).fetchone()
        if not row:
            flash('Capa no encontrada.', 'danger')
            return redirect(url_for('dashboard.dashboard'))
        new_value = 0 if row['is_active'] else 1
        conn.execute('UPDATE territorial_layers SET is_active=? WHERE id=?', (new_value, layer_id))
    flash('Estado de capa actualizado.', 'success')
    return redirect(url_for('dashboard.dashboard'))


@layers_bp.route('/layers/<int:layer_id>/delete', methods=['POST'])
def delete_layer(layer_id: int):
    validate_csrf()
    with get_conn() as conn:
        row = conn.execute('SELECT * FROM territorial_layers WHERE id=?', (layer_id,)).fetchone()
        if not row:
            flash('Capa no encontrada.', 'danger')
            return redirect(url_for('dashboard.dashboard'))
        file_path = Path(row['file_path'])
        conn.execute('DELETE FROM territorial_layers WHERE id=?', (layer_id,))
    file_path.unlink(missing_ok=True)
    flash('Capa eliminada.', 'success')
    return redirect(url_for('dashboard.dashboard'))
