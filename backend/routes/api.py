from flask import Blueprint, jsonify
from backend.services.api_service import api_key_required
from backend.services.admin_service import get_manager_metrics

api_bp = Blueprint('api_v1', __name__, url_prefix='/api/v1')

@api_bp.route('/indicators', methods=['GET'])
@api_key_required
def get_indicators():
    """Returns the main management indicators in JSON format."""
    try:
        metrics = get_manager_metrics()
        return jsonify({
            "status": "success",
            "data": {
                "sla_metrics": metrics['sla'],
                "productivity": metrics['staff'],
                "visits_summary": metrics['completion']
            }
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@api_bp.route('/health', methods=['GET'])
def health_check():
    """Public health check endpoint."""
    return jsonify({"status": "ok", "service": "GeoBusca API v1"})
