
import sys
import os
from pathlib import Path

# Agregar el directorio raíz al path para importar el backend
sys.path.append(str(Path(__file__).parent.parent))

from backend.services.report_service import build_visit_pdf_bytes
from backend.notifications import send_html_email, build_visit_email_subject, build_visit_email_html, build_visit_email_text

def run_test_send():
    print("Iniciando prueba de envío de RVT...")
    
    # 1. Datos de prueba
    dataset = {
        'id': 999,
        'city': 'Rionegro',
        'brand_name': 'GeoBusca Territorial'
    }
    
    visit = {
        'row_idx': 0,
        'rvt_tipo_visita': 'INSCRITO',
        'rvt_razon_social': 'Ferretería El Progreso - PRUEBA SISTEMA',
        'rvt_nit_cc': '800.123.456-7',
        'rvt_direccion_establecimiento': 'Carrera 50 # 48-20, Rionegro',
        'rvt_municipio': 'Rionegro',
        'rvt_departamento': 'Antioquia',
        'rvt_telefono_movil': '3001234567',
        'rvt_correo_electronico': 'camilo152893@gmail.com',
        'rvt_codigo_ciiu_1': '4752',
        'rvt_descripcion_actividad': 'Comercio al por menor de artículos de ferretería',
        'rvt_rep_legal_a_nombre': 'JUAN PEREZ TEST',
        'deuda_estado': 'AL DÍA',
        'visit_latitude': '6.151234',
        'visit_longitude': '-75.373456',
        'visit_gps_accuracy': '3',
        'visita_fecha': '2026-04-29',
        'visita_hora': '15:10',
        'visita_observaciones': 'Visita de prueba técnica.\n[MEDICIÓN DE VALLA: 250cm x 100cm = 25,000 cm²]',
        'updated_at': '2026-04-29 15:10:00',
        'visit_signature_receiver_hash': 'abc123test_hash_recibe',
        'visit_signature_officer_hash': 'def456test_hash_funcionario'
    }
    
    # 2. Generar PDF
    print("Generando PDF de prueba...")
    try:
        pdf_bytes = build_visit_pdf_bytes(dataset, visit)
        print(f"PDF generado exitosamente ({len(pdf_bytes)} bytes)")
    except Exception as e:
        print(f"Error generando PDF: {e}")
        return

    # 3. Preparar Email
    subject = build_visit_email_subject(visit)
    html_body = build_visit_email_html(visit, "https://geobusca.rionegro.gov.co/test_view")
    text_body = build_visit_email_text(visit, "https://geobusca.rionegro.gov.co/test_view")
    
    # 4. Enviar
    print(f"Enviando correo a {visit['rvt_correo_electronico']}...")
    ok, error = send_html_email(
        to_email=visit['rvt_correo_electronico'],
        subject=subject,
        html_body=html_body,
        text_body=text_body,
        attachment_data=pdf_bytes,
        attachment_filename="RVT_Prueba_Ferreteria.pdf"
    )
    
    if ok:
        print("¡ÉXITO! El correo de prueba ha sido enviado.")
    else:
        print(f"FALLO en el envío: {error}")

if __name__ == "__main__":
    run_test_send()
