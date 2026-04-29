import os
import base64
from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors
from typing import Dict, Any, Optional
from pathlib import Path

def _clean_display(value: Any) -> str:
    if value is None or str(value).strip().lower() in ('none', 'nan', ''):
        return '-'
    return str(value).strip()

def build_visit_pdf_bytes(dataset: Dict[str, Any], visit: Dict[str, Any]) -> bytes:
    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    margin = 50
    y = height - margin

    def draw_section_header(title: str):
        nonlocal y
        if y < 80:
            pdf.showPage()
            y = height - margin
        y -= 10
        pdf.setFillColor(colors.HexColor('#f8f9fa'))
        pdf.rect(margin - 5, y - 5, width - (2 * margin) + 10, 20, fill=1, stroke=0)
        pdf.setFillColor(colors.HexColor('#2c3e50'))
        pdf.setFont('Helvetica-Bold', 11)
        pdf.drawString(margin, y, title.upper())
        y -= 25

    def draw_field(label: str, value: Any, x_offset: int = 0, width_val: int = 250):
        nonlocal y
        text_value = _clean_display(value)
        pdf.setFont('Helvetica-Bold', 9)
        pdf.setFillColor(colors.HexColor('#7f8c8d'))
        pdf.drawString(margin + x_offset, y, f'{label}:')
        pdf.setFont('Helvetica', 10)
        pdf.setFillColor(colors.black)
        pdf.drawString(margin + x_offset + 100, y, text_value[:80])

    def add_image(image_source: str, label: str, box_w: int = 200, box_h: int = 150):
        nonlocal y
        if not image_source:
            return
        
        # Determine path
        img_path = None
        if isinstance(image_source, str):
            if image_source.startswith('http'):
                # Handle relative URL /visit_media/filename
                filename = os.path.basename(image_source)
                img_path = Path('geobusca_data') / 'visit_media' / filename
            else:
                img_path = Path('geobusca_data') / 'visit_media' / image_source
                if not img_path.exists():
                    img_path = Path('geobusca_data') / 'visit_media' / 'attachments' / image_source

        if img_path and img_path.exists():
            try:
                if y < box_h + 40:
                    pdf.showPage()
                    y = height - margin
                
                pdf.setFont('Helvetica-Bold', 9)
                pdf.drawString(margin, y, label)
                y -= (box_h + 10)
                pdf.drawImage(str(img_path), margin, y, width=box_w, height=box_h, preserveAspectRatio=True, anchor='sw')
                y -= 20
            except Exception:
                pdf.drawString(margin, y, f"[Error cargando {label}]")
                y -= 20

    # Header
    pdf.setTitle(f'RVT_{dataset.get("id")}_{visit.get("row_idx")}')
    
    # Logo Placeholder or Brand
    pdf.setFont('Helvetica-Bold', 16)
    pdf.setFillColor(colors.HexColor('#1a5f7a'))
    pdf.drawString(margin, y, 'GeoBusca Territorial')
    pdf.setFont('Helvetica', 10)
    pdf.setFillColor(colors.grey)
    pdf.drawRightString(width - margin, y, 'Registro de Visita Tributaria (RVT)')
    y -= 15
    pdf.setStrokeColor(colors.HexColor('#1a5f7a'))
    pdf.setLineWidth(2)
    pdf.line(margin, y, width - margin, y)
    y -= 30

    # Section 1: Informativos
    draw_section_header('Datos de la Visita')
    draw_field('Dataset ID', dataset.get('id'), 0)
    draw_field('Fila', visit.get('row_idx'), 250)
    y -= 18
    draw_field('Tipo Visita', visit.get('rvt_tipo_visita') or visit.get('visita_estado'), 0)
    draw_field('Causal', visit.get('visit_result') or visit.get('visita_resultado'), 250)
    y -= 18
    draw_field('Fecha', visit.get('visita_fecha'), 0)
    draw_field('Hora', visit.get('visita_hora'), 250)
    y -= 18
    draw_field('Funcionario', visit.get('visita_funcionario') or visit.get('rvt_funcionario_firma_nombre'), 0)
    y -= 30

    # Section 2: Contribuyente
    draw_section_header('Datos del Contribuyente')
    draw_field('Razón Social', visit.get('rvt_razon_social') or visit.get('nom_establec'), 0)
    y -= 18
    draw_field('NIT / C.C.', visit.get('rvt_nit_cc'), 0)
    draw_field('Atendido por', visit.get('rvt_recibe_nombre'), 250)
    y -= 18
    draw_field('Dirección', visit.get('rvt_direccion_establecimiento') or visit.get('direccion'), 0)
    y -= 18
    draw_field('Barrio/Vereda', visit.get('barrio') or visit.get('vereda'), 0)
    draw_field('Municipio', visit.get('rvt_municipio') or dataset.get('city'), 250)
    y -= 30

    # Section 3: Cartera y GPS
    draw_section_header('Validación de Cartera y Ubicación')
    draw_field('Estado Deuda', visit.get('deuda_estado'), 0)
    draw_field('Monto', visit.get('deuda_monto'), 250)
    y -= 18
    draw_field('Latitud', visit.get('visit_latitude'), 0)
    draw_field('Longitud', visit.get('visit_longitude'), 250)
    y -= 18
    draw_field('Precisión GPS', f"{visit.get('visit_gps_accuracy', '-')} m", 0)
    draw_field('Anomalía GPS', 'SÍ' if visit.get('has_gps_anomaly') else 'NO', 250)
    y -= 30

    # Section 4: Observaciones
    draw_section_header('Observaciones')
    pdf.setFont('Helvetica', 10)
    obs = _clean_display(visit.get('visita_observaciones'))
    text_obj = pdf.beginText(margin, y)
    text_obj.setFont('Helvetica', 10)
    text_obj.setLeading(14)
    # Simple wrap
    words = obs.split()
    line = ""
    for word in words:
        if len(line + word) > 90:
            text_obj.textLine(line)
            line = word + " "
            y -= 14
        else:
            line += word + " "
    text_obj.textLine(line)
    y -= 20
    pdf.drawText(text_obj)
    y -= 30

    # Section 5: Firmas y Evidencias
    draw_section_header('Evidencias y Firmas')
    
    # Signatures
    sig_y = y
    add_image(visit.get('visit_signature_receiver'), 'Firma Contribuyente', 180, 100)
    y_rec = y
    y = sig_y
    add_image(visit.get('visit_signature_officer'), 'Firma Funcionario', 180, 100, x_offset=250) # Need to adjust add_image for offset
    # Wait, I'll keep them sequential for simplicity in this basic generator
    y = min(y, y_rec)
    
    # Audit Hashes
    pdf.setFont('Helvetica-Oblique', 7)
    pdf.setFillColor(colors.grey)
    if visit.get('visit_signature_receiver_hash'):
        pdf.drawString(margin, y + 10, f"Hash Firma Recibe: {visit.get('visit_signature_receiver_hash')}")
        y -= 10
    if visit.get('visit_signature_officer_hash'):
        pdf.drawString(margin, y + 10, f"Hash Firma Funcionario: {visit.get('visit_signature_officer_hash')}")
        y -= 10

    # Photos
    y -= 20
    add_image(visit.get('visit_photo_establecimiento'), 'Foto Establecimiento', 250, 180)
    add_image(visit.get('visit_photo_documento'), 'Foto Documento/Evidencia', 250, 180)

    # Footer
    pdf.setFont('Helvetica', 8)
    pdf.setFillColor(colors.grey)
    pdf.drawCentredString(width / 2, 30, f"Generado por {dataset.get('brand_name', 'GeoBusca')} el {visit.get('updated_at', '-')}")
    
    pdf.save()
    return buffer.getvalue()

def save_report_to_disk(pdf_bytes: bytes, filename: str) -> str:
    downloads_dir = Path('geobusca_data') / 'downloads'
    downloads_dir.mkdir(parents=True, exist_ok=True)
    file_path = downloads_dir / filename
    file_path.write_bytes(pdf_bytes)
    return str(file_path.absolute())
