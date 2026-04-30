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
    margin = 40
    y = height - margin

    def draw_section_header(title: str):
        nonlocal y
        if y < 100:
            pdf.showPage()
            y = height - margin
        y -= 15
        pdf.setFillColor(colors.HexColor('#2c3e50'))
        pdf.rect(margin, y - 5, width - (2 * margin), 18, fill=1, stroke=0)
        pdf.setFillColor(colors.white)
        pdf.setFont('Helvetica-Bold', 10)
        pdf.drawString(margin + 10, y, title.upper())
        pdf.setFillColor(colors.black)
        y -= 25

    def draw_row(fields: list[tuple[str, Any]], x_start: int = margin, col_w: int = 170):
        nonlocal y
        curr_x = x_start
        max_y_drop = 15
        for label, value in fields:
            pdf.setFont('Helvetica-Bold', 8)
            pdf.setFillColor(colors.grey)
            pdf.drawString(curr_x, y, f"{label}:")
            pdf.setFont('Helvetica', 9)
            pdf.setFillColor(colors.black)
            val_s = _clean_display(value)
            # Truncate if too long for the column
            pdf.drawString(curr_x, y - 10, val_s[:35])
            curr_x += col_w
        y -= 30

    def add_image(image_source: str, label: str, box_w: int = 240, box_h: int = 160, x_pos: int = margin):
        nonlocal y
        if not image_source: return
        img_path = None
        if isinstance(image_source, str):
            if image_source.startswith('http'):
                filename = os.path.basename(image_source)
                img_path = Path('geobusca_data') / 'visit_media' / filename
            else:
                img_path = Path('geobusca_data') / 'visit_media' / image_source
                if not img_path.exists():
                    img_path = Path('geobusca_data') / 'visit_media' / 'attachments' / image_source

        if img_path and img_path.exists():
            try:
                if y < box_h + 30:
                    pdf.showPage()
                    y = height - margin
                pdf.setFont('Helvetica-Bold', 8)
                pdf.drawString(x_pos, y, label)
                y -= (box_h + 5)
                pdf.drawImage(str(img_path), x_pos, y, width=box_w, height=box_h, preserveAspectRatio=True, anchor='sw')
                return True
            except: pass
        return False

    # Header con Logo Textual
    pdf.setFont('Helvetica-Bold', 14)
    pdf.drawString(margin, y, "ALCALDÍA DE RIONEGRO")
    pdf.setFont('Helvetica', 9)
    pdf.drawRightString(width - margin, y, "Registro de Visita Tributaria - RVT")
    y -= 12
    pdf.setFont('Helvetica', 8)
    pdf.drawString(margin, y, "Secretaría de Hacienda - Subsecretaría de Rentas")
    y -= 20
    pdf.line(margin, y, width - margin, y)
    y -= 20

    # SECCIÓN I: DATOS INFORMATIVOS
    draw_section_header("I. DATOS INFORMATIVOS")
    draw_row([
        ("Tipo Visita", visit.get('rvt_tipo_visita') or visit.get('visita_estado')),
        ("Código Estab.", visit.get('rvt_codigo_establecimiento')),
        ("Fecha/Hora", f"{visit.get('visita_fecha')} {visit.get('visita_hora')}")
    ])
    draw_row([
        ("Atendido por", visit.get('rvt_recibe_nombre')),
        ("Documento", f"{visit.get('rvt_recibe_tipo_documento')} {visit.get('rvt_recibe_numero_documento')}"),
        ("Cargo", visit.get('rvt_recibe_cargo'))
    ])

    # SECCIÓN II: DATOS DEL CONTRIBUYENTE
    draw_section_header("II. DATOS DEL CONTRIBUYENTE")
    draw_row([
        ("Razón Social", visit.get('rvt_razon_social') or visit.get('nom_establec')),
        ("NIT / C.C.", visit.get('rvt_nit_cc')),
        ("Avisos y Tableros", visit.get('rvt_avisos_tableros'))
    ])
    draw_row([
        ("Dirección Estab.", visit.get('rvt_direccion_establecimiento') or visit.get('direccion')),
        ("Municipio", visit.get('rvt_municipio')),
        ("Departamento", visit.get('rvt_departamento'))
    ])
    draw_row([
        ("Dirección Cobro", visit.get('rvt_direccion_cobro')),
        ("Teléfono", visit.get('rvt_telefono_movil') or visit.get('rvt_telefono_fijo')),
        ("Correo", visit.get('rvt_correo_electronico'))
    ])
    draw_row([
        ("Sector Econ.", visit.get('rvt_sector_economico')),
        ("Inicio Actividades", visit.get('rvt_fecha_inicio_actividades')),
        ("CIIU Prin.", visit.get('rvt_codigo_ciiu_1'))
    ])
    draw_row([
        ("Actividad", visit.get('rvt_descripcion_actividad'))
    ])

    # SECCIÓN III: REPRESENTACIÓN LEGAL
    draw_section_header("III. REPRESENTACIÓN LEGAL")
    draw_row([
        ("Rep. Legal A", visit.get('rvt_rep_legal_a_nombre')),
        ("ID", visit.get('rvt_rep_legal_a_identificacion')),
        ("Correo", visit.get('rvt_rep_legal_a_correo'))
    ])

    # SECCIÓN IV: CARTERA Y GPS
    draw_section_header("IV. VALIDACIÓN DE CARTERA Y UBICACIÓN")
    draw_row([
        ("Estado Deuda", visit.get('deuda_estado')),
        ("Monto", visit.get('deuda_monto')),
        ("Ref Deuda", visit.get('deuda_referencia'))
    ])
    draw_row([
        ("Latitud", visit.get('visit_latitude')),
        ("Longitud", visit.get('visit_longitude')),
        ("Precisión GPS", f"{visit.get('visit_gps_accuracy')} m")
    ])

    # SECCIÓN V: OBSERVACIONES (Incluye Medición)
    draw_section_header("V. OBSERVACIONES")
    obs = _clean_display(visit.get('visita_observaciones'))
    pdf.setFont('Helvetica', 9)
    text_obj = pdf.beginText(margin + 5, y)
    text_obj.setLeading(12)
    for line in obs.split('\n'):
        # Wrap simple
        if len(line) > 110:
            text_obj.textLine(line[:110])
            text_obj.textLine(line[110:220])
        else:
            text_obj.textLine(line)
        y -= 12
    pdf.drawText(text_obj)
    y -= 20

    # SECCIÓN VI: EVIDENCIAS Y FIRMAS
    draw_section_header("VI. EVIDENCIAS Y FIRMAS")
    
    # Fotos (Negocio)
    start_y_media = y
    has_photo = add_image(visit.get('visit_photo_establecimiento'), "EVIDENCIA: NEGOCIO / FACHADA", 240, 160)
    y_photo = y
    
    # Firmas
    y = start_y_media
    add_image(visit.get('visit_signature_receiver'), "FIRMA: QUIEN RECIBE", 240, 100, x_pos=width/2 + 10)
    y_sig1 = y
    add_image(visit.get('visit_signature_officer'), "FIRMA: FUNCIONARIO", 240, 100, x_pos=width/2 + 10)
    y_sig2 = y
    
    y = min(y_photo, y_sig2) - 20
    
    # Footer de Auditoría
    pdf.setFont('Helvetica-Oblique', 7)
    pdf.setFillColor(colors.grey)
    pdf.drawString(margin, y, f"Hash Auditoría Recibe: {visit.get('visit_signature_receiver_hash', 'N/A')}")
    y -= 10
    pdf.drawString(margin, y, f"Hash Auditoría Funcionario: {visit.get('visit_signature_officer_hash', 'N/A')}")
    
    pdf.setFont('Helvetica', 8)
    pdf.drawCentredString(width/2, 20, f"Este documento es un acta oficial generada por GeoBusca Territorial - Fecha: {visit.get('updated_at')}")

    pdf.save()
    return buffer.getvalue()

def save_report_to_disk(pdf_bytes: bytes, filename: str) -> str:
    downloads_dir = Path('geobusca_data') / 'downloads'
    downloads_dir.mkdir(parents=True, exist_ok=True)
    file_path = downloads_dir / filename
    file_path.write_bytes(pdf_bytes)
    return str(file_path.absolute())
