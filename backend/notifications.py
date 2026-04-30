from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from typing import Dict, Tuple
from urllib.parse import quote_plus
from backend.database_web import get_conn


def _clean(value: object) -> str:
    return '' if value is None else str(value).strip()


def _format_money(value: object) -> str:
    text = _clean(value)
    if not text:
        return ''
    try:
        number = float(text)
        return f"${number:,.0f}".replace(',', '.')
    except Exception as exc:
        logging.debug(f"Error formateando número '{text}': {exc}")
        return text


def smtp_config() -> Dict[str, object]:
    # Try database settings first
    try:
        with get_conn() as conn:
            row = conn.execute('SELECT * FROM smtp_settings WHERE id = 1').fetchone()
            if row:
                return {
                    'host': (row['smtp_server'] or '').strip(),
                    'port': int(row['smtp_port'] or 587),
                    'username': (row['smtp_user'] or '').strip(),
                    'password': (row['smtp_password'] or '').strip(),
                    'from_email': (row['smtp_from'] or '').strip(),
                    'from_name': os.getenv('SMTP_FROM_NAME', 'GeoBusca Territorial').strip(),
                    'use_tls': bool(row['use_tls'])
                }
    except:
        pass

    # Fallback to environment variables
    cfg = {
        'host': os.getenv('SMTP_HOST', 'smtp.gmail.com').strip(),
        'port': int(os.getenv('SMTP_PORT', '587').strip()),
        'username': os.getenv('SMTP_USERNAME', 'camilo152893@gmail.com').strip(),
        'password': os.getenv('SMTP_PASSWORD', 'iucihbjuscpxajyg').strip(),
        'from_email': os.getenv('SMTP_FROM_EMAIL', 'camilo152893@gmail.com').strip(),
        'from_name': os.getenv('SMTP_FROM_NAME', 'GeoBusca Territorial').strip(),
        'use_tls': os.getenv('SMTP_USE_TLS', '1').strip() != '0',
    }
    return cfg


def smtp_ready() -> bool:
    cfg = smtp_config()
    ready = bool(cfg['host'] and cfg['from_email'])

    if not ready:
        missing = []
        if not cfg['host']:
            missing.append('SMTP_HOST')
        if not cfg['from_email']:
            missing.append('SMTP_FROM_EMAIL')
        logging.warning(
            f"Configuración SMTP incompleta. Faltan: {
                ', '.join(missing)}")

    return ready


def build_visit_email_subject(context: Dict[str, object]) -> str:
    visit_type = _clean(context.get('rvt_tipo_visita')
                        or context.get('visita_estado') or 'RVT')
    establishment = _clean(context.get('rvt_razon_social')
                           or context.get('nom_establec') or 'establecimiento')
    row_idx = _clean(context.get('row_idx'))
    return f'RVT {visit_type} - fila {row_idx} - {establishment}'


def build_visit_email_text(context: Dict[str, object], print_url: str) -> str:
    lines = [
        'Registro de Visita Tributaria',
        '',
        f"Establecimiento: {
            _clean(
                context.get('rvt_razon_social') or context.get('nom_establec'))}",
        f"NIT / C.C.: {_clean(context.get('rvt_nit_cc'))}",
        f"Direccion: {
            _clean(
                context.get('rvt_direccion_establecimiento') or context.get('direccion'))}",
        f"Tipo visita: {
            _clean(
                context.get('rvt_tipo_visita') or context.get('visita_estado'))}",
        f"Fecha visita: {_clean(context.get('visita_fecha'))}",
        f"Funcionario: {
            _clean(
                context.get('rvt_funcionario_firma_nombre') or context.get('visita_funcionario'))}",
        f"Recibe: {_clean(context.get('rvt_recibe_nombre'))}",
        f"Estado deuda: {_clean(context.get('deuda_estado'))}",
    ]
    debt_amount = _format_money(context.get('deuda_monto'))
    if debt_amount:
        lines.append(f'Monto deuda: {debt_amount}')
    if print_url:
        lines.extend(['', f'Vista imprimible: {print_url}'])
    obs = _clean(context.get('visita_observaciones'))
    if obs:
        lines.extend(['', f'Observaciones: {obs}'])
    return '\n'.join(lines)


def build_visit_email_html(context: Dict[str, object], print_url: str) -> str:
    def row(label: str, value: object) -> str:
        value = _clean(value) or '&mdash;'
        return (
            '<tr>'
            '<td style="padding:6px 10px;border:1px solid #ddd;font-weight:600;background:#f8f9fa;">'
            f'{label}</td>'
            '<td style="padding:6px 10px;border:1px solid #ddd;">'
            f'{value}</td>'
            '</tr>'
        )

    rows = [
        row('Establecimiento', context.get('rvt_razon_social')
            or context.get('nom_establec')),
        row('NIT / C.C.', context.get('rvt_nit_cc')),
        row('Direccion', context.get('rvt_direccion_establecimiento')
            or context.get('direccion')),
        row('Tipo visita', context.get('rvt_tipo_visita')
            or context.get('visita_estado')),
        row('Fecha visita', context.get('visita_fecha')),
        row('Funcionario', context.get('rvt_funcionario_firma_nombre')
            or context.get('visita_funcionario')),
        row('Recibe', context.get('rvt_recibe_nombre')),
        row('Estado deuda', context.get('deuda_estado')),
        row('Monto deuda', _format_money(context.get('deuda_monto'))),
    ]
    button = ''
    if print_url:
        button = (
            '<p style="margin:20px 0;">'
            f'<a href="{print_url}" style="display:inline-block;padding:10px 16px;background:#0d6efd;color:#fff;text-decoration:none;border-radius:6px;">'
            'Abrir vista imprimible</a></p>'
        )
    rows_html = ''.join(rows)
    return (
        '<html><body style="font-family:Arial,Helvetica,sans-serif;color:#212529;">'
        '<h2 style="margin-bottom:8px;">Registro de Visita Tributaria</h2>'
        '<p style="margin-top:0;color:#555;">Subsecretaria de Rentas - Municipio de Rionegro</p>'
        f'<table style="border-collapse:collapse;width:100%;max-width:720px;">{rows_html}</table>'
        f'{button}'
        '<p style="color:#666;font-size:12px;">Correo generado automaticamente por GeoBusca Territorial.</p>'
        '</body></html>'
    )


def build_visit_whatsapp_url(
        context: Dict[str, object], print_url: str = '') -> str:
    establishment = _clean(context.get('rvt_razon_social')
                           or context.get('nom_establec') or 'Establecimiento')
    visit_type = _clean(context.get('rvt_tipo_visita')
                        or context.get('visita_estado') or 'RVT')
    visit_date = _clean(context.get('visita_fecha'))
    debt_state = _clean(context.get('deuda_estado') or 'SIN_VALIDAR')
    row_idx = _clean(context.get('row_idx'))
    parts = [
        '*Registro de Visita Tributaria*',
        f'Fila: {row_idx}',
        f'Establecimiento: {establishment}',
        f'Tipo: {visit_type}',
        f'Fecha: {visit_date}',
        f'Deuda: {debt_state}',
    ]
    if print_url:
        parts.extend(['', f'Vista imprimible: {print_url}'])
    text = '\n'.join(parts)
    return f'https://wa.me/?text={quote_plus(text)}'


def send_email(to_email: str, subject: str, body: str) -> tuple[bool, str]:
    """Helper to send a simple text email."""
    return send_html_email(
        to_email=to_email,
        subject=subject,
        html_body=f"<html><body><p>{body}</p></body></html>",
        text_body=body
    )


def send_html_email(to_email: str, subject: str, html_body: str,
                    text_body: str = '',
                    attachment_data: bytes = None,
                    attachment_filename: str = '') -> tuple[bool, str]:
    to_email = _clean(to_email)
    if not to_email:
        return False, 'sin destinatario'
    cfg = smtp_config()
    if not smtp_ready():
        return False, 'SMTP no configurado'

    msg = EmailMessage()
    msg['Subject'] = subject
    from_name = _clean(cfg['from_name']) or 'GeoBusca Territorial'
    msg['From'] = f"{from_name} <{cfg['from_email']}>"
    msg['To'] = to_email
    msg.set_content(text_body or 'Registro de Visita Tributaria')
    msg.add_alternative(html_body, subtype='html')

    if attachment_data and attachment_filename:
        maintype = 'application'
        subtype = 'pdf' if attachment_filename.lower().endswith('.pdf') else 'octet-stream'
        msg.add_attachment(
            attachment_data,
            maintype=maintype,
            subtype=subtype,
            filename=attachment_filename
        )

    try:
        with smtplib.SMTP(str(cfg['host']), int(cfg['port']), timeout=20) as server:
            if cfg['use_tls']:
                server.starttls()
            if cfg['username']:
                server.login(str(cfg['username']), str(cfg['password']))
            server.send_message(msg)
        return True, ''
    except Exception as exc:
        logging.error(f"FALLO ENVÍO EMAIL A {to_email}: {exc}")
        return False, str(exc)
