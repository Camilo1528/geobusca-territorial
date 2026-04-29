import threading
import json
import logging
import pandas as pd
from typing import Dict, Optional, Callable, Any
from backend.database_web import get_conn, now_iso
import backend.services.event_service as event_service
import backend.services.dataset_service as dataset_service
import backend.services.visit_service as visit_service
import backend.services.report_service as report_service

# Global dictionary to track active job threads
JOB_THREADS: Dict[int, threading.Thread] = {}

def create_job(user_id: int, dataset_id: int, job_type: str,
               payload: Optional[dict] = None) -> int:
    """Creates a job record in the database."""
    with get_conn() as conn:
        cur = conn.execute(
            'INSERT INTO jobs (dataset_id, user_id, job_type, status, progress, current_step, requested_payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (dataset_id, user_id, job_type, 'queued', 0, 'en cola',
             json.dumps(payload or {}, ensure_ascii=False), now_iso()),
        )
        return int(cur.lastrowid)

def update_job(job_id: int, *, status: Optional[str] = None, progress: Optional[int] = None, 
               current_step: Optional[str] = None, error_message: Optional[str] = None, 
               started: bool = False, finished: bool = False) -> None:
    """Updates a job record in the database."""
    updates = []
    params = []
    if status is not None:
        updates.append('status=?')
        params.append(status)
    if progress is not None:
        updates.append('progress=?')
        params.append(max(0, min(100, int(progress))))
    if current_step is not None:
        updates.append('current_step=?')
        params.append(current_step)
    if error_message is not None:
        updates.append('error_message=?')
        params.append(error_message)
    if started:
        updates.append('started_at=?')
        params.append(now_iso())
    if finished:
        updates.append('finished_at=?')
        params.append(now_iso())
    
    if not updates:
        return
        
    params.append(job_id)
    with get_conn() as conn:
        conn.execute(f'UPDATE jobs SET {", ".join(updates)} WHERE id=?', params)

def get_job(job_id: int) -> Optional[dict]:
    """Retrieves a job by ID."""
    with get_conn() as conn:
        row = conn.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        return dict(row) if row else None

def start_background_job(job_id: int, target: Callable, args: tuple) -> None:
    """Starts a target function in a background thread and tracks it."""
    thread = threading.Thread(target=target, args=args, daemon=True)
    JOB_THREADS[job_id] = thread
    thread.start()

def run_process_job_wrapper(job_id: int, dataset_id: int, user: dict, address_col: str,
                           provider: str, api_key: str, city: str, region: str, 
                           activity_col: str = '', process_func: Callable = None, 
                           summary_func: Callable = None) -> None:
    """
    Standard background task for dataset processing.
    Delegates to process_func and summary_func which should be passed from app core
    to avoid circular dependencies in this service.
    """
    try:
        update_job(job_id, status='running', progress=5, current_step='cargando dataset', started=True)
        
        df = dataset_service.load_dataset(dataset_id, user['id'])
        
        update_job(job_id, status='running', progress=15, current_step='geocodificando y enriqueciendo')
        
        # We assume process_func handles the heavy lifting
        if process_func:
            processed = process_func(
                df,
                address_col=address_col,
                provider=provider,
                api_key=api_key,
                city=city,
                region=region,
                activity_col=activity_col,
                job_id=job_id
            )
        else:
            # Fallback or error
            raise ValueError("No processing function provided to job runner")

        update_job(job_id, status='running', progress=80, current_step='guardando resultados')
        
        dataset_service.save_processed_dataset(dataset_id, user['id'], processed)
        
        summary = summary_func(processed) if summary_func else {}
        
        with get_conn() as conn:
            conn.execute(
                '''INSERT INTO runs (dataset_id, user_id, status, provider, total_rows, ok_rows, 
                   exportable_rows, manual_rows, duplicate_rows, created_at, notes) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (dataset_id, user['id'], 'finished', provider,
                 summary.get('total', 0), summary.get('ok_rows', 0), summary.get('exportable_rows', 0),
                 summary.get('manual_rows', 0), summary.get('duplicate_rows', 0),
                 now_iso(), 'background_job'),
            )
            
        event_service.log_audit(
            user['id'], 'process_async_completed', 'dataset', dataset_id,
            dataset_id=dataset_id, details={'job_id': job_id, 'summary': summary}
        )
        
        update_job(job_id, status='finished', progress=100, current_step='completado', finished=True)
        
    except Exception as exc:
        logging.error(f"Error in job {job_id}: {exc}", exc_info=True)
        update_job(job_id, status='failed', progress=100, current_step='falló', error_message=str(exc), finished=True)
        
        event_service.log_audit(
            user['id'], 'process_async_failed', 'dataset', dataset_id,
            dataset_id=dataset_id, details={'job_id': job_id, 'error': str(exc)}
        )
    finally:
        JOB_THREADS.pop(job_id, None)

def run_pdf_job_wrapper(job_id: int, user_id: int, dataset_id: int, row_idx: int) -> None:
    """Background task to generate a PDF report for a specific visit."""
    try:
        update_job(job_id, status='running', progress=10, current_step='cargando datos de visita', started=True)
        
        dataset = dataset_service.get_dataset(dataset_id, user_id)
        if not dataset:
            raise ValueError(f"Dataset {dataset_id} no encontrado")
            
        visit = visit_service.get_visit_record_by_row(dataset_id, row_idx)
        if not visit:
            raise ValueError(f"Registro de visita para fila {row_idx} no encontrado")
            
        update_job(job_id, status='running', progress=30, current_step='generando PDF')
        
        # Build PDF
        pdf_bytes = report_service.build_visit_pdf_bytes(dataset, visit)
        
        update_job(job_id, status='running', progress=80, current_step='guardando archivo')
        
        # Save to disk
        filename = f"RVT_DS{dataset_id}_R{row_idx}_{now_iso().replace(':', '-')}.pdf"
        file_path = report_service.save_report_to_disk(pdf_bytes, filename)
        
        # Store file path in job result or notes
        with get_conn() as conn:
            conn.execute('UPDATE jobs SET notes=? WHERE id=?', (json.dumps({'file_path': file_path}), job_id))
            
        event_service.create_notification(
            user_id, 'pdf_ready', 'PDF Generado',
            f'El reporte RVT para la fila {row_idx} del dataset {dataset_id} está listo para descargar.',
            f'/visit_media/{filename}'
        )
            
        event_service.log_audit(
            user_id, 'pdf_job_completed', 'visit', row_idx,
            dataset_id=dataset_id, details={'job_id': job_id, 'file': filename}
        )
        
        update_job(job_id, status='finished', progress=100, current_step='completado', finished=True)
        
    except Exception as exc:
        logging.error(f"Error in PDF job {job_id}: {exc}", exc_info=True)
        update_job(job_id, status='failed', progress=100, current_step='falló', error_message=str(exc), finished=True)
        
        event_service.log_audit(
            user_id, 'pdf_job_failed', 'visit', row_idx,
            dataset_id=dataset_id, details={'job_id': job_id, 'error': str(exc)}
        )
    finally:
        JOB_THREADS.pop(job_id, None)
