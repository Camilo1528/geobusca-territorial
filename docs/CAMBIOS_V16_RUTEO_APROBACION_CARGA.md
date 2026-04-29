# Cambios V16 - Ruteo, aprobación estructurada y carga masiva de agendas

## Mejoras implementadas

- Ruteo geográfico con optimización básica por vecino más cercano.
- Optimización desde asignación y desde agenda diaria.
- Métricas de ruta estimada en kilómetros y minutos.
- Aprobación por revisor con causales estructuradas.
- Observaciones obligatorias al devolver o rechazar.
- Carga masiva de agendas por funcionario desde CSV o Excel.

## Cambios técnicos

- Nuevos campos en `visit_records`:
  - `approval_reason_code`
  - `approval_reason_label`
  - `route_optimization_method`
  - `route_optimized_at`
  - `route_optimized_by`
  - `route_distance_km`
  - `route_estimated_minutes`
- Formulario de aprobación enriquecido.
- Agenda diaria con acción de optimización.
- Módulo de asignación con importación masiva.

## Supuestos

- La optimización de ruta usa coordenadas del registro o de la visita.
- Si un punto no tiene coordenadas, se deja al final de la ruta.
- La carga masiva acepta columnas como:
  - `dataset_id`
  - `row_idx`
  - `assigned_to`
  - `assigned_to_email`
  - `agenda_date`
  - `route_group`
  - `agenda_order`

## Pendientes recomendados

- Optimización usando red vial real.
- Distancias por API de ruteo.
- Mapas de agenda por funcionario.
- Plantilla modelo para carga masiva descargable.
