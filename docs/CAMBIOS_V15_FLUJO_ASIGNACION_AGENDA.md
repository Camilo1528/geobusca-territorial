# CAMBIOS V15 - Flujo de aprobación, asignación, agenda e indicadores territoriales

## Lo agregado

- Workflow de aprobación por revisor.
- Asignación de visitas por funcionario y territorio.
- Agenda/ruteo diario por funcionario.
- Indicadores gerenciales por comuna, barrio, vereda y corregimiento.

## Nuevos módulos

- approval_queue
- visit_assignments
- daily_agenda

## Nuevas pantallas

- `approval_queue.html`
- `visit_assignments.html`
- `daily_agenda.html`

## Backend

### visit_records
Se agregaron columnas para:
- `approval_status`
- `approval_notes`
- `reviewed_by`
- `reviewed_at`
- `assigned_to`
- `assigned_by`
- `assigned_at`
- `assignment_status`
- `territory_scope`
- `agenda_date`
- `agenda_order`
- `route_group`

### Flujo
- Cuando un funcionario guarda una visita, queda en estado `submitted`.
- Un revisor puede aprobar, devolver, rechazar o marcar en revisión.
- Se pueden asignar filas por territorio y fecha a un funcionario.
- La agenda diaria muestra el ruteo simple por orden y grupo.

## Indicadores gerenciales
Se agregó consolidado territorial para:
- comunas
- barrios
- veredas
- corregimientos

Incluye:
- total de registros por territorio
- pendientes de revisión por territorio

## Validación realizada
- Compilación sintáctica de Python.

## Pendiente futuro sugerido
- mover rutas a un módulo separado
- optimización del consolidado territorial para datasets muy grandes
- ruteo con optimización geográfica real
- aprobación con observaciones estructuradas por causal
