# GeoBusca Web Integrado v6

## Mejoras integradas

### 1. Procesamiento en background
- Nuevo modelo de jobs en SQLite.
- Nuevo endpoint y botón de procesamiento asíncrono.
- Vista de monitoreo de trabajos.

### 2. Auditoría por usuario
- Tabla `audit_log` para acciones críticas.
- Se registran procesamiento, ediciones manuales, visitas e importación de deudas.

### 3. API institucional
- `/api/datasets/<id>/summary`
- `/api/datasets/<id>/territories`
- `/api/datasets/<id>/visits`
- `/api/datasets/<id>/history`

### 4. Histórico por periodos
- Comparación del dataset actual contra el último dataset anterior de la misma ciudad/región.
- Cambios por comuna, barrio, corregimiento y vereda.

### 5. Visita de funcionario
- Nueva vista web para registrar visitas en campo.
- Permite actualizar nombre, dirección, teléfono, email, estado del establecimiento y observaciones.

### 6. Validación de deuda
- Registro manual del estado de deuda, monto, referencia y fuente.
- Importación masiva desde CSV/Excel por `row_idx` o `nom_establec`.
- Exportación del dato en CSV, mapa y Excel.

### 7. Excel institucional mejorado
- Hojas de visitas resumen y visitas detalle.
- Resumen general con indicadores de visitas y deuda.

## Limitaciones actuales
- La validación de deuda quedó lista para captura manual o carga masiva, pero **no** conectada a una API tributaria externa real.
- El procesamiento en background usa hilos del proceso Flask; para producción grande convendría migrarlo a un worker dedicado.
