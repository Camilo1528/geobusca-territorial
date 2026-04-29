# CAMBIOS V18 - Multi-staff, horario/capacidad y productividad diaria

## Incluye
- Planificador multi-funcionario / multi-vehículo desde asignación de visitas.
- Restricciones por horario y capacidad por funcionario.
- Horarios estimados por visita (inicio/fin) y minutos de servicio.
- Tracking de cumplimiento de agenda desde la vista diaria.
- Dashboard gerencial con productividad diaria por funcionario y territorio.
- Plantilla descargable para planificador multi-staff.

## Notas
- La distribución multi-funcionario usa una estrategia greedy con optimización de orden por ruta y posterior partición por capacidad.
- Las restricciones horarias se aplican sobre la agenda estimada; si no caben visitas, quedan sin asignar y se notifican.
- El dashboard diario usa la fecha seleccionada y calcula cumplimiento a partir de completion_status.
