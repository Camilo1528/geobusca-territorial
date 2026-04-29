# Cambios V7 - Registro de Visita Tributaria

## Ajuste principal
Se reemplazó la sección simplificada de "Actualización de datos desde visita" por un formulario web basado en el formato oficial **Registro de Visita Tributaria (RVT)** de la Subsecretaría de Rentas del Municipio de Rionegro.

## Qué quedó integrado
- Datos informativos de la visita:
  - tipo de visita RVT
  - código de establecimiento
  - fecha y hora
- Datos de quien recibe la visita
- Datos del contribuyente
- Representación legal
- Firmas de recepción
- Validación de deuda integrada al final del formulario

## Efectos funcionales
- Los datos capturados en el RVT actualizan el dataset cuando aplican
- La razón social actualiza el nombre del establecimiento
- La dirección del establecimiento actualiza la dirección operativa y geocodable
- Se conservan los estados de deuda y el historial de visitas
- La cola de visitas y el historial ahora muestran el tipo RVT y quién recibió la visita

## Archivos modificados
- institutional.py
- geobusca_saas_complete/app.py
- geobusca_saas_complete/templates/visits.html
