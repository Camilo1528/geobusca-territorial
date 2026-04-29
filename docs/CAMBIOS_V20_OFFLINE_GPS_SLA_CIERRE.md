# CAMBIOS V20 - campo y supervisión

## Mejoras integradas
- sincronización offline para check-in, check-out y actualizaciones de agenda
- endpoint JSON `/api/agenda/action` para sincronización desde tablet
- mapa en tiempo real de supervisión usando última posición reportada por funcionario
- mini mapa de última posición en agenda diaria
- cálculo de SLA por visita (`met`, `breached`, `open`, `active`, `planned`, `no_schedule`)
- resumen y cierre de jornada por funcionario
- corrección de firma táctil en RVT:
  - resize al abrir paso de firmas
  - captura de métricas de trazo
  - mejor soporte pointer/touch

## Notas
- la sincronización offline usa `localStorage` del navegador
- el mapa en tiempo real usa los últimos datos capturados de check-in/check-out/visita
- el cierre de jornada genera bitácora y notificación interna
