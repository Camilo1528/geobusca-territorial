# PLAN DE MANTENIMIENTO Y REQUISITOS DE HARDWARE
**GeoBusca Territorial v2.0**

## 1. REQUISITOS DE HARDWARE SUGERIDOS

### A. Para Servidor (Umbrel / Local Permanente)
*   **Procesador:** Mínimo 4 núcleos (Recomendado Intel i5 o superior).
*   **Memoria RAM:** Mínimo 8GB (Recomendado 16GB para procesos masivos de geocodificación).
*   **Almacenamiento:** 500GB SSD (Estado Sólido) para asegurar velocidad en lecturas de base de datos.
*   **Red:** Conexión estable a internet para consultas a APIs externas (Google/Nominatim).

### B. Para Estación de Trabajo (Usuario Analista)
*   **Navegador:** Google Chrome, Microsoft Edge o Firefox (Versiones actualizadas).
*   **Resolución de Pantalla:** Mínimo 1366x768 (Recomendado Full HD 1920x1080 para visualización de mapas).

---

## 2. PLAN DE MANTENIMIENTO SUGERIDO

### Mantenimiento Semanal (Operativo)
1.  **Backup de Base de Datos:** Copiar el archivo `geobusca_saas.db` a una unidad externa o nube.
2.  **Revisión de Logs:** Verificar en el archivo `app.log` si hay errores recurrentes de conexión con las APIs de mapas.
3.  **Limpieza de Temporales:** Eliminar archivos de exportación antiguos de la carpeta `exports`.

### Mantenimiento Mensual (Técnico)
1.  **Optimización de DB:** Ejecutar el comando `VACUUM` en SQLite para compactar la base de datos y mejorar la velocidad.
2.  **Actualización de Capas:** Verificar si hay nuevas versiones de los perímetros urbanos o barrios en la oficina de Planeación.
3.  **Revisión de Seguridad:** Auditar el `Audit Log` para detectar intentos de acceso fallidos o comportamientos inusuales.

### Mantenimiento Trimestral (Sistemas)
1.  **Actualización de Dependencias:** Revisar si existen parches de seguridad para las librerías de Python.
2.  **Escalabilidad:** Evaluar si el crecimiento de la base de datos requiere ampliar la memoria RAM del servidor.
