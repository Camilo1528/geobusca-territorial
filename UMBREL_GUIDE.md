# Guía de Instalación en Umbrel OS

Umbrel OS es ideal para correr GeoBusca Territorial. Tienes dos formas de subirlo "desde la web" sin usar la terminal (SSH).

## Opción 1: Usando Portainer (Recomendado / Más fácil)

Portainer es una herramienta web para gestionar Docker que puedes instalar desde la App Store oficial de Umbrel.

1.  **Instala Portainer** desde la tienda de aplicaciones de Umbrel.
2.  Abre Portainer y ve a **"Stacks"** -> **"Add stack"**.
3.  Ponle un nombre (ej. `geobusca`).
4.  Copia y pega el contenido del archivo [`docker-compose.umbrel.yml`](./docker-compose.umbrel.yml) en el editor web.
5.  **IMPORTANTE:** En Portainer, cambia `${APP_DATA_DIR}` por una ruta real en tu Umbrel (ej. `/data/geobusca`).
6.  Haz clic en **"Deploy the stack"**.

## Opción 2: Crear tu propia App Store (Nativo)

Si quieres que aparezca con icono y nombre en el panel principal de Umbrel:

1.  **Sube este proyecto a GitHub**: Crea un repositorio (público o privado).
2.  Asegúrate de que los archivos `umbrel-app.yml` y `docker-compose.umbrel.yml` estén en la raíz (puedes renombrar el compose a `docker-compose.yml` en el repo).
3.  En tu Umbrel, abre la **App Store**.
4.  Haz clic en los **tres puntos (⋮)** arriba a la derecha -> **Community App Stores**.
5.  Pega la URL de tu repositorio de GitHub y dale a **Add**.
6.  Busca "GeoBusca Territorial" en la lista e instálalo.

## Notas sobre el Icono
He generado un icono profesional para la aplicación. Si usas la **Opción 2**, debes guardarlo como `metadata/icon.png` en tu repositorio.

---
**Nota:** Antes de que cualquiera de estas opciones funcione, debes haber subido la imagen a Docker Hub siguiendo la [Guía de Docker](./DOCKER_GUIDE.md).
