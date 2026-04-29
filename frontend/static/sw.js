const CACHE_NAME = 'geobusca-v1-offline';
const TILE_CACHE_NAME = 'geobusca-tiles';

// Archivos básicos para que la app cargue offline
const STATIC_ASSETS = [
  '/',
  '/static/css/styles.css',
  '/static/css/visits.css',
  '/static/js/visits.js',
  'https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js'
];

self.addEventListener('install', (event) => {
  event.waitUntil(
    caches.open(CACHE_NAME).then((cache) => {
      return cache.addAll(STATIC_ASSETS);
    })
  );
});

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Estrategia especial para Tiles de Mapas (Stamen, OSM, etc.)
  if (url.hostname.includes('tile') || url.hostname.includes('openstreetmap') || url.hostname.includes('basemaps.cartocdn.com')) {
    event.respondWith(
      caches.open(TILE_CACHE_NAME).then((cache) => {
        return cache.match(event.request).then((response) => {
          return response || fetch(event.request).then((networkResponse) => {
            cache.put(event.request, networkResponse.clone());
            return networkResponse;
          });
        });
      })
    );
    return;
  }

  // Estrategia Cache-First para assets estáticos
  event.respondWith(
    caches.match(event.request).then((response) => {
      return response || fetch(event.request);
    })
  );
});
