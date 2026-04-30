const queueRows = window.queueRows || [];
const visitRecords = window.visitRecords || [];
const datasetId = window.datasetId || 0;
const totalRows = window.totalRows || 0;

const stepButtons = Array.from(document.querySelectorAll('.step-trigger'));
const steps = Array.from(document.querySelectorAll('.wizard-step'));
const form = document.getElementById('visit-form');
const rowInput = document.getElementById('row_idx');
const draftPrefix = `geobusca-rvt-draft-${datasetId}`;
const offlineQueueKey = `geobusca-rvt-offline-queue-${datasetId}`;
let currentStep = 1;
const gpsButton = document.getElementById('gpsButton');
const gpsStatus = document.getElementById('gpsStatus');
const visit_latitude = document.getElementById('visit_latitude');
const visit_longitude = document.getElementById('visit_longitude');
const visit_gps_accuracy = document.getElementById('visit_gps_accuracy');

function updateProgressBar() {
  const progress = (currentStep / steps.length) * 100;
  const bar = document.getElementById('wizardProgress');
  if (bar) {
    bar.style.width = `${progress}%`;
    bar.textContent = `Paso ${currentStep} de ${steps.length}`;
  }
  
  // Mark previous steps as completed visually
  stepButtons.forEach(btn => {
    const s = Number(btn.dataset.step);
    btn.classList.toggle('completed', s < currentStep);
  });
}

function validateCurrentStep() {
  const currentStepEl = steps.find(el => Number(el.dataset.step) === currentStep);
  if (!currentStepEl) return true;
  
  const requiredFields = currentStepEl.querySelectorAll('[required]');
  let isValid = true;
  requiredFields.forEach(field => {
    if (!field.value.trim()) {
      field.classList.add('is-invalid');
      isValid = false;
    } else {
      field.classList.remove('is-invalid');
    }
  });
  
  if (!isValid) {
    // Optionally show a toast or alert
    console.warn('Faltan campos obligatorios en el paso actual');
  }
  return isValid;
}

function showStep(step) {
  const targetStep = Math.max(1, Math.min(6, Number(step) || 1));
  
  // Only validate if moving forward
  if (targetStep > currentStep && !validateCurrentStep()) {
    return;
  }

  currentStep = targetStep;
  steps.forEach((el) => el.classList.toggle('active', Number(el.dataset.step) === currentStep));
  stepButtons.forEach((btn) => {
    const active = Number(btn.dataset.step) === currentStep;
    btn.classList.toggle('btn-primary', active);
    btn.classList.toggle('btn-outline-primary', !active);
  });
  document.getElementById('prevStepBtn').disabled = currentStep === 1;
  document.getElementById('nextStepBtn').disabled = currentStep === steps.length;
  steps.find((el) => Number(el.dataset.step) === currentStep)?.scrollIntoView({behavior:'smooth', block:'start'});
  if (currentStep === 6) { setTimeout(() => { receiverPad.resize(true); officerPad.resize(true); }, 80); }
  updateProgressBar();
}

stepButtons.forEach(btn => btn.addEventListener('click', () => showStep(btn.dataset.step)));
document.getElementById('prevStepBtn').addEventListener('click', () => showStep(currentStep - 1));
document.getElementById('nextStepBtn').addEventListener('click', () => showStep(currentStep + 1));
showStep(1);

document.getElementById('visit_device').value = `${navigator.platform || ''} | ${navigator.userAgent || ''}`.slice(0, 250);
if (!document.getElementById('visita_fecha').value) document.getElementById('visita_fecha').value = new Date().toISOString().slice(0, 10);
if (!document.getElementById('visita_hora').value) document.getElementById('visita_hora').value = new Date().toTimeString().slice(0, 5);

function draftKey(rowIdx) {
  const row = rowIdx || rowInput.value || 'generic';
  return `${draftPrefix}-${row}`;
}

function serializeForm() {
  const fd = new FormData(form);
  const payload = {};
  for (const [key, value] of fd.entries()) {
    if (value instanceof File) continue;
    payload[key] = value;
  }
  return payload;
}

function getOfflineQueue() {
  try { return JSON.parse(localStorage.getItem(offlineQueueKey) || '[]'); } catch (_) { return []; }
}

function normalizeQueueItem(item) {
  return {
    status: item.status || 'queued',
    attempts: Number(item.attempts || 0),
    last_error: item.last_error || '',
    queued_at: item.queued_at || new Date().toISOString(),
    ...item,
  };
}

function setOfflineQueue(items) {
  const normalized = items.map(normalizeQueueItem);
  localStorage.setItem(offlineQueueKey, JSON.stringify(normalized));
  updateOfflineStatus();
  renderOfflineQueue();
}

function updateOfflineStatus() {
  const online = navigator.onLine;
  const badge = document.getElementById('networkStatus');
  const count = document.getElementById('offlineQueueCount');
  const queue = getOfflineQueue();
  badge.textContent = online ? 'En línea' : 'Sin conexión';
  badge.className = `badge ${online ? 'text-bg-success' : 'text-bg-warning'}`;
  count.textContent = queue.filter(item => item.status !== 'synced').length;
}

function badgeClass(status) {
  if (status === 'synced') return 'text-bg-success';
  if (status === 'syncing') return 'text-bg-primary';
  if (status === 'conflict') return 'text-bg-warning';
  if (status === 'error') return 'text-bg-danger';
  return 'text-bg-secondary';
}

function renderOfflineQueue() {
  const root = document.getElementById('offlineQueueList');
  if (!root) return;
  const queue = getOfflineQueue();
  if (!queue.length) {
    root.innerHTML = '<div class="text-muted small">No hay elementos pendientes en la cola local.</div>';
    return;
  }
  root.innerHTML = queue.map((item, index) => {
    const row = item.row_idx || '?';
    const title = item.rvt_razon_social || item.nom_establec || 'Sin nombre';
    const status = item.status || 'queued';
    const updated = item.server_updated_at || '';
    return `
      <div class="sync-item">
        <div class="d-flex justify-content-between gap-2 flex-wrap align-items-center">
          <div>
            <div class="fw-semibold">Fila ${row} · ${title}</div>
            <small class="text-muted">Intentos: ${Number(item.attempts || 0)} · Guardado local: ${item.queued_at || ''}</small>
            ${updated ? `<small class="text-muted">Versión servidor: ${updated}</small>` : ''}
            ${item.last_error ? `<small class="text-danger">${item.last_error}</small>` : ''}
          </div>
          <div class="d-flex gap-2 flex-wrap align-items-center">
            <span class="badge ${badgeClass(status)}">${status}</span>
            <button type="button" class="btn btn-outline-primary btn-sm retry-offline-item" data-index="${index}">Reintentar</button>
            <button type="button" class="btn btn-outline-warning btn-sm overwrite-offline-item" data-index="${index}">Sobrescribir</button>
            <button type="button" class="btn btn-outline-danger btn-sm remove-offline-item" data-index="${index}">Quitar</button>
          </div>
        </div>
      </div>`;
  }).join('');
}

let draftTimer = null;
function showDraftFeedback() {
  const status = document.getElementById('draftStatus');
  if (!status) return;
  status.classList.remove('d-none');
  if (draftTimer) clearTimeout(draftTimer);
  draftTimer = setTimeout(() => {
    status.classList.add('d-none');
  }, 2000);
}

function saveDraft() {
  localStorage.setItem(draftKey(), JSON.stringify(serializeForm()));
  showDraftFeedback();
}

function loadDraftForRow(rowIdx) {
  const raw = localStorage.getItem(draftKey(rowIdx));
  if (!raw) return false;
  try {
    const data = JSON.parse(raw);
    Object.entries(data).forEach(([key, value]) => {
      const field = form.elements.namedItem(key);
      if (field && typeof value !== 'undefined') field.value = value;
    });
    if (data.visit_signature_receiver) receiverPad.fromDataURL(data.visit_signature_receiver);
    if (data.visit_signature_officer) officerPad.fromDataURL(data.visit_signature_officer);
    return true;
  } catch (_) {
    return false;
  }
}

function clearDraft() {
  localStorage.removeItem(draftKey());
}

document.getElementById('saveDraftBtn').addEventListener('click', () => { saveDraft(); alert('Borrador guardado en esta tablet.'); });
document.getElementById('clearDraftBtn').addEventListener('click', () => {
  if (confirm('¿Borrar el borrador local de esta fila?')) { clearDraft(); }
});
form.addEventListener('input', saveDraft);
form.addEventListener('change', saveDraft);

const SIDEBAR_PAGE_SIZE = 50;
let sidebarVisibleCount = SIDEBAR_PAGE_SIZE;

function renderQueueList() {
  const root = document.getElementById('queue-container');
  if (!root) return;
  
  const itemsToShow = queueRows.slice(0, sidebarVisibleCount);
  const activeIdx = queueRows.indexOf(queueRows.find(r => Number(r.row_idx) === Number(rowInput.value)));
  
  root.innerHTML = itemsToShow.map((row, index) => {
    const isActive = index === activeIdx;
    const title = row.nom_establec || row.rvt_razon_social || 'Sin nombre';
    return `
      <div class="queue-card ${isActive ? 'active' : ''}" data-index="${index}">
        <div class="d-flex justify-content-between align-items-start">
          <div class="fw-bold text-truncate" style="max-width: 200px;">${title}</div>
          <span class="badge bg-light text-dark" style="font-size: 10px;">Fila ${row.row_idx}</span>
        </div>
        <div class="small text-muted text-truncate mt-1">${row.direccion || row.rvt_direccion_establecimiento || 'Sin dirección'}</div>
      </div>
    `;
  }).join('');

  if (sidebarVisibleCount < queueRows.length) {
    const loadMore = document.createElement('button');
    loadMore.className = 'btn btn-link btn-sm w-100 text-muted mt-2';
    loadMore.textContent = `Cargar más (${queueRows.length - sidebarVisibleCount} restantes)`;
    loadMore.onclick = () => {
      sidebarVisibleCount += SIDEBAR_PAGE_SIZE;
      renderQueueList();
    };
    root.appendChild(loadMore);
  }
}

document.getElementById('newVisitBtn')?.addEventListener('click', () => {
  if (!confirm('¿Deseas iniciar un registro para un establecimiento NUEVO que no está en la lista?')) return;
  
  // Limpiar formulario y borradores
  form.reset();
  receiverPad.clear();
  officerPad.clear();
  clearDraft();
  
  const typeSelect = document.getElementById('rvt_tipo_visita');
  if (typeSelect) typeSelect.value = "Nuevo Establecimiento";
  
  // Asignar índice para fila nueva (al final del dataset real)

  const nextIdx = Number(window.totalRows || 0);
  rowInput.value = nextIdx;
  
  // Limpiar campos GPS
  visit_latitude.value = '';
  visit_longitude.value = '';
  visit_gps_accuracy.value = '';
  if (gpsStatus) gpsStatus.textContent = 'Punto nuevo: Captura el GPS ahora.';
  
  // Mostrar paso 1 y scroll arriba
  showStep(1);
  window.scrollTo({top: 0, behavior: 'smooth'});
  
  // Intentar capturar GPS automáticamente
  gpsButton?.click();
  
  // Quitar resaltado de la lista lateral
  document.querySelectorAll('.queue-card').forEach(c => c.classList.remove('active'));
});

document.getElementById('queue-container').addEventListener('click', (e) => {
  const card = e.target.closest('.queue-card');
  if (card) {
    const idx = Number(card.dataset.index);
    if (!isNaN(idx) && queueRows[idx]) applyQueueRow(queueRows[idx]);
  }
});

function applyQueueRow(row) {
  rowInput.value = row.row_idx ?? '';
  const record = visitRecords.find(item => Number(item.row_idx) === Number(row.row_idx));
  document.getElementById('force_overwrite').value = '0';
  const defaults = {
    rvt_razon_social: row.nom_establec || '',
    rvt_direccion_establecimiento: row.direccion || '',
    rvt_sector_economico: row.categoria_economica || '',
    deuda_estado: row.deuda_estado || '',
    deuda_monto: row.deuda_monto || '',
    visita_estado: 'REALIZADA',
  };
  Object.entries(defaults).forEach(([key, value]) => {
    const field = form.elements.namedItem(key);
    if (field && !field.value) field.value = value;
  });
  if (!loadDraftForRow(row.row_idx) && record) {
    Object.entries(record).forEach(([key, value]) => {
      const field = form.elements.namedItem(key);
      if (field && value !== null && typeof value !== 'undefined') field.value = value;
    });
    document.getElementById('server_updated_at').value = record.updated_at || '';
  }
  showStep(1);
  saveDraft();
  renderQueueList(); // Update active state
}
rowInput.addEventListener('change', () => loadDraftForRow(rowInput.value));

async function compressImageFile(file, maxSide = 1600, quality = 0.82) {
  if (!file || !file.type.startsWith('image/')) return '';
  const bitmap = await createImageBitmap(file);
  const scale = Math.min(1, maxSide / Math.max(bitmap.width, bitmap.height));
  const canvas = document.createElement('canvas');
  canvas.width = Math.max(1, Math.round(bitmap.width * scale));
  canvas.height = Math.max(1, Math.round(bitmap.height * scale));
  const ctx = canvas.getContext('2d');
  ctx.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
  return canvas.toDataURL('image/jpeg', quality);
}

function previewImage(inputId, previewId, hiddenId, emptyId) {
  const input = document.getElementById(inputId);
  const preview = document.getElementById(previewId);
  const hidden = document.getElementById(hiddenId);
  const empty = document.getElementById(emptyId);
  input.addEventListener('change', async () => {
    const file = input.files && input.files[0];
    if (!file || !file.type.startsWith('image/')) { 
      preview.classList.add('d-none'); 
      if (empty) empty.classList.remove('d-none');
      preview.removeAttribute('src'); 
      if (hidden) hidden.value = ''; 
      return; 
    }
    const dataUrl = await compressImageFile(file);
    if (hidden) hidden.value = dataUrl;
    preview.src = dataUrl;
    preview.classList.remove('d-none');
    if (empty) empty.classList.add('d-none');
    saveDraft();
  });
}
previewImage('visit_photo_establecimiento_file', 'preview_establecimiento', 'visit_photo_establecimiento_dataurl', 'empty_preview_establecimiento');
previewImage('visit_photo_documento_file', 'preview_documento', 'visit_photo_documento_dataurl', 'empty_preview_documento');

function readFileAsDataURL(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result || '');
    reader.onerror = () => reject(new Error('No se pudo leer el archivo'));
    reader.readAsDataURL(file);
  });
}

function formatFileSize(bytes) {
  const n = Number(bytes || 0);
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${Math.round(n / 1024)} KB`;
  return `${(n / (1024 * 1024)).toFixed(1)} MB`;
}

async function processAttachmentFiles() {
  const input = document.getElementById('visit_attachment_files');
  const previewRoot = document.getElementById('attachmentsPreviewList');
  const hidden = document.getElementById('visit_attachments_dataurls');
  const files = Array.from(input.files || []);
  const payload = [];
  previewRoot.innerHTML = '';
  for (const file of files.slice(0, 8)) {
    const col = document.createElement('div');
    col.className = 'col-md-3';
    let dataUrl = '';
    if (file.type.startsWith('image/')) {
      dataUrl = await compressImageFile(file, 1400, 0.8);
      col.innerHTML = `<div class="border rounded p-2 h-100"><img class="img-fluid rounded" src="${dataUrl}" alt="${file.name}"><div class="small mt-2 text-truncate">${file.name}</div><div class="text-muted small">${formatFileSize(file.size)}</div></div>`;
    } else {
      dataUrl = await readFileAsDataURL(file);
      const kind = file.type === 'application/pdf' ? 'PDF' : 'Archivo';
      col.innerHTML = `<div class="border rounded p-2 h-100"><div class="fw-semibold small">${kind}</div><div class="small text-truncate">${file.name}</div><div class="text-muted small">${formatFileSize(file.size)}</div><div class="small text-success mt-2">Disponible para sincronización offline</div></div>`;
    }
    if (dataUrl) payload.push({name: file.name, data_url: dataUrl, size: file.size, mime_type: file.type || ''});
    previewRoot.appendChild(col);
  }
  hidden.value = JSON.stringify(payload);
  saveDraft();
}

document.getElementById('visit_attachment_files').addEventListener('change', processAttachmentFiles);


class SignaturePadLite {
  constructor(canvas, hiddenInput, statsInput) {
    this.canvas = canvas;
    this.hiddenInput = hiddenInput;
    this.statsInput = statsInput;
    this.ctx = canvas.getContext('2d');
    this.drawing = false;
    this._lastDataUrl = '';
    this.points = [];
    this.strokeCount = 0;
    this.startedAt = null;
    this.bindEvents();
    this.resize();
    window.addEventListener('resize', () => this.resize(true));
  }
  bindEvents() {
    ['pointerdown','pointermove','pointerup','pointerleave','pointercancel'].forEach(evt => {
      this.canvas.addEventListener(evt, (e) => this.handle(evt, e), {passive:false});
    });
  }
  resize(preserve = false) {
    const rect = this.canvas.getBoundingClientRect();
    const ratio = window.devicePixelRatio || 1;
    const previous = preserve ? (this.hiddenInput.value || this._lastDataUrl || '') : '';
    this.canvas.width = Math.max(320, Math.round((rect.width || this.canvas.parentElement?.clientWidth || 320) * ratio));
    this.canvas.height = Math.max(180, Math.round((rect.height || 170) * ratio));
    this.ctx.setTransform(1, 0, 0, 1, 0, 0);
    this.ctx.scale(ratio, ratio);
    this.ctx.lineCap = 'round';
    this.ctx.lineJoin = 'round';
    this.ctx.strokeStyle = '#111';
    this.ctx.clearRect(0, 0, this.canvas.width / ratio, this.canvas.height / ratio);
    if (previous) this.fromDataURL(previous);
  }
  point(e) { 
    const rect = this.canvas.getBoundingClientRect(); 
    return { 
      x: e.clientX - rect.left, 
      y: e.clientY - rect.top, 
      t: Date.now(),
      pressure: e.pressure !== undefined ? e.pressure : 0.5 
    }; 
  }
  updateStats() {
    if (!this.statsInput) return;
    if (!this.points.length) { this.statsInput.value = ''; return; }
    const xs = this.points.map(p => p.x), ys = this.points.map(p => p.y);
    const payload = {
      points: this.points.length,
      strokes: this.strokeCount,
      duration_ms: this.startedAt ? Date.now() - this.startedAt : 0,
      min_x: Math.min(...xs), max_x: Math.max(...xs), min_y: Math.min(...ys), max_y: Math.max(...ys),
      viewport_w: Math.round(this.canvas.getBoundingClientRect().width || 0),
      viewport_h: Math.round(this.canvas.getBoundingClientRect().height || 0),
      device_pixel_ratio: window.devicePixelRatio || 1,
    };
    this.statsInput.value = JSON.stringify(payload);
  }
  handle(type, e) {
    // Evita el comportamiento por defecto (pan/scroll) del navegador en tablets
    if (e.cancelable) e.preventDefault();
    e.stopPropagation();
    
    // Sensibilidad a la presión si el stylus lo soporta
    const pressure = e.pressure !== undefined ? e.pressure : 0.5;
    const baseWidth = 2.4;
    const lineWidth = pressure > 0 ? Math.max(0.8, pressure * 4.5) : baseWidth;
    this.ctx.lineWidth = lineWidth;

    if (type === 'pointerdown') {
      this.drawing = true;
      this.canvas.setPointerCapture?.(e.pointerId);
      const p = this.point(e);
      if (!this.startedAt) this.startedAt = p.t;
      this.strokeCount += 1;
      this.points.push(p);
      this.ctx.beginPath();
      this.ctx.moveTo(p.x, p.y);
      this.ctx.lineTo(p.x + 0.01, p.y + 0.01);
      this.ctx.stroke();
      this.updateStats();
      return;
    }
    if (!this.drawing) return;
    if (type === 'pointermove') {
      const p = this.point(e);
      this.points.push(p);
      this.ctx.lineTo(p.x, p.y);
      this.ctx.stroke();
      this.updateStats();
      return;
    }
    if (type === 'pointerup' || type === 'pointerleave' || type === 'pointercancel') {
      this.drawing = false;
      this.canvas.releasePointerCapture?.(e.pointerId);
      this._lastDataUrl = this.canvas.toDataURL('image/png');
      this.hiddenInput.value = this._lastDataUrl;
      this.updateStats();
      saveDraft();
    }
  }
  clear() {
    const rect = this.canvas.getBoundingClientRect();
    this.ctx.clearRect(0, 0, rect.width, rect.height);
    this.hiddenInput.value = '';
    if (this.statsInput) this.statsInput.value = '';
    this._lastDataUrl = '';
    this.points = [];
    this.strokeCount = 0;
    this.startedAt = null;
  }
  fromDataURL(dataUrl) {
    if (!dataUrl) return;
    const img = new Image();
    img.onload = () => {
      const rect = this.canvas.getBoundingClientRect();
      this.ctx.clearRect(0, 0, rect.width, rect.height);
      this.ctx.drawImage(img, 0, 0, rect.width, rect.height);
      this.hiddenInput.value = dataUrl;
      this._lastDataUrl = dataUrl;
      this.updateStats();
    };
    img.src = dataUrl;
  }
}
const receiverPad = new SignaturePadLite(document.getElementById('signatureReceiver'), document.getElementById('visit_signature_receiver'), document.getElementById('visit_signature_receiver_stats'));
const officerPad = new SignaturePadLite(document.getElementById('signatureOfficer'), document.getElementById('visit_signature_officer'), document.getElementById('visit_signature_officer_stats'));
document.querySelectorAll('[data-clear-signature]').forEach(btn => btn.addEventListener('click', () => {
  if (btn.dataset.clearSignature === 'signatureReceiver') receiverPad.clear();
  if (btn.dataset.clearSignature === 'signatureOfficer') officerPad.clear();
  saveDraft();
}));

let visitMap = null;
let visitMarker = null;

function initVisitMap(lat, lon) {
  const mapEl = document.getElementById('visitMap');
  if (!mapEl) return;
  mapEl.classList.remove('d-none');
  
  if (!visitMap) {
    visitMap = L.map('visitMap').setView([lat, lon], 18);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '© OpenStreetMap contributors'
    }).addTo(visitMap);
    
    visitMarker = L.marker([lat, lon], { draggable: true }).addTo(visitMap);
    
    // Al arrastrar el pin, actualizar coordenadas
    visitMarker.on('dragend', function(e) {
      const pos = e.target.getLatLng();
      updateGpsFields(pos.lat, pos.lng, 5); // 5m como precisión manual
    });

    // Al hacer clic en el mapa, mover el pin
    visitMap.on('click', function(e) {
      visitMarker.setLatLng(e.latlng);
      updateGpsFields(e.latlng.lat, e.latlng.lng, 5);
    });
  } else {
    const pos = [lat, lon];
    visitMap.setView(pos, 18);
    visitMarker.setLatLng(pos);
  }
  setTimeout(() => visitMap.invalidateSize(), 100);
}

function updateGpsFields(lat, lon, acc) {
  if (visit_latitude) visit_latitude.value = Number(lat).toFixed(6);
  if (visit_longitude) visit_longitude.value = Number(lon).toFixed(6);
  if (visit_gps_accuracy) visit_gps_accuracy.value = Math.round(acc || 0);
  if (gpsStatus) gpsStatus.textContent = 'Ubicación ajustada manualmente.';
  saveDraft();
}

if (gpsButton) {
  gpsButton.addEventListener('click', () => {
    if (!navigator.geolocation) { 
      if (gpsStatus) gpsStatus.textContent = 'Este dispositivo no soporta geolocalización.'; 
      return; 
    }
    if (gpsStatus) gpsStatus.textContent = 'Obteniendo ubicación...';
    navigator.geolocation.getCurrentPosition((position) => {
      const lat = position.coords.latitude;
      const lon = position.coords.longitude;
      if (visit_latitude) visit_latitude.value = lat.toFixed(6);
      if (visit_longitude) visit_longitude.value = lon.toFixed(6);
      if (visit_gps_accuracy) visit_gps_accuracy.value = Math.round(position.coords.accuracy || 0);
      if (gpsStatus) gpsStatus.textContent = 'Ubicación capturada correctamente.';
      initVisitMap(lat, lon);
      saveDraft();
    }, (err) => {
      if (gpsStatus) gpsStatus.textContent = `No se pudo obtener ubicación: ${err.message}`;
    }, { enableHighAccuracy: true, timeout: 15000, maximumAge: 0 });
  });
}

async function queueOfflineVisit() {
  document.getElementById('visit_signature_receiver').value = receiverPad.hiddenInput.value || receiverPad.canvas.toDataURL('image/png');
  document.getElementById('visit_signature_receiver_stats').value = receiverPad.statsInput.value || '';
  document.getElementById('visit_signature_officer').value = officerPad.hiddenInput.value || officerPad.canvas.toDataURL('image/png');
  document.getElementById('visit_signature_officer_stats').value = officerPad.statsInput.value || '';
  const queue = getOfflineQueue();
  const payload = serializeForm();
  const rowIdx = payload.row_idx || String(Date.now());
  const existingIdx = queue.findIndex(item => String(item.row_idx) === String(rowIdx));
  const item = normalizeQueueItem({ ...payload, queued_at: new Date().toISOString(), status: 'queued', attempts: 0, last_error: '' });
  if (existingIdx >= 0) queue[existingIdx] = item; else queue.push(item);
  setOfflineQueue(queue);
  clearDraft();
  alert('Visita guardada sin conexión. Se sincronizará cuando vuelva internet.');
}

async function syncQueueItem(item, index, {forceOverwrite = false} = {}) {
  const queue = getOfflineQueue();
  const current = normalizeQueueItem(queue[index] || item);
  current.status = 'syncing';
  current.attempts = Number(current.attempts || 0) + 1;
  current.last_error = '';
  queue[index] = current;
  setOfflineQueue(queue);
  try {
    const payload = { ...current, force_overwrite: forceOverwrite ? '1' : (current.force_overwrite || '0') };
    const resp = await fetch(`/api/visits/${datasetId}/sync_save`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      credentials: 'same-origin',
      body: JSON.stringify(payload),
    });
    const data = await resp.json();
    if (resp.status === 409 || data.conflict) {
      current.status = 'conflict';
      current.last_error = 'Conflicto: existe una versión más reciente en el servidor.';
      current.server_updated_at = data.server_updated_at || '';
      current.server_record = data.server_record || null;
      current.force_overwrite = '0';
      queue[index] = current;
      setOfflineQueue(queue);
      return false;
    }
    if (!resp.ok || !data.ok) throw new Error(data.error || 'No se pudo sincronizar');
    queue[index] = { ...current, status: 'synced', server_updated_at: data.server_updated_at || '', last_error: '' };
    setOfflineQueue(queue);
    queue.splice(index, 1);
    setOfflineQueue(queue);
    return true;
  } catch (err) {
    current.status = 'error';
    current.last_error = err.message || 'Error de sincronización';
    queue[index] = current;
    setOfflineQueue(queue);
    return false;
  }
}

async function syncOfflineVisits() {
  if (!navigator.onLine) { updateOfflineStatus(); return; }
  const queue = getOfflineQueue();
  if (!queue.length) return;
  let okCount = 0;
  for (let i = 0; i < queue.length; i += 1) {
    const success = await syncQueueItem(queue[i], i);
    if (success) okCount += 1;
  }
  const remaining = getOfflineQueue().filter(item => item.status !== 'synced');
  if (!remaining.length && okCount) alert('Todas las visitas pendientes quedaron sincronizadas.');
}

document.getElementById('syncVisitsBtn').addEventListener('click', syncOfflineVisits);
document.getElementById('offlineQueueList').addEventListener('click', async (event) => {
  const retryBtn = event.target.closest('.retry-offline-item');
  const overwriteBtn = event.target.closest('.overwrite-offline-item');
  const removeBtn = event.target.closest('.remove-offline-item');
  if (retryBtn) {
    const index = Number(retryBtn.dataset.index);
    const queue = getOfflineQueue();
    if (queue[index]) await syncQueueItem(queue[index], index, {forceOverwrite: false});
  }
  if (overwriteBtn) {
    const index = Number(overwriteBtn.dataset.index);
    const queue = getOfflineQueue();
    if (queue[index]) await syncQueueItem(queue[index], index, {forceOverwrite: true});
  }
  if (removeBtn) {
    const index = Number(removeBtn.dataset.index);
    const queue = getOfflineQueue();
    queue.splice(index, 1);
    setOfflineQueue(queue);
  }
});
window.addEventListener('online', syncOfflineVisits);
window.addEventListener('online', updateOfflineStatus);
window.addEventListener('offline', updateOfflineStatus);

form.addEventListener('submit', async (event) => {
  // Manual validation for all steps before submitting
  let firstInvalidStep = 0;
  for (let i = 1; i <= steps.length; i++) {
    const stepEl = steps.find(el => Number(el.dataset.step) === i);
    const requiredFields = stepEl.querySelectorAll('[required]');
    let stepValid = true;
    requiredFields.forEach(field => {
      if (!field.value.trim()) {
        field.classList.add('is-invalid');
        stepValid = false;
        if (firstInvalidStep === 0) firstInvalidStep = i;
      } else {
        field.classList.remove('is-invalid');
      }
    });
  }

  if (firstInvalidStep > 0) {
    event.preventDefault();
    showStep(firstInvalidStep);
    alert('Faltan campos obligatorios. Por favor revisa los pasos marcados.');
    return false;
  }

  document.getElementById('visit_signature_receiver').value = receiverPad.hiddenInput.value || receiverPad.canvas.toDataURL('image/png');
  document.getElementById('visit_signature_receiver_stats').value = receiverPad.statsInput.value || '';
  document.getElementById('visit_signature_officer').value = officerPad.hiddenInput.value || officerPad.canvas.toDataURL('image/png');
  document.getElementById('visit_signature_officer_stats').value = officerPad.statsInput.value || '';
  if (!navigator.onLine) {
    event.preventDefault();
    await queueOfflineVisit();
    return false;
  }
  clearDraft();
});

updateOfflineStatus();
renderOfflineQueue();
renderQueueList();

if (queueRows.length) {
  applyQueueRow(queueRows[0]);
} else {
  loadDraftForRow();
}

// Bulk Media Upload Logic
const bulkMediaForm = document.getElementById('bulkMediaForm');
if (bulkMediaForm) {
  bulkMediaForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const btn = document.getElementById('bulkSubmitBtn');
    const resultsDiv = document.getElementById('bulkResults');
    
    btn.disabled = true;
    btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Procesando ZIP...';
    resultsDiv.classList.add('d-none');

    try {
      const fd = new FormData(bulkMediaForm);
      const resp = await fetch(`/dataset/${datasetId}/bulk_media`, {
        method: 'POST',
        body: fd
      });
      const data = await resp.json();
      
      if (!resp.ok) throw new Error(data.error || 'Error en el servidor');

      resultsDiv.classList.remove('d-none');
      resultsDiv.className = 'alert alert-success py-2 small';
      resultsDiv.innerHTML = `<strong>Éxito:</strong> Se mapearon ${data.mapped} de ${data.total} archivos correctamente.`;
      
      if (data.errors && data.errors.length > 0) {
        resultsDiv.className = 'alert alert-warning py-2 small';
        resultsDiv.innerHTML += `<br><small>Errores parciales: ${data.errors.join(', ')}</small>`;
      }

      // Refresh page after a delay to show results in history
      setTimeout(() => window.location.reload(), 3000);

    } catch (err) {
      resultsDiv.classList.remove('d-none');
      resultsDiv.className = 'alert alert-danger py-2 small';
      resultsDiv.textContent = 'Error: ' + err.message;
    } finally {
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-upload me-2"></i>Procesar ZIP';
    }
  });
}

// --- ASISTENTE DE MEDICIÓN DE VALLAS ---
let measureCtx = null;
let measureImg = new Image();
let measureStart = null;
let measureEnd = null;
let measureRect = null;

function initMeasureTool() {
  const preview = document.getElementById('preview_establecimiento');
  if (!preview || !preview.src) return;
  
  const overlay = document.getElementById('measureToolOverlay');
  const canvas = document.getElementById('measureCanvas');
  overlay.classList.remove('d-none');
  
  measureCtx = canvas.getContext('2d');
  measureImg.onload = () => {
    // Escalar canvas a la imagen manteniendo proporción
    const maxW = window.innerWidth * 0.9;
    const maxH = window.innerHeight * 0.6;
    const ratio = Math.min(maxW / measureImg.width, maxH / measureImg.height);
    
    canvas.width = measureImg.width * ratio;
    canvas.height = measureImg.height * ratio;
    drawMeasure();
  };
  measureImg.src = preview.src;
  
  // Eventos táctiles y ratón para dibujo
  canvas.onpointerdown = (e) => {
    const rect = canvas.getBoundingClientRect();
    measureStart = { x: e.clientX - rect.left, y: e.clientY - rect.top };
    measureRect = null;
  };
  
  canvas.onpointermove = (e) => {
    if (!measureStart) return;
    const rect = canvas.getBoundingClientRect();
    measureEnd = { x: e.clientX - rect.left, y: e.clientY - rect.top };
    measureRect = {
      x: Math.min(measureStart.x, measureEnd.x),
      y: Math.min(measureStart.y, measureEnd.y),
      w: Math.abs(measureStart.x - measureEnd.x),
      h: Math.abs(measureStart.y - measureEnd.y)
    };
    drawMeasure();
  };
  
  canvas.onpointerup = () => {
    measureStart = null;
    calculateMeasure();
  };
}

function drawMeasure() {
  const canvas = document.getElementById('measureCanvas');
  if (!measureCtx) return;
  measureCtx.clearRect(0, 0, canvas.width, canvas.height);
  measureCtx.drawImage(measureImg, 0, 0, canvas.width, canvas.height);
  
  if (measureRect) {
    measureCtx.strokeStyle = '#0d6efd';
    measureCtx.lineWidth = 3;
    measureCtx.strokeRect(measureRect.x, measureRect.y, measureRect.w, measureRect.h);
    measureCtx.fillStyle = 'rgba(13, 110, 253, 0.2)';
    measureCtx.fillRect(measureRect.x, measureRect.y, measureRect.w, measureRect.h);
  }
}

function calculateMeasure() {
  if (!measureRect) return;
  const refCm = parseFloat(document.getElementById('measureRefValue').value) || 100;
  // Calculamos por proporción: si el ancho del rect en px es refCm, cuánto es el alto en cm
  const heightCm = (measureRect.h * refCm) / measureRect.w;
  const area = refCm * heightCm;
  
  document.getElementById('measureWidthCm').textContent = Math.round(refCm);
  document.getElementById('measureHeightCm').textContent = Math.round(heightCm);
  document.getElementById('measureAreaResult').textContent = Math.round(area).toLocaleString() + ' cm²';
}

function saveMeasure() {
  const areaText = document.getElementById('measureAreaResult').textContent;
  const wCm = document.getElementById('measureWidthCm').textContent;
  const hCm = document.getElementById('measureHeightCm').textContent;
  const obsField = document.getElementById('visita_observaciones');
  
  if (obsField) {
    const msg = `\n[MEDICIÓN DE VALLA: ${wCm}cm x ${hCm}cm = ${areaText}]`;
    if (!obsField.value.includes(msg)) obsField.value += msg;
  }
  closeMeasureTool();
  saveDraft();
  alert('Medida en centímetros aplicada con éxito.');
}

function resetMeasure() {
  measureRect = null;
  drawMeasure();
  document.getElementById('measureAreaResult').textContent = '0 cm²';
  document.getElementById('measureWidthCm').textContent = '0';
  document.getElementById('measureHeightCm').textContent = '0';
}

function closeMeasureTool() {
  document.getElementById('measureToolOverlay').classList.add('d-none');
}

// Mostrar botón de medición solo cuando hay foto
const photoInputMain = document.getElementById('visit_photo_establecimiento_file');
if (photoInputMain) {
  photoInputMain.addEventListener('change', () => {
    setTimeout(() => {
      const preview = document.getElementById('preview_establecimiento');
      const btn = document.getElementById('btnOpenMeasure');
      if (preview && !preview.classList.contains('d-none')) {
        btn.classList.remove('d-none');
      } else {
        btn.classList.add('d-none');
      }
    }, 1000);
  });
}
// Compatibility alias for legacy calls or map popups
window.renderVisitForm = applyQueueRow;
