const API_CONFIGS = {
    'A': 'https://dbi.onrender.com',
    'B': 'https://dbi-2xer.onrender.com',
    'C': 'https://dbi2.onrender.com'
};
let selectedApi = 'B';
let renderApiUrl = API_CONFIGS['B'];
let currentDbUrl = '';
let tableSchema = null;
let selectedFile = null;
let csvHeader = [];
let csvRows = [];
let jsonObjects = null;
let previewOffset = 0;
let columnMapping = {};
let previewMode = 'matched'; // 'matched' | 'raw'
let selectedRows = new Set(); // Store selected row indices
let jobCardOrder = [];

function getActiveColumnMapping() {
    if (currentDataSource) return currentDataSource.columnMapping;
    return columnMapping;
}

async function refreshJobStatus(jobId) {
    if (!jobId) return;
    try {
        const res = await fetch(`${renderApiUrl}/job-status/${jobId}`);
        const job = await res.json();
        if (!res.ok) return;
        upsertJobCardFromServer(job);
    } catch {
        // silent
    }
}

function upsertJobCardFromServer(job) {
    if (!job || !job.job_id) return;

    const jobId = job.job_id;
    const label = job.label || job.filename || 'Upload';
    const existing = document.getElementById(`job-card-${jobId}`);
    if (!existing) {
        createJobCard(jobId, label);
    }
    updateJobCardFromServer(job);
}

function formatIsoTime(iso) {
    if (!iso) return '-';
    try {
        return new Date(iso).toLocaleString();
    } catch {
        return String(iso);
    }
}

function updateJobCardFromServer(job) {
    const jobId = job.job_id;
    const card = document.getElementById(`job-card-${jobId}`);
    if (!card) return;

    const statusEl = card.querySelector('.job-card-status');
    const progressEl = document.getElementById(`job-progress-${jobId}`);
    const progressBarEl = document.getElementById(`job-progress-bar-${jobId}`);
    const detailsEl = document.getElementById(`job-details-${jobId}`);

    const created = formatIsoTime(job.created_at);
    const updated = formatIsoTime(job.updated_at);
    const finished = formatIsoTime(job.finished_at);

    if (job.status === 'completed') {
        statusEl.className = 'job-card-status job-status-completed';
        statusEl.innerHTML = '✅ Completed';
        if (progressEl) progressEl.textContent = '100%';
        if (progressBarEl) progressBarEl.style.width = '100%';
        if (detailsEl) {
            detailsEl.innerHTML = `
                Created: ${created} | Updated: ${updated} | Finished: ${finished} | 
                Total: ${job.rows_total ?? '-'} | Inserted: ${job.rows_inserted ?? '-'} | Skipped: ${job.rows_skipped ?? '-'}
            `;
        }
    } else if (job.status === 'failed') {
        statusEl.className = 'job-card-status job-status-failed';
        statusEl.innerHTML = '❌ Failed';
        if (progressEl) progressEl.textContent = '0%';
        if (progressBarEl) progressBarEl.style.width = '0%';

        let errorEl = card.querySelector('.job-error');
        if (!errorEl) {
            errorEl = document.createElement('div');
            errorEl.className = 'job-error';
            card.appendChild(errorEl);
        }
        errorEl.textContent = job.error || 'Unknown error occurred';

        if (detailsEl) {
            detailsEl.innerHTML = `Created: ${created} | Updated: ${updated} | Finished: ${finished}`;
        }
    } else {
        statusEl.className = 'job-card-status job-status-processing';
        statusEl.innerHTML = '<div class="spinner"></div> Processing...';
        const progress = Number.isFinite(job.progress) ? job.progress : 0;
        if (progressEl) progressEl.textContent = `${progress}%`;
        if (progressBarEl) progressBarEl.style.width = `${progress}%`;

        if (detailsEl) {
            const rowsProcessed = job.rows_processed !== undefined ? job.rows_processed : '-';
            detailsEl.innerHTML = `Created: ${created} | Updated: ${updated} | Rows processed: ${rowsProcessed}`;
        }
    }

    if (typeof updateJobStats === 'function') updateJobStats();
    if (typeof filterJobCards === 'function') filterJobCards();
}

async function refreshRecentJobs() {
    try {
        const res = await fetch(`${renderApiUrl}/jobs/recent?hours=2`);
        const data = await res.json();
        if (!res.ok) return;
        const jobs = Array.isArray(data.jobs) ? data.jobs : [];
        jobs.forEach(j => upsertJobCardFromServer(j));
        jobs.filter(j => j.status === 'processing').forEach(j => {
            if (!window.__jobPolling || !window.__jobPolling.has(j.job_id)) {
                monitorJobCard(j.job_id);
            }
        });
    } catch {
        // silent
    }
}

function toggleCreateTableCard() {
    const card = document.getElementById('card-create-table');
    const body = document.getElementById('createTableBody');
    const btnText = document.getElementById('createTableToggleBtnText');
    if (!card || !body || !btnText) return;

    const isHidden = body.classList.contains('hidden');
    body.classList.toggle('hidden', !isHidden);
    btnText.textContent = isHidden ? 'Hide' : 'Create Table';
}

function autoMapToSchema() {
    if (!tableSchema || !Array.isArray(tableSchema.attributes) || tableSchema.attributes.length === 0) return;
    const sourceFields = getCurrentSourceFields();
    if (!sourceFields || sourceFields.length === 0) return;

    const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, '').replace(/_/g, '');
    const byNorm = new Map();
    sourceFields.forEach(f => byNorm.set(norm(f), f));

    let matches = 0;
    for (const attr of tableSchema.attributes) {
        if (getTargetMapping(attr) !== undefined && getTargetMapping(attr) !== '') continue;
        const hit = byNorm.get(norm(attr));
        if (hit) {
            setTargetMapping(attr, hit);
            matches++;
        }
    }

    buildAdvancedMappingUI();
    updateValidationStatus(matches > 0 ? `Auto-mapped ${matches} attributes` : 'No auto-matches found', matches > 0);
}

function setTargetMapping(targetAttr, sourceField) {
    const mapping = getActiveColumnMapping();
    mapping[targetAttr] = sourceField;
}

function getTargetMapping(targetAttr) {
    const mapping = getActiveColumnMapping();
    return mapping ? mapping[targetAttr] : undefined;
}

// ============================
// Unified DataSource Class
// ============================
class DataSource {
    constructor(type, name) {
        this.type = type; // 'csv', 'json', 'db'
        this.name = name;
        this.file = null;
        this.data = null; // rows for csv, objects for json, results for db
        this.header = [];
        this.selectedRows = new Set();
        this.columnMapping = {};
        this.previewOffset = 0;
        this.previewMode = 'raw';
        this.isLoaded = false;
    }

    async load(file) {
        this.file = file;
        try {
            if (this.type === 'csv') {
                const text = await file.text();
                const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n').filter(l => l.length > 0);
                if (lines.length === 0) throw new Error('CSV file is empty');
                
                this.header = parseCsvLine(lines[0]).map(s => s.trim()).filter(Boolean);
                this.data = lines.slice(1).map(parseCsvLine);
            } else if (this.type === 'json') {
                const text = await file.text();
                let parsed = JSON.parse(text);
                const arr = Array.isArray(parsed) ? parsed : [parsed];
                const objs = arr.filter(x => x && typeof x === 'object' && !Array.isArray(x));
                if (objs.length === 0) throw new Error('JSON must be an object or an array of objects');
                
                this.header = getJsonKeySet(objs);
                this.data = objs;
            }
            this.isLoaded = true;
            return true;
        } catch (err) {
            this.reset();
            throw err;
        }
    }

    loadFromQuery(results, columns) {
        this.type = 'db';
        this.data = results;
        this.header = columns;
        this.isLoaded = true;
    }

    reset() {
        this.file = null;
        this.data = null;
        this.header = [];
        this.selectedRows.clear();
        this.columnMapping = {};
        this.previewOffset = 0;
        this.previewMode = 'raw';
        this.isLoaded = false;
    }

    getSelectedData() {
        if (!this.data) return [];
        if (this.selectedRows.size === 0) return this.data;
        return Array.from(this.selectedRows).sort((a, b) => a - b).map(i => this.data[i]);
    }

    getRowValue(row, colIndex) {
        if (this.type === 'json') {
            return row[this.header[colIndex]];
        } else {
            return row[colIndex];
        }
    }

    getMappedAttributes(targetAttrs) {
        return targetAttrs.filter(attr => {
            const mapping = this.columnMapping[attr];
            return mapping && mapping !== '' && mapping !== '__AUTO__';
        });
    }

    buildMappedCsv(targetAttrs) {
        const mappedAttrs = this.getMappedAttributes(targetAttrs);
        if (mappedAttrs.length === 0) return null;

        const idxByHeader = {};
        this.header.forEach((h, i) => { idxByHeader[h] = i; });

        const lines = [];
        lines.push(mappedAttrs.join(','));

        const dataToProcess = this.getSelectedData();
        for (const row of dataToProcess) {
            const outRow = mappedAttrs.map(attr => {
                const mapped = this.columnMapping[attr];
                if (mapped === '__AUTO__' || !mapped) return '';
                const idx = idxByHeader[mapped];
                return csvEscape(idx === undefined ? '' : (this.getRowValue(row, idx) ?? ''));
            });
            lines.push(outRow.join(','));
        }

        return lines.join('\n');
    }

    buildMappedJson(targetAttrs) {
        const mappedAttrs = this.getMappedAttributes(targetAttrs);
        if (mappedAttrs.length === 0) return null;

        const dataToProcess = this.getSelectedData();
        const outObjects = dataToProcess.map(obj => {
            const out = {};
            for (const attr of mappedAttrs) {
                const mapped = this.columnMapping[attr];
                if (!mapped || mapped === '__AUTO__') continue;
                const rawVal = this.type === 'json' ? obj[mapped] : obj[this.header.indexOf(mapped)];
                if (rawVal === null || rawVal === undefined) {
                    out[attr] = '';
                } else if (typeof rawVal === 'object') {
                    if (isBufferLikeObject(rawVal)) {
                        const b64 = bufferObjectToBase64(rawVal);
                        out[attr] = b64 ? b64 : JSON.stringify(rawVal);
                    } else {
                        out[attr] = JSON.stringify(rawVal);
                    }
                } else {
                    out[attr] = rawVal;
                }
            }
            return out;
        });

        return JSON.stringify(outObjects);
    }

    setAsTableSchema() {
        if (!this.header || this.header.length === 0) {
            showNotification('No data to set as schema', 'error');
            return;
        }

        const tableName = `${this.type}_source_${Date.now()}`;
        document.getElementById('tableName').value = tableName;

        tableSchema = {
            table: tableName,
            attributes: this.header,
            primary_key: []
        };

        this.updateSchemaDisplay(tableName);
        window.currentDataSource = this;

        showMappingCard(this.name || this.type.toUpperCase(), tableName);
        buildAdvancedMappingUI();
        showPreviewCard();

        showNotification(`${this.type.toUpperCase()} data set as table schema! Click "Upload to Database" to insert data.`, 'success');
    }

    updateSchemaDisplay(tableName) {
        const schemaInfoEl = document.getElementById('schemaInfo');
        if (schemaInfoEl) schemaInfoEl.classList.remove('hidden');

        const schemaTableNameEl = document.getElementById('schemaTableName');
        if (schemaTableNameEl) schemaTableNameEl.textContent = tableName;

        const pkEl = document.getElementById('primaryKey');
        if (pkEl) pkEl.textContent = 'None (auto-generated)';

        const attrsCountEl = document.getElementById('attributesCount');
        if (attrsCountEl) attrsCountEl.textContent = String(this.header.length);

        const attrsEl = document.getElementById('attributes');
        if (attrsEl) {
            attrsEl.innerHTML = '';
            this.header.forEach(attr => {
                const span = document.createElement('span');
                span.className = 'pill';
                span.textContent = attr;
                attrsEl.appendChild(span);
            });
        }

        const schemaStatusEl = document.getElementById('schemaStatus');
        if (schemaStatusEl) {
            schemaStatusEl.textContent = 'Loaded';
            schemaStatusEl.classList.add('loaded');
        }
    }

    renderPreview(containerId, rangeId, selectedCounterId) {
        const container = document.getElementById(containerId);
        if (!container || !this.data || this.header.length === 0) return;

        const start = this.previewOffset;
        const end = Math.min(this.previewOffset + 10, this.data.length);
        
        const rangeEl = document.getElementById(rangeId);
        if (rangeEl) rangeEl.textContent = `${start + 1}-${end}`;

        const headerHtml = `<tr><th style="width: 30px;"><input type="checkbox" class="row-checkbox" onchange="${containerId === 'csvPreviewTable' ? 'toggleSelectAllCsv' : 'toggleSelectAllJson'}()"></th>${this.header.map(h => `<th>${h}</th>`).join('')}</tr>`;
        
        const bodyHtml = this.data.slice(start, end).map((row, rowIdx) => {
            const globalIdx = start + rowIdx;
            const isSelected = this.selectedRows.has(globalIdx);
            return `<tr class="${isSelected ? 'selected' : ''}" onclick="currentDataSource.toggleRowSelection(${globalIdx}); currentDataSource.renderPreview('${containerId}', '${rangeId}', '${selectedCounterId}')">
                        <td><input type="checkbox" class="row-checkbox" ${isSelected ? 'checked' : ''} onclick="event.stopPropagation(); currentDataSource.toggleRowSelection(${globalIdx}); currentDataSource.renderPreview('${containerId}', '${rangeId}', '${selectedCounterId}')"></td>
                        ${this.header.map((_, idx) => `<td>${escapeHtml(formatPreviewValue(this.getRowValue(row, idx)))}</td>`).join('')}
                    </tr>`;
        }).join('');

        container.innerHTML = `<thead>${headerHtml}</thead><tbody>${bodyHtml}</tbody>`;
        this.updateSelectedCounter(selectedCounterId);
    }

    updateSelectedCounter(counterId) {
        const counter = document.getElementById(counterId);
        if (counter) {
            if (this.selectedRows.size > 0) {
                counter.classList.remove('hidden');
                counter.querySelector('#selectedCount').textContent = this.selectedRows.size;
            } else {
                counter.classList.add('hidden');
            }
        }
    }

    toggleRowSelection(rowIndex) {
        if (this.selectedRows.has(rowIndex)) {
            this.selectedRows.delete(rowIndex);
        } else {
            this.selectedRows.add(rowIndex);
        }
    }

    toggleSelectAll(rangeId, selectedCounterId) {
        const start = this.previewOffset;
        const end = Math.min(this.previewOffset + 10, this.data.length);
        const allSelected = Array.from({ length: end - start }, (_, i) => start + i).every(i => this.selectedRows.has(i));

        if (allSelected) {
            for (let i = start; i < end; i++) {
                this.selectedRows.delete(i);
            }
        } else {
            for (let i = start; i < end; i++) {
                this.selectedRows.add(i);
            }
        }
        // Caller must re-render with the correct container.
    }

    selectAllRows() {
        for (let i = 0; i < this.data.length; i++) {
            this.selectedRows.add(i);
        }
    }

    clearSelection() {
        this.selectedRows.clear();
    }

    previewNext(containerId, rangeId, selectedCounterId) {
        if (!this.data || this.data.length === 0) return;
        this.previewOffset = Math.min(this.previewOffset + 10, Math.max(this.data.length - 1, 0));
        this.renderPreview(containerId, rangeId, selectedCounterId);
    }

    previewPrev(containerId, rangeId, selectedCounterId) {
        if (!this.data || this.data.length === 0) return;
        this.previewOffset = Math.max(this.previewOffset - 10, 0);
        this.renderPreview(containerId, rangeId, selectedCounterId);
    }

    async uploadToDatabase() {
        if (!tableSchema) {
            showNotification('Please set table schema first (click "Set as Table Schema")', 'error');
            return;
        }

        let content, mimeType, filename;
        if (this.type === 'csv') {
            content = this.buildMappedCsv(tableSchema.attributes);
            mimeType = 'text/csv';
            filename = 'upload.csv';
        } else if (this.type === 'json') {
            content = this.buildMappedJson(tableSchema.attributes);
            mimeType = 'application/json';
            filename = 'upload.json';
        } else {
            showNotification('Unsupported data source type for upload', 'error');
            return;
        }

        if (!content) {
            showNotification('Please map at least one attribute', 'error');
            return;
        }

        const blob = new Blob([content], { type: mimeType });
        const file = new File([blob], filename, { type: mimeType });

        const formData = new FormData();
        formData.append('file', file);
        formData.append('table', tableSchema.table);
        formData.append('columns', this.getMappedAttributes(tableSchema.attributes).join(','));
        formData.append('primary_key', ''); // Auto-generate PK

        try {
            showNotification('Uploading data...', 'info');
            const res = await fetch(`${renderApiUrl}/upload-data`, {
                method: 'POST',
                body: formData
            });

            const data = await res.json();

            if (res.ok) {
                showNotification('Upload started!', 'info');
                createJobCard(data.job_id, `${this.type.toUpperCase()} Upload`);
                monitorJobCard(data.job_id);
            } else {
                showNotification('Error: ' + errorToString(data.detail), 'error');
            }
        } catch (err) {
            showNotification('Error: ' + err.message, 'error');
        }
    }
}

// Global data source instance
let currentDataSource = null;

function escapeHtml(value) {
    return String(value)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function isBufferLikeObject(v) {
    return v && typeof v === 'object' && v.type === 'Buffer' && Array.isArray(v.data);
}

function bufferObjectToBase64(v) {
    try {
        const bytes = new Uint8Array(v.data);
        let binary = '';
        const chunk = 0x8000;
        for (let i = 0; i < bytes.length; i += chunk) {
            binary += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
        }
        return btoa(binary);
    } catch {
        return null;
    }
}

function formatPreviewValue(v) {
    if (v === null || v === undefined) return '';
    if (typeof v === 'string') {
        const s = v;
        return s.length > 500 ? (s.slice(0, 500) + '…') : s;
    }
    if (typeof v === 'number' || typeof v === 'boolean') return String(v);
    if (isBufferLikeObject(v)) {
        const b64 = bufferObjectToBase64(v);
        if (b64) return `base64:${b64.length > 500 ? (b64.slice(0, 500) + '…') : b64}`;
    }
    if (typeof v === 'object') {
        try {
            const s = JSON.stringify(v);
            return s && s.length > 500 ? (s.slice(0, 500) + '…') : (s ?? '');
        } catch {
            return '[unprintable]';
        }
    }
    return String(v);
}

// Check Render connection
async function checkRenderConnection() {
    const selectedConfig = API_CONFIGS[selectedApi];
    renderApiUrl = selectedConfig;

    try {
        const res = await fetch(`${renderApiUrl}/health`);
        if (res.ok) {
            updateStatus(true);
        } else {
            updateStatus(false);
        }
    } catch (err) {
        updateStatus(false);
    }
}

function updateStatus(connected) {
    const dot = document.getElementById('statusDot');
    const text = document.getElementById('statusText');

    if (connected) {
        dot.className = 'status-dot connected';
        text.textContent = 'Render: Connected';
    } else {
        dot.className = 'status-dot disconnected';
        text.textContent = 'Render: Disconnected';
    }
}

// Save DB connection
async function saveDbConnection() {
    const url = document.getElementById('dbUrl').value.trim();
    if (!url) {
        showNotification('Please enter a database URL', 'error');
        return;
    }

    try {
        const res = await fetch(`${renderApiUrl}/save-db`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ database_url: url })
        });

        if (res.ok) {
            currentDbUrl = url;
            document.getElementById('dbUrlText').textContent = maskUrl(url);
            document.getElementById('dbUrlDisplay').classList.remove('hidden');
            document.getElementById('dbUrlInput').classList.add('hidden');
            showNotification('Database connection saved!', 'success');
        } else {
            showNotification('Failed to save connection', 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

function editDbUrl() {
    document.getElementById('dbUrlDisplay').classList.add('hidden');
    document.getElementById('dbUrlInput').classList.remove('hidden');
    document.getElementById('dbUrl').value = currentDbUrl;
}

function maskUrl(url) {
    try {
        const parsed = new URL(url);
        return `${parsed.protocol}//${parsed.username}:***@${parsed.host}${parsed.pathname}`;
    } catch {
        return url.substring(0, 30) + '...';
    }
}

// Toggle create table section
function toggleCreateTable() {
    toggleCreateTableCard();
}

// Execute create table
async function executeCreateTable() {
    const sql = document.getElementById('createTableSql').value.trim();
    if (!sql) {
        showNotification('Please enter SQL schema', 'error');
        return;
    }

    try {
        const res = await fetch(`${renderApiUrl}/create-table`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ create_sql: sql })
        });

        const data = await res.json();

        if (res.ok) {
            showNotification('Table created successfully!', 'success');
            document.getElementById('createTableSql').value = '';
        } else {
            showNotification('Error: ' + errorToString(data.detail), 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

// Fetch table schema
async function fetchTableSchema() {
    const table = document.getElementById('tableName').value.trim();
    if (!table) {
        showNotification('Please enter a table name', 'error');
        return;
    }

    try {
        const res = await fetch(`${renderApiUrl}/table-schema?table=${table}`);
        const data = await res.json();

        if (res.ok) {
            tableSchema = data;

            const schemaInfoEl = document.getElementById('schemaInfo');
            if (schemaInfoEl) schemaInfoEl.classList.remove('hidden');

            const schemaTableNameEl = document.getElementById('schemaTableName');
            if (schemaTableNameEl) schemaTableNameEl.textContent = data.table || table;

            const pkEl = document.getElementById('primaryKey');
            if (pkEl) pkEl.textContent = (data.primary_key || []).join(', ') || 'None';

            const attrsCountEl = document.getElementById('attributesCount');
            if (attrsCountEl) attrsCountEl.textContent = String((data.attributes || []).length);

            const attrsEl = document.getElementById('attributes');
            if (attrsEl) {
                attrsEl.innerHTML = '';
                (data.attributes || []).forEach(attr => {
                    const span = document.createElement('span');
                    span.className = 'pill';
                    span.textContent = attr;
                    attrsEl.appendChild(span);
                });
            }

            const schemaStatusEl = document.getElementById('schemaStatus');
            if (schemaStatusEl) {
                schemaStatusEl.textContent = 'Loaded';
                schemaStatusEl.classList.add('loaded');
            }

            showNotification('Schema loaded!', 'success');

            // If we already have a loaded source (CSV/JSON/DB), refresh mapping UI for the new schema.
            const activeSourceFields = getCurrentSourceFields();
            if (activeSourceFields && activeSourceFields.length > 0) {
                showMappingCard(currentDataSource?.name || (currentSourceType === 'db' ? 'DB Query' : currentSourceType.toUpperCase()), data.table || table);
                autoMapToSchema();
            }
        } else {
            tableSchema = null;

            const schemaInfoEl = document.getElementById('schemaInfo');
            if (schemaInfoEl) schemaInfoEl.classList.add('hidden');

            const schemaStatusEl = document.getElementById('schemaStatus');
            if (schemaStatusEl) {
                schemaStatusEl.textContent = 'Not Loaded';
                schemaStatusEl.classList.remove('loaded');
            }

            showNotification('Error: ' + errorToString(data.detail), 'error');
        }
    } catch (err) {
        tableSchema = null;

        const schemaInfoEl = document.getElementById('schemaInfo');
        if (schemaInfoEl) schemaInfoEl.classList.add('hidden');

        const schemaStatusEl = document.getElementById('schemaStatus');
        if (schemaStatusEl) {
            schemaStatusEl.textContent = 'Not Loaded';
            schemaStatusEl.classList.remove('loaded');
        }

        showNotification('Error: ' + err.message, 'error');
    }
}

function parseCsvLine(line) {
    const out = [];
    let cur = '';
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
        const ch = line[i];
        if (inQuotes) {
            if (ch === '"') {
                if (line[i + 1] === '"') {
                    cur += '"';
                    i++;
                } else {
                    inQuotes = false;
                }
            } else {
                cur += ch;
            }
        } else {
            if (ch === ',') {
                out.push(cur);
                cur = '';
            } else if (ch === '"') {
                inQuotes = true;
            } else {
                cur += ch;
            }
        }
    }
    out.push(cur);
    return out;
}

function csvEscape(value) {
    const v = value == null ? '' : String(value);
    if (v.includes('"') || v.includes(',') || v.includes('\n') || v.includes('\r')) {
        return '"' + v.replaceAll('"', '""') + '"';
    }
    return v;
}

function buildMappingUI() {
    const section = document.getElementById('mappingSection');
    const rowsEl = document.getElementById('mappingRows');
    rowsEl.innerHTML = '';

    if (!tableSchema || !Array.isArray(tableSchema.attributes) || tableSchema.attributes.length === 0) {
        section.classList.add('hidden');
        return;
    }

    if (!csvHeader || csvHeader.length === 0) {
        section.classList.add('hidden');
        return;
    }

    // Default mapping = same name if present, else keep empty
    const headerSet = new Set(csvHeader);
    for (const attr of tableSchema.attributes) {
        if (columnMapping[attr] === undefined) {
            columnMapping[attr] = headerSet.has(attr) ? attr : '';
        }
    }

    for (const attr of tableSchema.attributes) {
        const mapped = columnMapping[attr] ?? '';
        const isMatch = mapped && mapped === attr;
        const isMapped = Boolean(mapped);

        const row = document.createElement('div');
        row.className = 'mapping-row';

        const left = document.createElement('div');
        left.innerHTML = `
                    <div style="display:flex; align-items:center; gap: 10px; flex-wrap: wrap;">
                        <span class="pill">${attr}</span>
                        <span class="pill ${isMapped ? (isMatch ? 'match' : 'mismatch') : 'mismatch'}">
                            ${isMapped ? mapped : 'keep empty'}
                        </span>
                    </div>
                `;

        const right = document.createElement('div');
        const select = document.createElement('select');
        select.dataset.tableAttr = attr;

        const optEmpty = document.createElement('option');
        optEmpty.value = '';
        optEmpty.textContent = 'keep empty';
        select.appendChild(optEmpty);

        const optAuto = document.createElement('option');
        optAuto.value = '__AUTO__';
        optAuto.textContent = 'auto-generate';
        select.appendChild(optAuto);

        for (const h of csvHeader) {
            const opt = document.createElement('option');
            opt.value = h;
            opt.textContent = h;
            select.appendChild(opt);
        }
        select.value = mapped;
        select.addEventListener('change', (e) => {
            columnMapping[attr] = e.target.value;
            buildMappingUI();
        });
        right.appendChild(select);

        row.appendChild(left);
        row.appendChild(right);
        rowsEl.appendChild(row);
    }

    section.classList.remove('hidden');
}

function setPreviewTitle() {
    const titleEl = document.getElementById('previewTitle');
    if (!titleEl) return;
    if (!selectedFile) {
        titleEl.textContent = 'Preview';
        return;
    }
    titleEl.textContent = selectedFile.name.toLowerCase().endsWith('.json') ? 'JSON Preview' : 'CSV Preview';
}

function getJsonKeySet(dataArr) {
    const keys = new Set();
    for (const row of dataArr) {
        if (row && typeof row === 'object' && !Array.isArray(row)) {
            Object.keys(row).forEach(k => keys.add(k));
        }
    }
    return Array.from(keys);
}

function renderPreview() {
    const card = document.getElementById('previewCard');
    const table = document.getElementById('previewTable');

    if (!csvHeader || csvHeader.length === 0 || !csvRows) {
        card.classList.add('hidden');
        return;
    }

    const toggleBtn = document.getElementById('previewToggleBtn');
    if (toggleBtn) {
        toggleBtn.textContent = previewMode === 'raw' ? 'CSV' : 'Matched';
    }

    const start = previewOffset;
    const end = Math.min(previewOffset + 10, csvRows.length);
    document.getElementById('previewRange').textContent = `${start + 1}-${end}`;

    if (previewMode === 'raw') {
        const headerHtml = `<tr><th style="width: 30px;"><input type="checkbox" class="row-checkbox" onchange="toggleSelectAll()"></th>${csvHeader.map(h => `<th>${h}</th>`).join('')}</tr>`;
        const bodyHtml = csvRows.slice(start, end).map((r, rowIdx) => {
            const globalIdx = start + rowIdx;
            const isSelected = selectedRows.has(globalIdx);
            return `<tr class="${isSelected ? 'selected' : ''}" onclick="toggleRowSelection(${globalIdx})">
                        <td><input type="checkbox" class="row-checkbox" ${isSelected ? 'checked' : ''} onclick="event.stopPropagation(); toggleRowSelection(${globalIdx})"></td>
                        ${csvHeader.map((_, idx) => `<td>${escapeHtml(formatPreviewValue(r[idx]))}</td>`).join('')}
                    </tr>`;
        }).join('');
        table.innerHTML = `<thead>${headerHtml}</thead><tbody>${bodyHtml}</tbody>`;
        updateSelectedCounter();
        card.classList.remove('hidden');
        return;
    }

    // matched/mapped preview
    if (!tableSchema || !Array.isArray(tableSchema.attributes) || tableSchema.attributes.length === 0) {
        card.classList.add('hidden');
        return;
    }

    const mappedTableAttrs = tableSchema.attributes.filter(a => {
        const m = columnMapping[a];
        return m && m !== '' && m !== '__AUTO__';
    });

    if (mappedTableAttrs.length === 0) {
        card.classList.add('hidden');
        return;
    }

    const idxByCsv = {};
    csvHeader.forEach((h, i) => { idxByCsv[h] = i; });

    const headerHtml = `<tr><th style="width: 30px;"><input type="checkbox" class="row-checkbox" onchange="toggleSelectAll()"></th>${mappedTableAttrs.map(h => `<th>${h}</th>`).join('')}</tr>`;
    const bodyHtml = csvRows.slice(start, end).map((r, rowIdx) => {
        const globalIdx = start + rowIdx;
        const isSelected = selectedRows.has(globalIdx);
        return `<tr class="${isSelected ? 'selected' : ''}" onclick="toggleRowSelection(${globalIdx})">
                    <td><input type="checkbox" class="row-checkbox" ${isSelected ? 'checked' : ''} onclick="event.stopPropagation(); toggleRowSelection(${globalIdx})"></td>
                    ${mappedTableAttrs.map((attr) => {
            const mapped = columnMapping[attr];
            const idx = idxByCsv[mapped];
            return `<td>${escapeHtml(formatPreviewValue(idx === undefined ? '' : (r[idx] ?? '')))}</td>`;
        }).join('')}
                </tr>`;
    }).join('');

    table.innerHTML = `<thead>${headerHtml}</thead><tbody>${bodyHtml}</tbody>`;
    updateSelectedCounter();
    card.classList.remove('hidden');
}

function togglePreviewMode() {
    previewMode = previewMode === 'raw' ? 'matched' : 'raw';
    renderPreview();
}

// Row selection functions
function toggleRowSelection(rowIndex) {
    if (selectedRows.has(rowIndex)) {
        selectedRows.delete(rowIndex);
    } else {
        selectedRows.add(rowIndex);
    }
    renderPreview();
}

function toggleSelectAll() {
    const start = previewOffset;
    const end = Math.min(previewOffset + 10, csvRows.length);
    const allSelected = Array.from({ length: end - start }, (_, i) => start + i).every(i => selectedRows.has(i));

    if (allSelected) {
        // Deselect all in current view
        for (let i = start; i < end; i++) {
            selectedRows.delete(i);
        }
    } else {
        // Select all in current view
        for (let i = start; i < end; i++) {
            selectedRows.add(i);
        }
    }
    renderPreview();
}

function selectAllRows() {
    for (let i = 0; i < csvRows.length; i++) {
        selectedRows.add(i);
    }
    renderPreview();
}

function clearSelection() {
    selectedRows.clear();
    renderPreview();
}

function updateSelectedCounter() {
    const counter = document.getElementById('selectedCounter');
    const count = document.getElementById('selectedCount');

    if (selectedRows.size > 0) {
        counter.classList.remove('hidden');
        count.textContent = selectedRows.size;
    } else {
        counter.classList.add('hidden');
    }
}

function previewNext() {
    if (!csvRows || csvRows.length === 0) return;
    previewOffset = Math.min(previewOffset + 10, Math.max(csvRows.length - 1, 0));
    renderPreview();
}

function previewPrev() {
    if (!csvRows || csvRows.length === 0) return;
    previewOffset = Math.max(previewOffset - 10, 0);
    renderPreview();
}

async function verifyFile() {
    if (!selectedFile) {
        showNotification('No file selected', 'error');
        return;
    }
    if (!tableSchema) {
        showNotification('Please set table schema first', 'error');
        return;
    }

    try {
        setPreviewTitle();

        if (selectedFile.name.toLowerCase().endsWith('.csv')) {
            const text = await selectedFile.text();
            const lines = text.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n').filter(l => l.length > 0);
            if (lines.length === 0) {
                showNotification('CSV file is empty', 'error');
                return;
            }
            csvHeader = parseCsvLine(lines[0]).map(s => s.trim()).filter(Boolean);
            csvRows = lines.slice(1).map(parseCsvLine);
            jsonObjects = null;
        } else if (selectedFile.name.toLowerCase().endsWith('.json')) {
            const text = await selectedFile.text();
            let parsed;
            try {
                parsed = JSON.parse(text);
            } catch {
                showNotification('Invalid JSON: could not parse file', 'error');
                return;
            }

            const arr = Array.isArray(parsed) ? parsed : [parsed];
            const objs = arr.filter(x => x && typeof x === 'object' && !Array.isArray(x));
            if (objs.length === 0) {
                showNotification('JSON must be an object or an array of objects', 'error');
                return;
            }

            jsonObjects = objs;
            csvHeader = getJsonKeySet(objs);
            if (!csvHeader || csvHeader.length === 0) {
                showNotification('JSON has no keys to preview', 'error');
                return;
            }
            csvRows = objs.map(o => csvHeader.map(k => (o?.[k] ?? '')));
        } else {
            showNotification('Only CSV and JSON files are supported', 'error');
            return;
        }

        previewOffset = 0;
        previewMode = 'matched';
        selectedRows.clear();

        buildMappingUI();
        renderPreview();
        showNotification((selectedFile.name.toLowerCase().endsWith('.json') ? 'JSON' : 'CSV') + ' verified!', 'success');
    } catch (err) {
        showNotification('Verify error: ' + err.message, 'error');
    }
}

// File handling - Old upload zone (deprecated, kept for backward compatibility)
const uploadZone = document.getElementById('uploadZone');
if (uploadZone) {
    uploadZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadZone.classList.add('dragover');
    });

    uploadZone.addEventListener('dragleave', () => {
        uploadZone.classList.remove('dragover');
    });

    uploadZone.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadZone.classList.remove('dragover');
        const file = e.dataTransfer.files[0];
        if (file) {
            handleFile(file);
        }
    });
}

function handleFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        handleFile(file);
    }
}

function handleFile(file) {
    if (!file.name.endsWith('.csv') && !file.name.endsWith('.json')) {
        showNotification('Only CSV and JSON files are supported', 'error');
        return;
    }

    selectedFile = file;
    csvHeader = [];
    csvRows = [];
    jsonObjects = null;
    previewOffset = 0;
    columnMapping = {};
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileSize').textContent = formatBytes(file.size);
    document.getElementById('fileInfoSection').classList.remove('hidden');
    document.getElementById('mappingSection').classList.add('hidden');
    document.getElementById('previewCard').classList.add('hidden');
    setPreviewTitle();
}

async function uploadFile() {
    if (!selectedFile) {
        showNotification('No file selected', 'error');
        return;
    }

    if (!tableSchema) {
        showNotification('Please set table schema first', 'error');
        return;
    }

    if (!renderApiUrl) {
        showNotification('Please set Render API URL first', 'error');
        return;
    }

    if (!Array.isArray(tableSchema.attributes) || tableSchema.attributes.length === 0) {
        showNotification('Table attributes not loaded', 'error');
        return;
    }

    // primary_key can be empty; backend supports auto mode.

    // For CSV, require verify + mapping
    if (!csvHeader || csvHeader.length === 0) {
        showNotification('Click Verify to load preview and mapping first', 'error');
        return;
    }

    const isCsv = selectedFile.name.toLowerCase().endsWith('.csv');
    const isJson = selectedFile.name.toLowerCase().endsWith('.json');

    // PK can be auto-generated. If user maps PK, keep it; otherwise we'll omit it and send empty primary_key.
    const includedTableAttrs = tableSchema.attributes.filter(a => columnMapping[a] && columnMapping[a] !== '__AUTO__');
    if (includedTableAttrs.length === 0) {
        showNotification('Please map at least one attribute (or use Raw preview)', 'error');
        return;
    }

    if (isCsv) {
        const outHeader = includedTableAttrs;
        const idxByCsv = {};
        csvHeader.forEach((h, i) => { idxByCsv[h] = i; });

        const outLines = [];
        outLines.push(outHeader.map(csvEscape).join(','));

        const rowsToProcess = selectedRows.size > 0
            ? Array.from(selectedRows).sort((a, b) => a - b).map(i => csvRows[i])
            : csvRows;

        for (const row of rowsToProcess) {
            const outRow = outHeader.map(attr => {
                const mapped = columnMapping[attr];
                const idx = idxByCsv[mapped];
                return csvEscape(idx === undefined ? '' : (row[idx] ?? ''));
            });
            outLines.push(outRow.join(','));
        }

        const blob = new Blob([outLines.join('\n')], { type: 'text/csv' });
        selectedFile = new File([blob], selectedFile.name, { type: 'text/csv' });
    }

    if (isJson) {
        if (!jsonObjects || !Array.isArray(jsonObjects) || jsonObjects.length === 0) {
            showNotification('JSON not verified. Click Verify first', 'error');
            return;
        }

        const rowsToProcess = selectedRows.size > 0
            ? Array.from(selectedRows).sort((a, b) => a - b).map(i => jsonObjects[i]).filter(Boolean)
            : jsonObjects;

        const outObjects = rowsToProcess.map((obj) => {
            const out = {};
            for (const attr of includedTableAttrs) {
                const mapped = columnMapping[attr];
                if (!mapped || mapped === '__AUTO__') continue;
                const rawVal = obj?.[mapped];
                if (rawVal === null || rawVal === undefined) {
                    out[attr] = '';
                } else if (typeof rawVal === 'object') {
                    if (isBufferLikeObject(rawVal)) {
                        const b64 = bufferObjectToBase64(rawVal);
                        out[attr] = b64 ? b64 : JSON.stringify(rawVal);
                    } else {
                        out[attr] = JSON.stringify(rawVal);
                    }
                } else {
                    out[attr] = rawVal;
                }
            }
            return out;
        });

        const blob = new Blob([JSON.stringify(outObjects)], { type: 'application/json' });
        selectedFile = new File([blob], selectedFile.name, { type: 'application/json' });
    }

    const formData = new FormData();
    formData.append('file', selectedFile);
    formData.append('table', tableSchema.table);
    formData.append('columns', includedTableAttrs.join(','));

    const mappedPk = (tableSchema.primary_key || []).filter(pk => columnMapping[pk] && columnMapping[pk] !== '__AUTO__');
    formData.append('primary_key', mappedPk.join(','));

    try {
        const res = await fetch(`${renderApiUrl}/upload-data`, {
            method: 'POST',
            body: formData
        });

        const data = await res.json();

        if (res.ok) {
            showNotification('Upload started!', 'info');
            createJobCard(data.job_id, selectedFile.name);
            monitorJobCard(data.job_id);
        } else {
            showNotification('Error: ' + errorToString(data.detail), 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

// Create job card
function createJobCard(jobId, fileName) {
    const container = document.getElementById('jobCardsContainer');

    const emptyState = document.getElementById('emptyJobsState');
    if (emptyState) emptyState.style.display = 'none';

    const jobCard = document.createElement('div');
    jobCard.className = 'job-card';
    jobCard.id = `job-card-${jobId}`;

    jobCard.innerHTML = `
                <div class="job-card-header">
                    <div class="job-card-title">
                        📤 ${fileName}
                        <span class="job-id-badge">ID: ${jobId}</span>
                    </div>
                    <div style="display:flex; align-items:center; gap: 10px;">
                        <button class="btn btn-secondary btn-sm" onclick="refreshJobStatus('${jobId}')">🔄</button>
                    <div class="job-card-status job-status-processing">
                        <div class="spinner"></div>
                        Processing...
                    </div>
                    </div>
                </div>
                <div class="job-progress-section">
                    <div class="job-progress-info">
                        <span>Upload Progress</span>
                        <span id="job-progress-${jobId}">0%</span>
                    </div>
                    <div class="job-progress-bar">
                        <div class="job-progress-fill" id="job-progress-bar-${jobId}" style="width: 0%"></div>
                    </div>
                </div>
                <div class="job-details" id="job-details-${jobId}">
                    Started: ${new Date().toLocaleString()} | API: ${selectedApi}
                </div>
            `;

    // Newest card should be on top of the stack
    container.insertBefore(jobCard, container.firstChild);
    jobCardOrder = [jobId, ...jobCardOrder.filter(id => id !== jobId)];
    updateJobCardStack();

    if (typeof updateJobStats === 'function') updateJobStats();
    if (typeof filterJobCards === 'function') filterJobCards();
}

function updateJobCardStack() {
    const container = document.getElementById('jobCardsContainer');
    const isStacked = window.innerWidth > 640;
    container.classList.toggle('stacked', isStacked);

    const cards = jobCardOrder
        .map(id => document.getElementById(`job-card-${id}`))
        .filter(Boolean);

    if (!isStacked) {
        container.style.minHeight = '0px';
        cards.forEach((card, i) => {
            card.style.zIndex = '';
            card.style.transform = '';
            card.style.position = 'relative';
            card.style.marginBottom = '12px';
        });
        return;
    }

    const offsetY = 12;
    const scaleStep = 0.018;

    const topCard = cards[0];
    const baseHeight = topCard ? topCard.getBoundingClientRect().height : 0;
    const minHeight = baseHeight ? (baseHeight + Math.min(cards.length - 1, 7) * offsetY + 16) : 0;
    container.style.minHeight = minHeight ? `${Math.max(minHeight, 140)}px` : '0px';

    cards.forEach((card, i) => {
        const clamped = Math.min(i, 7);
        const translateY = clamped * offsetY;
        const scale = 1 - clamped * scaleStep;
        card.style.position = 'absolute';
        card.style.marginBottom = '0';
        card.style.zIndex = String(1000 - i);
        card.style.transform = `translateY(${translateY}px) scale(${scale})`;
    });
}

window.addEventListener('resize', () => {
    updateJobCardStack();
});

// Monitor job with card updates
async function monitorJobCard(jobId) {
    if (!window.__jobPolling) window.__jobPolling = new Set();
    if (window.__jobPolling.has(jobId)) return;
    window.__jobPolling.add(jobId);

    const interval = setInterval(async () => {
        try {
            const res = await fetch(`${renderApiUrl}/job-status/${jobId}`);
            const job = await res.json();

            // Always update card with server truth
            updateJobCardFromServer(job);

            const card = document.getElementById(`job-card-${jobId}`);
            if (!card) {
                clearInterval(interval);
                window.__jobPolling.delete(jobId);
                return;
            }

            const statusEl = card.querySelector('.job-card-status');
            const progressEl = document.getElementById(`job-progress-${jobId}`);
            const progressBarEl = document.getElementById(`job-progress-bar-${jobId}`);
            const detailsEl = document.getElementById(`job-details-${jobId}`);

            if (job.status === 'completed') {
                clearInterval(interval);
                window.__jobPolling.delete(jobId);
                statusEl.className = 'job-card-status job-status-completed';
                statusEl.innerHTML = '✅ Completed';
                progressEl.textContent = '100%';
                progressBarEl.style.width = '100%';
                detailsEl.innerHTML = `
                            Completed: ${new Date().toLocaleString()} | 
                            Total rows: ${job.rows_total} | 
                            Inserted: ${job.rows_inserted} | 
                            Skipped: ${job.rows_skipped} | 
                            API: ${selectedApi}
                        `;
                showNotification('File uploaded successfully!', 'success');

                if (typeof updateJobStats === 'function') updateJobStats();
                if (typeof filterJobCards === 'function') filterJobCards();
            } else if (job.status === 'failed') {
                clearInterval(interval);
                window.__jobPolling.delete(jobId);
                statusEl.className = 'job-card-status job-status-failed';
                statusEl.innerHTML = '❌ Failed';
                progressEl.textContent = '0%';
                progressBarEl.style.width = '0%';

                // Add error section
                const errorSection = document.createElement('div');
                errorSection.className = 'job-error';
                errorSection.textContent = job.error || 'Unknown error occurred';
                card.appendChild(errorSection);

                detailsEl.innerHTML = `
                            Failed: ${new Date().toLocaleString()} | 
                            API: ${selectedApi}
                        `;
                showNotification('Upload failed', 'error');

                if (typeof updateJobStats === 'function') updateJobStats();
                if (typeof filterJobCards === 'function') filterJobCards();
            } else if (job.status === 'processing') {
                // Update progress if available
                const progress = job.progress || 0;
                progressEl.textContent = `${progress}%`;
                progressBarEl.style.width = `${progress}%`;

                if (job.rows_processed !== undefined) {
                    detailsEl.innerHTML = `
                                Processing... | 
                                Rows processed: ${job.rows_processed} | 
                                API: ${selectedApi}
                            `;
                }
            }
        } catch (err) {
            clearInterval(interval);
            window.__jobPolling.delete(jobId);
            const card = document.getElementById(`job-card-${jobId}`);
            if (card) {
                const statusEl = card.querySelector('.job-card-status');
                const detailsEl = document.getElementById(`job-details-${jobId}`);
                statusEl.className = 'job-card-status job-status-failed';
                statusEl.innerHTML = '❌ Connection Error';
                detailsEl.innerHTML = `
                            Error: Could not check job status | 
                            API: ${selectedApi}
                        `;
            }
            showNotification('Error checking job status', 'error');

            if (typeof updateJobStats === 'function') updateJobStats();
            if (typeof filterJobCards === 'function') filterJobCards();
        }
    }, 2000);
}

function formatBytes(bytes) {
    if (bytes === 0) return '0 Bytes';
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
}

function showNotification(message, type) {
    const notification = document.createElement('div');
    notification.className = `notification ${type}`;
    notification.textContent = message;
    document.body.appendChild(notification);

    setTimeout(() => {
        notification.remove();
    }, 3000);
}

// Initialize API selector
function initializeApiSelector() {
    const container = document.getElementById('apiBoxes');
    if (!container) {
        console.error('apiBoxes container not found!');
        return;
    }
    container.innerHTML = '';

    Object.keys(API_CONFIGS).forEach(key => {
        const box = document.createElement('div');
        box.className = `api-box ${key === selectedApi ? 'selected' : ''}`;
        box.textContent = key;
        box.onclick = () => selectApi(key, box);
        container.appendChild(box);
        console.log('Added API box:', key);
    });
    
    console.log('API selector initialized with', Object.keys(API_CONFIGS).length, 'boxes');
}

function selectApi(key, el) {
    selectedApi = key;
    renderApiUrl = API_CONFIGS[key];
    document.querySelectorAll('.api-box').forEach(box => {
        box.classList.remove('selected');
    });
    if (el) {
        el.classList.add('selected');
    }

    // Update the display text
    document.getElementById('renderUrlText').textContent = API_CONFIGS[key];

    checkRenderConnection();

    refreshRecentJobs();
}

document.addEventListener('DOMContentLoaded', () => {
    console.log('=== DOMContentLoaded fired ===');
    
    // Always reset to B on reload
    selectedApi = 'B';
    renderApiUrl = API_CONFIGS[selectedApi];
    
    console.log('API_CONFIGS:', API_CONFIGS);
    console.log('selectedApi:', selectedApi);
    console.log('renderApiUrl:', renderApiUrl);
    
    // Check if apiBoxes exists
    const apiBoxesEl = document.getElementById('apiBoxes');
    console.log('apiBoxes element:', apiBoxesEl);
    
    console.log('Calling initializeApiSelector...');
    initializeApiSelector();
    
    const urlText = document.getElementById('renderUrlText');
    console.log('renderUrlText element:', urlText);
    if (urlText) urlText.textContent = API_CONFIGS[selectedApi];
    
    console.log('Checking connection to:', renderApiUrl);
    checkRenderConnection();

    refreshRecentJobs();

    // Check if running from file:// protocol
    if (isFileProtocol()) {
        console.error('WARNING: Running from file:// protocol. API calls will fail.');
        showNotification('ERROR: Open this file via HTTP server, not directly! Use: python -m http.server 8000', 'error');
    }
    
    console.log('=== Initialization complete ===');
});

function errorToString(detail) {
    if (typeof detail === 'string') return detail;
    if (detail?.error) return detail.error;
    return JSON.stringify(detail);
}

// ============================
// API Diagnostics
// ============================
async function checkApiHealth() {
    try {
        const res = await fetch(`${renderApiUrl}/health`, { method: 'GET' });
        console.log('Health check status:', res.status);
        return res.ok;
    } catch (err) {
        console.error('Health check failed:', err);
        return false;
    }
}

// Check if running from file:// protocol
function isFileProtocol() {
    return window.location.protocol === 'file:';
}

// ============================
// Source Type Toggler
// ============================
let currentSourceType = 'db';
let sourceDbConnected = false;
let queryResults = null;
let queryColumns = [];

function setSourceType(type) {
    currentSourceType = type;

    // Update button states
    document.getElementById('btnSourceDb').classList.toggle('active', type === 'db');
    document.getElementById('btnSourceCsv').classList.toggle('active', type === 'csv');
    document.getElementById('btnSourceJson').classList.toggle('active', type === 'json');

    // Show/hide sections
    document.getElementById('dbSourceSection').classList.toggle('hidden', type !== 'db');
    document.getElementById('csvSourceSection').classList.toggle('hidden', type !== 'csv');
    document.getElementById('jsonSourceSection').classList.toggle('hidden', type !== 'json');

    // Reset current data source when switching types
    if (currentDataSource && currentDataSource.type !== type) {
        currentDataSource = null;
    }
}

// ============================
// DB Source Functions
// ============================
async function connectSourceDb(evt) {
    const url = document.getElementById('sourceDbUrl').value.trim();
    if (!url) {
        showNotification('Please enter a database URL', 'error');
        return;
    }

    const connectBtn = (evt && evt.target) ? evt.target : document.getElementById('connectSourceDbBtn');
    if (!connectBtn) {
        showNotification('Connect button not found', 'error');
        return;
    }
    connectBtn.disabled = true;
    connectBtn.textContent = '⏳ Connecting...';

    try {
        const endpoint = `${renderApiUrl}/connect-source-db`;
        console.log('Connecting to:', endpoint);
        console.log('renderApiUrl:', renderApiUrl);

        const res = await fetch(endpoint, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ database_url: url })
        });

        console.log('Connect response status:', res.status);

        const data = await res.json();
        console.log('Connect response data:', data);

        if (res.ok) {
            sourceDbConnected = true;
            document.getElementById('sourceDbStatus').classList.remove('hidden');
            document.getElementById('sourceDbStatusText').textContent = `Connected: ${data.database_url}`;
            document.getElementById('executeQueryBtn').disabled = false;
            showNotification('Source database connected!', 'success');
        } else {
            sourceDbConnected = false;
            document.getElementById('sourceDbStatus').classList.add('hidden');
            document.getElementById('executeQueryBtn').disabled = true;
            showNotification('Error: ' + errorToString(data.detail), 'error');
        }
    } catch (err) {
        console.error('Connect error:', err);
        console.error('Error name:', err.name);
        console.error('Error message:', err.message);
        sourceDbConnected = false;
        document.getElementById('sourceDbStatus').classList.add('hidden');
        document.getElementById('executeQueryBtn').disabled = true;

        let errorMsg = 'Network Error: ' + err.message;
        if (isFileProtocol()) {
            errorMsg = 'Cannot fetch from file:// protocol. Start a server: python -m http.server 8000';
        } else if (err.name === 'TypeError' && err.message.includes('fetch')) {
            errorMsg = 'Server unreachable. Is the backend running at ' + renderApiUrl + '?';
        }
        showNotification(errorMsg, 'error');
    } finally {
        connectBtn.disabled = false;
        connectBtn.textContent = '🔗 Connect';
    }
}

async function executeSourceQuery() {
    if (!sourceDbConnected) {
        showNotification('Please connect to source database first', 'error');
        return;
    }

    const query = document.getElementById('sourceDbQuery').value.trim();
    if (!query) {
        showNotification('Please enter a SQL query', 'error');
        return;
    }

    const executeBtn = document.getElementById('executeQueryBtn');
    executeBtn.disabled = true;
    executeBtn.textContent = '⏳ Executing...';

    try {
        const url = `${renderApiUrl}/execute-query`;
        console.log('Executing query to:', url);
        console.log('Query:', query);
        console.log('renderApiUrl:', renderApiUrl);

        const res = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ query: query, limit: 1000 })
        });

        console.log('Response status:', res.status);
        console.log('Response ok:', res.ok);

        const data = await res.json();
        console.log('Response data:', data);

        if (res.ok) {
            queryResults = data.rows;
            queryColumns = data.columns;
            renderQueryResults();
            showNotification(`Query executed! ${data.total_count} rows returned.`, 'success');
        } else {
            showNotification('Error: ' + errorToString(data.detail), 'error');
        }
    } catch (err) {
        console.error('Execute query error:', err);
        console.error('Error name:', err.name);
        console.error('Error message:', err.message);

        let errorMsg = 'Network Error: ' + err.message;
        if (isFileProtocol()) {
            errorMsg = 'Cannot fetch from file:// protocol. Start a server: python -m http.server 8000';
        } else if (err.name === 'TypeError' && err.message.includes('fetch')) {
            errorMsg = 'Server unreachable. Is the backend running at ' + renderApiUrl + '?';
        }
        showNotification(errorMsg + ' (Check console for details)', 'error');
    } finally {
        executeBtn.disabled = false;
        executeBtn.textContent = '▶️ Execute Query';
    }
}

function renderQueryResults() {
    const section = document.getElementById('queryResultsSection');
    const table = document.getElementById('queryResultsTable');
    const countEl = document.getElementById('queryResultCount');

    if (!queryResults || queryResults.length === 0) {
        section.classList.add('hidden');
        return;
    }

    countEl.textContent = `${queryResults.length} rows`;

    // Build table header
    const headerHtml = `<tr>${queryColumns.map(col => `<th>${escapeHtml(col)}</th>`).join('')}</tr>`;

    // Build table body (show first 10 rows)
    const displayRows = queryResults.slice(0, 10);
    const bodyHtml = displayRows.map(row => {
        return `<tr>${queryColumns.map(col => `<td>${escapeHtml(formatPreviewValue(row[col]))}</td>`).join('')}</tr>`;
    }).join('');

    table.innerHTML = `<thead>${headerHtml}</thead><tbody>${bodyHtml}</tbody>`;
    section.classList.remove('hidden');
}

function setAsTableSchema() {
    showNotification('Set as Target Schema has been removed. Use Target Table Schema (Load Schema) instead.', 'info');
}

// ============================
// CSV Source Functions
// ============================
function handleCsvFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        handleCsvFile(file);
    }
}

function handleCsvFile(file) {
    if (!file.name.toLowerCase().endsWith('.csv')) {
        showNotification('Only CSV files are supported', 'error');
        return;
    }

    currentDataSource = new DataSource('csv', file.name);
    currentDataSource.file = file;
    
    document.getElementById('csvFileName').textContent = file.name;
    document.getElementById('csvFileSize').textContent = formatBytes(file.size);
    document.getElementById('csvFileInfoSection').classList.remove('hidden');
    document.getElementById('csvPreviewCard').classList.add('hidden');

    // Direct preview (no verify step)
    verifyCsvFile();
}

async function verifyCsvFile() {
    if (!currentDataSource || currentDataSource.type !== 'csv') {
        showNotification('No CSV file selected', 'error');
        return;
    }

    try {
        await currentDataSource.load(currentDataSource.file);
        currentDataSource.renderPreview('csvPreviewTable', 'csvPreviewRange', 'csvSelectedCounter');
        document.getElementById('csvPreviewCard').classList.remove('hidden');
        if (tableSchema) {
            showMappingCard(currentDataSource.name || 'CSV', tableSchema.table);
            autoMapToSchema();
        }
        showNotification('CSV verified!', 'success');
    } catch (err) {
        showNotification('Verify error: ' + err.message, 'error');
    }
}

function toggleSelectAllCsv() {
    if (currentDataSource) {
        currentDataSource.toggleSelectAll('csvPreviewRange', 'csvSelectedCounter');
        currentDataSource.renderPreview('csvPreviewTable', 'csvPreviewRange', 'csvSelectedCounter');
    }
}

function selectAllCsvRows() {
    if (currentDataSource) {
        currentDataSource.selectAllRows();
        currentDataSource.renderPreview('csvPreviewTable', 'csvPreviewRange', 'csvSelectedCounter');
    }
}

function clearCsvSelection() {
    if (currentDataSource) {
        currentDataSource.clearSelection();
        currentDataSource.renderPreview('csvPreviewTable', 'csvPreviewRange', 'csvSelectedCounter');
    }
}

function csvPreviewPrev() {
    if (currentDataSource) {
        currentDataSource.previewPrev('csvPreviewTable', 'csvPreviewRange', 'csvSelectedCounter');
    }
}

function csvPreviewNext() {
    if (currentDataSource) {
        currentDataSource.previewNext('csvPreviewTable', 'csvPreviewRange', 'csvSelectedCounter');
    }
}

function setCsvAsTableSchema() {
    showNotification('Set as Table Schema has been removed. Use Target Table Schema (Load Schema) instead.', 'info');
}

// ============================
// JSON Source Functions
// ============================
function handleJsonFileSelect(event) {
    const file = event.target.files[0];
    if (file) {
        handleJsonFile(file);
    }
}

function handleJsonFile(file) {
    if (!file.name.toLowerCase().endsWith('.json')) {
        showNotification('Only JSON files are supported', 'error');
        return;
    }

    currentDataSource = new DataSource('json', file.name);
    currentDataSource.file = file;
    
    document.getElementById('jsonFileName').textContent = file.name;
    document.getElementById('jsonFileSize').textContent = formatBytes(file.size);
    document.getElementById('jsonFileInfoSection').classList.remove('hidden');
    document.getElementById('jsonPreviewCard').classList.add('hidden');

    // Direct preview (no verify step)
    verifyJsonFile();
}

async function verifyJsonFile() {
    if (!currentDataSource || currentDataSource.type !== 'json') {
        showNotification('No JSON file selected', 'error');
        return;
    }

    try {
        await currentDataSource.load(currentDataSource.file);
        currentDataSource.renderPreview('jsonPreviewTable', 'jsonPreviewRange', 'jsonSelectedCounter');
        document.getElementById('jsonPreviewCard').classList.remove('hidden');
        if (tableSchema) {
            showMappingCard(currentDataSource.name || 'JSON', tableSchema.table);
            autoMapToSchema();
        }
        showNotification('JSON verified!', 'success');
    } catch (err) {
        showNotification('Verify error: ' + err.message, 'error');
    }
}

function toggleSelectAllJson() {
    if (currentDataSource) {
        currentDataSource.toggleSelectAll('jsonPreviewRange', 'jsonSelectedCounter');
        currentDataSource.renderPreview('jsonPreviewTable', 'jsonPreviewRange', 'jsonSelectedCounter');
    }
}

function selectAllJsonRows() {
    if (currentDataSource) {
        currentDataSource.selectAllRows();
        currentDataSource.renderPreview('jsonPreviewTable', 'jsonPreviewRange', 'jsonSelectedCounter');
    }
}

function clearJsonSelection() {
    if (currentDataSource) {
        currentDataSource.clearSelection();
        currentDataSource.renderPreview('jsonPreviewTable', 'jsonPreviewRange', 'jsonSelectedCounter');
    }
}

function jsonPreviewPrev() {
    if (currentDataSource) {
        currentDataSource.previewPrev('jsonPreviewTable', 'jsonPreviewRange', 'jsonSelectedCounter');
    }
}

function jsonPreviewNext() {
    if (currentDataSource) {
        currentDataSource.previewNext('jsonPreviewTable', 'jsonPreviewRange', 'jsonSelectedCounter');
    }
}

function setJsonAsTableSchema() {
    showNotification('Set as Table Schema has been removed. Use Target Table Schema (Load Schema) instead.', 'info');
}

// ============================
// Upload Functions for Source Data
// ============================
async function uploadCsvToDatabase() {
    if (currentDataSource && currentDataSource.type === 'csv') {
        await currentDataSource.uploadToDatabase();
    }
}

async function uploadJsonToDatabase() {
    if (currentDataSource && currentDataSource.type === 'json') {
        await currentDataSource.uploadToDatabase();
    }
}

// Initialize drag/drop for source upload zones
document.addEventListener('DOMContentLoaded', () => {
    // CSV Upload Zone
    const csvZone = document.getElementById('csvUploadZone');
    if (csvZone) {
        csvZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            csvZone.classList.add('dragover');
        });
        csvZone.addEventListener('dragleave', () => {
            csvZone.classList.remove('dragover');
        });
        csvZone.addEventListener('drop', (e) => {
            e.preventDefault();
            csvZone.classList.remove('dragover');
            const file = e.dataTransfer.files[0];
            if (file) handleCsvFile(file);
        });
    }

    // JSON Upload Zone
    const jsonZone = document.getElementById('jsonUploadZone');
    if (jsonZone) {
        jsonZone.addEventListener('dragover', (e) => {
            e.preventDefault();
            jsonZone.classList.add('dragover');
        });
        jsonZone.addEventListener('dragleave', () => {
            jsonZone.classList.remove('dragover');
        });
        jsonZone.addEventListener('drop', (e) => {
            e.preventDefault();
            jsonZone.classList.remove('dragover');
            const file = e.dataTransfer.files[0];
            if (file) handleJsonFile(file);
        });
    }
});

// ============================
// CARD 2: Data Source - Enhanced Functions
// ============================

function selectAllQueryRows() {
    if (!queryResults || queryResults.length === 0) return;
    for (let i = 0; i < queryResults.length; i++) {
        queryResults[i]._selected = true;
    }
    renderQueryResults();
    showNotification(`Selected all ${queryResults.length} rows`, 'success');
}

function clearQuerySelection() {
    if (!queryResults) return;
    queryResults.forEach(row => row._selected = false);
    renderQueryResults();
    showNotification('Selection cleared', 'info');
}

function clearCsvSource() {
    if (currentDataSource && currentDataSource.type === 'csv') {
        currentDataSource.reset();
    }
    document.getElementById('csvFileInfoSection').classList.add('hidden');
    document.getElementById('csvPreviewCard').classList.add('hidden');
    document.getElementById('csvFileInput').value = '';
    showNotification('CSV source cleared', 'info');
}

function clearJsonSource() {
    if (currentDataSource && currentDataSource.type === 'json') {
        currentDataSource.reset();
    }
    document.getElementById('jsonFileInfoSection').classList.add('hidden');
    document.getElementById('jsonPreviewCard').classList.add('hidden');
    document.getElementById('jsonFileInput').value = '';
    showNotification('JSON source cleared', 'info');
}

// ============================
// CARD 3: Create Table Functions
// ============================

function generateTableSql() {
    const createEl = document.getElementById('createTableSql');
    if (!createEl) return;
    if (createEl.value.trim()) {
        showNotification('SQL is already filled', 'info');
        return;
    }
    const sql = `CREATE TABLE new_table (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);`;
    createEl.value = sql;
    showNotification('SQL template generated!', 'success');
}

// ============================
// CARD 5: Advanced Mapping Functions
// ============================

function showMappingCard(sourceName, targetTable) {
    document.getElementById('card-mapping').classList.remove('hidden');
    document.getElementById('mappingSourceName').textContent = sourceName;
    document.getElementById('mappingTargetTable').textContent = targetTable;
}

function autoMapAttributes() {
    autoMapToSchema();
}

function clearAllMappings() {
    if (currentDataSource) {
        currentDataSource.columnMapping = {};
    }
    buildAdvancedMappingUI();
    updateValidationStatus('All mappings cleared', false);
}

function previewMapping() {
    showPreviewCard();
    setPreviewTab('matched');
}

function getCurrentSourceFields() {
    if (currentDataSource) return currentDataSource.header;
    if (currentSourceType === 'db') return queryColumns;
    return [];
}

function buildAdvancedMappingUI() {
    const container = document.getElementById('advancedMappingRows');
    if (!container) return;
    
    const sourceFields = getCurrentSourceFields();
    const targetAttrs = tableSchema ? tableSchema.attributes : [];
    
    container.innerHTML = '';
    
    targetAttrs.forEach(attr => {
        const row = document.createElement('div');
        row.className = 'mapping-row';
        row.style.gridTemplateColumns = '1fr 1fr 100px';
        
        const currentMapping = getTargetMapping(attr) ?? '';
        const isMapped = Boolean(currentMapping);
        
        row.innerHTML = `
            <div><span class="pill">${attr}</span></div>
            <div>
                <select onchange="updateMappingTarget('${attr}', this.value)">
                    <option value="">-- Select --</option>
                    ${sourceFields.map(field => 
                        `<option value="${field}" ${currentMapping === field ? 'selected' : ''}>${field}</option>`
                    ).join('')}
                </select>
            </div>
            <div>
                ${isMapped ? '<span class="status-icon">✅</span>' : '<span class="status-icon">⚪</span>'}
            </div>
        `;
        
        container.appendChild(row);
    });
}

function updateMappingTarget(targetAttr, value) {
    setTargetMapping(targetAttr, value);
    buildAdvancedMappingUI();
    updateValidationStatus('Mapping updated', true);
}

function updateValidationStatus(message, isValid) {
    const statusEl = document.getElementById('validationStatus');
    if (!statusEl) return;
    
    statusEl.innerHTML = `
        <span class="status-icon">${isValid ? '✅' : '⚠️'}</span>
        <span>${message}</span>
    `;
    statusEl.className = isValid ? 'validation-status valid' : 'validation-status';
}

function getAllMappings() {
    return getActiveColumnMapping();
}

// ============================
// CARD 6: Preview Functions
// ============================

function showPreviewCard() {
    document.getElementById('card-preview').classList.remove('hidden');
    renderAdvancedPreview();
}

function setPreviewTab(tab) {
    document.getElementById('tabSource').classList.toggle('active', tab === 'source');
    document.getElementById('tabMatched').classList.toggle('active', tab === 'matched');
    document.getElementById('sourcePreviewPanel').classList.toggle('active', tab === 'source');
    document.getElementById('sourcePreviewPanel').classList.toggle('hidden', tab !== 'source');
    document.getElementById('matchedPreviewPanel').classList.toggle('active', tab === 'matched');
    document.getElementById('matchedPreviewPanel').classList.toggle('hidden', tab !== 'matched');
    
    if (tab === 'source') renderAdvancedPreview();
    else renderMatchedPreview();
}

function renderAdvancedPreview() {
    const table = document.getElementById('advancedPreviewTable');
    if (!table) return;
    
    let data, headers, selectedSet;
    
    if (currentDataSource && currentDataSource.isLoaded) {
        data = currentDataSource.data;
        headers = currentDataSource.header;
        selectedSet = currentDataSource.selectedRows;
    } else if (currentSourceType === 'db') {
        data = queryResults || [];
        headers = queryColumns || [];
        selectedSet = new Set(data.filter(r => r._selected).map((_, i) => i));
    } else {
        table.innerHTML = '<tr><td>No data to preview</td></tr>';
        return;
    }
    
    if (!data || data.length === 0) {
        table.innerHTML = '<tr><td>No data to preview</td></tr>';
        return;
    }
    
    const headerHtml = `<tr><th style="width: 30px;"><input type="checkbox" onchange="toggleSelectAllPreview()"></th>${headers.map(h => `<th>${h}</th>`).join('')}</tr>`;
    
    const bodyHtml = data.slice(0, 50).map((row, idx) => {
        const isSelected = selectedSet.has(idx);
        const rowData = headers.map(h => {
            let val;
            if (currentDataSource) {
                val = currentDataSource.getRowValue(row, headers.indexOf(h));
            } else {
                val = row[h];
            }
            return `<td>${escapeHtml(formatPreviewValue(val))}</td>`;
        }).join('');
        
        return `<tr class="${isSelected ? 'selected' : ''}">
            <td><input type="checkbox" ${isSelected ? 'checked' : ''} onchange="togglePreviewRow(${idx})"></td>
            ${rowData}
        </tr>`;
    }).join('');
    
    table.innerHTML = `<thead>${headerHtml}</thead><tbody>${bodyHtml}</tbody>`;
    
    document.getElementById('sourcePreviewStats').textContent = `Showing ${Math.min(50, data.length)} of ${data.length} rows`;
    document.getElementById('sourceSelectedStats').textContent = `${selectedSet.size} selected`;
}

function renderMatchedPreview() {
    const table = document.getElementById('matchedPreviewTable');
    if (!table || !tableSchema) return;
    
    const targetAttrs = tableSchema.attributes;
    const mappedAttrs = targetAttrs.filter(a => {
        const m = getTargetMapping(a);
        return m && m !== '__AUTO__';
    });
    const mappedCount = mappedAttrs.length;
    const matchRate = targetAttrs.length > 0 ? Math.round((mappedCount / targetAttrs.length) * 100) : 0;
    
    document.getElementById('matchRate').textContent = `${matchRate}% matched`;
    
    // Advanced matched preview with selectable rows
    let data = [];
    let selectedSet = new Set();
    if (currentDataSource && currentDataSource.isLoaded) {
        data = currentDataSource.data || [];
        selectedSet = currentDataSource.selectedRows;
    } else if (currentSourceType === 'db') {
        data = queryResults || [];
        selectedSet = new Set(data.filter(r => r._selected).map((_, i) => i));
    }

    const visibleAttrs = mappedAttrs.length > 0 ? mappedAttrs : targetAttrs;
    const headerHtml = `<tr><th style="width: 30px;"><input type="checkbox" onchange="toggleSelectAllPreview()"></th>${visibleAttrs.map(a => {
        const isMapped = Boolean(getTargetMapping(a));
        return `<th class="${isMapped ? 'mapped' : ''}">${a}</th>`;
    }).join('')}</tr>`;

    const bodyRows = (data || []).slice(0, 50).map((row, idx) => {
        const isSelected = selectedSet.has(idx);
        const cells = visibleAttrs.map(attr => {
            const sourceField = getTargetMapping(attr);
            if (!sourceField) return '<td></td>';
            let val;
            if (currentDataSource) {
                const sourceIdx = currentDataSource.header.indexOf(sourceField);
                val = currentDataSource.getRowValue(row, sourceIdx);
            } else {
                val = row[sourceField];
            }
            return `<td>${escapeHtml(formatPreviewValue(val))}</td>`;
        }).join('');
        return `<tr class="${isSelected ? 'selected' : ''}">
            <td><input type="checkbox" ${isSelected ? 'checked' : ''} onchange="togglePreviewRow(${idx})"></td>
            ${cells}
        </tr>`;
    }).join('');

    table.innerHTML = `<thead>${headerHtml}</thead><tbody>${bodyRows || `<tr><td colspan="${visibleAttrs.length + 1}">Mapped ${mappedCount} of ${targetAttrs.length} attributes</td></tr>`}</tbody>`;
}

function toggleSelectAllPreview() {
    if (currentDataSource && currentDataSource.isLoaded) {
        const allSelected = currentDataSource.selectedRows.size === currentDataSource.data.length;
        if (allSelected) currentDataSource.clearSelection();
        else currentDataSource.selectAllRows();
        renderAdvancedPreview();
        renderMatchedPreview();
        return;
    }
    if (currentSourceType === 'db' && queryResults && queryResults.length > 0) {
        const allSelected = queryResults.every(r => r._selected);
        queryResults.forEach(r => r._selected = !allSelected);
        renderAdvancedPreview();
        renderMatchedPreview();
        return;
    }
}

function togglePreviewRow(idx) {
    if (currentDataSource && currentDataSource.isLoaded) {
        currentDataSource.toggleRowSelection(idx);
    } else if (currentSourceType === 'db' && queryResults[idx]) {
        queryResults[idx]._selected = !queryResults[idx]._selected;
    }
    renderAdvancedPreview();
}

function selectAllPreviewRows() {
    if (currentDataSource && currentDataSource.isLoaded) {
        currentDataSource.selectAllRows();
        renderAdvancedPreview();
    } else {
        showNotification('Select all not implemented for this source type', 'info');
    }
}

function clearPreviewSelection() {
    if (currentDataSource && currentDataSource.isLoaded) {
        currentDataSource.clearSelection();
    } else if (currentSourceType === 'db' && queryResults) {
        queryResults.forEach(r => r._selected = false);
    }
    renderAdvancedPreview();
}

function previewPrev() {
    showNotification('Previous page', 'info');
}

function previewNext() {
    showNotification('Next page', 'info');
}

function uploadToDatabase() {
    if (currentDataSource && currentDataSource.isLoaded) {
        currentDataSource.uploadToDatabase();
    } else if (currentSourceType === 'db') {
        uploadQueryResultsToDatabase();
    } else {
        showNotification('No data source to upload', 'error');
    }
}

function uploadQueryResultsToDatabase() {
    if (!tableSchema) {
        showNotification('Please set table schema first', 'error');
        return;
    }
    if (!queryResults || queryResults.length === 0) {
        showNotification('No query results to upload', 'error');
        return;
    }
    
    const selectedData = queryResults.filter(r => r._selected);
    const dataToUpload = selectedData.length > 0 ? selectedData : queryResults;

    const targetAttrs = Array.isArray(tableSchema.attributes) ? tableSchema.attributes : [];
    const mappedAttrs = targetAttrs.filter(a => {
        const m = getTargetMapping(a);
        return m && m !== '__AUTO__';
    });
    if (mappedAttrs.length === 0) {
        showNotification('Please map at least one attribute', 'error');
        return;
    }

    const outObjects = dataToUpload.map(row => {
        const out = {};
        for (const attr of mappedAttrs) {
            const sourceField = getTargetMapping(attr);
            out[attr] = sourceField ? (row[sourceField] ?? '') : '';
        }
        return out;
    });

    const blob = new Blob([JSON.stringify(outObjects)], { type: 'application/json' });
    const file = new File([blob], 'query_results.json', { type: 'application/json' });

    const formData = new FormData();
    formData.append('file', file);
    formData.append('table', tableSchema.table);
    formData.append('columns', mappedAttrs.join(','));
    formData.append('primary_key', '');

    uploadFormData(formData, 'Query Results Upload');
}

async function uploadFormData(formData, label) {
    try {
        showNotification('Uploading...', 'info');
        const res = await fetch(`${renderApiUrl}/upload-data`, {
            method: 'POST',
            body: formData
        });
        
        const data = await res.json();
        
        if (res.ok) {
            createJobCard(data.job_id, label);
            monitorJobCard(data.job_id);
            updateJobStats();
            showNotification('Upload started!', 'success');
        } else {
            showNotification('Error: ' + errorToString(data.detail), 'error');
        }
    } catch (err) {
        showNotification('Error: ' + err.message, 'error');
    }
}

// ============================
// CARD 7: Job Search & Filter Functions
// ============================

let jobFilter = 'all';
let jobSearchTerm = '';

function searchJobs() {
    jobSearchTerm = document.getElementById('jobSearchInput').value.trim().toLowerCase();
    filterJobCards();
}

function clearJobSearch() {
    document.getElementById('jobSearchInput').value = '';
    jobSearchTerm = '';
    filterJobCards();
}

function filterJobs(status) {
    jobFilter = status;
    
    // Update button states
    ['all', 'processing', 'completed', 'failed'].forEach(s => {
        const btn = document.getElementById('filter' + s.charAt(0).toUpperCase() + s.slice(1));
        if (btn) btn.classList.toggle('active', s === status);
    });
    
    filterJobCards();
}

function filterJobCards() {
    const cards = document.querySelectorAll('.job-card');
    let visibleCount = 0;
    
    cards.forEach(card => {
        const jobId = card.id.replace('job-card-', '');
        const statusEl = card.querySelector('.job-card-status');
        const status = statusEl ? getStatusFromClass(statusEl.className) : 'unknown';
        
        const matchesSearch = !jobSearchTerm || jobId.toLowerCase().includes(jobSearchTerm);
        const matchesFilter = jobFilter === 'all' || status === jobFilter;
        
        const shouldShow = matchesSearch && matchesFilter;
        card.style.display = shouldShow ? 'block' : 'none';
        if (shouldShow) visibleCount++;
    });
    
    // Show/hide empty state
    const emptyState = document.getElementById('emptyJobsState');
    if (emptyState) {
        emptyState.style.display = (cards.length === 0 || visibleCount === 0) ? 'block' : 'none';
    }
}

function getStatusFromClass(className) {
    if (className.includes('job-status-completed')) return 'completed';
    if (className.includes('job-status-failed')) return 'failed';
    if (className.includes('job-status-processing')) return 'processing';
    return 'unknown';
}

function updateJobStats() {
    const cards = document.querySelectorAll('.job-card');
    let total = cards.length;
    let processing = 0, completed = 0, failed = 0;
    
    cards.forEach(card => {
        const statusEl = card.querySelector('.job-card-status');
        const status = getStatusFromClass(statusEl ? statusEl.className : '');
        if (status === 'processing') processing++;
        else if (status === 'completed') completed++;
        else if (status === 'failed') failed++;
    });
    
    document.getElementById('totalJobs').textContent = total;
    document.getElementById('processingJobs').textContent = processing;
    document.getElementById('completedJobs').textContent = completed;
    document.getElementById('failedJobs').textContent = failed;
}