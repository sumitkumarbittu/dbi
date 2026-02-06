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
    const section = document.getElementById('schemaSection');
    const btnText = document.getElementById('createTableBtnText');

    if (section.classList.contains('hidden')) {
        section.classList.remove('hidden');
        btnText.textContent = '✖️ Cancel';
    } else {
        section.classList.add('hidden');
        btnText.textContent = '➕ Create Table';
    }
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
            toggleCreateTable();
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
            document.getElementById('primaryKey').textContent = data.primary_key.join(', ') || 'None';
            document.getElementById('attributes').textContent = data.attributes.join(', ');
            document.getElementById('schemaInfo').style.display = 'grid';
            document.getElementById('uploadCard').classList.remove('hidden');

            selectedFile = null;
            csvHeader = [];
            csvRows = [];
            previewOffset = 0;
            columnMapping = {};
            document.getElementById('fileInfoSection').classList.add('hidden');
            document.getElementById('fileInput').value = '';
            document.getElementById('jobStatusSection').classList.add('hidden');
            document.getElementById('mappingSection').classList.add('hidden');
            document.getElementById('previewCard').classList.add('hidden');
            showNotification('Schema loaded!', 'success');
        } else {
            tableSchema = null;
            document.getElementById('schemaInfo').style.display = 'none';
            document.getElementById('uploadCard').classList.add('hidden');
            showNotification('Error: ' + errorToString(data.detail), 'error');
        }
    } catch (err) {
        tableSchema = null;
        document.getElementById('schemaInfo').style.display = 'none';
        document.getElementById('uploadCard').classList.add('hidden');
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
                    <div style="margin-top:6px;">
                        <label>Table attribute</label>
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

// File handling
const uploadZone = document.getElementById('uploadZone');

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

    if (!Array.isArray(tableSchema.primary_key) || tableSchema.primary_key.length === 0) {
        showNotification('Table primary key not loaded', 'error');
        return;
    }

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

    const jobCard = document.createElement('div');
    jobCard.className = 'job-card';
    jobCard.id = `job-card-${jobId}`;

    jobCard.innerHTML = `
                <div class="job-card-header">
                    <div class="job-card-title">
                        📤 ${fileName}
                        <span class="job-id-badge">ID: ${jobId}</span>
                    </div>
                    <div class="job-card-status job-status-processing">
                        <div class="spinner"></div>
                        Processing...
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
    const interval = setInterval(async () => {
        try {
            const res = await fetch(`${renderApiUrl}/job-status/${jobId}`);
            const job = await res.json();

            const card = document.getElementById(`job-card-${jobId}`);
            if (!card) {
                clearInterval(interval);
                return;
            }

            const statusEl = card.querySelector('.job-card-status');
            const progressEl = document.getElementById(`job-progress-${jobId}`);
            const progressBarEl = document.getElementById(`job-progress-bar-${jobId}`);
            const detailsEl = document.getElementById(`job-details-${jobId}`);

            if (job.status === 'completed') {
                clearInterval(interval);
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
            } else if (job.status === 'failed') {
                clearInterval(interval);
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
    container.innerHTML = '';

    Object.keys(API_CONFIGS).forEach(key => {
        const box = document.createElement('div');
        box.className = `api-box ${key === selectedApi ? 'selected' : ''}`;
        box.textContent = key;
        box.onclick = () => selectApi(key, box);
        container.appendChild(box);
    });
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
}

document.addEventListener('DOMContentLoaded', () => {
    // Always reset to B on reload
    selectedApi = 'B';
    renderApiUrl = API_CONFIGS[selectedApi];
    initializeApiSelector();
    const urlText = document.getElementById('renderUrlText');
    if (urlText) urlText.textContent = API_CONFIGS[selectedApi];
    checkRenderConnection();
});

function errorToString(detail) {
    if (typeof detail === 'string') return detail;
    if (detail?.error) return detail.error;
    return JSON.stringify(detail);
}