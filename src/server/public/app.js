'use strict';

const form = document.getElementById('gen-form');
const submitBtn = document.getElementById('submit-btn');
const pathInput = document.getElementById('path');
const credSection = document.getElementById('cred-section');
const storageType = document.getElementById('storage_type');
const awsFields = document.getElementById('aws-fields');
const ksyunFields = document.getElementById('ksyun-fields');

const aiPromptInput = document.getElementById('ai-prompt');
const aiBtn = document.getElementById('ai-btn');
const sqlTextarea = document.getElementById('sql');

const taskTable = document.getElementById('task-table');
const taskTbody = document.getElementById('task-tbody');
const taskListEmpty = document.getElementById('task-list-empty');
const refreshBtn = document.getElementById('refresh-btn');

const helpBtn = document.getElementById('help-btn');
const formatSelect = document.getElementById('format');
const formatModal = document.getElementById('format-modal');
const formatModalClose = document.getElementById('format-modal-close');
const formatOptionsBtn = document.getElementById('format-options-btn');
const helpModal = document.getElementById('help-modal');
const modalClose = document.querySelector('.modal-close');

let pollTimer = null;

// ── Help modal ──

function openHelp(e) {
  e.preventDefault();
  e.stopPropagation();
  helpModal.classList.add('open');
}

function closeHelp() {
  helpModal.classList.remove('open');
}

helpBtn.addEventListener('click', openHelp);
modalClose.addEventListener('click', closeHelp);
helpModal.addEventListener('click', (e) => {
  if (e.target === helpModal) closeHelp();
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && helpModal.classList.contains('open')) closeHelp();
});

// ── Format options modal ──

function openFormatModal(e) {
  e.preventDefault();
  e.stopPropagation();
  const isCsv = formatSelect.value === 'csv';
  document.getElementById('csv-options').hidden = !isCsv;
  document.getElementById('parquet-options').hidden = isCsv;
  formatModal.classList.add('open');
}

function closeFormatModal() {
  formatModal.classList.remove('open');
}

formatOptionsBtn.addEventListener('click', openFormatModal);
formatModalClose.addEventListener('click', closeFormatModal);
formatModal.addEventListener('click', (e) => {
  if (e.target === formatModal) closeFormatModal();
});
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && formatModal.classList.contains('open')) closeFormatModal();
});

// ── Credential section toggling ──

function updateCredSection() {
  const path = pathInput.value.trim().toLowerCase();
  const isRemote = path.startsWith('s3://');
  credSection.hidden = !isRemote;
  updateCredFields();
}

function updateCredFields() {
  const isKsyun = storageType.value === 'ksyun';
  awsFields.hidden = isKsyun;
  ksyunFields.hidden = !isKsyun;
  const ec2Group = document.getElementById('ec2-toggle-group');
  const ec2Checkbox = document.getElementById('run-on-ec2');
  ec2Group.hidden = isKsyun;
  if (isKsyun) {
    ec2Checkbox.checked = false;
  }
  updateEc2Toggle();
}

function updateEc2Toggle() {
  const ec2Checkbox = document.getElementById('run-on-ec2');
  const ec2Hint = document.getElementById('ec2-hint');
  const awsCredFields = document.getElementById('aws-cred-fields');
  if (!ec2Checkbox) return;
  const isEc2 = ec2Checkbox.checked;
  ec2Hint.hidden = !isEc2;
  awsCredFields.hidden = isEc2;
}

pathInput.addEventListener('input', updateCredSection);
storageType.addEventListener('change', updateCredFields);
document.getElementById('run-on-ec2').addEventListener('change', updateEc2Toggle);

// ── Inline SQL validation ──

sqlTextarea.addEventListener('input', () => {
  const val = sqlTextarea.value.trim();
  if (val === '' || schemaTablePattern.test(val)) {
    sqlTextarea.classList.remove('invalid');
  } else {
    sqlTextarea.classList.add('invalid');
  }
});

// ── Validation ──

// Match CREATE TABLE schema.table (...). Allows backticks/quotes around names.
const schemaTablePattern = /CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+[`"']?[A-Za-z0-9_-]+[`"']?\.[`"']?[A-Za-z0-9_-]+[`"']?\s*\(/i;

function validateForm() {
  const errors = [];

  const sql = sqlTextarea.value.trim();
  if (!sql) {
    errors.push('SQL Schema is required');
  } else if (!schemaTablePattern.test(sql)) {
    errors.push('SQL must use qualified name: CREATE TABLE schema.table (...)');
  }
  if (!pathInput.value.trim()) {
    errors.push('Storage Path is required');
  }

  const end = parseInt(document.getElementById('end_fileno').value, 10) || 0;
  if (end <= 0) {
    errors.push('Files count must be greater than 0');
  }

  const rows = parseInt(document.getElementById('rows').value, 10) || 0;
  if (rows <= 0) {
    errors.push('Rows per File must be greater than 0');
  }

  return errors;
}

// ── Form body builder ──

function buildRequestBody() {
  const path = pathInput.value.trim();
  const foldersVal = document.getElementById('folders').value;

  const body = {
    sql: sqlTextarea.value,
    path,
    start_fileno: 0,
    end_fileno: parseInt(document.getElementById('end_fileno').value, 10) || 0,
    rows: parseInt(document.getElementById('rows').value, 10) || 0,
    format: document.getElementById('format').value,
  };

  if (foldersVal !== '') {
    body.folders = parseInt(foldersVal, 10) || 0;
  }

  // Only send target=ec2 when the path actually needs EC2 (S3) AND the
  // checkbox is checked. Local paths are always target=local — the EC2 toggle
  // is hidden for them anyway but retains its default-checked state in the DOM.
  const isRemotePath = path.toLowerCase().startsWith('s3://');
  if (isRemotePath && document.getElementById('run-on-ec2').checked) {
    body.target = 'ec2';
  }

  // Format-specific options
  if (body.format === 'csv') {
    body.csv = {
      separator: document.getElementById('csv_separator').value || ',',
      endline: document.getElementById('csv_endline').value.replace(/\\n/g, '\n').replace(/\\r/g, '\r'),
      base64: document.getElementById('csv_base64').checked,
    };
  } else {
    body.parquet = {
      compression: document.getElementById('parquet_compression').value || 'zstd',
      row_groups: parseInt(document.getElementById('parquet_row_groups').value, 10) || 1,
      page_size: document.getElementById('parquet_page_size').value || '1MiB',
    };
  }

  if (!credSection.hidden) {
    if (storageType.value === 'ksyun') {
      body.ksyun = true;
    } else {
      body.s3 = {
        region: document.getElementById('s3_region').value,
        provider: document.getElementById('s3_provider').value,
        access_key: document.getElementById('s3_access_key').value,
        secret_key: document.getElementById('s3_secret_key').value,
        endpoint: document.getElementById('s3_endpoint').value,
      };
    }
  }

  if (goEditor) {
    const text = goEditor.getValue().trim();
    if (text) {
      body.generators_go = text;
      body.target = 'ec2'; // custom generators only supported on EC2
    }
  }

  return body;
}

// ── Task list ──

function formatTime(iso) {
  if (!iso) return '\u2014';
  const d = new Date(iso);
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

const taskListFooter = document.getElementById('task-list-footer');

function renderTaskRow(task) {
  const tr = document.createElement('tr');
  const canCancel = task.state === 'pending' || task.state === 'running';
  const targetLabel = task.target === 'ec2' ? 'EC2' : 'Local';
  tr.dataset.id = task.id;
  tr.dataset.state = task.state;
  tr.innerHTML =
    `<td>${task.id}</td>` +
    `<td>${targetLabel}</td>` +
    `<td><span class="status-dot ${task.state}">${task.state}</span></td>` +
    `<td>${task.progress}</td>` +
    `<td>${task.files_written} / ${task.total_files}</td>` +
    `<td>${task.written_size || '\u2014'}</td>` +
    `<td>${formatTime(task.created_at)}</td>` +
    `<td><button class="actions-trigger" data-id="${task.id}">\u22EF</button></td>`;
  return tr;
}

async function cancelTask(id) {
  try {
    const res = await fetch('/api/cancel?id=' + encodeURIComponent(id), { method: 'POST' });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      alert(data.error || 'Cancel failed');
      return;
    }
    loadTasks();
  } catch (err) {
    alert('Cancel failed: ' + err.message);
  }
}

// Fixed-position actions popup.
let actionsPopup = null;

function closePopup() {
  if (actionsPopup) {
    actionsPopup.remove();
    actionsPopup = null;
  }
}

function showActionsPopup(trigger, taskId, taskState) {
  closePopup();
  const canCancel = taskState === 'pending' || taskState === 'running';
  if (!canCancel) return;

  const rect = trigger.getBoundingClientRect();
  const popup = document.createElement('div');
  popup.className = 'actions-popup';
  popup.innerHTML = `<button class="actions-popup-item danger" data-action="cancel" data-id="${taskId}">Cancel</button>`;
  popup.style.top = (rect.bottom + 4) + 'px';
  popup.style.left = (rect.right - 100) + 'px';
  document.body.appendChild(popup);
  actionsPopup = popup;

  popup.addEventListener('click', (e) => {
    e.stopPropagation();
    const item = e.target.closest('.actions-popup-item');
    if (item && item.dataset.action === 'cancel') {
      cancelTask(item.dataset.id);
    }
    closePopup();
  });

  // Close on next click anywhere else.
  setTimeout(() => {
    document.addEventListener('click', closePopup, { once: true });
  }, 0);
}

taskTbody.addEventListener('click', (e) => {
  const trigger = e.target.closest('.actions-trigger');
  if (trigger) {
    e.stopPropagation();
    const tr = trigger.closest('tr');
    showActionsPopup(trigger, tr.dataset.id, tr.dataset.state);
  }
});

async function loadTasks() {
  try {
    const res = await fetch('/api/tasks');
    if (!res.ok) return;
    const tasks = await res.json();

    if (tasks.length === 0) {
      taskTable.hidden = true;
      taskListEmpty.hidden = false;
      taskListFooter.hidden = true;
      stopPolling();
      return;
    }

    taskTable.hidden = false;
    taskListEmpty.hidden = true;
    taskTbody.innerHTML = '';
    tasks.forEach((t) => taskTbody.appendChild(renderTaskRow(t)));

    taskListFooter.hidden = false;
    taskListFooter.textContent = `Total: ${tasks.length}`;

    // Keep polling if any task is pending or running.
    const hasActive = tasks.some((t) => t.state === 'pending' || t.state === 'running');
    if (hasActive) {
      startPolling();
    } else {
      stopPolling();
    }
  } catch (err) {
    console.error('Failed to load tasks:', err);
  }
}

function startPolling() {
  if (pollTimer !== null) return;
  pollTimer = setInterval(loadTasks, 2000);
}

function stopPolling() {
  if (pollTimer !== null) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

refreshBtn.addEventListener('click', loadTasks);

// ── AI Assist ──

aiBtn.addEventListener('click', async () => {
  const prompt = aiPromptInput.value.trim();
  if (!prompt) return;

  aiBtn.disabled = true;
  aiBtn.classList.add('loading');

  try {
    const res = await fetch('/api/ai-assist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sql: sqlTextarea.value,
        prompt,
      }),
    });

    if (!res.ok) {
      let msg = 'AI error: ' + res.status;
      try {
        const errData = await res.json();
        if (errData.error) msg = errData.error;
      } catch (_) { /* ignore */ }
      alert(msg);
      return;
    }

    const data = await res.json();
    if (data.sql) {
      sqlTextarea.value = data.sql;
    }
    aiPromptInput.value = '';
  } catch (err) {
    alert('AI request failed: ' + err.message);
  } finally {
    aiBtn.disabled = false;
    aiBtn.classList.remove('loading');
  }
});

aiPromptInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter' && !e.isComposing) {
    e.preventDefault();
    aiBtn.click();
  }
});

// ── Form submit ──

form.addEventListener('submit', async (e) => {
  e.preventDefault();

  const errors = validateForm();
  if (errors.length > 0) {
    alert(errors.join('\n'));
    return;
  }

  const body = buildRequestBody();
  submitBtn.disabled = true;

  try {
    const res = await fetch('/api/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      let msg = 'Server error: ' + res.status;
      try {
        const errData = await res.json();
        if (errData.error) msg = errData.error;
      } catch (_) { /* ignore */ }
      alert(msg);
      return;
    }

    // Task created — refresh list and start polling.
    loadTasks();
  } catch (err) {
    alert('Network error: ' + err.message);
  } finally {
    submitBtn.disabled = false;
  }
});

// ── Initial load ──
updateEc2Toggle();
loadTasks();

// ── Custom generators panel ──

let goEditor = null;

function initGoEditor() {
  if (goEditor) return;
  if (typeof require === 'undefined' || typeof require !== 'function') return;
  require(['vs/editor/editor.main'], function () {
    goEditor = monaco.editor.create(document.getElementById('generatorsEditor'), {
      value: '',
      language: 'go',
      automaticLayout: true,
      minimap: { enabled: false },
      fontSize: 13,
    });
  });
}

const generatorsPanel = document.querySelector('.generators-panel');
if (generatorsPanel) {
  generatorsPanel.addEventListener('toggle', () => {
    if (generatorsPanel.open) initGoEditor();
  });
}

const scaffoldBtn = document.getElementById('scaffoldBtn');
if (scaffoldBtn) {
  scaffoldBtn.addEventListener('click', async () => {
    const sql = sqlTextarea.value.trim();
    const status = document.getElementById('scaffoldStatus');
    if (!sql) {
      status.textContent = 'Enter SQL first';
      return;
    }
    status.textContent = 'Generating...';
    try {
      const resp = await fetch('/api/scaffold', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql }),
      });
      const data = await resp.json();
      if (!resp.ok) {
        status.textContent = 'Error: ' + (data.error || resp.status);
        return;
      }
      if (goEditor) {
        goEditor.setValue(data.generators_go);
      } else {
        // Editor not yet initialized — stash content and install after init.
        initGoEditor();
        setTimeout(() => { if (goEditor) goEditor.setValue(data.generators_go); }, 300);
      }
      const lines = (data.generators_go || '').split('\n').length;
      status.textContent = 'Scaffolded ' + lines + ' lines';
    } catch (e) {
      status.textContent = 'Error: ' + e.message;
    }
  });
}

// ── Custom generators help modal ──

const genHelpBtn = document.getElementById('gen-help-btn');
const genHelpModal = document.getElementById('gen-help-modal');
const genHelpModalClose = document.getElementById('gen-help-modal-close');

function openGenHelp(e) {
  // The hint lives inside <details>/<summary>; stop propagation so clicking
  // the help link doesn't toggle the panel.
  if (e) e.preventDefault();
  if (e) e.stopPropagation();
  genHelpModal.classList.add('open');
}

function closeGenHelp() {
  genHelpModal.classList.remove('open');
}

if (genHelpBtn) {
  genHelpBtn.addEventListener('click', openGenHelp);
}
if (genHelpModalClose) {
  genHelpModalClose.addEventListener('click', closeGenHelp);
}
if (genHelpModal) {
  genHelpModal.addEventListener('click', (e) => {
    if (e.target === genHelpModal) closeGenHelp();
  });
}
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && genHelpModal && genHelpModal.classList.contains('open')) closeGenHelp();
});

// ── Custom generators AI Assist ──

const genAiPromptInput = document.getElementById('gen-ai-prompt');
const genAiBtn = document.getElementById('gen-ai-btn');

async function runGenAi() {
  if (!genAiBtn || !genAiPromptInput) return;
  const prompt = genAiPromptInput.value.trim();
  if (!prompt) return;
  const sql = sqlTextarea.value.trim();
  if (!sql) { alert('Enter SQL first'); return; }

  genAiBtn.disabled = true;
  genAiBtn.classList.add('loading');

  try {
    // Ensure editor is initialized so we can populate it.
    initGoEditor();
    const currentCode = goEditor ? goEditor.getValue() : '';

    const res = await fetch('/api/ai-generator-assist', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql, current_code: currentCode, prompt }),
    });

    if (!res.ok) {
      let msg = 'AI error: ' + res.status;
      let fallback = null;
      try {
        const errData = await res.json();
        if (errData.error) msg = errData.error;
        if (errData.generators_go) fallback = errData.generators_go;
      } catch (_) { /* ignore */ }
      alert(msg);
      if (fallback && goEditor) {
        goEditor.setValue(fallback); // user can inspect / fix manually
      }
      return;
    }
    const data = await res.json();
    if (data.generators_go) {
      if (goEditor) {
        goEditor.setValue(data.generators_go);
      } else {
        setTimeout(() => { if (goEditor) goEditor.setValue(data.generators_go); }, 300);
      }
      genAiPromptInput.value = '';
    }
  } catch (err) {
    alert('AI request failed: ' + err.message);
  } finally {
    genAiBtn.disabled = false;
    genAiBtn.classList.remove('loading');
  }
}

if (genAiBtn) {
  genAiBtn.addEventListener('click', runGenAi);
}
if (genAiPromptInput) {
  genAiPromptInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.isComposing) {
      e.preventDefault();
      runGenAi();
    }
  });
}

// ── Custom generators validate ──

const validateBtn = document.getElementById('validateBtn');

async function runValidate() {
  if (!validateBtn) return;
  if (!goEditor) {
    initGoEditor();
    setTimeout(runValidate, 300);
    return;
  }
  const text = goEditor.getValue().trim();
  const status = document.getElementById('scaffoldStatus');
  if (!text) {
    status.textContent = 'Editor is empty';
    return;
  }
  status.textContent = 'Validating... (runs codegen + go build)';
  validateBtn.disabled = true;
  try {
    const res = await fetch('/api/validate-generators', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ generators_go: text }),
    });
    const data = await res.json();
    if (!res.ok) {
      status.textContent = 'Invalid ✗';
      alert(data.error || 'Validation failed');
      return;
    }
    status.textContent = 'Valid ✓ (builds cleanly)';
  } catch (e) {
    status.textContent = 'Validate failed';
    alert('Request failed: ' + e.message);
  } finally {
    validateBtn.disabled = false;
  }
}

if (validateBtn) {
  validateBtn.addEventListener('click', runValidate);
}
