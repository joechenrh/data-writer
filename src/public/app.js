'use strict';

const form = document.getElementById('gen-form');
const submitBtn = document.getElementById('submit-btn');
const pathInput = document.getElementById('path');
const s3Section = document.getElementById('s3-section');
const gcsSection = document.getElementById('gcs-section');
const statusSection = document.getElementById('status-section');
const stateText = document.getElementById('state-text');
const progressBar = document.getElementById('progress-bar');
const filesWritten = document.getElementById('files-written');
const writtenSize = document.getElementById('written-size');
const errorMsg = document.getElementById('error-msg');

let pollTimer = null;

// Toggle credential sections based on storage path prefix.
function updateCredSections() {
  const path = pathInput.value.trim();
  s3Section.hidden = !path.startsWith('s3://');
  gcsSection.hidden = !path.startsWith('gcs://');
}

pathInput.addEventListener('input', updateCredSections);

// Collect form values into the request body.
function buildRequestBody() {
  const path = pathInput.value.trim();

  const body = {
    sql: document.getElementById('sql').value,
    path,
    prefix: document.getElementById('prefix').value,
    start_fileno: parseInt(document.getElementById('start_fileno').value, 10) || 0,
    end_fileno: parseInt(document.getElementById('end_fileno').value, 10) || 0,
    rows: parseInt(document.getElementById('rows').value, 10) || 0,
    format: document.getElementById('format').value,
  };

  if (path.startsWith('s3://')) {
    body.s3 = {
      region: document.getElementById('s3_region').value,
      provider: document.getElementById('s3_provider').value,
      access_key: document.getElementById('s3_access_key').value,
      secret_key: document.getElementById('s3_secret_key').value,
      endpoint: document.getElementById('s3_endpoint').value,
    };
  } else if (path.startsWith('gcs://')) {
    body.gcs = {
      credential: document.getElementById('gcs_credential').value,
    };
  }

  return body;
}

// Format bytes into a human-readable string.
function formatBytes(bytes) {
  if (typeof bytes !== 'number' || isNaN(bytes)) return '—';
  if (bytes === 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(1024));
  return (bytes / Math.pow(1024, i)).toFixed(1) + ' ' + units[i];
}

// Apply status update from API response.
function applyStatus(data) {
  const state = (data.state || '').toLowerCase();

  stateText.textContent = data.state || '—';
  stateText.className = 'status-state ' + state;

  const pct = typeof data.progress === 'number' ? Math.min(100, Math.max(0, data.progress)) : 0;
  progressBar.style.width = pct + '%';

  filesWritten.textContent = typeof data.files_written === 'number' ? String(data.files_written) : '—';
  writtenSize.textContent = formatBytes(data.written_bytes);

  if (data.error) {
    errorMsg.textContent = data.error;
    errorMsg.hidden = false;
  } else {
    errorMsg.hidden = true;
  }

  if (state === 'completed' || state === 'failed') {
    stopPolling();
    submitBtn.disabled = false;
  }
}

async function pollStatus() {
  try {
    const res = await fetch('/api/status');
    if (!res.ok) {
      console.warn('Status poll returned', res.status);
      return;
    }
    const data = await res.json();
    applyStatus(data);
  } catch (err) {
    console.error('Failed to poll status:', err);
  }
}

function startPolling() {
  stopPolling();
  pollStatus(); // immediate first poll
  pollTimer = setInterval(pollStatus, 2000);
}

function stopPolling() {
  if (pollTimer !== null) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

form.addEventListener('submit', async (e) => {
  e.preventDefault();

  const body = buildRequestBody();

  // Reset status display.
  stateText.textContent = 'Submitting…';
  stateText.className = 'status-state running';
  progressBar.style.width = '0%';
  filesWritten.textContent = '—';
  writtenSize.textContent = '—';
  errorMsg.hidden = true;
  statusSection.hidden = false;
  submitBtn.disabled = true;

  try {
    const res = await fetch('/api/create', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      let msg = `Server error: ${res.status}`;
      try {
        const errData = await res.json();
        if (errData.error) msg = errData.error;
      } catch (_) { /* ignore */ }
      errorMsg.textContent = msg;
      errorMsg.hidden = false;
      stateText.textContent = 'failed';
      stateText.className = 'status-state failed';
      submitBtn.disabled = false;
      return;
    }

    // Task accepted — start polling for status.
    startPolling();
  } catch (err) {
    errorMsg.textContent = 'Network error: ' + err.message;
    errorMsg.hidden = false;
    stateText.textContent = 'failed';
    stateText.className = 'status-state failed';
    submitBtn.disabled = false;
  }
});
