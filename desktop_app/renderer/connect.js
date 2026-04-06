function byId(id) {
  return document.getElementById(id);
}

function currentConfig() {
  return {
    protocol: byId('protocol').value,
    host: byId('host').value.trim(),
    port: byId('port').value.trim(),
    page: byId('page').value,
    autoConnect: byId('auto-connect').checked,
  };
}

function fillForm(config = {}) {
  byId('protocol').value = config.protocol || 'http';
  byId('host').value = config.host || 'localhost';
  byId('port').value = String(config.port || 5000);
  byId('page').value = config.page || '/dev';
  byId('auto-connect').checked = Boolean(config.autoConnect);
}

function setBusy(busy) {
  byId('connect-button').disabled = busy;
  byId('test-button').disabled = busy;
}

function setStatus(state, text, detail = '') {
  const card = byId('status-card');
  card.dataset.state = state || 'idle';
  byId('status-text').textContent = text || '';
  byId('status-detail').innerHTML = detail || '';
}

function buildPreviewUrl(config) {
  const protocol = config.protocol === 'https' ? 'https' : 'http';
  const host = config.host || 'localhost';
  const port = config.port || '5000';
  const page = config.page || '/dev';
  return `${protocol}://${host}:${port}${page}`;
}

async function testConnection() {
  const config = currentConfig();
  setBusy(true);
  setStatus('warning', 'Checking robot UI…', `Trying <code>${buildPreviewUrl(config)}</code>`);
  try {
    const result = await window.desktopApi.probeTarget(config);
    if (result.ok) {
      setStatus(
        'success',
        'Robot UI is reachable.',
        `Connected path looks good: <code>${result.targetUrl}</code>`
      );
      return true;
    }
    setStatus(
      'error',
      'Robot UI is not reachable.',
      `${result.message || 'Connection failed.'} Tried <code>${buildPreviewUrl(config)}</code>`
    );
    return false;
  } catch (error) {
    setStatus('error', 'Connection test failed.', String(error?.message || error));
    return false;
  } finally {
    setBusy(false);
  }
}

async function connectToRobot() {
  const config = currentConfig();
  setBusy(true);
  setStatus('warning', 'Opening robot UI…', `Loading <code>${buildPreviewUrl(config)}</code>`);
  try {
    const result = await window.desktopApi.connectTarget(config);
    if (!result.ok) {
      setStatus(
        'error',
        'Could not connect.',
        `${result.message || 'Connection failed.'} Tried <code>${buildPreviewUrl(config)}</code>`
      );
      return;
    }
    setStatus('success', 'Connected.', `Opening <code>${result.targetUrl}</code>`);
  } catch (error) {
    setStatus('error', 'Could not connect.', String(error?.message || error));
  } finally {
    setBusy(false);
  }
}

async function bootstrap() {
  const payload = await window.desktopApi.getBootstrap();
  const config = (payload.bootstrap && payload.bootstrap.config) || payload.config || {};
  fillForm(config);

  if (payload.bootstrap && payload.bootstrap.error) {
    setStatus('error', 'Auto-connect failed.', payload.bootstrap.error);
  } else {
    setStatus('idle', 'Ready to test and connect.', 'Expected robot UI endpoint: <code>/api/stack/health</code>');
  }

  byId('test-button').addEventListener('click', () => {
    testConnection();
  });

  byId('connect-button').addEventListener('click', () => {
    connectToRobot();
  });

  document.querySelectorAll('.preset').forEach((button) => {
    button.addEventListener('click', () => {
      byId('host').value = button.dataset.host || 'localhost';
      byId('port').value = button.dataset.port || '5000';
      byId('page').value = button.dataset.page || '/dev';
      setStatus(
        'idle',
        'Preset applied.',
        `Target preview: <code>${buildPreviewUrl(currentConfig())}</code>`
      );
    });
  });

  document.querySelectorAll('input, select').forEach((element) => {
    element.addEventListener('input', () => {
      window.desktopApi.saveConfig(currentConfig()).catch(() => {});
    });
    element.addEventListener('change', () => {
      window.desktopApi.saveConfig(currentConfig()).catch(() => {});
    });
  });
}

window.addEventListener('DOMContentLoaded', bootstrap);

