const { app, BrowserWindow, ipcMain, Menu, shell } = require('electron');
const fs = require('fs/promises');
const path = require('path');

const ROBOTS_FILE = path.join(app.getPath('userData'), 'robots.json');
const SETTINGS_FILE = path.join(app.getPath('userData'), 'settings.json');
const APP_PARTITION = 'persist:testbuild-robot-ui';

const DEFAULT_SETTINGS = Object.freeze({
  maximizeOnStartup: true,
  autoRefresh: false,
  defaultPort: 5000,
  defaultPage: '/dev',
});

let managerWindow = null;
const robotWindows = new Map();

function normalizeProtocol(value) {
  return String(value || '').trim().toLowerCase() === 'https' ? 'https' : 'http';
}

function normalizeHost(value) {
  let host = String(value || '').trim();
  if (!host) {
    return '';
  }
  host = host.replace(/^https?:\/\//i, '');
  host = host.split('/')[0];
  return host;
}

function normalizePort(value) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  if (Number.isInteger(parsed) && parsed > 0 && parsed < 65536) {
    return parsed;
  }
  return DEFAULT_SETTINGS.defaultPort;
}

function normalizePage(value) {
  const page = String(value || '').trim();
  return ['/dev', '/user', '/editor'].includes(page) ? page : DEFAULT_SETTINGS.defaultPage;
}

function normalizeRobot(robot = {}) {
  const host = normalizeHost(robot.host);
  return {
    id: String(robot.id || '').trim() || Date.now().toString(),
    name: String(robot.name || host || 'Unnamed robot').trim(),
    protocol: normalizeProtocol(robot.protocol),
    host,
    port: normalizePort(robot.port),
    page: normalizePage(robot.page),
    note: String(robot.note || '').trim(),
    group: String(robot.group || 'Ungrouped').trim() || 'Ungrouped',
  };
}

async function readRobots() {
  try {
    const data = await fs.readFile(ROBOTS_FILE, 'utf-8');
    const parsed = JSON.parse(data);
    return Array.isArray(parsed) ? parsed.map(normalizeRobot).filter((robot) => robot.host) : [];
  } catch (_error) {
    return [];
  }
}

async function writeRobots(robots) {
  const normalized = Array.isArray(robots) ? robots.map(normalizeRobot).filter((robot) => robot.host) : [];
  await fs.mkdir(path.dirname(ROBOTS_FILE), { recursive: true });
  await fs.writeFile(ROBOTS_FILE, JSON.stringify(normalized, null, 2), 'utf-8');
  return normalized;
}

async function readSettings() {
  try {
    const data = await fs.readFile(SETTINGS_FILE, 'utf-8');
    return { ...DEFAULT_SETTINGS, ...JSON.parse(data) };
  } catch (_error) {
    return { ...DEFAULT_SETTINGS };
  }
}

async function writeSettings(settings) {
  const merged = { ...(await readSettings()), ...(settings || {}) };
  await fs.mkdir(path.dirname(SETTINGS_FILE), { recursive: true });
  await fs.writeFile(SETTINGS_FILE, JSON.stringify(merged, null, 2), 'utf-8');
  return merged;
}

function robotBaseUrl(robot) {
  const safe = normalizeRobot(robot);
  return `${safe.protocol}://${safe.host}:${safe.port}`;
}

function robotUiUrl(robot) {
  const safe = normalizeRobot(robot);
  return `${robotBaseUrl(safe)}${safe.page}`;
}

async function fetchRobotJson(robot, endpoint, init = {}, timeoutMs = 6000) {
  const target = `${robotBaseUrl(robot)}${endpoint}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(target, {
      ...init,
      headers: {
        'Content-Type': 'application/json',
        ...(init.headers || {}),
      },
      signal: controller.signal,
    });
    const text = await response.text();
    let payload = {};
    try {
      payload = text ? JSON.parse(text) : {};
    } catch (_error) {
      payload = { ok: response.ok, raw: text };
    }
    return {
      ok: response.ok,
      status: response.status,
      payload,
      url: target,
    };
  } catch (error) {
    return {
      ok: false,
      status: 0,
      payload: {
        ok: false,
        message: error?.name === 'AbortError' ? 'Request timed out' : String(error?.message || error),
      },
      url: target,
    };
  } finally {
    clearTimeout(timeout);
  }
}

async function probeRobot(robot) {
  const started = Date.now();
  const response = await fetchRobotJson(robot, '/api/stack/health', { method: 'GET' }, 3500);
  const latencyMs = Date.now() - started;
  return {
    ok: Boolean(response.ok || response.status === 401 || response.status === 403),
    latencyMs,
    status: response.status,
    message: response.payload?.message || '',
    diagnostics: response.payload?.diagnostics || {},
  };
}

function summarizeMapName(payload) {
  if (!payload || typeof payload !== 'object') {
    return '';
  }
  const mapName = String(payload.map_name || payload.name || '').trim();
  if (mapName) {
    return mapName;
  }
  const mapYamlPath = String(payload.map_yaml_path || '').trim();
  if (!mapYamlPath) {
    return '';
  }
  const base = path.basename(mapYamlPath);
  return base.replace(/\.ya?ml$/i, '');
}

async function fetchRobotSnapshot(robot) {
  const safeRobot = normalizeRobot(robot);
  const probe = await probeRobot(safeRobot);
  if (!probe.ok) {
    return {
      ...probe,
      online: false,
      robot: safeRobot,
      confidence: '',
      map: '',
      pose: null,
      networkStatus: null,
      note: safeRobot.note,
    };
  }

  const [previewResponse, mapResponse, poseResponse, networkResponse] = await Promise.all([
    fetchRobotJson(safeRobot, '/api/settings/preview', { method: 'GET' }, 4500),
    fetchRobotJson(safeRobot, '/api/map/get_active', { method: 'GET' }, 4500),
    fetchRobotJson(safeRobot, '/api/robot/pose', { method: 'GET' }, 4500),
    fetchRobotJson(safeRobot, '/api/next/network/status', { method: 'GET' }, 5000),
  ]);

  const preview = previewResponse.payload || {};
  const mapPayload = mapResponse.payload || {};
  const posePayload = poseResponse.payload || {};
  const networkPayload = networkResponse.payload || {};
  const diagnostics = probe.diagnostics || {};
  const currentState = diagnostics.current_state || {};

  return {
    ok: true,
    online: true,
    latencyMs: probe.latencyMs,
    status: probe.status,
    robot: safeRobot,
    confidence: String(preview.preview?.localization || currentState.localization_confidence || '').trim(),
    map: summarizeMapName(mapPayload),
    pose: posePayload.pose || null,
    stack: diagnostics.stack || {},
    networkStatus: networkPayload.ok ? networkPayload : null,
    networkDelay: `${probe.latencyMs} ms`,
    note: safeRobot.note,
  };
}

async function openRobotUi(robot) {
  const safeRobot = normalizeRobot(robot);
  if (robotWindows.has(safeRobot.id)) {
    robotWindows.get(safeRobot.id).focus();
    return;
  }

  const settings = await readSettings();
  const win = new BrowserWindow({
    width: 1600,
    height: 980,
    title: `Robot: ${safeRobot.name} (${safeRobot.host})`,
    backgroundColor: '#000',
    webPreferences: {
      partition: APP_PARTITION,
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  win.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: 'deny' };
  });

  await win.loadURL(robotUiUrl(safeRobot));
  if (settings.maximizeOnStartup) {
    win.maximize();
  }

  robotWindows.set(safeRobot.id, win);
  win.on('closed', () => robotWindows.delete(safeRobot.id));
}

function createManagerWindow() {
  if (managerWindow) {
    managerWindow.focus();
    return;
  }

  managerWindow = new BrowserWindow({
    width: 1480,
    height: 920,
    minWidth: 1200,
    minHeight: 760,
    title: 'NEXT Device Manager',
    backgroundColor: '#17181d',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
    },
  });

  managerWindow.loadFile(path.join(__dirname, 'renderer', 'connect.html'));
  managerWindow.on('closed', () => {
    managerWindow = null;
  });
}

function registerIpc() {
  ipcMain.handle('robots:get-all', async () => readRobots());

  ipcMain.handle('robots:save', async (_event, robot) => {
    const robots = await readRobots();
    const normalized = normalizeRobot(robot);
    const index = robots.findIndex((entry) => entry.id === normalized.id);
    if (index >= 0) {
      robots[index] = { ...robots[index], ...normalized };
    } else {
      robots.push(normalized);
    }
    return writeRobots(robots);
  });

  ipcMain.handle('robots:delete', async (_event, id) => {
    const robots = await readRobots();
    return writeRobots(robots.filter((robot) => robot.id !== String(id || '')));
  });

  ipcMain.handle('robots:connect', async (_event, robot) => {
    await openRobotUi(robot);
    return { ok: true };
  });

  ipcMain.handle('robots:probe', async (_event, robot) => probeRobot(robot));
  ipcMain.handle('robots:refresh-one', async (_event, robot) => fetchRobotSnapshot(robot));

  ipcMain.handle('robots:fetch-network-status', async (_event, robot) => {
    const response = await fetchRobotJson(robot, '/api/next/network/status', { method: 'GET' }, 6000);
    return response.payload;
  });

  ipcMain.handle('robots:scan-wifi', async (_event, robot, interfaceName = '') => {
    const query = interfaceName ? `?interface=${encodeURIComponent(interfaceName)}&rescan=1` : '?rescan=1';
    const response = await fetchRobotJson(robot, `/api/next/network/wifi/scan${query}`, { method: 'GET' }, 12000);
    return response.payload;
  });

  ipcMain.handle('robots:connect-wifi', async (_event, robot, payload) => {
    const response = await fetchRobotJson(
      robot,
      '/api/next/network/wifi/connect',
      {
        method: 'POST',
        body: JSON.stringify(payload || {}),
      },
      25000,
    );
    return response.payload;
  });

  ipcMain.handle('robots:configure-interface', async (_event, robot, payload) => {
    const response = await fetchRobotJson(
      robot,
      '/api/next/network/interface/configure',
      {
        method: 'POST',
        body: JSON.stringify(payload || {}),
      },
      25000,
    );
    return response.payload;
  });

  ipcMain.handle('robots:set-wireless-enabled', async (_event, robot, payload) => {
    const response = await fetchRobotJson(
      robot,
      '/api/next/network/wifi/enabled',
      {
        method: 'POST',
        body: JSON.stringify(payload || {}),
      },
      15000,
    );
    return response.payload;
  });

  ipcMain.handle('settings:get', async () => readSettings());
  ipcMain.handle('settings:save', async (_event, settings) => writeSettings(settings));
  ipcMain.handle('desktop:get-bootstrap', async () => ({ version: app.getVersion() }));
}

app.whenReady().then(() => {
  registerIpc();
  createManagerWindow();

  const template = [
    {
      label: 'Connection',
      submenu: [
        { label: 'Device Manager', click: createManagerWindow },
        { type: 'separator' },
        { role: 'quit' },
      ],
    },
    {
      label: 'View',
      submenu: [
        { role: 'reload' },
        { role: 'toggleDevTools' },
        { type: 'separator' },
        { role: 'resetZoom' },
        { role: 'zoomIn' },
        { role: 'zoomOut' },
      ],
    },
  ];
  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    createManagerWindow();
  }
});
