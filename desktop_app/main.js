const { app, BrowserWindow, ipcMain, Menu, shell } = require('electron');
const fs = require('fs/promises');
const path = require('path');

const DEFAULT_CONFIG = Object.freeze({
  protocol: 'http',
  host: 'localhost',
  port: 5000,
  page: '/dev',
  autoConnect: false,
});

const CONNECT_WINDOW_SIZE = Object.freeze({
  width: 560,
  height: 720,
});

const MAIN_WINDOW_SIZE = Object.freeze({
  width: 1600,
  height: 980,
});

const APP_PARTITION = 'persist:next-robot-ui';

let connectWindow = null;
let mainWindow = null;
let connectBootstrap = null;
let ipcRegistered = false;

function configFilePath() {
  return path.join(app.getPath('userData'), 'connection.json');
}

function sanitizeProtocol(value) {
  return String(value || '').trim().toLowerCase() === 'https' ? 'https' : 'http';
}

function sanitizeHost(value) {
  let host = String(value || '').trim();
  if (!host) {
    return DEFAULT_CONFIG.host;
  }
  host = host.replace(/^https?:\/\//i, '');
  host = host.split('/')[0];
  if (!host) {
    return DEFAULT_CONFIG.host;
  }
  if (host.startsWith('[')) {
    return host;
  }
  const maybeHostPort = host.split(':');
  if (maybeHostPort.length === 2 && /^\d+$/.test(maybeHostPort[1])) {
    return maybeHostPort[0] || DEFAULT_CONFIG.host;
  }
  return host;
}

function sanitizePort(value) {
  const parsed = Number.parseInt(String(value ?? '').trim(), 10);
  if (Number.isInteger(parsed) && parsed > 0 && parsed < 65536) {
    return parsed;
  }
  return DEFAULT_CONFIG.port;
}

function sanitizePage(value) {
  const page = String(value || '').trim();
  if (page === '/user' || page === '/editor') {
    return page;
  }
  return '/dev';
}

function sanitizeConfig(raw = {}) {
  return {
    protocol: sanitizeProtocol(raw.protocol),
    host: sanitizeHost(raw.host),
    port: sanitizePort(raw.port),
    page: sanitizePage(raw.page),
    autoConnect: Boolean(raw.autoConnect),
  };
}

async function readConfig() {
  try {
    const raw = await fs.readFile(configFilePath(), 'utf-8');
    return sanitizeConfig(JSON.parse(raw));
  } catch (_error) {
    return { ...DEFAULT_CONFIG };
  }
}

async function writeConfig(raw) {
  const nextConfig = sanitizeConfig(raw);
  await fs.mkdir(path.dirname(configFilePath()), { recursive: true });
  await fs.writeFile(configFilePath(), JSON.stringify(nextConfig, null, 2), 'utf-8');
  return nextConfig;
}

function buildBaseUrl(config) {
  const safe = sanitizeConfig(config);
  return `${safe.protocol}://${safe.host}:${safe.port}`;
}

function buildTargetUrl(config) {
  const safe = sanitizeConfig(config);
  return `${buildBaseUrl(safe)}${safe.page}`;
}

async function probeRobot(config) {
  const safe = sanitizeConfig(config);
  const baseUrl = buildBaseUrl(safe);
  const probeTargets = [
    `${baseUrl}/api/stack/health`,
    `${baseUrl}/api/map/get_active`,
    buildTargetUrl(safe),
  ];

  for (const target of probeTargets) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 4000);
    try {
      const response = await fetch(target, {
        method: 'GET',
        signal: controller.signal,
        redirect: 'follow',
      });
      clearTimeout(timeout);
      if (response.ok || response.status === 401 || response.status === 403) {
        return {
          ok: true,
          checkedUrl: target,
          targetUrl: buildTargetUrl(safe),
          status: response.status,
        };
      }
    } catch (_error) {
      clearTimeout(timeout);
    }
  }

  return {
    ok: false,
    targetUrl: buildTargetUrl(safe),
    message: 'Could not reach the robot UI. Make sure zone_web_ui is running and the IP/port are correct.',
  };
}

function buildMenu() {
  const template = [
    {
      label: 'Connection',
      submenu: [
        {
          label: 'Change Robot',
          accelerator: 'CmdOrCtrl+L',
          click: async () => {
            const config = await readConfig();
            await showConnectWindow({ config });
          },
        },
        {
          label: 'Reload Robot UI',
          accelerator: 'CmdOrCtrl+R',
          click: () => {
            if (mainWindow && !mainWindow.isDestroyed()) {
              mainWindow.webContents.reloadIgnoringCache();
            }
          },
        },
        {
          label: 'Open Current UI In Browser',
          click: async () => {
            if (mainWindow && !mainWindow.isDestroyed()) {
              await shell.openExternal(mainWindow.webContents.getURL());
            }
          },
        },
        { type: 'separator' },
        { role: 'quit' },
      ],
    },
    {
      label: 'View',
      submenu: [
        { role: 'reload' },
        { role: 'forceReload' },
        { role: 'toggleDevTools' },
        { type: 'separator' },
        { role: 'resetZoom' },
        { role: 'zoomIn' },
        { role: 'zoomOut' },
        { type: 'separator' },
        { role: 'togglefullscreen' },
      ],
    },
  ];

  Menu.setApplicationMenu(Menu.buildFromTemplate(template));
}

async function showConnectWindow(bootstrap = null) {
  connectBootstrap = bootstrap;
  if (connectWindow && !connectWindow.isDestroyed()) {
    connectWindow.focus();
    return connectWindow;
  }

  connectWindow = new BrowserWindow({
    width: CONNECT_WINDOW_SIZE.width,
    height: CONNECT_WINDOW_SIZE.height,
    minWidth: 520,
    minHeight: 680,
    title: 'Connect To Robot',
    backgroundColor: '#0d1118',
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false,
    },
  });

  connectWindow.on('closed', () => {
    connectWindow = null;
  });

  await connectWindow.loadFile(path.join(__dirname, 'renderer', 'connect.html'));
  return connectWindow;
}

async function showMainWindow(config) {
  const safe = sanitizeConfig(config);
  const targetUrl = buildTargetUrl(safe);

  if (connectWindow && !connectWindow.isDestroyed()) {
    connectWindow.close();
  }

  if (!mainWindow || mainWindow.isDestroyed()) {
    mainWindow = new BrowserWindow({
      width: MAIN_WINDOW_SIZE.width,
      height: MAIN_WINDOW_SIZE.height,
      minWidth: 1280,
      minHeight: 760,
      title: 'NEXT Robot UI',
      backgroundColor: '#0d1118',
      autoHideMenuBar: true,
      webPreferences: {
        preload: path.join(__dirname, 'preload.js'),
        contextIsolation: true,
        nodeIntegration: false,
        sandbox: false,
        partition: APP_PARTITION,
      },
    });

    mainWindow.webContents.setWindowOpenHandler(({ url }) => {
      shell.openExternal(url);
      return { action: 'deny' };
    });

    mainWindow.on('closed', () => {
      mainWindow = null;
    });
  }

  await mainWindow.loadURL(targetUrl);
  mainWindow.focus();
  return mainWindow;
}

function registerIpc() {
  if (ipcRegistered) {
    return;
  }
  ipcRegistered = true;

  ipcMain.handle('desktop:get-bootstrap', async () => {
    const config = await readConfig();
    return {
      config,
      bootstrap: connectBootstrap,
      version: app.getVersion(),
    };
  });

  ipcMain.handle('desktop:probe-target', async (_event, rawConfig) => {
    return probeRobot(rawConfig);
  });

  ipcMain.handle('desktop:save-config', async (_event, rawConfig) => {
    return writeConfig(rawConfig);
  });

  ipcMain.handle('desktop:connect-target', async (_event, rawConfig) => {
    const safe = sanitizeConfig(rawConfig);
    const probe = await probeRobot(safe);
    if (!probe.ok) {
      return probe;
    }
    await writeConfig(safe);
    await showMainWindow(safe);
    return {
      ok: true,
      targetUrl: buildTargetUrl(safe),
    };
  });

  ipcMain.handle('desktop:reload-ui', async () => {
    if (!mainWindow || mainWindow.isDestroyed()) {
      return { ok: false, message: 'Main window is not available' };
    }
    mainWindow.webContents.reloadIgnoringCache();
    return { ok: true };
  });
}

async function bootstrapApp() {
  buildMenu();
  registerIpc();

  const config = await readConfig();
  if (config.autoConnect) {
    const probe = await probeRobot(config);
    if (probe.ok) {
      await showMainWindow(config);
      return;
    }
    await showConnectWindow({
      config,
      error: probe.message || 'Auto-connect failed.',
    });
    return;
  }

  await showConnectWindow({ config });
}

app.whenReady().then(bootstrapApp);

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

app.on('activate', async () => {
  if (BrowserWindow.getAllWindows().length === 0) {
    await bootstrapApp();
  }
});
