const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('desktopApi', {
  getBootstrap: () => ipcRenderer.invoke('desktop:get-bootstrap'),
  getSettings: () => ipcRenderer.invoke('desktop:get-settings'),
  probeTarget: (config) => ipcRenderer.invoke('desktop:probe-target', config),
  saveConfig: (config) => ipcRenderer.invoke('desktop:save-config', config),
  updateSettings: (config) => ipcRenderer.invoke('desktop:update-settings', config),
  connectTarget: (config) => ipcRenderer.invoke('desktop:connect-target', config),
});

contextBridge.exposeInMainWorld('desktopShell', {
  isElectron: true,
  reloadUi: () => ipcRenderer.invoke('desktop:reload-ui'),
});
