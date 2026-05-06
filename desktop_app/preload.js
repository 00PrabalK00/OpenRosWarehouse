const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('robotApi', {
  getAll: () => ipcRenderer.invoke('robots:get-all'),
  save: (robot) => ipcRenderer.invoke('robots:save', robot),
  delete: (id) => ipcRenderer.invoke('robots:delete', id),
  connect: (robot) => ipcRenderer.invoke('robots:connect', robot),
  probe: (robot) => ipcRenderer.invoke('robots:probe', robot),
  refreshOne: (robot) => ipcRenderer.invoke('robots:refresh-one', robot),
  fetchNetworkStatus: (robot) => ipcRenderer.invoke('robots:fetch-network-status', robot),
  scanWifi: (robot, interfaceName) => ipcRenderer.invoke('robots:scan-wifi', robot, interfaceName),
  connectWifi: (robot, payload) => ipcRenderer.invoke('robots:connect-wifi', robot, payload),
  configureInterface: (robot, payload) => ipcRenderer.invoke('robots:configure-interface', robot, payload),
  setWirelessEnabled: (robot, payload) => ipcRenderer.invoke('robots:set-wireless-enabled', robot, payload),
});

contextBridge.exposeInMainWorld('settingsApi', {
  get: () => ipcRenderer.invoke('settings:get'),
  save: (settings) => ipcRenderer.invoke('settings:save', settings),
});

contextBridge.exposeInMainWorld('desktopApi', {
  getBootstrap: () => ipcRenderer.invoke('desktop:get-bootstrap'),
});
