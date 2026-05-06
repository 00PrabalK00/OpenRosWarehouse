const state = {
  robots: [],
  snapshots: new Map(),
  selectedRobotId: '',
  selectedGroup: 'All',
  searchTerm: '',
  settings: { maximizeOnStartup: true, autoRefresh: false },
  bootstrap: {},
  wifiNetworks: [],
  lastNetworkStatus: null,
  activeTab: 'overview',
};

const byId = (id) => document.getElementById(id);
const robotGrid = byId('robot-grid');
const groupList = byId('group-list');
const dashboardStatus = byId('dashboard-status');
const detailTitle = byId('detail-title');
const detailSummary = byId('detail-summary');
const networkStatusEl = byId('network-status');
const wifiList = byId('wifi-list');
const wifiSelect = byId('wifi-network-select');
const wifiPassword = byId('wifi-password');
const advancedInterface = byId('advanced-interface');
const advancedUseDhcp = byId('advanced-use-dhcp');
const advancedIp = byId('advanced-ip');
const advancedSubnet = byId('advanced-subnet');
const advancedGateway = byId('advanced-gateway');
const advancedDns = byId('advanced-dns');
const hostInfo = byId('host-info');

function selectedRobot() {
  return state.robots.find((robot) => robot.id === state.selectedRobotId) || null;
}

function selectedSnapshot() {
  return state.snapshots.get(state.selectedRobotId) || null;
}

function setBanner(message, tone = 'neutral') {
  dashboardStatus.textContent = message;
  dashboardStatus.dataset.tone = tone;
}

function openModal(id) {
  byId(id).classList.add('active');
}

function closeModal(id) {
  byId(id).classList.remove('active');
}

function groupNames() {
  const groups = new Set(state.robots.map((robot) => robot.group || 'Ungrouped'));
  return ['All', ...Array.from(groups).sort((left, right) => left.localeCompare(right))];
}

function filteredRobots() {
  const search = state.searchTerm.trim().toLowerCase();
  return state.robots.filter((robot) => {
    const inGroup = state.selectedGroup === 'All' || (robot.group || 'Ungrouped') === state.selectedGroup;
    if (!inGroup) {
      return false;
    }
    if (!search) {
      return true;
    }
    const haystack = [robot.name, robot.host, robot.note, robot.group].join(' ').toLowerCase();
    return haystack.includes(search);
  });
}

function renderGroups() {
  const groups = groupNames();
  groupList.innerHTML = groups.map((group) => {
    const count = group === 'All'
      ? state.robots.length
      : state.robots.filter((robot) => (robot.group || 'Ungrouped') === group).length;
    return `
      <div class="group-card ${state.selectedGroup === group ? 'active' : ''}">
        <button type="button" data-group="${group}">${group}</button>
        <span class="group-count">${count}</span>
      </div>
    `;
  }).join('');
}

function snapshotFor(robotId) {
  return state.snapshots.get(robotId) || { online: false, note: '' };
}

function cardMetric(label, value) {
  return `
    <div class="metric-row">
      <span class="metric-label">${label}</span>
      <span>${value || '—'}</span>
    </div>
  `;
}

function renderRobots() {
  const robots = filteredRobots();
  if (!robots.length) {
    robotGrid.innerHTML = '<div class="empty-card">No devices match the current filter.</div>';
    return;
  }

  robotGrid.innerHTML = robots.map((robot) => {
    const snapshot = snapshotFor(robot.id);
    const statusClass = snapshot.online ? 'status-online' : (snapshot.pending ? 'status-checking' : 'status-offline');
    const confidence = snapshot.confidence || '—';
    const mapName = snapshot.map || '—';
    const delay = snapshot.networkDelay || (snapshot.latencyMs ? `${snapshot.latencyMs} ms` : '—');
    const note = snapshot.note || robot.note || '—';

    return `
      <article class="robot-card ${robot.id === state.selectedRobotId ? 'selected' : ''}" data-robot-id="${robot.id}">
        <div class="robot-topline ${statusClass}">
          <div class="robot-title">
            <span class="status-dot"></span>
            <span>${snapshot.online ? 'Online' : 'Offline'}</span>
          </div>
          <span>${robot.group || 'Ungrouped'}</span>
        </div>
        <div>
          <h3>${robot.name}</h3>
          <div class="robot-ip">${robot.host}</div>
        </div>
        <div class="robot-metrics">
          ${cardMetric('Map', mapName)}
          ${cardMetric('Name', robot.name)}
          ${cardMetric('Confidence', confidence)}
          ${cardMetric('Network Delay', delay)}
          ${cardMetric('Note', note)}
        </div>
        <div class="robot-footline">
          <span class="pill cyan">${robot.page}</span>
          <span class="pill">${snapshot.status ? `HTTP ${snapshot.status}` : 'Saved device'}</span>
        </div>
        <div class="robot-actions">
          <button class="action-button success" type="button" data-action="connect" data-robot-id="${robot.id}">Connect</button>
          <button class="action-button secondary" type="button" data-action="advanced" data-robot-id="${robot.id}">Advanced Configuration</button>
          <button class="action-button secondary" type="button" data-action="edit" data-robot-id="${robot.id}">Edit</button>
          <button class="action-button danger" type="button" data-action="delete" data-robot-id="${robot.id}">Delete</button>
        </div>
      </article>
    `;
  }).join('');
}

function setControlState(enabled) {
  [
    'btn-connect-selected',
    'btn-edit-selected',
    'btn-refresh-network',
    'btn-refresh-wifi',
    'btn-connect-wifi',
    'btn-disable-wireless',
    'btn-enable-wireless',
    'advanced-interface',
    'advanced-use-dhcp',
    'advanced-ip',
    'advanced-subnet',
    'advanced-gateway',
    'advanced-dns',
    'btn-write-network',
  ].forEach((id) => {
    const element = byId(id);
    if (!element) {
      return;
    }
    element.disabled = !enabled;
  });
  wifiSelect.disabled = !enabled;
  wifiPassword.disabled = !enabled;
}

function renderDetailSummary(robot, snapshot) {
  if (!robot) {
    detailTitle.textContent = 'No device selected';
    detailSummary.className = 'detail-summary empty';
    detailSummary.textContent = 'Select a robot card to inspect network and connection details.';
    networkStatusEl.className = 'network-status empty';
    networkStatusEl.textContent = 'Pick a robot to load wireless status.';
    wifiList.innerHTML = '';
    wifiSelect.innerHTML = '';
    advancedInterface.innerHTML = '';
    setControlState(false);
    return;
  }

  setControlState(true);
  detailTitle.textContent = `${robot.name} · ${robot.host}`;
  detailSummary.className = 'detail-summary';
  detailSummary.innerHTML = `
    <div class="detail-summary-grid">
      <div class="summary-card"><strong>Status</strong>${snapshot?.online ? 'Online' : 'Offline'}</div>
      <div class="summary-card"><strong>Confidence</strong>${snapshot?.confidence || '—'}</div>
      <div class="summary-card"><strong>Network Delay</strong>${snapshot?.networkDelay || '—'}</div>
      <div class="summary-card"><strong>Map</strong>${snapshot?.map || '—'}</div>
      <div class="summary-card"><strong>Device Name</strong>${robot.name}</div>
      <div class="summary-card"><strong>Note</strong>${snapshot?.note || robot.note || '—'}</div>
    </div>
  `;
}

function renderNetworkStatus(statusPayload) {
  state.lastNetworkStatus = statusPayload;
  const status = statusPayload?.status || statusPayload;
  if (!status || !status.ok) {
    networkStatusEl.className = 'network-status empty';
    networkStatusEl.textContent = statusPayload?.message || 'Network information is unavailable for this robot.';
    advancedInterface.innerHTML = '';
    return;
  }

  const devices = Array.isArray(status.devices) ? status.devices : [];
  const wireless = devices.find((device) => device.device === status.wireless_interface) || null;
  const ethernet = devices.find((device) => device.device === status.ethernet_interface) || null;
  const wirelessIp = (wireless?.ipv4 || []).join(', ') || '—';
  const ethernetIp = (ethernet?.ipv4 || []).join(', ') || '—';
  networkStatusEl.className = 'network-status';
  networkStatusEl.innerHTML = `
    <div class="detail-summary-grid">
      <div class="summary-card"><strong>Wireless Interface</strong>${status.wireless_interface || 'Not found'}</div>
      <div class="summary-card"><strong>Wireless IP</strong>${wirelessIp}</div>
      <div class="summary-card"><strong>Connected Wi‑Fi</strong>${wireless?.connection || wireless?.active_connection || 'Not connected'}</div>
      <div class="summary-card"><strong>Ethernet Interface</strong>${status.ethernet_interface || 'Not found'}</div>
      <div class="summary-card"><strong>Ethernet IP</strong>${ethernetIp}</div>
      <div class="summary-card"><strong>Link State</strong>${wireless?.state || ethernet?.state || 'Unknown'}</div>
    </div>
  `;

  advancedInterface.innerHTML = devices.map((device) => `
    <option value="${device.device}">${device.device} · ${device.type} · ${device.state}</option>
  `).join('');
  if (status.ethernet_interface) {
    advancedInterface.value = status.ethernet_interface;
  }
}

function renderWifiNetworks(networks = []) {
  state.wifiNetworks = networks;
  wifiSelect.innerHTML = networks.length
    ? networks.map((network) => `<option value="${network.ssid}">${network.ssid || '(hidden)'} · ${network.signal}%</option>`).join('')
    : '<option value="">No Wi‑Fi found</option>';

  wifiList.innerHTML = networks.length
    ? networks.map((network) => `
      <div class="wifi-row ${network.in_use ? 'active' : ''}">
        <div>
          <div class="wifi-name">${network.ssid || '(hidden)'}</div>
          <div class="wifi-meta">${network.security || 'Open'} · Channel ${network.channel || '—'} · ${network.bssid || '—'}</div>
        </div>
        <div>${network.signal}%</div>
        <div>${network.in_use ? 'Connected' : 'Available'}</div>
      </div>
    `).join('')
    : '<div class="empty-card">No Wi‑Fi networks reported by the robot.</div>';
}

function updateAdvancedDhcpState() {
  const useDhcp = advancedUseDhcp.checked;
  [advancedIp, advancedSubnet, advancedGateway, advancedDns].forEach((element) => {
    element.disabled = useDhcp || !selectedRobot();
  });
}

async function refreshRobot(robot, { quiet = false } = {}) {
  const snapshot = { pending: true, ...(state.snapshots.get(robot.id) || {}) };
  state.snapshots.set(robot.id, snapshot);
  renderRobots();
  if (!quiet && robot.id === state.selectedRobotId) {
    setBanner(`Refreshing ${robot.name}…`);
  }

  const nextSnapshot = await window.robotApi.refreshOne(robot);
  state.snapshots.set(robot.id, nextSnapshot);
  renderRobots();
  if (robot.id === state.selectedRobotId) {
    renderDetailSummary(robot, nextSnapshot);
  }
  return nextSnapshot;
}

async function refreshAllRobots() {
  if (!state.robots.length) {
    setBanner('No devices saved yet.');
    return;
  }
  setBanner('Refreshing saved devices…');
  await Promise.all(state.robots.map((robot) => refreshRobot(robot, { quiet: true })));
  setBanner('Device list updated.');
}

async function refreshSelectedNetworkStatus() {
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  const status = await window.robotApi.fetchNetworkStatus(robot);
  renderNetworkStatus(status);
}

async function refreshSelectedWifiScan() {
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  const statusPayload = state.lastNetworkStatus?.status || state.lastNetworkStatus || {};
  const iface = statusPayload.wireless_interface || '';
  const result = await window.robotApi.scanWifi(robot, iface);
  if (result.ok) {
    renderWifiNetworks(result.networks || []);
    setBanner(`Wi‑Fi scan complete for ${robot.name}.`);
  } else {
    renderWifiNetworks([]);
    setBanner(result.message || 'Wi‑Fi scan failed.', 'error');
  }
}

async function selectRobot(robotId, { refresh = true } = {}) {
  state.selectedRobotId = robotId;
  renderGroups();
  renderRobots();
  const robot = selectedRobot();
  const snapshot = selectedSnapshot();
  renderDetailSummary(robot, snapshot);

  if (!robot) {
    return;
  }

  if (refresh) {
    const freshSnapshot = await refreshRobot(robot, { quiet: true });
    renderDetailSummary(robot, freshSnapshot);
  }
  await refreshSelectedNetworkStatus();
  await refreshSelectedWifiScan();
}

function populateRobotForm(robot = null) {
  byId('robot-id').value = robot?.id || '';
  byId('robot-name').value = robot?.name || '';
  byId('robot-group').value = robot?.group || 'Ungrouped';
  byId('robot-host').value = robot?.host || '';
  byId('robot-protocol').value = robot?.protocol || 'http';
  byId('robot-port').value = robot?.port || 5000;
  byId('robot-page').value = robot?.page || '/dev';
  byId('robot-note').value = robot?.note || '';
}

async function loadInitialData() {
  state.bootstrap = await window.desktopApi.getBootstrap();
  state.settings = await window.settingsApi.get();
  state.robots = await window.robotApi.getAll();
  byId('desktop-version').textContent = state.bootstrap.version ? `v${state.bootstrap.version}` : '';
  hostInfo.textContent = window.location.hostname || 'Desktop manager';

  renderGroups();
  renderRobots();
  if (state.robots.length) {
    await refreshAllRobots();
    await selectRobot(state.robots[0].id, { refresh: false });
  }
}

document.addEventListener('click', async (event) => {
  const modalCloser = event.target.closest('[data-close-modal]');
  if (modalCloser) {
    closeModal(modalCloser.getAttribute('data-close-modal'));
    return;
  }

  const groupButton = event.target.closest('[data-group]');
  if (groupButton) {
    state.selectedGroup = groupButton.getAttribute('data-group');
    renderGroups();
    renderRobots();
    return;
  }

  const tabButton = event.target.closest('.tab-button');
  if (tabButton) {
    state.activeTab = tabButton.dataset.tab;
    document.querySelectorAll('.tab-button').forEach((button) => {
      button.classList.toggle('active', button.dataset.tab === state.activeTab);
    });
    document.querySelectorAll('.tab-panel').forEach((panel) => {
      panel.classList.toggle('active', panel.id === `tab-${state.activeTab}`);
    });
    return;
  }

  const robotCard = event.target.closest('.robot-card');
  if (robotCard && !event.target.closest('[data-action]')) {
    await selectRobot(robotCard.dataset.robotId);
    return;
  }

  const actionButton = event.target.closest('[data-action]');
  if (!actionButton) {
    return;
  }
  const robotId = actionButton.dataset.robotId;
  const robot = state.robots.find((entry) => entry.id === robotId);
  if (!robot) {
    return;
  }

  if (actionButton.dataset.action === 'connect') {
    await window.robotApi.connect(robot);
    return;
  }
  if (actionButton.dataset.action === 'advanced') {
    await selectRobot(robotId);
    return;
  }
  if (actionButton.dataset.action === 'edit') {
    populateRobotForm(robot);
    byId('robot-modal-title').textContent = 'Edit device';
    openModal('robot-modal');
    return;
  }
  if (actionButton.dataset.action === 'delete') {
    if (!window.confirm(`Delete ${robot.name}?`)) {
      return;
    }
    state.robots = await window.robotApi.delete(robot.id);
    state.snapshots.delete(robot.id);
    if (state.selectedRobotId === robot.id) {
      state.selectedRobotId = '';
    }
    renderGroups();
    renderRobots();
    renderDetailSummary(null, null);
    setBanner(`${robot.name} removed.`);
  }
});

byId('search-input').addEventListener('input', (event) => {
  state.searchTerm = event.target.value;
  renderRobots();
});

byId('btn-add-group').addEventListener('click', () => {
  const name = window.prompt('Group name');
  if (!name) {
    return;
  }
  state.selectedGroup = name.trim() || 'All';
  renderGroups();
  renderRobots();
});

byId('btn-add-robot').addEventListener('click', () => {
  populateRobotForm();
  byId('robot-modal-title').textContent = 'Add device';
  openModal('robot-modal');
});

byId('btn-enter-ip').addEventListener('click', () => openModal('enter-ip-modal'));
byId('btn-refresh-all').addEventListener('click', refreshAllRobots);

byId('btn-connect-selected').addEventListener('click', async () => {
  const robot = selectedRobot();
  if (robot) {
    await window.robotApi.connect(robot);
  }
});

byId('btn-edit-selected').addEventListener('click', () => {
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  populateRobotForm(robot);
  byId('robot-modal-title').textContent = 'Edit device';
  openModal('robot-modal');
});

byId('btn-refresh-network').addEventListener('click', refreshSelectedNetworkStatus);
byId('btn-refresh-wifi').addEventListener('click', refreshSelectedWifiScan);
advancedUseDhcp.addEventListener('change', updateAdvancedDhcpState);

byId('btn-connect-wifi').addEventListener('click', async () => {
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  const networkStatus = state.lastNetworkStatus?.status || state.lastNetworkStatus || {};
  const payload = {
    interface: networkStatus.wireless_interface || '',
    ssid: wifiSelect.value,
    password: wifiPassword.value,
  };
  const result = await window.robotApi.connectWifi(robot, payload);
  setBanner(result.message || 'Wi‑Fi connection request finished.');
  if (result.ok) {
    await refreshSelectedNetworkStatus();
    await refreshSelectedWifiScan();
    await refreshRobot(robot, { quiet: true });
  }
});

byId('btn-disable-wireless').addEventListener('click', async () => {
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  const networkStatus = state.lastNetworkStatus?.status || state.lastNetworkStatus || {};
  const result = await window.robotApi.setWirelessEnabled(robot, {
    interface: networkStatus.wireless_interface || '',
    enabled: false,
  });
  setBanner(result.message || 'Wireless disable request finished.');
  if (result.ok) {
    renderNetworkStatus(result.status || result);
  }
});

byId('btn-enable-wireless').addEventListener('click', async () => {
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  const networkStatus = state.lastNetworkStatus?.status || state.lastNetworkStatus || {};
  const result = await window.robotApi.setWirelessEnabled(robot, {
    interface: networkStatus.wireless_interface || '',
    enabled: true,
  });
  setBanner(result.message || 'Wireless enable request finished.');
  if (result.ok) {
    renderNetworkStatus(result.status || result);
  }
});

byId('btn-write-network').addEventListener('click', async () => {
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  const payload = {
    interface: advancedInterface.value,
    use_dhcp: advancedUseDhcp.checked,
    address: advancedIp.value.trim(),
    subnet_mask: advancedSubnet.value.trim(),
    gateway: advancedGateway.value.trim(),
    dns_servers: advancedDns.value.split(',').map((item) => item.trim()).filter(Boolean),
  };
  const result = await window.robotApi.configureInterface(robot, payload);
  setBanner(result.message || 'Network write finished.');
  if (result.ok) {
    renderNetworkStatus(result.status || result);
    await refreshRobot(robot, { quiet: true });
  }
});

byId('robot-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const payload = {
    id: byId('robot-id').value || undefined,
    name: byId('robot-name').value.trim(),
    group: byId('robot-group').value.trim() || 'Ungrouped',
    host: byId('robot-host').value.trim(),
    protocol: byId('robot-protocol').value,
    port: Number(byId('robot-port').value || 5000),
    page: byId('robot-page').value,
    note: byId('robot-note').value.trim(),
  };
  state.robots = await window.robotApi.save(payload);
  closeModal('robot-modal');
  renderGroups();
  renderRobots();
  const saved = state.robots.find((robot) => robot.host === payload.host && robot.name === payload.name) || state.robots[state.robots.length - 1];
  if (saved) {
    await selectRobot(saved.id);
  }
  setBanner(`${payload.name || payload.host} saved.`);
});

byId('enter-ip-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const host = byId('enter-ip-value').value.trim();
  const name = byId('enter-ip-name').value.trim() || host;
  state.robots = await window.robotApi.save({
    name,
    group: 'Direct IP',
    host,
    protocol: 'http',
    port: 5000,
    page: '/dev',
  });
  closeModal('enter-ip-modal');
  renderGroups();
  renderRobots();
  const saved = state.robots.find((robot) => robot.host === host);
  if (saved) {
    await selectRobot(saved.id);
  }
  setBanner(`Direct IP ${host} added.`);
});

byId('btn-save-settings').addEventListener('click', async () => {
  state.settings = await window.settingsApi.save({
    maximizeOnStartup: byId('setting-maximize').checked,
    autoRefresh: byId('setting-refresh').checked,
  });
  closeModal('settings-modal');
  setBanner('Desktop settings saved.');
});

document.querySelector('.menu-link:nth-child(2)').addEventListener('click', async () => {
  const settings = await window.settingsApi.get();
  byId('setting-maximize').checked = Boolean(settings.maximizeOnStartup);
  byId('setting-refresh').checked = Boolean(settings.autoRefresh);
  openModal('settings-modal');
});

setInterval(async () => {
  if (!state.settings.autoRefresh) {
    return;
  }
  const robot = selectedRobot();
  if (!robot) {
    return;
  }
  await refreshRobot(robot, { quiet: true });
}, 30000);

document.addEventListener('DOMContentLoaded', async () => {
  await loadInitialData();
  updateAdvancedDhcpState();
});
