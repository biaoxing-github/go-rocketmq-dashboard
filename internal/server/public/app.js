// state 保存页面需要复用的最新响应，避免局部刷新时反复遍历 DOM。
const state = {
  config: null,
  health: null,
  clusters: [],
  features: null,
  lastFeaturePayload: null,
  featureConfigSearch: "",
  topics: [],
  lastTopicPayload: null,
  topicSearch: "",
  consumers: [],
  lastConsumerPayload: null,
  consumerSearch: "",
  selectedClusterName: "",
  selectedTopicName: "",
  activeTopicSubtab: "topic-mutation-panel",
  activeConsumerSubtab: "consumer-summary-panel",
  lastChainPayload: null,
  lastChainQueryString: "",
  chainPollTimer: null,
  chainPollCount: 0,
  lastTopicRoutePayload: null,
  selectedTopicRouteTopic: "",
  topicRoutePollTimer: null,
  topicRoutePollCount: 0,
  lastTopicStatusPayload: null,
  selectedTopicStatusTopic: "",
  topicStatusPollTimer: null,
  topicStatusPollCount: 0,
  lastTopicMessagesPayload: null,
  selectedTopicMessagesTopic: "",
  topicMessagesPollTimer: null,
  topicMessagesPollCount: 0,
  topicMutationScope: "cluster",
  topicMutationDirty: false,
  topicMutationNotice: "",
  topicMutationAutoTopic: "",
  topicMutationAutoCluster: "",
  topicMutationAutoBroker: "",
  topicMessageSendAutoTopic: "",
  topicMessageSendAutoBroker: "",
  lastTopicMessageSendResult: null,
  lastConsumerDetailPayload: null,
  selectedConsumerGroup: "",
  selectedConsumerTopic: "",
  consumerDetailPollTimer: null,
  consumerDetailPollCount: 0,
  consumerOffsetResetAutoGroup: "",
  consumerOffsetResetAutoTopic: "",
  lastConsumerOffsetResetResult: null,
  nameServerEnvironments: [],
  lastRefreshTriggerText: "",
  snapshotPollTimer: null,
  snapshotPollCount: 0
};

const NAME_SERVER_STORAGE_KEY = "rmqdash.nameServers";

// $ 是页面内最小化的选择器助手，减少重复书写 document.querySelector。
const $ = (selector) => document.querySelector(selector);

// fetchJSON 统一封装 JSON 请求和错误提取，保证所有 API 入口展示一致的异常信息。
async function fetchJSON(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      Accept: "application/json",
      ...(options.headers || {})
    }
  });
  const payload = await response.json();
  if (!response.ok) {
    throw new Error(payload.message || `请求失败: ${response.status}`);
  }
  return payload;
}

// setLoading 控制按钮加载态，防止重复提交和重复刷新。
function setLoading(button, loading) {
  button.disabled = loading;
  button.setAttribute("aria-busy", loading ? "true" : "false");
}

// tableCell 给数据单元写入列名，窄屏时 CSS 会把表格行转换成字段卡片。
function tableCell(label, content, className = "") {
  const classAttr = className ? ` class="${className}"` : "";
  return `<td data-label="${escapeAttr(label)}"${classAttr}>${content}</td>`;
}

// dataTableHTML 生成可在页面和弹窗复用的响应式表格结构。
function dataTableHTML(headers, rowsHTML, emptyText) {
  const head = headers.map((header) => `<th>${escapeHTML(header)}</th>`).join("");
  const body = rowsHTML || `<tr><td colspan="${headers.length}">${escapeHTML(emptyText)}</td></tr>`;
  return `
    <div class="table-wrap card-table detail-table">
      <table>
        <thead><tr>${head}</tr></thead>
        <tbody>${body}</tbody>
      </table>
    </div>
  `;
}

// openDetailDialog 将无法在窄屏优雅承载的大块明细放到弹窗中，页面本体只保留可扫读内容。
function openDetailDialog(title, meta, bodyHTML) {
  const dialog = $("#detailDialog");
  $("#detailDialogTitle").textContent = title || "详情";
  $("#detailDialogMeta").textContent = meta || "Detail";
  $("#detailDialogBody").innerHTML = bodyHTML || `<div class="empty-state">暂无可展示数据。</div>`;
  if (!dialog.open) {
    dialog.showModal();
  }
}

function closeDetailDialog() {
  const dialog = $("#detailDialog");
  if (dialog.open) {
    dialog.close();
  }
}

// openTopicRouteDetail 从当前 Topic 路由缓存打开完整明细，适合地址很多或 Broker 很多的场景。
function openTopicRouteDetail() {
  const payload = state.lastTopicRoutePayload || {};
  const route = payload.data || {};
  openDetailDialog(
    "消息路由",
    route.topic || state.selectedTopicRouteTopic || "Topic",
    topicRouteTableHTML(route, payload.lastError || "未返回路由数据。")
  );
}

// openTopicStatusDetail 从当前队列水位缓存打开完整明细，避免九宫格式主页面过长。
function openTopicStatusDetail() {
  const payload = state.lastTopicStatusPayload || {};
  const status = payload.data || {};
  openDetailDialog(
    "队列水位",
    status.topic || state.selectedTopicStatusTopic || "Topic",
    topicStatusTableHTML(status, payload.lastError || "未返回队列水位。")
  );
}

// openTopicMessagesDetail 展示完整消息浏览结果，并保留每行直接跳转链路的操作。
function openTopicMessagesDetail() {
  const payload = state.lastTopicMessagesPayload || {};
  const result = payload.data || {};
  openDetailDialog(
    "Topic 消息",
    result.topic || state.selectedTopicMessagesTopic || "Topic",
    topicMessagesTableHTML(result, payload.lastError || "未回查到可展示消息。")
  );
  bindTopicMessageButtons($("#detailDialogBody"), result.rows || []);
}

// openConsumerConnectionsDetail 展示消费者连接完整明细。
function openConsumerConnectionsDetail() {
  const payload = state.lastConsumerDetailPayload || {};
  const detail = payload.data || {};
  openDetailDialog(
    "消费者连接",
    detail.group || state.selectedConsumerGroup || "Consumer",
    consumerConnectionsTableHTML(detail, payload.lastError || "未返回连接。")
  );
}

// openConsumerSubscriptionsDetail 展示消费者订阅完整明细。
function openConsumerSubscriptionsDetail() {
  const payload = state.lastConsumerDetailPayload || {};
  const detail = payload.data || {};
  openDetailDialog(
    "消费者订阅",
    detail.group || state.selectedConsumerGroup || "Consumer",
    consumerSubscriptionsTableHTML(detail, payload.lastError || "未返回订阅。")
  );
}

// openConsumerProgressDetail 展示消费者进度完整明细，长位点和 IP 在弹窗内仍会折行。
function openConsumerProgressDetail() {
  const payload = state.lastConsumerDetailPayload || {};
  const detail = payload.data || {};
  openDetailDialog(
    "消费进度",
    detail.group || state.selectedConsumerGroup || "Consumer",
    consumerProgressTableHTML(detail, payload.lastError || "未返回消费进度。")
  );
}

// setActiveTab 控制单页内的页面切换，避免所有表格和链路表单同时挤在一个长页面里。
function setActiveTab(tab) {
  const pages = Array.from(document.querySelectorAll("[data-page]"));
  const nextTab = pages.some((page) => page.dataset.page === tab) ? tab : "overview";
  const activeElement = document.activeElement;
  if (activeElement && pages.some((page) => page.dataset.page !== nextTab && page.contains(activeElement))) {
    activeElement.blur();
  }
  for (const page of pages) {
    const active = page.dataset.page === nextTab;
    page.hidden = !active;
    page.setAttribute("aria-hidden", active ? "false" : "true");
  }
  for (const tabElement of document.querySelectorAll("[data-tab]")) {
    const active = tabElement.dataset.tab === nextTab;
    tabElement.classList.toggle("active", active);
    tabElement.setAttribute("aria-selected", active ? "true" : "false");
  }
  if (window.location.hash !== `#${nextTab}`) {
    window.history.replaceState(null, "", `#${nextTab}`);
  }
  window.scrollTo({ top: 0, behavior: "auto" });
}

// bindTabs 绑定所有 tab 入口，左侧导航切换页面，顶部快捷按钮只负责跳转到对应页面。
function bindTabs() {
  for (const tabElement of document.querySelectorAll("[data-tab]")) {
    tabElement.addEventListener("click", (event) => {
      event.preventDefault();
      setActiveTab(tabElement.dataset.tab);
    });
  }
  for (const button of document.querySelectorAll("[data-tab-link]")) {
    button.addEventListener("click", () => {
      setActiveTab(button.dataset.tabLink);
    });
  }
  window.addEventListener("hashchange", () => {
    setActiveTab(window.location.hash.slice(1));
  });
  setActiveTab(window.location.hash.slice(1) || "overview");
}

// bindSubtabs 绑定父页面内的二级 tab，避免一个页面同时堆叠过多运维面板和表格。
function bindSubtabs() {
  document.querySelectorAll("[data-subtab-target]").forEach((button) => {
    button.addEventListener("click", () => {
      setSubtab(button.dataset.subtabGroup, button.dataset.subtabTarget);
    });
  });
  setSubtab("topic", state.activeTopicSubtab);
  setSubtab("consumer", state.activeConsumerSubtab);
}

// setSubtab 显示指定子功能面板，并同步按钮选中态。
function setSubtab(group, target) {
  const nextGroup = String(group || "").trim();
  const nextTarget = String(target || "").trim();
  if (!nextGroup || !nextTarget) {
    return;
  }
  document.querySelectorAll(`[data-subtab-panel="${nextGroup}"]`).forEach((panel) => {
    const active = panel.id === nextTarget;
    panel.hidden = !active;
    panel.setAttribute("aria-hidden", active ? "false" : "true");
  });
  document.querySelectorAll(`[data-subtab-group="${nextGroup}"]`).forEach((button) => {
    const active = button.dataset.subtabTarget === nextTarget;
    button.classList.toggle("active", active);
    button.setAttribute("aria-selected", active ? "true" : "false");
  });
  if (nextGroup === "topic") {
    state.activeTopicSubtab = nextTarget;
  }
  if (nextGroup === "consumer") {
    state.activeConsumerSubtab = nextTarget;
  }
}

// renderCurrentNameServerLabel 在侧栏同时展示自定义环境名和真实 NameServer 地址。
function renderCurrentNameServerLabel() {
  const current = currentNameServer();
  const environment = findNameServerEnvironment(current);
  const label = environment && environment.name !== environment.address
    ? `${environment.name} · ${environment.address}`
    : current;
  $("#nameServer").textContent = label || "-";
}

// renderHealth 把后端 health 信息写入侧栏，让 NameServer 和运行模式一眼可见。
function renderHealth(payload) {
  state.health = payload.data || {};
  renderCurrentNameServerLabel();
  $("#serviceMode").textContent = state.health.mode || "-";
  $("#latencyBudget").textContent = `${state.health.latencyBudgetMillis ?? "-"} ms`;
  renderNameServerOptions();
}

// loadHealth 只拉取服务健康信息，不和快照刷新混在一起，降低首屏耦合。
async function loadHealth() {
  const payload = await fetchJSON("/api/health");
  renderHealth(payload);
}

// renderConfig 把后端当前运行配置写入侧栏表单，确保切换 NameServer 有明确的当前态。
function renderConfig(payload) {
  state.config = payload.data || {};
  const current = state.config.nameServer || "";
  const currentEnvironment = findNameServerEnvironment(current);
  renderCurrentNameServerLabel();
  $("#nameServerInput").value = current;
  $("#nameServerNameInput").value = nameServerInputName(currentEnvironment);
  renderNameServerOptions();
  $("#nameServerSwitchStatus").textContent = current ? "当前连接" : "未配置";
  $("#nameServerDialogStatus").textContent = current ? "当前连接" : "等待输入";
}

// readStoredNameServerEnvironments 读取浏览器里保存的命名环境，并兼容旧版纯地址数组。
function readStoredNameServerEnvironments() {
  try {
    const parsed = JSON.parse(window.localStorage.getItem(NAME_SERVER_STORAGE_KEY) || "[]");
    return mergeNameServerEnvironments(Array.isArray(parsed) ? parsed : []);
  } catch {
    return [];
  }
}

// writeStoredNameServerEnvironments 保存命名环境，下一次打开页面仍可直接按名称切换。
function writeStoredNameServerEnvironments(values) {
  try {
    window.localStorage.setItem(NAME_SERVER_STORAGE_KEY, JSON.stringify(mergeNameServerEnvironments(values)));
  } catch {
    $("#nameServerDialogStatus").textContent = "本地列表保存失败";
  }
}

// normalizeNameServerEnvironment 把旧字符串和新对象统一成可渲染的环境条目。
function normalizeNameServerEnvironment(value) {
  if (typeof value === "string") {
    const address = value.trim();
    return address ? { name: address, address } : null;
  }
  if (!value || typeof value !== "object") {
    return null;
  }
  const address = String(value.address || value.nameServer || value.value || "").trim();
  if (!address) {
    return null;
  }
  const name = String(value.name || value.label || "").trim() || address;
  return { name, address };
}

// mergeNameServerEnvironments 按地址去重，并保留用户保存过的自定义名称。
function mergeNameServerEnvironments(...groups) {
  const byAddress = new Map();
  for (const group of groups) {
    for (const value of group || []) {
      const environment = normalizeNameServerEnvironment(value);
      if (!environment) {
        continue;
      }
      const existing = byAddress.get(environment.address);
      if (!existing) {
        byAddress.set(environment.address, environment);
        continue;
      }
      if (existing.name === existing.address && environment.name !== environment.address) {
        existing.name = environment.name;
      }
    }
  }
  return Array.from(byAddress.values());
}

function currentNameServer() {
  return state.config?.nameServer || state.health?.nameServer || "";
}

function configuredNameServers() {
  return mergeNameServerEnvironments(
    state.config?.availableNameServers || [],
    state.health?.availableNameServers || []
  );
}

function getNameServerEnvironments() {
  const current = currentNameServer();
  const currentEntry = current ? [{ address: current, name: current }] : [];
  return mergeNameServerEnvironments(
    currentEntry,
    readStoredNameServerEnvironments(),
    configuredNameServers()
  );
}

function findNameServerEnvironment(address) {
  const trimmed = String(address || "").trim();
  if (!trimmed) {
    return null;
  }
  return getNameServerEnvironments().find((environment) => environment.address === trimmed) || null;
}

function nameServerInputName(environment) {
  if (!environment || environment.name === environment.address) {
    return "";
  }
  return environment.name;
}

function rememberNameServerEnvironment(nameServer, name, extraNameServers = []) {
  const address = String(nameServer || "").trim();
  if (!address) {
    return;
  }
  const saved = mergeNameServerEnvironments(
    [{ address, name: String(name || "").trim() || address }],
    extraNameServers,
    readStoredNameServerEnvironments()
  );
  writeStoredNameServerEnvironments(saved);
  state.nameServerEnvironments = saved;
}

function renderNameServerOptions() {
  const list = $("#nameServerChoiceList");
  if (!list) {
    return;
  }
  const current = currentNameServer();
  const environments = getNameServerEnvironments();
  state.nameServerEnvironments = environments;
  $("#nameServerChoiceCount").textContent = String(environments.length);
  if (!environments.length) {
    list.innerHTML = `<button type="button" class="nameserver-choice empty-choice" disabled>暂无已添加 NameServer</button>`;
    return;
  }
  list.innerHTML = environments.map((environment) => {
    const active = environment.address === current;
    return `
      <button class="nameserver-choice ${active ? "active" : ""}" type="button" data-name-server-choice="${escapeAttr(environment.address)}" data-name-server-name="${escapeAttr(nameServerInputName(environment))}">
        <span class="nameserver-choice-main">
          <strong>${escapeHTML(environment.name)}</strong>
          <em>${escapeHTML(environment.address)}</em>
        </span>
        <small>${active ? "当前连接" : "点击切换"}</small>
      </button>
    `;
  }).join("");
  list.querySelectorAll("[data-name-server-choice]").forEach((button) => {
    button.addEventListener("click", () => {
      const nextNameServer = button.dataset.nameServerChoice || "";
      const nextNameServerName = button.dataset.nameServerName || "";
      $("#nameServerInput").value = nextNameServer;
      $("#nameServerNameInput").value = nextNameServerName;
      if (nextNameServer === currentNameServer()) {
        $("#nameServerDialogStatus").textContent = "当前连接";
        return;
      }
      switchNameServer(nextNameServer, nextNameServerName);
    });
  });
}

async function loadConfig() {
  const payload = await fetchJSON("/api/config");
  renderConfig(payload);
}

function openNameServerDialog() {
  const current = currentNameServer();
  const currentEnvironment = findNameServerEnvironment(current);
  $("#nameServerInput").value = current;
  $("#nameServerNameInput").value = nameServerInputName(currentEnvironment);
  $("#nameServerDialogStatus").textContent = current ? "当前连接" : "等待输入";
  renderNameServerOptions();
  $("#nameServerDialog").showModal();
  window.setTimeout(() => $("#nameServerNameInput").focus(), 0);
}

function closeNameServerDialog() {
  const dialog = $("#nameServerDialog");
  if (dialog.open) {
    dialog.close();
  }
}

function setNameServerDialogBusy(loading) {
  document
    .querySelectorAll("#nameServerButton, #nameServerDialogClose, #nameServerDialogCancel, [data-name-server-choice]")
    .forEach((button) => setLoading(button, loading));
}

// handleNameServerSubmit 切换后端运行时 NameServer，并清空前端选择态重新拉取新集群快照。
async function handleNameServerSubmit(event) {
  event.preventDefault();
  const nextNameServer = $("#nameServerInput").value.trim();
  const nextNameServerName = $("#nameServerNameInput").value.trim();
  await switchNameServer(nextNameServer, nextNameServerName);
}

// switchNameServer 复用后端运行时配置接口，弹窗只负责收集和记住用户输入。
async function switchNameServer(nextNameServer, nextNameServerName = "") {
  if (!nextNameServer) {
    $("#nameServerSwitchStatus").textContent = "NameServer 必填";
    $("#nameServerDialogStatus").textContent = "NameServer 必填";
    return;
  }
  const previousNameServer = currentNameServer();
  if (nextNameServer === previousNameServer) {
    rememberNameServerEnvironment(nextNameServer, nextNameServerName, configuredNameServers());
    renderCurrentNameServerLabel();
    renderNameServerOptions();
    $("#nameServerSwitchStatus").textContent = "已保存";
    $("#nameServerDialogStatus").textContent = "已保存";
    closeNameServerDialog();
    return;
  }
  setNameServerDialogBusy(true);
  $("#nameServerSwitchStatus").textContent = "切换中";
  $("#nameServerDialogStatus").textContent = "切换中";
  try {
    const payload = await fetchJSON("/api/config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ nameServer: nextNameServer })
    });
    rememberNameServerEnvironment(nextNameServer, nextNameServerName, [
      previousNameServer,
      ...(payload.data?.availableNameServers || [])
    ]);
    renderConfig(payload);
    resetRuntimeSelections();
    $("#nameServerSwitchStatus").textContent = "已切换";
    $("#nameServerDialogStatus").textContent = "已切换";
    closeNameServerDialog();
    await loadHealth();
    await loadSnapshots({ manageButton: false });
  } catch (error) {
    $("#nameServerSwitchStatus").textContent = error.message;
    $("#nameServerDialogStatus").textContent = error.message;
  } finally {
    setNameServerDialogBusy(false);
    renderNameServerOptions();
  }
}

// loadSnapshots 并行拉取三个只读快照，尽量把首屏响应压进一个 RTT。
async function loadSnapshots(options = {}) {
  const manageButton = options.manageButton !== false;
  const button = $("#refreshButton");
  if (manageButton) {
    setLoading(button, true);
  }
  $("#snapshotState").textContent = "加载中";
  try {
    const [clusterPayload, featurePayload, topicPayload, consumerPayload] = await Promise.all([
      fetchJSON("/api/clusters"),
      fetchJSON("/api/features"),
      fetchJSON("/api/topics"),
      fetchJSON("/api/consumers")
    ]);
    state.clusters = clusterPayload.data || [];
    state.features = featurePayload.data || null;
    state.lastFeaturePayload = featurePayload;
    state.topics = topicPayload.data || [];
    state.consumers = consumerPayload.data || [];
    state.lastConsumerPayload = consumerPayload;
    renderClusters(clusterPayload);
    renderFeatures(featurePayload);
    renderTopics(topicPayload);
    renderConsumers(consumerPayload);
    renderOverview(clusterPayload, featurePayload, topicPayload, consumerPayload);
    scheduleSnapshotPoll(clusterPayload, featurePayload, topicPayload, consumerPayload);
  } catch (error) {
    $("#clusterStatus").textContent = error.message;
    $("#featureStatus").textContent = error.message;
    $("#topicStatus").textContent = error.message;
    $("#consumerStatus").textContent = error.message;
    $("#snapshotState").textContent = "异常";
  } finally {
    if (manageButton) {
      setLoading(button, false);
    }
  }
}

// forceRefreshSnapshots 先强制启动线上快照刷新，再读取当前快照状态，避免按钮只读缓存造成误解。
async function forceRefreshSnapshots() {
  const button = $("#refreshButton");
  setLoading(button, true);
  $("#snapshotState").textContent = "刷新中";
  try {
    const payload = await fetchJSON("/api/refresh", { method: "POST" });
    state.lastRefreshTriggerText = refreshTriggerText(payload.data || {});
    state.snapshotPollCount = 0;
    $("#snapshotMeta").textContent = state.lastRefreshTriggerText;
    await loadSnapshots({ manageButton: false });
  } catch (error) {
    $("#snapshotState").textContent = "异常";
    $("#snapshotMeta").textContent = error.message;
  } finally {
    setLoading(button, false);
  }
}

// scheduleSnapshotPoll 只在后台刷新未完成时短暂追读快照，避免首屏长期停在 Consumer=0。
function scheduleSnapshotPoll(...payloads) {
  const refreshing = payloads.some((payload) => payload.refreshing);
  if (!refreshing) {
    state.snapshotPollCount = 0;
    return;
  }
  if (state.snapshotPollTimer || state.snapshotPollCount >= 45) {
    return;
  }
  state.snapshotPollCount += 1;
  state.snapshotPollTimer = window.setTimeout(async () => {
    state.snapshotPollTimer = null;
    await loadSnapshots({ manageButton: false });
  }, 1500);
}

// renderOverview 汇总核心快照里的结论，先给用户结果，再给细节。
function renderOverview(clusterPayload, featurePayload, topicPayload, consumerPayload) {
  const clusterBrokerCount = state.clusters.reduce((total, cluster) => total + (cluster.brokers?.length || 0), 0);
  const version = firstBrokerVersion(state.clusters);
  const maxLag = state.consumers.reduce((max, consumer) => Math.max(max, Number(consumer.diffTotal || 0)), 0);
  const transaction = capabilityByKey(featurePayload?.data, "transaction");

  $("#brokerVersion").textContent = version;
  $("#clusterMeta").textContent = metaText(clusterPayload, `${state.clusters.length} 个 cluster · ${clusterBrokerCount} 个 broker`);
  $("#transactionSupport").textContent = capabilityStatusText(transaction?.status);
  $("#transactionMeta").textContent = transaction?.detail || snapshotStatusText(featurePayload || {});
  $("#topicCount").textContent = `${state.topics.length}`;
  $("#topicMeta").textContent = metaText(topicPayload, "normal / retry / dlq / system");
  $("#consumerCount").textContent = `${state.consumers.length}`;
  $("#consumerMeta").textContent = metaText(consumerPayload, "在线与离线组一起展示");
  $("#maxLag").textContent = `${maxLag}`;
  $("#consumerLagMeta").textContent = maxLag > 0 ? "有堆积" : "无堆积";
  $("#snapshotState").textContent = summariseSnapshot(clusterPayload, featurePayload, topicPayload, consumerPayload);
  $("#snapshotMeta").textContent = state.lastRefreshTriggerText || "核心快照与能力画像并行刷新";
  $("#chainMeta").textContent = "输入消息后展示";
}

// renderClusters 把 broker 版本、地址和连通状态输出成可扫描表格。
function renderClusters(payload) {
  const rows = [];
  for (const cluster of state.clusters) {
    for (const broker of cluster.brokers || []) {
      rows.push(`
        <tr class="${state.selectedClusterName === cluster.name ? "selected-row" : ""}">
          ${tableCell("Cluster", escapeHTML(cluster.name), "wrap-cell")}
          ${tableCell("Broker", escapeHTML(broker.name), "wrap-cell")}
          ${tableCell("地址", escapeHTML(broker.address), "mono wrap-cell")}
          ${tableCell("版本", `<span class="text-strong">${escapeHTML(broker.version)}</span>`)}
          ${tableCell("InTPS", escapeHTML(broker.inTps || "-"), "mono")}
          ${tableCell("OutTPS", escapeHTML(broker.outTps || "-"), "mono")}
          ${tableCell("状态", `<span class="status-dot ${broker.activated ? "status-on" : "status-off"}"></span>${broker.activated ? "online" : "offline"}`)}
          ${tableCell("操作", `
            <button class="route-action" type="button" data-cluster-select="${escapeAttr(cluster.name)}">
              选择
            </button>
          `)}
        </tr>
      `);
    }
  }
  $("#clusterRows").innerHTML = rows.join("") || `<tr><td colspan="8">暂无 Broker 数据</td></tr>`;
  $("#clusterStatus").textContent = snapshotStatusText(payload);
  $("#clusterMetaPills").innerHTML = snapshotPills(payload);
  bindClusterSelectButtons();
  renderTopicMutationPanel();
  renderTopicMessageSendPanel();
}

function bindClusterSelectButtons() {
  $("#clusterRows").querySelectorAll("[data-cluster-select]").forEach((button) => {
    button.addEventListener("click", () => {
      selectCluster(button.dataset.clusterSelect);
    });
  });
}

function selectCluster(clusterName) {
  state.selectedClusterName = String(clusterName || "").trim();
  applyTopicMutationScope("cluster");
  setTopicMutationCluster(state.selectedClusterName);
  refreshTopicMutationBrokerFromCluster(state.selectedClusterName);
  $("#clusterRows").querySelectorAll("[data-cluster-select]").forEach((button) => {
    button.classList.toggle("active", button.dataset.clusterSelect === state.selectedClusterName);
    button.closest("tr")?.classList.toggle("selected-row", button.dataset.clusterSelect === state.selectedClusterName);
  });
  $("#topicStatus").textContent = state.selectedClusterName ? `已选择 ${state.selectedClusterName}` : "等待刷新";
  renderTopicMutationPanel();
  renderTopicMessageSendPanel();
  setActiveTab("topics");
}

function renderFeatures(payload) {
  state.lastFeaturePayload = payload;
  state.features = payload?.data || null;
  const report = state.features || {};
  $("#featureStatus").textContent = snapshotStatusText(payload);
  $("#featureMetaPills").innerHTML = snapshotPills(payload);
  renderFeatureSummary(report);
  renderCapabilityGrid(report);
  renderFeatureWarnings(report.warnings || []);
  renderTransactionRuntime(report.transactionRuntime || {});
  renderCommonConfigPanels(report.commonConfigPanels || []);
  renderSystemTopics(report.systemTopics || []);
  renderNameServerConfigGroups(report.nameServerConfigs || []);
  renderBrokerConfigGroups(report.brokerConfigs || []);
}

function renderFeatureSummary(report) {
  const brokerConfigCount = (report.brokerConfigs || []).reduce((total, broker) => total + (broker.entries?.length || 0), 0);
  const transactionRuntime = report.transactionRuntime || {};
  $("#featureSummary").innerHTML = `
    <div>
      <span>NameServer</span>
      <strong>${escapeHTML(report.nameServer || currentNameServer() || "-")}</strong>
    </div>
    <div>
      <span>事务健康</span>
      <strong>${escapeHTML(transactionRuntime.healthLabel || "-")}</strong>
    </div>
    <div>
      <span>Broker 配置</span>
      <strong>${escapeHTML(report.brokerConfigs?.length || 0)} / ${escapeHTML(brokerConfigCount)}</strong>
    </div>
    <div>
      <span>系统 Topic</span>
      <strong>${escapeHTML(report.systemTopicCount || 0)} / ${escapeHTML((report.systemTopics || []).length)}</strong>
    </div>
    <div>
      <span>采集时间</span>
      <strong>${escapeHTML(formatTime(report.generatedAtUnixMilli))}</strong>
    </div>
  `;
}

function renderCapabilityGrid(report) {
  const capabilities = report.capabilities || [];
  if (!capabilities.length) {
    $("#capabilityGrid").innerHTML = `<div class="empty-state">能力画像刷新中。</div>`;
    return;
  }
  $("#capabilityGrid").innerHTML = capabilities.map((capability) => {
    const evidence = (capability.evidence || []).slice(0, 4);
    return `
      <article class="capability-item capability-${escapeAttr(capability.status || "unknown")}">
        <div class="capability-head">
          <span>${escapeHTML(capability.category || "Feature")}</span>
          <strong>${escapeHTML(capability.label || capability.key)}</strong>
          <em class="pill ${capabilityPillClass(capability.status)}">${escapeHTML(capabilityStatusText(capability.status))}</em>
        </div>
        <p>${escapeHTML(capability.detail || "-")}</p>
        <ul>
          ${evidence.map((item) => `<li>${escapeHTML(item)}</li>`).join("") || "<li>暂无证据</li>"}
        </ul>
      </article>
    `;
  }).join("");
}

function renderFeatureWarnings(warnings) {
  const container = $("#featureWarnings");
  if (!warnings.length) {
    container.hidden = true;
    container.innerHTML = "";
    return;
  }
  container.hidden = false;
  container.innerHTML = warnings.map((warning) => `<span class="pill pill-warn">${escapeHTML(warning)}</span>`).join("");
}

function renderTransactionRuntime(runtime) {
  const operations = runtime.recentOperations || [];
  const warnings = runtime.warnings || [];
  const topics = [runtime.halfTopic, runtime.opTopic].filter(Boolean);
  const oldestPending = runtime.oldestPendingMessage || null;
  const consumerImpact = runtime.consumerImpact || {};
  const supportDiagnostic = runtime.supportDiagnostic || {};
  const actionItems = runtime.actionItems || [];
  $("#transactionRuntimeCount").textContent = `${operations.length} 个样本`;
  const summary = [
    ["支持状态", supportDiagnostic.label || (runtime.supported ? "支持事务消息" : "未确认")],
    ["事务健康", runtime.healthLabel || (runtime.supported ? "已发现" : "部分")],
    ["最老待决", oldestPending ? formatDuration(oldestPending.pendingMillis) : "无"],
    ["半消息水位", formatCount(runtime.halfTopic?.totalMessageCount)],
    ["操作消息水位", formatCount(runtime.opTopic?.totalMessageCount)],
    ["消费影响", consumerImpact.label || "-"],
    ["回滚样本", formatCount(runtime.rollbackCount)],
    ["清理/未识别", `${formatCount(runtime.cleanupCount)} / ${formatCount(runtime.unknownCount)}`]
  ];
  $("#transactionRuntimePanel").innerHTML = `
    <div class="transaction-health-strip transaction-health-${escapeAttr(runtime.healthStatus || "unknown")}">
      <div>
        <span class="pill ${transactionHealthClass(runtime.healthStatus)}">${escapeHTML(runtime.healthLabel || "未采集")}</span>
        <strong>${escapeHTML(runtime.healthDetail || runtime.detail || "-")}</strong>
      </div>
    </div>
    <div class="transaction-summary-grid">
      ${summary.map(([label, value]) => `<div><span>${escapeHTML(label)}</span><strong>${escapeHTML(value)}</strong></div>`).join("")}
    </div>
    ${transactionSupportDiagnosticHTML(supportDiagnostic)}
    <p class="transaction-runtime-detail">${escapeHTML(runtime.detail || "-")}</p>
    <p class="transaction-evidence-source"><strong>证据口径</strong>${escapeHTML(runtime.rollbackEvidenceSource || "事务提交、回滚和清理数量来自近期操作消息样本。")}</p>
    ${transactionPendingHTML(oldestPending)}
    ${transactionConsumerImpactHTML(consumerImpact)}
    ${transactionActionItemsHTML(actionItems)}
    ${warnings.length ? `<div class="feature-warnings transaction-warnings">${warnings.map((warning) => `<span class="pill pill-warn">${escapeHTML(warning)}</span>`).join("")}</div>` : ""}
    <div class="transaction-topic-grid">
      ${topics.map(transactionTopicHTML).join("")}
    </div>
    <div class="transaction-operations">
      <div class="detail-section-head">
        <h4>近期事务操作消息样本</h4>
        <span>${escapeHTML(operations.length)} 条</span>
      </div>
      ${transactionOperationTableHTML(operations)}
    </div>
  `;
}

function transactionSupportDiagnosticHTML(diagnostic) {
  const evidence = diagnostic.evidence || [];
  const presentTopics = diagnostic.presentTopics || [];
  const missingTopics = diagnostic.missingTopics || [];
  return `
    <div class="transaction-support-diagnostic transaction-support-${escapeAttr(diagnostic.status || "unsupported")}">
      <div class="transaction-topic-head">
        <span>NameServer 支持诊断</span>
        <strong>${escapeHTML(diagnostic.label || "未确认")}</strong>
      </div>
      <p>${escapeHTML(diagnostic.detail || "当前 NameServer 事务支持状态尚未采集。")}</p>
      <div class="summary-grid transaction-topic-metrics">
        <div><span>必需 Topic</span><strong>${escapeHTML(formatCount((diagnostic.requiredTopics || []).length))}</strong></div>
        <div><span>已采集</span><strong>${escapeHTML(formatCount(presentTopics.length))}</strong></div>
        <div><span>缺失 Topic</span><strong>${escapeHTML(formatCount(missingTopics.length))}</strong></div>
        <div><span>下一步</span><strong>${escapeHTML(missingTopics.length ? "补证据" : "看运行态")}</strong></div>
      </div>
      ${presentTopics.length || missingTopics.length ? `
        <div class="transaction-topic-tags">
          ${presentTopics.map((topic) => `<span class="tag tag-transaction">${escapeHTML(topic)}</span>`).join("")}
          ${missingTopics.map((topic) => `<span class="tag tag-danger">${escapeHTML(topic)}</span>`).join("")}
        </div>
      ` : ""}
      ${evidence.length ? `<ul>${evidence.map((item) => `<li>${escapeHTML(item)}</li>`).join("")}</ul>` : ""}
    </div>
  `;
}

function transactionPendingHTML(message) {
  if (!message) {
    return `<div class="transaction-pending-box"><span>最老待决</span><strong>无采样半消息</strong><p>半消息水位为 0 或采样窗口内未读取到半消息详情。</p></div>`;
  }
  const evidence = (message.evidence || []).join("；") || `${message.brokerName || "-"} / ${message.queueId ?? "-"} / ${message.queueOffset ?? "-"}`;
  return `
    <div class="transaction-pending-box">
      <span>最老待决</span>
      <strong>${escapeHTML(formatDuration(message.pendingMillis))}</strong>
      <p>${escapeHTML(message.messageId || "-")} · ${escapeHTML(formatTime(message.storeTimestamp))}</p>
      <small>${escapeHTML(evidence)}</small>
    </div>
  `;
}

function transactionConsumerImpactHTML(impact) {
  const relatedTopics = impact.relatedTopics || [];
  const evidence = impact.evidence || [];
  return `
    <div class="transaction-consumer-impact transaction-impact-${escapeAttr(impact.status || "unknown")}">
      <div class="transaction-topic-head">
        <span>消费影响</span>
        <strong>${escapeHTML(impact.label || "未采集")}</strong>
      </div>
      <p>${escapeHTML(impact.detail || "未采集 consumerProgress 消费组汇总。")}</p>
      <div class="summary-grid transaction-topic-metrics">
        <div><span>消费组</span><strong>${escapeHTML(formatCount(impact.consumerGroupCount))}</strong></div>
        <div><span>堆积组</span><strong>${escapeHTML(formatCount(impact.laggingGroupCount))}</strong></div>
        <div><span>总堆积</span><strong>${escapeHTML(formatCount(impact.totalLag))}</strong></div>
        <div><span>Retry / DLQ</span><strong>${escapeHTML(`${formatCount(impact.retryTopicCount)} / ${formatCount(impact.dlqTopicCount)}`)}</strong></div>
      </div>
      ${relatedTopics.length ? `<div class="transaction-related-topics">${relatedTopics.map((topic) => `<span class="tag tag-${escapeAttr(topic.kind || "system")}">${escapeHTML(topic.name)}</span>`).join("")}</div>` : ""}
      ${evidence.length ? `<ul>${evidence.map((item) => `<li>${escapeHTML(item)}</li>`).join("")}</ul>` : ""}
    </div>
  `;
}

function transactionActionItemsHTML(items) {
  if (!items.length) {
    return "";
  }
  return `
    <div class="transaction-action-list">
      <div class="transaction-topic-head">
        <span>处理清单</span>
        <strong>${escapeHTML(items.length)} 项</strong>
      </div>
      <div class="transaction-action-grid">
        ${items.map((item) => `
          <article class="transaction-action-item transaction-action-${escapeAttr(item.priority || "low")}">
            <div class="transaction-action-head">
              <span class="pill ${transactionActionPriorityClass(item.priority)}">${escapeHTML(transactionActionPriorityText(item.priority))}</span>
              <strong>${escapeHTML(item.title || item.key || "下一步")}</strong>
            </div>
            <p>${escapeHTML(item.detail || "-")}</p>
            ${(item.evidence || []).length ? `<ul>${(item.evidence || []).map((evidence) => `<li>${escapeHTML(evidence)}</li>`).join("")}</ul>` : ""}
          </article>
        `).join("")}
      </div>
    </div>
  `;
}

function transactionHealthClass(status) {
  switch (status) {
    case "healthy":
      return "pill-ok";
    case "risk":
      return "pill-danger";
    case "partial":
      return "pill-warn";
    case "observed":
      return "pill-info";
    default:
      return "pill-muted";
  }
}

function transactionActionPriorityClass(priority) {
  switch (priority) {
    case "high":
      return "pill-danger";
    case "medium":
      return "pill-warn";
    default:
      return "pill-muted";
  }
}

function transactionActionPriorityText(priority) {
  switch (priority) {
    case "high":
      return "高优先级";
    case "medium":
      return "中优先级";
    default:
      return "低优先级";
  }
}

function transactionTopicHTML(topic) {
  const rows = topic.rows || [];
  return `
    <article class="transaction-topic-box">
      <div class="transaction-topic-head">
        <span>${escapeHTML(topic.label || topic.topic || "事务 Topic")}</span>
        <strong>${escapeHTML(topic.present ? "已采集" : "未采集")}</strong>
      </div>
      <div class="summary-grid transaction-topic-metrics">
        <div><span>队列</span><strong>${escapeHTML(topic.totalQueues || 0)}</strong></div>
        <div><span>消息数</span><strong>${escapeHTML(formatCount(topic.totalMessageCount))}</strong></div>
        <div><span>最小位点</span><strong>${escapeHTML(formatCount(topic.minOffsetTotal))}</strong></div>
        <div><span>最大位点</span><strong>${escapeHTML(formatCount(topic.maxOffsetTotal))}</strong></div>
      </div>
      <div class="transaction-topic-updated">最近写入：${escapeHTML(topic.latestUpdated || "-")}</div>
      ${transactionTopicRowsHTML(rows)}
    </article>
  `;
}

function transactionTopicRowsHTML(rows) {
  const visibleRows = rows.slice(0, 4).map((row) => `
    <tr>
      ${tableCell("Broker", escapeHTML(row.brokerName || "-"), "mono wrap-cell")}
      ${tableCell("QID", escapeHTML(row.queueId ?? "-"), "mono")}
      ${tableCell("消息数", escapeHTML(formatCount(row.messageCount)), "mono")}
      ${tableCell("更新时间", escapeHTML(row.lastUpdated || "-"), "mono wrap-cell")}
    </tr>
  `).join("");
  return dataTableHTML(["Broker", "QID", "消息数", "更新时间"], visibleRows, "暂无队列水位。");
}

function transactionOperationTableHTML(operations) {
  const rows = operations.map((item) => `
    <tr>
      ${tableCell("操作", `<span class="pill ${transactionOperationClass(item.operation)}">${escapeHTML(item.operationLabel || "-")}</span>`)}
      ${tableCell("MessageID", escapeHTML(item.messageId || "-"), "mono wrap-cell")}
      ${tableCell("队列", escapeHTML(`${item.brokerName || "-"} / ${item.queueId ?? "-"} / ${item.queueOffset ?? "-"}`), "mono wrap-cell")}
      ${tableCell("存储时间", escapeHTML(formatTime(item.storeTimestamp)), "mono wrap-cell")}
      ${tableCell("证据", escapeHTML((item.evidence || []).join("；") || item.bodyPreview || "-"), "wrap-cell")}
    </tr>
  `).join("");
  return dataTableHTML(["操作", "MessageID", "队列", "存储时间", "证据"], rows, "暂无可回查的事务操作样本。");
}

function transactionOperationClass(operation) {
  switch (operation) {
    case "commit":
      return "pill-ok";
    case "rollback":
      return "pill-warn";
    case "cleanup":
      return "pill-info";
    default:
      return "pill-muted";
  }
}

function renderCommonConfigPanels(panels) {
  const total = panels.reduce((sum, panel) => sum + (panel.items?.length || 0), 0);
  $("#commonConfigPanelCount").textContent = `${panels.length} 类 / ${total} 项`;
  if (!panels.length) {
    $("#commonConfigPanels").innerHTML = `<div class="empty-state">暂无常用配置项。</div>`;
    return;
  }
  $("#commonConfigPanels").innerHTML = panels.map((panel) => `
    <section class="common-config-group">
      <div class="common-config-head">
        <h4>${escapeHTML(panel.category || "常用配置")}</h4>
        <span>${escapeHTML(panel.items?.length || 0)} 项</span>
      </div>
      <div class="common-config-grid">
        ${(panel.items || []).map(commonConfigItemHTML).join("")}
      </div>
    </section>
  `).join("");
}

function commonConfigItemHTML(item) {
  const evidence = (item.evidence || []).slice(0, 2);
  return `
    <article class="common-config-item common-config-${escapeAttr(item.status || "unknown")}">
      <div class="common-config-title">
        <span>${escapeHTML(item.label || item.key)}</span>
        <em class="pill ${commonConfigPillClass(item.status)}">${escapeHTML(commonConfigStatusText(item.status))}</em>
      </div>
      <strong class="mono">${escapeHTML(item.value || "-")}</strong>
      <p>${escapeHTML(item.description || "-")}</p>
      <small>${escapeHTML(item.impact || "-")}</small>
      ${evidence.length ? `<ul>${evidence.map((entry) => `<li>${escapeHTML(entry)}</li>`).join("")}</ul>` : ""}
    </article>
  `;
}

function commonConfigStatusText(status) {
  switch (status) {
    case "enabled":
      return "开启";
    case "disabled":
      return "关闭";
    case "mixed":
      return "不一致";
    case "configured":
      return "已配置";
    default:
      return "-";
  }
}

function commonConfigPillClass(status) {
  switch (status) {
    case "enabled":
    case "configured":
      return "pill-ok";
    case "mixed":
      return "pill-warn";
    case "disabled":
      return "pill-muted";
    default:
      return "pill-info";
  }
}

function renderSystemTopics(topics) {
  const present = topics.filter((topic) => topic.present).length;
  $("#systemTopicCount").textContent = `${present} / ${topics.length} 个`;
  const rows = topics.map((topic) => `
    <tr>
      ${tableCell("Topic", escapeHTML(topic.name || "-"), "mono wrap-cell")}
      ${tableCell("能力", escapeHTML(topic.label || "-"))}
      ${tableCell("类型", `<span class="tag tag-${escapeAttr(topic.kind || "system")}">${escapeHTML(topic.kind || "system")}</span>`)}
      ${tableCell("状态", `<span class="pill ${topic.present ? "pill-ok" : "pill-muted"}">${topic.present ? "已注册" : "未发现"}</span>`)}
      ${tableCell("说明", escapeHTML(topic.detail || "-"), "wrap-cell")}
    </tr>
  `);
  $("#systemTopicRows").innerHTML = rows.join("") || `<tr><td colspan="5">暂无系统 Topic 数据。</td></tr>`;
}

function renderNameServerConfigGroups(groups) {
  const total = groups.reduce((sum, group) => sum + (group.entries?.length || 0), 0);
  $("#nameServerConfigCount").textContent = `${groups.length} 组 / ${total} 项`;
  if (!groups.length) {
    $("#nameServerConfigGroups").innerHTML = `<div class="empty-state">暂无 NameServer 配置。</div>`;
    return;
  }
  $("#nameServerConfigGroups").innerHTML = groups.map((group) => `
    <details class="config-group" open>
      <summary>
        <span>${escapeHTML(group.nameServer || "NameServer")}</span>
        <strong>${escapeHTML(group.entries?.length || 0)} 项</strong>
      </summary>
      ${configEntriesTableHTML(group.entries || [], "暂无 NameServer 配置项。")}
    </details>
  `).join("");
}

function renderBrokerConfigGroups(groups) {
  const keyword = normalizeSearchText(state.featureConfigSearch);
  const total = groups.reduce((sum, group) => sum + (group.entries?.length || 0), 0);
  $("#brokerConfigCount").textContent = keyword ? `过滤中 / ${total} 项` : `${groups.length} 个 Broker / ${total} 项`;
  if (!groups.length) {
    $("#brokerConfigGroups").innerHTML = `<div class="empty-state">暂无 Broker 配置。</div>`;
    return;
  }
  $("#brokerConfigGroups").innerHTML = groups.map((group) => {
    const entries = filterConfigEntries(group, keyword);
    const title = [group.cluster, group.brokerName, group.brokerId ? `ID ${group.brokerId}` : ""].filter(Boolean).join(" · ") || group.brokerAddr || "Broker";
    return `
      <details class="config-group" open>
        <summary>
          <span>${escapeHTML(title)}</span>
          <strong>${escapeHTML(entries.length)} / ${escapeHTML(group.entries?.length || 0)} 项</strong>
        </summary>
        <div class="summary-grid config-highlight-grid">
          ${configHighlightHTML(group)}
        </div>
        ${configEntriesTableHTML(entries, keyword ? "没有匹配的配置项。" : "暂无 Broker 配置项。")}
      </details>
    `;
  }).join("");
}

function configHighlightHTML(group) {
  const highlights = (group.highlights || []).slice(0, 8);
  const base = [
    ["地址", group.brokerAddr],
    ["角色", group.role],
    ["版本", group.version]
  ].filter(([, value]) => value);
  const rows = base.map(([key, value]) => `<div><span>${escapeHTML(key)}</span><strong>${escapeHTML(value)}</strong></div>`);
  rows.push(...highlights.map((entry) => `<div><span>${escapeHTML(entry.key)}</span><strong>${escapeHTML(entry.value || "-")}</strong></div>`));
  return rows.join("") || `<div><span>摘要</span><strong>-</strong></div>`;
}

function configEntriesTableHTML(entries, emptyText) {
  const rows = entries.map((entry) => `
    <tr>
      ${tableCell("Key", escapeHTML(entry.key || "-"), "mono wrap-cell")}
      ${tableCell("Value", escapeHTML(entry.value || "-"), "mono wrap-cell")}
    </tr>
  `).join("");
  return dataTableHTML(["Key", "Value"], rows, emptyText);
}

function filterConfigEntries(group, keyword) {
  const entries = group.entries || [];
  if (!keyword) {
    return entries;
  }
  const brokerText = normalizeSearchText(`${group.cluster || ""} ${group.brokerName || ""} ${group.brokerAddr || ""}`);
  return entries.filter((entry) => {
    const text = normalizeSearchText(`${entry.key || ""} ${entry.value || ""}`);
    return text.includes(keyword) || brokerText.includes(keyword);
  });
}

function handleFeatureConfigSearchInput(event) {
  state.featureConfigSearch = event.target.value || "";
  renderBrokerConfigGroups(state.features?.brokerConfigs || []);
}

function clearFeatureConfigSearch() {
  state.featureConfigSearch = "";
  $("#featureConfigSearch").value = "";
  renderBrokerConfigGroups(state.features?.brokerConfigs || []);
}

function capabilityByKey(report, key) {
  return (report?.capabilities || []).find((capability) => capability.key === key) || null;
}

function capabilityStatusText(status) {
  switch (status) {
    case "enabled":
      return "已开启";
    case "supported":
      return "支持";
    case "disabled":
      return "未开启";
    case "partial":
      return "部分";
    case "warning":
      return "注意";
    case "unknown":
    default:
      return "-";
  }
}

function capabilityPillClass(status) {
  switch (status) {
    case "enabled":
    case "supported":
      return "pill-ok";
    case "partial":
    case "warning":
      return "pill-warn";
    case "disabled":
      return "pill-muted";
    default:
      return "pill-info";
  }
}

// renderTopics 把 Topic 列表按类型区分展示，并为每行保留路由和队列水位查询入口。
function renderTopics(payload) {
  state.lastTopicPayload = payload;
  const filteredTopics = filteredTopicRows();
  const visibleTopics = filteredTopics.slice(0, 120);
  const rows = visibleTopics.map((topic) => `
    <tr class="${state.selectedTopicName === topic.name ? "selected-row" : ""}">
      <td class="topic-name-cell wrap-cell" title="${escapeHTML(topic.name)}">${escapeHTML(topic.name)}</td>
      <td class="topic-kind-cell"><span class="tag tag-${escapeHTML(topic.kind)}">${escapeHTML(topic.kind)}</span></td>
      <td class="topic-select-cell">
        <div class="topic-actions">
          <button class="route-action" type="button" data-topic-select="${escapeAttr(topic.name)}" aria-label="选择 ${escapeAttr(topic.name)}">
            选择
          </button>
        </div>
      </td>
    </tr>
  `);
  $("#topicRows").innerHTML = rows.join("") || `<tr><td colspan="3">${escapeHTML(topicEmptyText())}</td></tr>`;
  renderTopicSearchCount(filteredTopics.length, visibleTopics.length);
  $("#topicStatus").textContent = snapshotStatusText(payload);
  $("#topicMetaPills").innerHTML = snapshotPills(payload);
  bindTopicSelectButtons();
  restoreSelectedTopicDetails();
  renderTopicMutationPanel();
  renderTopicMessageSendPanel();
}

// filteredTopicRows 根据搜索框内容过滤 Topic 名称和类型，保持列表选择流程不跳页。
function filteredTopicRows() {
  const keyword = normalizeSearchText(state.topicSearch);
  if (!keyword) {
    return state.topics;
  }
  return state.topics.filter((topic) => {
    const name = normalizeSearchText(topic.name);
    const kind = normalizeSearchText(topic.kind);
    return name.includes(keyword) || kind.includes(keyword);
  });
}

// renderTopicSearchCount 显示当前匹配数量，并提示列表最多渲染前 120 条以保持页面滚动稳定。
function renderTopicSearchCount(filteredCount, visibleCount) {
  const total = state.topics.length;
  const suffix = filteredCount > visibleCount ? `，显示前 ${visibleCount}` : "";
  $("#topicSearchCount").textContent = state.topicSearch.trim()
    ? `匹配 ${filteredCount} / ${total}${suffix}`
    : `共 ${total} 个${suffix}`;
}

function topicEmptyText() {
  return state.topicSearch.trim() ? "没有匹配的 Topic" : "暂无 Topic 数据";
}

function handleTopicSearchInput(event) {
  state.topicSearch = event.target.value || "";
  renderTopics(state.lastTopicPayload || { data: state.topics });
}

function clearTopicSearch() {
  state.topicSearch = "";
  $("#topicSearchInput").value = "";
  renderTopics(state.lastTopicPayload || { data: state.topics });
}

function normalizeSearchText(value) {
  return String(value || "").trim().toLowerCase();
}

// bindTopicSelectButtons 在 Topic 表格刷新后绑定选择按钮，并联动路由、水位和消息浏览。
function bindTopicSelectButtons() {
  $("#topicRows").querySelectorAll("[data-topic-select]").forEach((button) => {
    button.addEventListener("click", () => {
      selectTopic(button.dataset.topicSelect, button);
    });
  });
}

// restoreSelectedTopicDetails 在 Topic 列表刷新后恢复路由和状态按钮高亮。
function restoreSelectedTopicDetails() {
  if (state.selectedTopicName) {
    setActiveTopicButton(state.selectedTopicName);
  }
  if (state.selectedTopicRouteTopic) {
    setActiveRouteButton(state.selectedTopicRouteTopic);
  }
  if (state.selectedTopicStatusTopic) {
    setActiveStatusButton(state.selectedTopicStatusTopic);
  }
}

// renderTopicMutationPanel 负责把当前选中的 Topic、集群和 Broker 目标同步到写入表单。
function renderTopicMutationPanel() {
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }

  const clusters = state.clusters || [];
  const clusterSelect = form.elements.clusterName;
  const brokerSelect = form.elements.brokerAddr;
  const topicInput = form.elements.topic;
  const currentScope = state.topicMutationScope || "cluster";

  clusterSelect.innerHTML = renderTopicMutationClusterOptions(clusters);
  brokerSelect.innerHTML = renderTopicMutationBrokerOptions(clusters);

  syncTopicMutationField(topicInput, state.selectedTopicName || "", "topicMutationAutoTopic");

  const preferredCluster = preferredTopicMutationCluster(clusters);
  syncTopicMutationField(clusterSelect, preferredCluster, "topicMutationAutoCluster");

  const preferredBroker = preferredTopicMutationBroker(clusters, clusterSelect.value || preferredCluster);
  syncTopicMutationField(brokerSelect, preferredBroker, "topicMutationAutoBroker");

  applyTopicMutationScope(currentScope);
  renderTopicMutationSummary();
}

// renderTopicMutationClusterOptions 渲染 Topic 写入目标中的集群选项。
function renderTopicMutationClusterOptions(clusters) {
  const options = clusters.map((cluster) => `
    <option value="${escapeAttr(cluster.name)}">${escapeHTML(cluster.name)}</option>
  `).join("");
  return `<option value="">选择集群</option>${options}`;
}

// renderTopicMutationBrokerOptions 渲染 Topic 写入目标中的 Broker 选项，并保留集群上下文。
function renderTopicMutationBrokerOptions(clusters) {
  const options = [];
  for (const cluster of clusters || []) {
    for (const broker of cluster.brokers || []) {
      options.push(`
        <option value="${escapeAttr(broker.address || "")}" data-broker-cluster="${escapeAttr(cluster.name)}">
          ${escapeHTML(cluster.name)} / ${escapeHTML(broker.name || "-")} / ${escapeHTML(broker.address || "-")}
        </option>
      `);
    }
  }
  return `<option value="">选择 Broker</option>${options.join("")}`;
}

// preferredTopicMutationCluster 返回写入表单的默认集群。
function preferredTopicMutationCluster(clusters) {
  if (state.selectedClusterName) {
    return state.selectedClusterName;
  }
  return clusters?.[0]?.name || "";
}

// preferredTopicMutationBroker 返回写入表单的默认 Broker 地址。
function preferredTopicMutationBroker(clusters, clusterName) {
  const cluster = (clusters || []).find((item) => item.name === clusterName) || clusters?.[0];
  if (cluster) {
    const broker = (cluster.brokers || [])[0];
    if (broker?.address) {
      return broker.address;
    }
  }
  for (const item of clusters || []) {
    const broker = (item.brokers || [])[0];
    if (broker?.address) {
      return broker.address;
    }
  }
  return "";
}

// syncTopicMutationField 在字段仍保持系统自动值时，刷新默认值；用户已手动更改时保持原值。
function syncTopicMutationField(element, nextValue, autoKey) {
  if (!element) {
    return "";
  }
  const currentValue = String(element.value || "").trim();
  const lastAutoValue = String(state[autoKey] || "").trim();
  const nextTrimmed = String(nextValue || "").trim();
  if (!currentValue || currentValue === lastAutoValue) {
    element.value = nextTrimmed;
    state[autoKey] = nextTrimmed;
    return nextTrimmed;
  }
  state[autoKey] = currentValue;
  return currentValue;
}

// applyTopicMutationScope 切换 Topic 写入表单的目标模式。
function applyTopicMutationScope(scope) {
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }
  const nextScope = scope === "broker" ? "broker" : "cluster";
  state.topicMutationScope = nextScope;
  form.querySelectorAll("[data-topic-mutation-scope]").forEach((button) => {
    const active = button.dataset.topicMutationScope === nextScope;
    button.classList.toggle("active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
  form.elements.clusterName.disabled = nextScope !== "cluster";
  form.elements.brokerAddr.disabled = nextScope !== "broker";
  renderTopicMutationSummary();
}

// renderTopicMutationSummary 把当前写入目标压缩成一句摘要，便于快速确认会写到哪里。
function renderTopicMutationSummary() {
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }
  const clusterName = String(form.elements.clusterName.value || "").trim() || "-";
  const brokerAddr = String(form.elements.brokerAddr.value || "").trim() || "-";
  const topicName = String(form.elements.topic.value || "").trim() || state.selectedTopicName || "-";
  const target = state.topicMutationScope === "broker" ? brokerAddr : clusterName;
  const scopeLabel = state.topicMutationScope === "broker" ? "Broker 写入" : "集群写入";
  $("#topicMutationStatus").textContent = state.topicMutationNotice || `${scopeLabel} · ${target === "-" ? "等待选择" : target}`;
  $("#topicMutationSummary").innerHTML = `
    <div>
      <span>Topic</span>
      <strong>${escapeHTML(topicName)}</strong>
    </div>
    <div>
      <span>写入目标</span>
      <strong>${escapeHTML(target)}</strong>
    </div>
    <div>
      <span>Cluster</span>
      <strong>${escapeHTML(clusterName)}</strong>
    </div>
    <div>
      <span>Broker</span>
      <strong>${escapeHTML(brokerAddr)}</strong>
    </div>
  `;
}

// setTopicMutationTopic 用页面上当前选中的 Topic 预填写入表单。
function setTopicMutationTopic(topicName) {
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }
  form.elements.topic.value = String(topicName || "").trim();
  state.topicMutationAutoTopic = String(topicName || "").trim();
  renderTopicMutationSummary();
}

// setTopicMutationCluster 用页面上当前选中的集群预填写入表单。
function setTopicMutationCluster(clusterName) {
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }
  const nextCluster = String(clusterName || "").trim();
  if (nextCluster) {
    form.elements.clusterName.value = nextCluster;
    state.topicMutationAutoCluster = nextCluster;
    refreshTopicMutationBrokerFromCluster(nextCluster);
  }
  applyTopicMutationScope("cluster");
  renderTopicMutationSummary();
}

// renderTopicMutationPanel 后的辅助：把当前集群对应的第一个 Broker 补到表单里。
function refreshTopicMutationBrokerFromCluster(clusterName) {
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }
  const brokerAddr = preferredTopicMutationBroker(state.clusters, clusterName);
  if (brokerAddr) {
    form.elements.brokerAddr.value = brokerAddr;
    state.topicMutationAutoBroker = brokerAddr;
  }
}

// handleTopicMutationScopeClick 切换 Topic 写入目标模式，并同步禁用态。
function handleTopicMutationScopeClick(event) {
  const scope = event.currentTarget.dataset.topicMutationScope || "cluster";
  applyTopicMutationScope(scope);
  renderTopicMutationSummary();
}

// handleTopicMutationFieldChange 在用户修改写入表单后同步摘要和默认 Broker/Cluster。
function handleTopicMutationFieldChange(event) {
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }
  state.topicMutationDirty = true;
  state.topicMutationNotice = "";

  if (event.target.name === "clusterName") {
    const nextCluster = String(event.target.value || "").trim();
    state.topicMutationAutoCluster = nextCluster;
    refreshTopicMutationBrokerFromCluster(nextCluster);
    applyTopicMutationScope("cluster");
  }
  if (event.target.name === "brokerAddr") {
    const option = event.target.selectedOptions?.[0];
    const nextCluster = String(option?.dataset?.brokerCluster || "").trim();
    if (nextCluster) {
      form.elements.clusterName.value = nextCluster;
      state.topicMutationAutoCluster = nextCluster;
    }
    state.topicMutationAutoBroker = String(event.target.value || "").trim();
    applyTopicMutationScope("broker");
  }
  if (event.target.name === "topic") {
    state.topicMutationAutoTopic = String(event.target.value || "").trim();
  }

  renderTopicMutationSummary();
}

// topicMutationPayload 从表单提取 updateTopic 所需字段。
function topicMutationPayload(form) {
  const scope = state.topicMutationScope === "broker" ? "broker" : "cluster";
  const topic = String(form.elements.topic.value || "").trim();
  const clusterName = String(form.elements.clusterName.value || "").trim();
  const brokerAddr = String(form.elements.brokerAddr.value || "").trim();
  return {
    topic,
    clusterName: scope === "cluster" ? clusterName : "",
    brokerAddr: scope === "broker" ? brokerAddr : "",
    readQueueNums: Number(form.elements.readQueueNums.value || 0),
    writeQueueNums: Number(form.elements.writeQueueNums.value || 0),
    perm: Number(form.elements.perm.value || 0),
    order: Boolean(form.elements.order.checked),
    unit: Boolean(form.elements.unit.checked),
    hasUnitSub: Boolean(form.elements.hasUnitSub.checked),
    attributes: String(form.elements.attributes.value || "").trim()
  };
}

// topicDeletePayload 从表单提取 deleteTopic 所需字段。
function topicDeletePayload(form) {
  return {
    topic: String(form.elements.topic.value || "").trim(),
    clusterName: String(form.elements.clusterName.value || state.selectedClusterName || "").trim()
  };
}

// handleTopicMutationSubmit 提交 Topic 创建/更新请求，并在成功后刷新 Topic 列表和读缓存。
async function handleTopicMutationSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const button = $("#topicMutationSubmit");
  setLoading(button, true);
  $("#topicMutationStatus").textContent = "提交中";
  try {
    const payload = topicMutationPayload(form);
    if (!payload.topic) {
      throw new Error("topic 必填");
    }
    if (state.topicMutationScope === "broker") {
      if (!payload.brokerAddr) {
        throw new Error("brokerAddr 必填");
      }
    } else if (!payload.clusterName) {
      throw new Error("clusterName 必填");
    }
    const response = await fetchJSON("/api/topics", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const result = response.data || {};
    state.topicMutationNotice = `${result.operation || "upsertTopic"} 成功`;
    $("#topicMutationStatus").textContent = state.topicMutationNotice;
    state.topicMutationDirty = false;
    await loadSnapshots({ manageButton: false });
    await selectTopic(payload.topic);
  } catch (error) {
    $("#topicMutationStatus").textContent = error.message;
  } finally {
    setLoading(button, false);
    renderTopicMutationSummary();
  }
}

// handleTopicDelete 提交 Topic 删除请求，并在成功后清理当前 Topic 选择与缓存。
async function handleTopicDelete() {
  const form = $("#topicMutationForm");
  const button = $("#topicDeleteButton");
  setLoading(button, true);
  $("#topicMutationStatus").textContent = "删除中";
  try {
    const payload = topicDeletePayload(form);
    if (!payload.topic) {
      throw new Error("topic 必填");
    }
    if (!payload.clusterName) {
      throw new Error("clusterName 必填");
    }
    const response = await fetchJSON("/api/topics", {
      method: "DELETE",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const result = response.data || {};
    state.topicMutationNotice = `${result.operation || "deleteTopic"} 成功`;
    $("#topicMutationStatus").textContent = state.topicMutationNotice;
    state.topicMutationDirty = false;
    const deletedCurrentTopic = state.selectedTopicName === payload.topic;
    if (deletedCurrentTopic) {
      resetTopicSelectionView();
    }
    await loadSnapshots({ manageButton: false });
    renderTopicMutationPanel();
  } catch (error) {
    $("#topicMutationStatus").textContent = error.message;
  } finally {
    setLoading(button, false);
    renderTopicMutationSummary();
  }
}

// resetTopicSelectionView 在删除当前 Topic 后清理右侧详情区域，避免残留旧路由和旧消息。
function resetTopicSelectionView() {
  resetTopicRoutePoll();
  resetTopicStatusPoll();
  resetTopicMessagesPoll();
  state.selectedTopicName = "";
  state.selectedTopicRouteTopic = "";
  state.selectedTopicStatusTopic = "";
  state.selectedTopicMessagesTopic = "";
  $("#topicRouteStatus").textContent = "等待选择";
  $("#topicQueueStatus").textContent = "等待选择";
  $("#topicMessagesStatus").textContent = "等待选择";
  $("#topicRouteSummary").innerHTML = routeSummaryCells("-", "-", "-", "-");
  $("#topicQueueSummary").innerHTML = topicStatusSummaryCells("-", "-", "-", "-");
  $("#topicMessagesSummary").innerHTML = topicMessagesSummaryCells("-", "-", "-", "-", "-");
  $("#topicRouteRows").innerHTML = `<tr><td colspan="6">选择一个 Topic 查看路由。</td></tr>`;
  $("#topicQueueRows").innerHTML = `<tr><td colspan="6">选择一个 Topic 查看队列水位。</td></tr>`;
  $("#topicMessageRows").innerHTML = `<tr><td colspan="8">选择一个 Topic 浏览消息。</td></tr>`;
}

// resetTopicMutationPanel 清空 Topic 写入表单的自动值缓存，让切换集群后重新补默认值。
function resetTopicMutationPanel() {
  state.topicMutationScope = "cluster";
  state.topicMutationDirty = false;
  state.topicMutationNotice = "";
  state.topicMutationAutoTopic = "";
  state.topicMutationAutoCluster = "";
  state.topicMutationAutoBroker = "";
  const form = $("#topicMutationForm");
  if (!form) {
    return;
  }
  form.reset();
  applyTopicMutationScope("cluster");
  form.elements.topic.value = "";
  form.elements.clusterName.value = "";
  form.elements.brokerAddr.value = "";
  form.elements.readQueueNums.value = "8";
  form.elements.writeQueueNums.value = "8";
  form.elements.perm.value = "6";
  renderTopicMutationSummary();
}

// renderTopicMessageSendPanel 同步发送消息表单的 Topic 和 Broker 选项，和当前选择保持一致。
function renderTopicMessageSendPanel() {
  const form = $("#topicMessageSendForm");
  if (!form) {
    return;
  }
  const brokerSelect = form.elements.brokerName;
  brokerSelect.innerHTML = renderTopicMessageBrokerOptions(state.clusters);
  syncTopicMessageSendField(form.elements.topic, state.selectedTopicName || "", "topicMessageSendAutoTopic");
  syncTopicMessageSendField(brokerSelect, "", "topicMessageSendAutoBroker");
  if (!state.lastTopicMessageSendResult) {
    $("#topicMessageSendStatus").textContent = form.elements.topic.value ? "等待发送" : "等待选择";
  }
}

// renderTopicMessageBrokerOptions 生成 sendMessage -b 可选 Broker 名称列表。
function renderTopicMessageBrokerOptions(clusters) {
  const seen = new Set();
  const options = [];
  for (const cluster of clusters || []) {
    for (const broker of cluster.brokers || []) {
      if (!broker.name || seen.has(broker.name)) {
        continue;
      }
      seen.add(broker.name);
      options.push(`
        <option value="${escapeAttr(broker.name)}">
          ${escapeHTML(cluster.name || broker.cluster || "-")} / ${escapeHTML(broker.name)}
        </option>
      `);
    }
  }
  return `<option value="">自动路由</option>${options.join("")}`;
}

// syncTopicMessageSendField 只覆盖仍处于自动同步状态的字段，避免用户输入被快照刷新抹掉。
function syncTopicMessageSendField(element, nextValue, autoKey) {
  if (!element) {
    return;
  }
  const currentValue = String(element.value || "").trim();
  const lastAutoValue = String(state[autoKey] || "").trim();
  const nextTrimmed = String(nextValue || "").trim();
  if (!currentValue || currentValue === lastAutoValue) {
    element.value = nextTrimmed;
    state[autoKey] = nextTrimmed;
    return;
  }
  state[autoKey] = currentValue;
}

// topicMessageSendPayload 从发送表单生成后端 /api/topic-messages/send 请求体。
function topicMessageSendPayload(form) {
  const payload = {
    topic: String(form.elements.topic.value || "").trim(),
    body: String(form.elements.body.value || ""),
    keys: String(form.elements.keys.value || "").trim(),
    tags: String(form.elements.tags.value || "").trim(),
    brokerName: String(form.elements.brokerName.value || "").trim(),
    traceEnable: Boolean(form.elements.traceEnable.checked)
  };
  const queueID = optionalNonNegativeInteger(form.elements.queueId.value, "queueId");
  if (queueID !== null) {
    payload.queueId = queueID;
  }
  return payload;
}

// handleTopicMessageSendSubmit 调用官方 sendMessage 后端封装，并把返回 messageId 接到链路查询。
async function handleTopicMessageSendSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const button = $("#topicMessageSendButton");
  setLoading(button, true);
  $("#topicMessageSendStatus").textContent = "发送中";
  $("#topicMessageSendChainButton").disabled = true;
  try {
    const payload = topicMessageSendPayload(form);
    if (!payload.topic) {
      throw new Error("topic 必填");
    }
    if (!payload.body.trim()) {
      throw new Error("body 必填");
    }
    const response = await fetchJSON("/api/topic-messages/send", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const result = response.data || {};
    state.lastTopicMessageSendResult = { ...result, keys: payload.keys };
    $("#topicMessageSendStatus").textContent = `发送成功 · ${response.latencyMillis ?? 0} ms`;
    renderTopicMessageSendResult(result);
    $("#topicMessageSendChainButton").disabled = !result.messageId;
    if (payload.topic) {
      state.topicMessagesPollCount = 0;
      await loadTopicMessages(payload.topic, { force: true });
    }
  } catch (error) {
    state.lastTopicMessageSendResult = null;
    $("#topicMessageSendStatus").textContent = error.message;
    renderTopicMessageSendError(error.message);
  } finally {
    setLoading(button, false);
  }
}

// handleTopicMessageSendFieldChange 记录用户手动调整，避免下一次快照刷新覆盖发送目标。
function handleTopicMessageSendFieldChange(event) {
  state.lastTopicMessageSendResult = null;
  $("#topicMessageSendChainButton").disabled = true;
  if (event.target.name === "topic") {
    state.topicMessageSendAutoTopic = String(event.target.value || "").trim();
  }
  if (event.target.name === "brokerName") {
    state.topicMessageSendAutoBroker = String(event.target.value || "").trim();
  }
  if ($("#topicMessageSendStatus").textContent.startsWith("发送成功")) {
    $("#topicMessageSendStatus").textContent = "已修改，等待发送";
  }
}

// renderTopicMessageSendResult 展示 sendMessage 返回的关键字段，messageId 是后续链路入口。
function renderTopicMessageSendResult(result) {
  $("#topicMessageSendResult").innerHTML = `
    <div>
      <span>Message ID</span>
      <strong class="mono wrap-cell">${escapeHTML(result.messageId || "-")}</strong>
    </div>
    <div>
      <span>发送状态</span>
      <strong>${escapeHTML(result.sendStatus || "-")}</strong>
    </div>
    <div>
      <span>Broker</span>
      <strong>${escapeHTML(result.brokerName || "-")}</strong>
    </div>
    <div>
      <span>队列</span>
      <strong>${escapeHTML(result.queueId ?? "-")}</strong>
    </div>
  `;
}

// renderTopicMessageSendError 保持发送失败时的结果区结构稳定。
function renderTopicMessageSendError(message) {
  $("#topicMessageSendResult").innerHTML = `
    <div>
      <span>Message ID</span>
      <strong>-</strong>
    </div>
    <div>
      <span>发送状态</span>
      <strong>失败</strong>
    </div>
    <div>
      <span>Broker</span>
      <strong>-</strong>
    </div>
    <div>
      <span>错误</span>
      <strong>${escapeHTML(message)}</strong>
    </div>
  `;
}

// openChainForSentMessage 使用 sendMessage 返回的 messageId 直接打开链路页，避免用户手动复制。
async function openChainForSentMessage() {
  const result = state.lastTopicMessageSendResult;
  if (!result?.messageId || !result?.topic) {
    return;
  }
  await openChainForTopicMessage({
    topic: result.topic,
    messageId: result.messageId,
    keys: result.keys ? [result.keys] : [],
    brokerName: result.brokerName,
    queueId: result.queueId,
    queueOffset: "-"
  });
}

async function selectTopic(topic, button = null) {
  const topicName = String(topic || "").trim();
  if (!topicName) {
    return;
  }
  state.selectedTopicName = topicName;
  setTopicMutationTopic(topicName);
  renderTopicMessageSendPanel();
  setSubtab("topic", "topic-messages-panel");
  setActiveTopicButton(topicName);
  if (button) {
    setLoading(button, true);
  }
  try {
    await Promise.all([
      loadTopicRoute(topicName, null),
      loadTopicStatus(topicName, null),
      loadTopicMessages(topicName)
    ]);
  } finally {
    if (button) {
      setLoading(button, false);
    }
  }
}

// loadTopicRoute 读取指定 Topic 的路由快照；无缓存时由后端异步刷新并由前端短轮询。
async function loadTopicRoute(topic, button = null, options = {}) {
  const topicName = String(topic || "").trim();
  if (!topicName) {
    return;
  }
  if (state.selectedTopicRouteTopic !== topicName) {
    resetTopicRoutePoll();
  }
  state.selectedTopicRouteTopic = topicName;
  setActiveRouteButton(topicName);
  if (button) {
    setLoading(button, true);
  }
  if (!options.quiet) {
    $("#topicRouteStatus").textContent = "读取缓存";
    $("#topicRouteSummary").innerHTML = routeSummaryCells(topicName, "-", "-", "-");
    $("#topicRouteRows").innerHTML = `<tr><td colspan="6">正在检查 ${escapeHTML(topicName)} 的路由缓存。</td></tr>`;
  }
  try {
    const params = new URLSearchParams({ topic: topicName });
    if (options.force) {
      params.set("refresh", "true");
    }
    const payload = await fetchJSON(`/api/topic-route?${params.toString()}`);
    state.lastTopicRoutePayload = payload;
    renderTopicRoute(payload);
    if (payload.refreshing) {
      scheduleTopicRoutePoll(topicName);
    } else {
      state.topicRoutePollCount = 0;
    }
  } catch (error) {
    $("#topicRouteStatus").textContent = "失败";
    $("#topicRouteSummary").innerHTML = routeSummaryCells(topicName, "-", "-", "-");
    $("#topicRouteRows").innerHTML = `<tr><td colspan="6">${escapeHTML(error.message)}</td></tr>`;
  } finally {
    if (button) {
      setLoading(button, false);
    }
  }
}

// loadTopicStatus 读取指定 Topic 的队列水位快照；冷命令由后端后台刷新，前端只展示缓存状态。
async function loadTopicStatus(topic, button = null, options = {}) {
  const topicName = String(topic || "").trim();
  if (!topicName) {
    return;
  }
  if (state.selectedTopicStatusTopic !== topicName) {
    resetTopicStatusPoll();
  }
  state.selectedTopicStatusTopic = topicName;
  setActiveStatusButton(topicName);
  if (button) {
    setLoading(button, true);
  }
  if (!options.quiet) {
    $("#topicQueueStatus").textContent = "读取缓存";
    $("#topicQueueSummary").innerHTML = topicStatusSummaryCells(topicName, "-", "-", "-");
    $("#topicQueueRows").innerHTML = `<tr><td colspan="6">正在检查 ${escapeHTML(topicName)} 的队列水位缓存。</td></tr>`;
  }
  try {
    const params = new URLSearchParams({ topic: topicName });
    if (options.force) {
      params.set("refresh", "true");
    }
    const payload = await fetchJSON(`/api/topic-status?${params.toString()}`);
    state.lastTopicStatusPayload = payload;
    renderTopicStatus(payload);
    if (payload.refreshing) {
      scheduleTopicStatusPoll(topicName);
    } else {
      state.topicStatusPollCount = 0;
    }
  } catch (error) {
    $("#topicQueueStatus").textContent = "失败";
    $("#topicQueueSummary").innerHTML = topicStatusSummaryCells(topicName, "-", "-", "-");
    $("#topicQueueRows").innerHTML = `<tr><td colspan="6">${escapeHTML(error.message)}</td></tr>`;
  } finally {
    if (button) {
      setLoading(button, false);
    }
  }
}

// topicRouteTableRows 将路由里的队列和纯 broker 信息合并成可折行表格行。
function topicRouteTableRows(route) {
  const queues = route.queues || [];
  const brokers = route.brokers || [];
  const brokerMap = new Map(brokers.map((broker) => [broker.brokerName, broker]));
  const queueRows = queues.map((queue) => {
    const broker = brokerMap.get(queue.brokerName) || {};
    return `
      <tr>
        ${tableCell("Broker", escapeHTML(queue.brokerName || "-"), "wrap-cell")}
        ${tableCell("集群", escapeHTML(broker.cluster || "-"), "wrap-cell")}
        ${tableCell("地址", escapeHTML(formatBrokerAddrs(broker.addrs)), "mono route-addresses")}
        ${tableCell("Read", escapeHTML(queue.readQueueNums ?? "-"), "mono")}
        ${tableCell("Write", escapeHTML(queue.writeQueueNums ?? "-"), "mono")}
        ${tableCell("权限", `<span class="pill ${queue.permissionLabel === "RW" ? "pill-ok" : "pill-warn"}">${escapeHTML(queue.permissionLabel || queue.perm || "-")}</span>`)}
      </tr>
    `;
  });
  const brokerOnlyRows = brokers
    .filter((broker) => !queues.some((queue) => queue.brokerName === broker.brokerName))
    .map((broker) => `
      <tr>
        ${tableCell("Broker", escapeHTML(broker.brokerName || "-"), "wrap-cell")}
        ${tableCell("集群", escapeHTML(broker.cluster || "-"), "wrap-cell")}
        ${tableCell("地址", escapeHTML(formatBrokerAddrs(broker.addrs)), "mono route-addresses")}
        ${tableCell("Read", "-")}
        ${tableCell("Write", "-")}
        ${tableCell("权限", `<span class="pill pill-muted">-</span>`)}
      </tr>
    `);
  return queueRows.concat(brokerOnlyRows).join("");
}

function topicRouteTableHTML(route, emptyText = "未返回路由数据。") {
  return dataTableHTML(["Broker", "集群", "地址", "Read", "Write", "权限"], topicRouteTableRows(route), emptyText);
}

// renderTopicRoute 将 mqadmin topicRoute 结果拆成摘要和 broker 队列表，便于排障时快速确认路由。
function renderTopicRoute(payload) {
  const route = payload.data || {};
  const queues = route.queues || [];
  const brokers = route.brokers || [];
  if (!payload.hasData) {
    renderTopicRoutePending(payload, route);
    return;
  }

  const statusParts = ["已加载"];
  if (payload.refreshing) {
    statusParts.push("后台刷新中");
  }
  if (payload.stale) {
    statusParts.push("过期缓存");
  }
  if (payload.lastError) {
    statusParts.push("上次刷新失败");
  }
  statusParts.push(`${payload.latencyMillis} ms`);
  $("#topicRouteStatus").textContent = statusParts.join(" · ");
  $("#topicRouteSummary").innerHTML = routeSummaryCells(
    route.topic || "-",
    `${route.totalReadQueues ?? 0} / ${route.totalWriteQueues ?? 0}`,
    `${queues.length}`,
    `${brokers.length}`
  );
  $("#topicRouteRows").innerHTML = topicRouteTableRows(route) || `<tr><td colspan="6">未返回路由数据。</td></tr>`;
}

// renderTopicRoutePending 展示无缓存时的异步刷新状态，避免 Topic 路由冷命令阻塞页面。
function renderTopicRoutePending(payload, route) {
  const status = payload.refreshing ? "后台刷新中" : (payload.lastError ? "刷新失败" : "无缓存");
  const topic = route.topic || state.selectedTopicRouteTopic || "-";
  $("#topicRouteStatus").textContent = `${status} · ${payload.latencyMillis} ms`;
  $("#topicRouteSummary").innerHTML = routeSummaryCells(topic, "-", "-", "-");
  const message = payload.refreshing ? "路由后台刷新中，完成后自动更新。" : (payload.lastError || "暂无路由缓存。");
  $("#topicRouteRows").innerHTML = `<tr><td colspan="6">${escapeHTML(message)}</td></tr>`;
}

// topicStatusTableRows 输出队列水位明细，数值列保持等宽并允许长时间字段换行。
function topicStatusTableRows(rows) {
  return rows.map((row) => `
    <tr>
      ${tableCell("Broker", escapeHTML(row.brokerName || "-"), "wrap-cell")}
      ${tableCell("QID", escapeHTML(row.queueId ?? "-"), "mono")}
      ${tableCell("最小位点", escapeHTML(row.minOffset ?? "-"), "mono")}
      ${tableCell("最大位点", escapeHTML(row.maxOffset ?? "-"), "mono")}
      ${tableCell("消息数", escapeHTML(row.messageCount ?? 0), `${row.messageCount > 0 ? "good-text" : "muted-text"} mono`)}
      ${tableCell("最后写入时间", escapeHTML(row.lastUpdated || "-"), "mono wrap-cell")}
    </tr>
  `).join("");
}

function topicStatusTableHTML(status, emptyText = "未返回队列水位。") {
  return dataTableHTML(["Broker", "QID", "最小位点", "最大位点", "消息数", "最后写入时间"], topicStatusTableRows(status.rows || []), emptyText);
}

// renderTopicStatus 将 mqadmin topicStatus 结果拆成摘要和队列水位表，贴近原版 Topic 状态视图。
function renderTopicStatus(payload) {
  const status = payload.data || {};
  const rows = status.rows || [];
  if (!payload.hasData) {
    renderTopicStatusPending(payload, status);
    return;
  }

  const statusParts = ["已加载"];
  if (payload.refreshing) {
    statusParts.push("后台刷新中");
  }
  if (payload.stale) {
    statusParts.push("过期缓存");
  }
  if (payload.lastError) {
    statusParts.push("上次刷新失败");
  }
  statusParts.push(`${payload.latencyMillis} ms`);
  $("#topicQueueStatus").textContent = statusParts.join(" · ");
  $("#topicQueueSummary").innerHTML = topicStatusSummaryCells(
    status.topic || "-",
    `${status.totalQueues ?? rows.length} / ${status.totalMessageCount ?? 0}`,
    `${status.minOffsetTotal ?? 0} / ${status.maxOffsetTotal ?? 0}`,
    `${rows.filter((row) => row.lastUpdated).length}`
  );
  $("#topicQueueRows").innerHTML = topicStatusTableRows(rows) || `<tr><td colspan="6">未返回队列水位。</td></tr>`;
}

// renderTopicStatusPending 展示无缓存时的异步刷新状态，避免 topicStatus 冷命令阻塞页面。
function renderTopicStatusPending(payload, status) {
  const label = payload.refreshing ? "后台刷新中" : (payload.lastError ? "刷新失败" : "无缓存");
  const topic = status.topic || state.selectedTopicStatusTopic || "-";
  $("#topicQueueStatus").textContent = `${label} · ${payload.latencyMillis} ms`;
  $("#topicQueueSummary").innerHTML = topicStatusSummaryCells(topic, "-", "-", "-");
  const message = payload.refreshing ? "队列水位后台刷新中，完成后自动更新。" : (payload.lastError || "暂无队列水位缓存。");
  $("#topicQueueRows").innerHTML = `<tr><td colspan="6">${escapeHTML(message)}</td></tr>`;
}

// scheduleTopicRoutePoll 在 Topic 路由后台刷新期间短轮询当前 Topic，避免旧请求覆盖新选择。
function scheduleTopicRoutePoll(topicName) {
  if (state.topicRoutePollTimer || state.topicRoutePollCount >= 45) {
    return;
  }
  state.topicRoutePollCount += 1;
  state.topicRoutePollTimer = window.setTimeout(async () => {
    state.topicRoutePollTimer = null;
    await loadTopicRoute(topicName, null, { quiet: true });
  }, 1500);
}

// scheduleTopicStatusPoll 在 Topic 状态后台刷新期间短轮询当前 Topic，避免用户等待 mqadmin。
function scheduleTopicStatusPoll(topicName) {
  if (state.topicStatusPollTimer || state.topicStatusPollCount >= 45) {
    return;
  }
  state.topicStatusPollCount += 1;
  state.topicStatusPollTimer = window.setTimeout(async () => {
    state.topicStatusPollTimer = null;
    await loadTopicStatus(topicName, null, { quiet: true });
  }, 1500);
}

// resetTopicRoutePoll 切换 Topic 时停止上一条路由的轮询，保持右侧面板只反映当前选择。
function resetTopicRoutePoll() {
  if (state.topicRoutePollTimer) {
    window.clearTimeout(state.topicRoutePollTimer);
    state.topicRoutePollTimer = null;
  }
  state.topicRoutePollCount = 0;
}

// resetTopicStatusPoll 切换 Topic 时停止上一条队列水位轮询，防止旧结果覆盖新选择。
function resetTopicStatusPoll() {
  if (state.topicStatusPollTimer) {
    window.clearTimeout(state.topicStatusPollTimer);
    state.topicStatusPollTimer = null;
  }
  state.topicStatusPollCount = 0;
}

// topicMessagesTableRows 将消息浏览结果做成窄屏可读的卡片行，并保留链路跳转动作。
function topicMessagesTableRows(rows) {
  return rows.map((message, index) => `
    <tr>
      ${tableCell("Message ID", escapeHTML(message.messageId || "-"), "mono wrap-cell")}
      ${tableCell("Keys", escapeHTML((message.keys || []).join(", ") || "-"), "wrap-cell")}
      ${tableCell("Broker", escapeHTML(message.brokerName || "-"), "wrap-cell")}
      ${tableCell("队列", escapeHTML(message.queueId ?? "-"), "mono")}
      ${tableCell("Offset", escapeHTML(message.queueOffset ?? "-"), "mono")}
      ${tableCell("存储时间", escapeHTML(formatTime(message.storeTimestamp)), "mono wrap-cell")}
      ${tableCell("Body", escapeHTML(message.bodyPreview || "-"), "wrap-cell")}
      ${tableCell("链路", `
        <button class="route-action" type="button" data-message-index="${index}">
          链路
        </button>
      `)}
    </tr>
  `).join("");
}

function topicMessagesTableHTML(result, emptyText = "未回查到可展示消息。") {
  return dataTableHTML(["Message ID", "Keys", "Broker", "队列", "Offset", "存储时间", "Body", "链路"], topicMessagesTableRows(result.rows || []), emptyText);
}

function bindTopicMessageButtons(container, rows) {
  container.querySelectorAll("[data-message-index]").forEach((button) => {
    button.addEventListener("click", () => {
      const message = rows[Number(button.dataset.messageIndex)];
      openChainForTopicMessage(message);
      closeDetailDialog();
    });
  });
}

async function loadTopicMessages(topic, options = {}) {
  const topicName = String(topic || "").trim();
  if (!topicName) {
    return;
  }
  if (state.selectedTopicMessagesTopic !== topicName) {
    resetTopicMessagesPoll();
  }
  state.selectedTopicMessagesTopic = topicName;
  const limit = Math.max(1, Math.min(24, Number($("#topicMessageLimit")?.value || 12)));
  if (!options.quiet) {
    $("#topicMessagesStatus").textContent = "读取缓存";
    $("#topicMessagesSummary").innerHTML = topicMessagesSummaryCells(topicName, "-", "-", "-", "-");
    $("#topicMessageRows").innerHTML = `<tr><td colspan="8">正在检查 ${escapeHTML(topicName)} 的消息缓存。</td></tr>`;
  }
  const params = new URLSearchParams({ topic: topicName, limit: String(limit) });
  if (options.force) {
    params.set("refresh", "true");
  }
  try {
    const payload = await fetchJSON(`/api/topic-messages?${params.toString()}`);
    state.lastTopicMessagesPayload = payload;
    renderTopicMessages(payload);
    if (payload.refreshing) {
      scheduleTopicMessagesPoll(topicName);
    } else {
      state.topicMessagesPollCount = 0;
    }
  } catch (error) {
    $("#topicMessagesStatus").textContent = "失败";
    $("#topicMessagesSummary").innerHTML = topicMessagesSummaryCells(topicName, "-", "-", "-", "-");
    $("#topicMessageRows").innerHTML = `<tr><td colspan="8">${escapeHTML(error.message)}</td></tr>`;
  }
}

function renderTopicMessages(payload) {
  const result = payload.data || {};
  const rows = result.rows || [];
  if (!payload.hasData) {
    renderTopicMessagesPending(payload, result);
    return;
  }

  const statusParts = ["已加载"];
  if (payload.refreshing) {
    statusParts.push("后台刷新中");
  }
  if (payload.stale) {
    statusParts.push("过期缓存");
  }
  if (payload.lastError) {
    statusParts.push("上次刷新失败");
  }
  statusParts.push(`${payload.latencyMillis} ms`);
  $("#topicMessagesStatus").textContent = statusParts.join(" · ");
  $("#topicMessagesSummary").innerHTML = topicMessagesSummaryCells(
    result.topic || "-",
    `${rows.length} / ${result.scannedOffsets ?? 0}`,
    `${result.limit ?? "-"}`,
    `${result.fetchedOffsets ?? 0} / ${result.reusedOffsets ?? 0}`,
    `${(result.warnings || []).length}`
  );
  $("#topicMessageRows").innerHTML = topicMessagesTableRows(rows) || `<tr><td colspan="8">未回查到可展示消息。</td></tr>`;
  bindTopicMessageButtons($("#topicMessageRows"), rows);
}

function renderTopicMessagesPending(payload, result) {
  const label = payload.refreshing ? "后台刷新中" : (payload.lastError ? "刷新失败" : "无缓存");
  const topic = result.topic || state.selectedTopicMessagesTopic || "-";
  $("#topicMessagesStatus").textContent = `${label} · ${payload.latencyMillis} ms`;
  $("#topicMessagesSummary").innerHTML = topicMessagesSummaryCells(topic, "-", "-", "-", "-");
  const message = payload.refreshing ? "消息后台刷新中，完成后自动更新。" : (payload.lastError || "暂无消息缓存。");
  $("#topicMessageRows").innerHTML = `<tr><td colspan="8">${escapeHTML(message)}</td></tr>`;
}

function scheduleTopicMessagesPoll(topicName) {
  if (state.topicMessagesPollTimer || state.topicMessagesPollCount >= 45) {
    return;
  }
  state.topicMessagesPollCount += 1;
  state.topicMessagesPollTimer = window.setTimeout(async () => {
    state.topicMessagesPollTimer = null;
    await loadTopicMessages(topicName, { quiet: true });
  }, 1500);
}

function resetTopicMessagesPoll() {
  if (state.topicMessagesPollTimer) {
    window.clearTimeout(state.topicMessagesPollTimer);
    state.topicMessagesPollTimer = null;
  }
  state.topicMessagesPollCount = 0;
}

async function openChainForTopicMessage(message) {
  if (!message || !message.topic || !message.messageId) {
    return;
  }
  const form = $("#chainForm");
  form.elements.topic.value = message.topic;
  form.elements.messageId.value = message.messageId;
  form.elements.key.value = (message.keys || [])[0] || "";
  const params = new URLSearchParams({
    topic: message.topic,
    messageId: message.messageId
  });
  if ((message.keys || [])[0]) {
    params.set("key", message.keys[0]);
  }
  if (message.brokerName && message.queueId !== undefined && message.queueId !== null && message.queueOffset !== undefined && message.queueOffset !== null && message.queueOffset !== "-") {
    params.set("brokerName", message.brokerName);
    params.set("queueId", String(message.queueId));
    params.set("queueOffset", String(message.queueOffset));
  }
  if (message.storeTimestamp && Number.isFinite(Number(message.storeTimestamp))) {
    const storeTimestamp = Number(message.storeTimestamp);
    const windowMillis = 30 * 60 * 1000;
    params.set("beginTimestamp", String(storeTimestamp - windowMillis));
    params.set("endTimestamp", String(storeTimestamp + windowMillis));
  }
  const queryString = params.toString();
  if (state.lastChainQueryString !== queryString) {
    resetChainPoll();
  }
  state.lastChainQueryString = queryString;
  $("#chainSource").textContent = `${message.topic} · ${message.brokerName || "-"} · Q${message.queueId ?? "-"} / ${message.queueOffset ?? "-"}`;
  setActiveTab("messages");
  $("#chainStatus").textContent = "查询中";
  $("#chainSummaryStatus").textContent = "查询中";
  $("#timelineHint").textContent = "查询中";
  try {
    await loadMessageChain(queryString, { force: true });
  } catch (error) {
    $("#messageSummary").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
    $("#candidateList").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
    $("#timeline").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
    $("#chainStatus").textContent = "失败";
    $("#chainSummaryStatus").textContent = "失败";
    $("#timelineHint").textContent = "失败";
  }
}

function setActiveTopicButton(topicName) {
  $("#topicRows").querySelectorAll("[data-topic-select]").forEach((button) => {
    const active = button.dataset.topicSelect === topicName;
    button.classList.toggle("active", active);
    button.closest("tr")?.classList.toggle("selected-row", active);
  });
}

// setActiveRouteButton 高亮当前路由查询目标，避免用户在长 Topic 表里失去上下文。
function setActiveRouteButton(topicName) {
  $("#topicRows").querySelectorAll("[data-topic-route]").forEach((button) => {
    button.classList.toggle("active", button.dataset.topicRoute === topicName);
  });
}

// setActiveStatusButton 高亮当前队列水位查询目标，和路由按钮互不覆盖。
function setActiveStatusButton(topicName) {
  $("#topicRows").querySelectorAll("[data-topic-status]").forEach((button) => {
    button.classList.toggle("active", button.dataset.topicStatus === topicName);
  });
}

// routeSummaryCells 生成路由摘要单元，保持加载、成功和失败态 DOM 结构一致。
function routeSummaryCells(topic, queues, queueRows, brokers) {
  return `
    <div>
      <span>Topic</span>
      <strong>${escapeHTML(topic)}</strong>
    </div>
    <div>
      <span>读 / 写队列</span>
      <strong>${escapeHTML(queues)}</strong>
    </div>
    <div>
      <span>路由行</span>
      <strong>${escapeHTML(queueRows)}</strong>
    </div>
    <div>
      <span>Broker 数</span>
      <strong>${escapeHTML(brokers)}</strong>
    </div>
  `;
}

// topicStatusSummaryCells 生成队列水位摘要单元，保持加载、成功和失败态结构稳定。
function topicStatusSummaryCells(topic, queueAndMessages, offsets, updatedRows) {
  return `
    <div>
      <span>Topic</span>
      <strong>${escapeHTML(topic)}</strong>
    </div>
    <div>
      <span>队列 / 消息数</span>
      <strong>${escapeHTML(queueAndMessages)}</strong>
    </div>
    <div>
      <span>最小 / 最大位点</span>
      <strong>${escapeHTML(offsets)}</strong>
    </div>
    <div>
      <span>有写入时间队列</span>
      <strong>${escapeHTML(updatedRows)}</strong>
    </div>
  `;
}

function topicMessagesSummaryCells(topic, messagesAndOffsets, limit, fetchedAndReused, warningCount) {
  return `
    <div>
      <span>Topic</span>
      <strong>${escapeHTML(topic)}</strong>
    </div>
    <div>
      <span>消息 / 位点</span>
      <strong>${escapeHTML(messagesAndOffsets)}</strong>
    </div>
    <div>
      <span>Limit</span>
      <strong>${escapeHTML(limit)}</strong>
    </div>
    <div>
      <span>新拉 / 复用</span>
      <strong>${escapeHTML(fetchedAndReused)}</strong>
    </div>
    <div>
      <span>Warnings</span>
      <strong>${escapeHTML(warningCount)}</strong>
    </div>
  `;
}

// formatBrokerAddrs 将 brokerId 到地址的映射排序展示，方便核对 master/slave 地址。
function formatBrokerAddrs(addrs) {
  const entries = Object.entries(addrs || {}).sort(([left], [right]) => Number(left) - Number(right));
  if (!entries.length) {
    return "-";
  }
  return entries.map(([id, address]) => `${id}: ${address}`).join(" · ");
}

// renderConsumers 把在线状态和堆积量并列展示，方便一眼锁定异常消费组。
function renderConsumers(payload) {
  state.lastConsumerPayload = payload;
  const filteredConsumers = filteredConsumerRows();
  const visibleConsumers = filteredConsumers.slice(0, 160);
  const rows = visibleConsumers.map((consumer) => `
    <tr>
      <td class="consumer-name-cell" title="${escapeHTML(consumer.name)}">${escapeHTML(consumer.name)}</td>
      <td class="consumer-status-cell"><span class="status-dot ${consumer.online ? "status-on" : "status-off"}"></span>${consumer.online ? "online" : "offline"}</td>
      <td class="consumer-version-cell">${escapeHTML(consumer.version || "-")}</td>
      <td class="consumer-model-cell">${escapeHTML(consumer.model || "-")}</td>
      <td class="consumer-lag-cell ${consumer.diffTotal > 0 ? "warn-text" : "good-text"} mono">${escapeHTML(consumer.diffTotal ?? 0)}</td>
      <td class="consumer-action-cell">
        <button class="detail-action" type="button" data-consumer-group="${escapeAttr(consumer.name)}" aria-label="查看 ${escapeAttr(consumer.name)} 的消费者详情">
          详情
        </button>
      </td>
    </tr>
  `);
  $("#consumerRows").innerHTML = rows.join("") || `<tr><td colspan="6">${escapeHTML(consumerEmptyText())}</td></tr>`;
  renderConsumerSearchCount(filteredConsumers.length, visibleConsumers.length);
  $("#consumerStatus").textContent = snapshotStatusText(payload);
  $("#consumerMetaPills").innerHTML = snapshotPills(payload);
  bindConsumerDetailButtons();
  restoreSelectedConsumerDetail();
}

// filteredConsumerRows 根据关键词过滤消费者组、在线状态、版本、消费模式和堆积量。
function filteredConsumerRows() {
  const keyword = normalizeSearchText(state.consumerSearch);
  if (!keyword) {
    return state.consumers;
  }
  return state.consumers.filter((consumer) => {
    const searchable = [
      consumer.name,
      consumer.online ? "online 在线" : "offline 离线",
      consumer.version,
      consumer.model,
      consumer.diffTotal
    ].map(normalizeSearchText).join(" ");
    return searchable.includes(keyword);
  });
}

// renderConsumerSearchCount 显示 Consumer 匹配数，列表很大时只渲染前 160 条以保持滚动稳定。
function renderConsumerSearchCount(filteredCount, visibleCount) {
  const total = state.consumers.length;
  const suffix = filteredCount > visibleCount ? `，显示前 ${visibleCount}` : "";
  $("#consumerSearchCount").textContent = state.consumerSearch.trim()
    ? `匹配 ${filteredCount} / ${total}${suffix}`
    : `共 ${total} 个${suffix}`;
}

function consumerEmptyText() {
  return state.consumerSearch.trim() ? "没有匹配的 Consumer" : "暂无 Consumer 数据";
}

function handleConsumerSearchInput(event) {
  state.consumerSearch = event.target.value || "";
  renderConsumers(state.lastConsumerPayload || { data: state.consumers });
}

function clearConsumerSearch() {
  state.consumerSearch = "";
  $("#consumerSearchInput").value = "";
  renderConsumers(state.lastConsumerPayload || { data: state.consumers });
}

// bindConsumerDetailButtons 在 Consumer 表格刷新后绑定详情按钮，保持接口业务参数走 query string。
function bindConsumerDetailButtons() {
  $("#consumerRows").querySelectorAll("[data-consumer-group]").forEach((button) => {
    button.addEventListener("click", () => {
      loadConsumerDetail(button.dataset.consumerGroup, "", button);
    });
  });
}

// restoreSelectedConsumerDetail 在快照追读刷新列表后恢复当前选中的消费者组高亮。
function restoreSelectedConsumerDetail() {
  if (!state.selectedConsumerGroup) {
    return;
  }
  setActiveConsumerButton(state.selectedConsumerGroup);
}

// loadConsumerDetail 读取消费者详情快照；无缓存时后端会异步刷新，前端只短轮询不阻塞按钮。
async function loadConsumerDetail(group, topic = "", button = null, options = {}) {
  const groupName = String(group || "").trim();
  const topicName = String(topic || "").trim();
  if (!groupName) {
    return;
  }

  const sameSelection = state.selectedConsumerGroup === groupName && state.selectedConsumerTopic === topicName;
  if (!sameSelection) {
    resetConsumerDetailPoll();
    setSubtab("consumer", "consumer-summary-panel");
  }
  state.selectedConsumerGroup = groupName;
  state.selectedConsumerTopic = topicName;
  syncConsumerOffsetResetForm({ group: groupName, topic: topicName });
  setActiveConsumerButton(groupName);
  if (button) {
    setLoading(button, true);
  }
  if (!options.quiet) {
    $("#consumerDetailStatus").textContent = "读取缓存";
    $("#consumerDetailSummary").innerHTML = consumerDetailSummaryCells(groupName, topicName || "自动选择", "-", "-");
    $("#consumerConnectionRows").innerHTML = `<tr><td colspan="4">正在检查 ${escapeHTML(groupName)} 的详情缓存。</td></tr>`;
    $("#consumerSubscriptionRows").innerHTML = `<tr><td colspan="2">正在检查 ${escapeHTML(groupName)} 的订阅缓存。</td></tr>`;
    $("#consumerProgressRows").innerHTML = `<tr><td colspan="9">正在检查 ${escapeHTML(groupName)} 的消费进度缓存。</td></tr>`;
  }

  const params = new URLSearchParams({ group: groupName });
  if (topicName) {
    params.set("topic", topicName);
  }
  if (options.force) {
    params.set("refresh", "true");
  }
  try {
    const payload = await fetchJSON(`/api/consumer-detail?${params.toString()}`);
    state.lastConsumerDetailPayload = payload;
    renderConsumerDetail(payload);
    if (payload.refreshing) {
      scheduleConsumerDetailPoll(groupName, topicName);
    } else {
      state.consumerDetailPollCount = 0;
    }
  } catch (error) {
    $("#consumerDetailStatus").textContent = "失败";
    $("#consumerDetailSummary").innerHTML = consumerDetailSummaryCells(groupName, topicName || "-", "-", "-");
    $("#consumerConnectionRows").innerHTML = `<tr><td colspan="4">${escapeHTML(error.message)}</td></tr>`;
    $("#consumerSubscriptionRows").innerHTML = `<tr><td colspan="2">${escapeHTML(error.message)}</td></tr>`;
    $("#consumerProgressRows").innerHTML = `<tr><td colspan="9">${escapeHTML(error.message)}</td></tr>`;
  } finally {
    if (button) {
      setLoading(button, false);
    }
  }
}

// consumerConnectionsTableRows 输出客户端连接，clientId 和地址允许折行显示。
function consumerConnectionsTableRows(connections) {
  return connections.map((connection) => `
    <tr>
      ${tableCell("Client ID", escapeHTML(connection.clientId || "-"), "mono wrap-cell")}
      ${tableCell("客户端地址", escapeHTML(connection.clientAddr || "-"), "mono wrap-cell")}
      ${tableCell("语言", escapeHTML(connection.language || "-"))}
      ${tableCell("版本", escapeHTML(connection.version || "-"), "wrap-cell")}
    </tr>
  `).join("");
}

function consumerConnectionsTableHTML(detail, emptyText = "未返回连接。") {
  return dataTableHTML(["Client ID", "客户端地址", "语言", "版本"], consumerConnectionsTableRows(detail.connections || []), detail.connectionError || emptyText);
}

// consumerSubscriptionsTableRows 输出订阅关系，Topic 和表达式都按长文本处理。
function consumerSubscriptionsTableRows(subscriptions) {
  return subscriptions.map((subscription) => `
    <tr>
      ${tableCell("Topic", escapeHTML(subscription.topic || "-"), "wrap-cell")}
      ${tableCell("SubExpression", escapeHTML(subscription.expression || "-"), "wrap-cell")}
    </tr>
  `).join("");
}

function consumerSubscriptionsTableHTML(detail, emptyText = "未返回订阅。") {
  return dataTableHTML(["Topic", "SubExpression"], consumerSubscriptionsTableRows(detail.subscriptions || []), emptyText);
}

// consumerProgressTableRows 输出消费进度明细，九列在窄屏下按字段卡片排列。
function consumerProgressTableRows(progressRows) {
  return progressRows.map((row) => `
    <tr>
      ${tableCell("Topic", escapeHTML(row.topic || "-"), "wrap-cell")}
      ${tableCell("Broker", escapeHTML(row.brokerName || "-"), "wrap-cell")}
      ${tableCell("队列", escapeHTML(row.queueId ?? "-"), "mono")}
      ${tableCell("Broker 位点", escapeHTML(row.brokerOffset ?? "-"), "mono")}
      ${tableCell("消费位点", escapeHTML(row.consumerOffset ?? "-"), "mono")}
      ${tableCell("客户端 IP", escapeHTML(row.clientIp || "-"), "mono wrap-cell")}
      ${tableCell("堆积", escapeHTML(row.diff ?? 0), `${row.diff > 0 ? "warn-text" : "good-text"} mono`)}
      ${tableCell("处理中", escapeHTML(row.inflight ?? 0), `${row.inflight > 0 ? "warn-text" : "good-text"} mono`)}
      ${tableCell("最后时间", escapeHTML(row.lastTime || "-"), "mono wrap-cell")}
    </tr>
  `).join("");
}

function consumerProgressTableHTML(detail, emptyText = "未返回消费进度。") {
  return dataTableHTML(
    ["Topic", "Broker", "队列", "Broker 位点", "消费位点", "客户端 IP", "堆积", "处理中", "最后时间"],
    consumerProgressTableRows(detail.progressRows || []),
    detail.progressError || emptyText
  );
}

// renderConsumerDetail 将消费者详情拆成摘要、连接、订阅和队列进度四块，减少运维排障时的扫描成本。
function renderConsumerDetail(payload) {
  const detail = payload.data || {};
  const connections = detail.connections || [];
  const subscriptions = detail.subscriptions || [];
  const progressRows = detail.progressRows || [];
  const hasProgressIssue = Boolean(detail.progressError);
  const hasConnectionIssue = Boolean(detail.connectionError);

  if (!payload.hasData) {
    renderConsumerDetailPending(payload, detail);
    return;
  }
  syncConsumerOffsetResetForm(detail);

  const statusParts = [hasConnectionIssue || hasProgressIssue ? "部分异常" : "已加载"];
  if (payload.refreshing) {
    statusParts.push("后台刷新中");
  }
  if (payload.stale) {
    statusParts.push("过期缓存");
  }
  if (payload.lastError) {
    statusParts.push("上次刷新失败");
  }
  statusParts.push(`${payload.latencyMillis} ms`);
  $("#consumerDetailStatus").textContent = statusParts.join(" · ");
  $("#consumerDetailSummary").innerHTML = consumerDetailSummaryCells(
    detail.group || "-",
    detail.topic || "-",
    `${detail.diffTotal ?? 0} / ${detail.inflightTotal ?? 0}`,
    `${detail.consumeTps ?? 0}`
  ) + `
    <div>
      <span>消费类型</span>
      <strong>${escapeHTML(detail.consumeType || "-")}</strong>
    </div>
    <div>
      <span>消息模式</span>
      <strong>${escapeHTML(detail.messageModel || "-")}</strong>
    </div>
    <div>
      <span>起始策略</span>
      <strong>${escapeHTML(detail.consumeFromWhere || "-")}</strong>
    </div>
    <div>
      <span>连接数</span>
      <strong>${escapeHTML(connections.length)}</strong>
    </div>
  `;

  $("#consumerConnectionRows").innerHTML = consumerConnectionsTableRows(connections) || `<tr><td colspan="4">${escapeHTML(detail.connectionError || "未返回连接。")}</td></tr>`;
  $("#consumerSubscriptionRows").innerHTML = consumerSubscriptionsTableRows(subscriptions) || `<tr><td colspan="2">未返回订阅。</td></tr>`;
  $("#consumerProgressRows").innerHTML = consumerProgressTableRows(progressRows) || `<tr><td colspan="9">${escapeHTML(detail.progressError || "未返回消费进度。")}</td></tr>`;
}

// renderConsumerDetailPending 展示无缓存时的异步刷新状态，让首次点击也能保持 500-1000ms 内响应。
function renderConsumerDetailPending(payload, detail) {
  syncConsumerOffsetResetForm({
    group: detail.group || state.selectedConsumerGroup || "",
    topic: detail.topic || state.selectedConsumerTopic || ""
  });
  const status = payload.refreshing ? "后台刷新中" : (payload.lastError ? "刷新失败" : "无缓存");
  $("#consumerDetailStatus").textContent = `${status} · ${payload.latencyMillis} ms`;
  $("#consumerDetailSummary").innerHTML = consumerDetailSummaryCells(
    detail.group || state.selectedConsumerGroup || "-",
    detail.topic || state.selectedConsumerTopic || "自动选择",
    "-",
    "-"
  );
  const message = payload.refreshing ? "详情后台刷新中，完成后自动更新。" : (payload.lastError || "暂无详情缓存。");
  $("#consumerConnectionRows").innerHTML = `<tr><td colspan="4">${escapeHTML(message)}</td></tr>`;
  $("#consumerSubscriptionRows").innerHTML = `<tr><td colspan="2">${escapeHTML(message)}</td></tr>`;
  $("#consumerProgressRows").innerHTML = `<tr><td colspan="9">${escapeHTML(message)}</td></tr>`;
}

// syncConsumerOffsetResetForm 用消费者详情里的 group/topic 预填 resetOffsetByTime 表单。
function syncConsumerOffsetResetForm(detail = {}) {
  const form = $("#consumerOffsetResetForm");
  if (!form) {
    return;
  }
  const group = String(detail.group || state.selectedConsumerGroup || "").trim();
  const topic = String(
    detail.topic ||
    state.selectedConsumerTopic ||
    detail.subscriptions?.[0]?.topic ||
    detail.progressRows?.[0]?.topic ||
    ""
  ).trim();
  syncConsumerOffsetResetField(form.elements.group, group, "consumerOffsetResetAutoGroup");
  syncConsumerOffsetResetField(form.elements.topic, topic, "consumerOffsetResetAutoTopic");
  if (!form.elements.timestamp.value.trim()) {
    form.elements.timestamp.value = "now";
  }
  if (!state.lastConsumerOffsetResetResult) {
    $("#consumerOffsetResetStatus").textContent = topic ? "等待重置" : "等待 Topic";
  }
}

// syncConsumerOffsetResetField 只覆盖仍处于自动同步状态的 reset 表单字段。
function syncConsumerOffsetResetField(element, nextValue, autoKey) {
  if (!element) {
    return;
  }
  const currentValue = String(element.value || "").trim();
  const lastAutoValue = String(state[autoKey] || "").trim();
  const nextTrimmed = String(nextValue || "").trim();
  if (!currentValue || currentValue === lastAutoValue) {
    element.value = nextTrimmed;
    state[autoKey] = nextTrimmed;
    return;
  }
  state[autoKey] = currentValue;
}

// consumerOffsetResetPayload 从表单生成 resetOffsetByTime 请求体，空的高级字段不会发送。
function consumerOffsetResetPayload(form) {
  const payload = {
    group: String(form.elements.group.value || "").trim(),
    topic: String(form.elements.topic.value || "").trim(),
    timestamp: String(form.elements.timestamp.value || "").trim() || "now",
    force: Boolean(form.elements.force.checked),
    brokerAddr: String(form.elements.brokerAddr.value || "").trim()
  };
  const queueID = optionalNonNegativeInteger(form.elements.queueId.value, "queueId");
  if (queueID !== null) {
    payload.queueId = queueID;
  }
  const offset = optionalNonNegativeInteger(form.elements.offset.value, "offset");
  if (offset !== null) {
    payload.offset = offset;
  }
  return payload;
}

// handleConsumerOffsetResetSubmit 调用 resetOffsetByTime，并刷新 Consumer 快照与当前详情缓存。
async function handleConsumerOffsetResetSubmit(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const button = $("#consumerOffsetResetButton");
  setLoading(button, true);
  $("#consumerOffsetResetStatus").textContent = "重置中";
  try {
    const payload = consumerOffsetResetPayload(form);
    if (!payload.group) {
      throw new Error("group 必填");
    }
    if (!payload.topic) {
      throw new Error("topic 必填");
    }
    if ((payload.brokerAddr && payload.queueId == null) || (!payload.brokerAddr && payload.queueId != null)) {
      throw new Error("brokerAddr 和 queueId 必须同时填写");
    }
    const response = await fetchJSON("/api/consumer-offset/reset", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    const result = response.data || {};
    state.lastConsumerOffsetResetResult = result;
    $("#consumerOffsetResetStatus").textContent = `重置成功 · ${response.latencyMillis ?? 0} ms`;
    renderConsumerOffsetResetResult(result);
    await loadSnapshots({ manageButton: false });
    await loadConsumerDetail(payload.group, payload.topic, null, { force: true });
  } catch (error) {
    state.lastConsumerOffsetResetResult = null;
    $("#consumerOffsetResetStatus").textContent = error.message;
    renderConsumerOffsetResetError(error.message);
  } finally {
    setLoading(button, false);
  }
}

// handleConsumerOffsetResetFieldChange 记录用户手动修改 group/topic，避免详情轮询覆盖输入。
function handleConsumerOffsetResetFieldChange(event) {
  state.lastConsumerOffsetResetResult = null;
  if (event.target.name === "group") {
    state.consumerOffsetResetAutoGroup = String(event.target.value || "").trim();
  }
  if (event.target.name === "topic") {
    state.consumerOffsetResetAutoTopic = String(event.target.value || "").trim();
  }
  if ($("#consumerOffsetResetStatus").textContent.startsWith("重置成功")) {
    $("#consumerOffsetResetStatus").textContent = "已修改，等待重置";
  }
}

// renderConsumerOffsetResetResult 展示消费点重置命令的目标、时间和输出摘要。
function renderConsumerOffsetResetResult(result) {
  $("#consumerOffsetResetResult").innerHTML = `
    <div>
      <span>目标</span>
      <strong>${escapeHTML(result.target || "-")}</strong>
    </div>
    <div>
      <span>时间</span>
      <strong>${escapeHTML(result.timestamp || "-")}</strong>
    </div>
    <div>
      <span>操作</span>
      <strong>${escapeHTML(result.operation || "-")}</strong>
    </div>
    <div>
      <span>输出</span>
      <strong class="wrap-cell">${escapeHTML(result.output || "-")}</strong>
    </div>
  `;
}

// renderConsumerOffsetResetError 保持失败态 DOM 稳定，便于下一次提交覆盖。
function renderConsumerOffsetResetError(message) {
  $("#consumerOffsetResetResult").innerHTML = `
    <div>
      <span>目标</span>
      <strong>-</strong>
    </div>
    <div>
      <span>时间</span>
      <strong>-</strong>
    </div>
    <div>
      <span>操作</span>
      <strong>失败</strong>
    </div>
    <div>
      <span>错误</span>
      <strong>${escapeHTML(message)}</strong>
    </div>
  `;
}

// scheduleConsumerDetailPoll 在后台刷新未完成时短轮询当前详情，不阻塞用户切换其它 tab。
function scheduleConsumerDetailPoll(group, topic) {
  if (state.consumerDetailPollTimer || state.consumerDetailPollCount >= 45) {
    return;
  }
  state.consumerDetailPollCount += 1;
  state.consumerDetailPollTimer = window.setTimeout(async () => {
    state.consumerDetailPollTimer = null;
    await loadConsumerDetail(group, topic, null, { quiet: true });
  }, 1500);
}

// resetConsumerDetailPoll 切换消费者组时清理上一组的轮询，避免旧结果覆盖新选择。
function resetConsumerDetailPoll() {
  if (state.consumerDetailPollTimer) {
    window.clearTimeout(state.consumerDetailPollTimer);
    state.consumerDetailPollTimer = null;
  }
  state.consumerDetailPollCount = 0;
}

// setActiveConsumerButton 高亮当前详情目标，列表刷新后用户仍能看到右侧面板对应哪一行。
function setActiveConsumerButton(groupName) {
  $("#consumerRows").querySelectorAll("[data-consumer-group]").forEach((button) => {
    button.classList.toggle("active", button.dataset.consumerGroup === groupName);
  });
}

// consumerDetailSummaryCells 生成消费者详情摘要单元，加载、成功和失败态复用同一结构。
function consumerDetailSummaryCells(group, topic, lag, tps) {
  return `
    <div>
      <span>消费者组</span>
      <strong>${escapeHTML(group)}</strong>
    </div>
    <div>
      <span>Topic</span>
      <strong>${escapeHTML(topic)}</strong>
    </div>
    <div>
      <span>堆积 / 处理中</span>
      <strong>${escapeHTML(lag)}</strong>
    </div>
    <div>
      <span>Consume TPS</span>
      <strong>${escapeHTML(tps)}</strong>
    </div>
  `;
}

// handleChainSubmit 收集链路查询参数并触发查询，保持页面内完成完整排障。
async function handleChainSubmit(event) {
  event.preventDefault();
  const form = new FormData(event.currentTarget);
  const params = new URLSearchParams();
  for (const [key, value] of form.entries()) {
    const trimmed = String(value).trim();
    if (trimmed) {
      params.set(key, trimmed);
    }
  }

  const queryString = params.toString();
  if (state.lastChainQueryString !== queryString) {
    resetChainPoll();
  }
  state.lastChainQueryString = queryString;

  const button = $("#chainButton");
  setLoading(button, true);
  $("#chainStatus").textContent = "查询中";
  $("#chainSummaryStatus").textContent = "查询中";
  $("#timelineHint").textContent = "查询中";
  try {
    await loadMessageChain(queryString, { force: true });
  } catch (error) {
    resetChainPoll();
    $("#messageSummary").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
    $("#candidateList").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
    $("#timeline").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
    $("#chainStatus").textContent = "失败";
    $("#chainSummaryStatus").textContent = "失败";
    $("#timelineHint").textContent = "失败";
  } finally {
    setLoading(button, false);
  }
}

// loadMessageChain 读取链路快照；无缓存首包由后端后台刷新，前端只负责轻量短轮询。
async function loadMessageChain(queryString, options = {}) {
  if (!queryString) {
    return;
  }
  if (!options.quiet) {
    $("#messageSummary").innerHTML = `<div class="empty-state">正在检查消息链路缓存。</div>`;
    $("#candidateList").innerHTML = `<div class="empty-state">候选消息后台刷新中。</div>`;
    $("#timeline").innerHTML = `<div class="empty-state">状态链路后台刷新中。</div>`;
  }
  const requestParams = new URLSearchParams(queryString);
  if (options.force) {
    requestParams.set("refresh", "true");
  }
  const payload = await fetchJSON(`/api/message-chain?${requestParams.toString()}`);
  state.lastChainPayload = payload;
  renderTimeline(payload);
  if (payload.refreshing) {
    scheduleChainPoll(queryString);
  } else {
    state.chainPollCount = 0;
  }
}

// renderTimeline 把消息摘要、候选消息和状态节点分块输出，减少单屏的信息噪声。
function renderTimeline(payload) {
  const chain = payload.data || {};
  const detail = chain.detail || {};
  const candidates = chain.candidates || [];
  const steps = chain.steps || [];

  if (!payload.hasData) {
    renderTimelinePending(payload, chain, detail);
    return;
  }

  const statusParts = [chain.overallStatus || "未知"];
  if (payload.refreshing) {
    statusParts.push("后台刷新中");
  }
  if (payload.stale) {
    statusParts.push("过期缓存");
  }
  if (payload.lastError) {
    statusParts.push("上次刷新失败");
  }
  statusParts.push(`${payload.latencyMillis} ms`);

  $("#chainLatency").textContent = `${payload.latencyMillis} ms`;
  $("#chainMeta").textContent = `${steps.length} 个节点`;
  $("#chainStatus").textContent = statusParts.join(" · ");
  $("#chainSummaryStatus").textContent = statusParts.join(" · ");
  $("#timelineHint").textContent = `${steps.length} 个节点`;
  $("#chainMetaPills").innerHTML = [
    `<span class="pill pill-ok">${escapeHTML(chain.overallStatus || "UNKNOWN")}</span>`,
    `<span class="pill ${candidates.length > 0 ? "pill-info" : "pill-muted"}">${candidates.length} 个候选</span>`,
    `<span class="pill ${payload.cacheHit ? "pill-ok" : "pill-info"}">${payload.cacheHit ? "cache hit" : "cache miss"}</span>`,
    `<span class="pill ${payload.refreshing ? "pill-info" : "pill-muted"}">${payload.refreshing ? "refreshing" : "idle"}</span>`,
    `<span class="pill ${payload.stale ? "pill-warn" : "pill-ok"}">${payload.stale ? "stale" : "fresh"}</span>`,
    `<span class="pill pill-muted">${payload.latencyMillis} ms</span>`
  ].join("");

  $("#messageSummary").innerHTML = `
    <div class="summary-grid">
      <div><span>Message ID</span><strong>${escapeHTML(chain.messageId || detail.messageId || "-")}</strong></div>
      <div><span>Topic</span><strong>${escapeHTML(chain.topic || detail.topic || "-")}</strong></div>
      <div><span>Keys</span><strong>${escapeHTML((chain.keys || detail.keys || []).join(", ") || "-")}</strong></div>
      <div><span>Queue</span><strong>${escapeHTML(detail.queueId ?? "-")}</strong></div>
      <div><span>Queue Offset</span><strong class="mono">${escapeHTML(detail.queueOffset ?? "-")}</strong></div>
      <div><span>Reconsume Times</span><strong>${escapeHTML(detail.reconsumeTimes ?? "-")}</strong></div>
      <div><span>Store Time</span><strong>${escapeHTML(formatTime(detail.storeTimestamp))}</strong></div>
      <div><span>Store Host</span><strong>${escapeHTML(detail.storeHost || "-")}</strong></div>
    </div>
    <div class="body-preview">
      <span>Body Preview</span>
      <code>${escapeHTML(detail.bodyPreview || "-")}</code>
    </div>
  `;

  renderCandidates(candidates);
  renderSteps(steps);
}

// renderTimelinePending 展示无缓存或刷新失败状态，保留查询目标并为异步结果预留稳定空间。
function renderTimelinePending(payload, chain, detail) {
  const keys = chain.keys || detail.keys || [];
  const status = payload.refreshing ? "链路后台刷新中" : (payload.lastError ? "链路刷新失败" : "暂无链路缓存");
  const message = payload.refreshing ? "消息链路后台刷新中，完成后自动更新。" : (payload.lastError || "暂无链路缓存。");
  $("#chainLatency").textContent = `${payload.latencyMillis} ms`;
  $("#chainMeta").textContent = payload.refreshing ? "后台刷新中" : "暂无结果";
  $("#chainStatus").textContent = `${status} · ${payload.latencyMillis} ms`;
  $("#chainSummaryStatus").textContent = status;
  $("#timelineHint").textContent = payload.refreshing ? "刷新中" : "无节点";
  $("#chainMetaPills").innerHTML = [
    `<span class="pill ${payload.refreshing ? "pill-info" : "pill-warn"}">${escapeHTML(status)}</span>`,
    `<span class="pill pill-muted">cache miss</span>`,
    `<span class="pill ${payload.stale ? "pill-warn" : "pill-muted"}">${payload.stale ? "stale" : "cold"}</span>`,
    `<span class="pill pill-muted">${payload.latencyMillis} ms</span>`
  ].join("");
  $("#messageSummary").innerHTML = `
    <div class="summary-grid">
      <div><span>Message ID</span><strong>${escapeHTML(chain.messageId || detail.messageId || "-")}</strong></div>
      <div><span>Topic</span><strong>${escapeHTML(chain.topic || detail.topic || "-")}</strong></div>
      <div><span>Keys</span><strong>${escapeHTML(keys.join(", ") || "-")}</strong></div>
      <div><span>状态</span><strong>${escapeHTML(status)}</strong></div>
    </div>
    <div class="body-preview">
      <span>刷新说明</span>
      <code>${escapeHTML(message)}</code>
    </div>
  `;
  $("#candidateHint").textContent = payload.refreshing ? "刷新中" : "0 条";
  $("#candidateList").innerHTML = `<div class="empty-state">${escapeHTML(message)}</div>`;
  $("#timeline").innerHTML = `<div class="empty-state">${escapeHTML(message)}</div>`;
}

// scheduleChainPoll 在链路后台刷新未完成时短轮询当前查询，避免用户重复点击查询按钮。
function scheduleChainPoll(queryString) {
  if (state.chainPollTimer || state.chainPollCount >= 45) {
    return;
  }
  state.chainPollCount += 1;
  state.chainPollTimer = window.setTimeout(async () => {
    state.chainPollTimer = null;
    try {
      await loadMessageChain(queryString, { quiet: true });
    } catch (error) {
      resetChainPoll();
      $("#chainStatus").textContent = "失败";
      $("#chainSummaryStatus").textContent = "失败";
      $("#timelineHint").textContent = "失败";
      $("#candidateList").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
      $("#timeline").innerHTML = `<div class="empty-state">${escapeHTML(error.message)}</div>`;
    }
  }, 1500);
}

// resetChainPoll 切换 messageId/key/topic 时清理上一条链路轮询，避免旧结果覆盖新查询。
function resetChainPoll() {
  if (state.chainPollTimer) {
    window.clearTimeout(state.chainPollTimer);
    state.chainPollTimer = null;
  }
  state.chainPollCount = 0;
}

function resetRuntimeSelections() {
  resetChainPoll();
  resetTopicRoutePoll();
  resetTopicStatusPoll();
  resetTopicMessagesPoll();
  resetConsumerDetailPoll();
  state.clusters = [];
  state.features = null;
  state.topics = [];
  state.consumers = [];
  state.lastFeaturePayload = null;
  state.featureConfigSearch = "";
  state.selectedClusterName = "";
  state.selectedTopicName = "";
  state.selectedTopicRouteTopic = "";
  state.selectedTopicStatusTopic = "";
  state.selectedTopicMessagesTopic = "";
  state.selectedConsumerGroup = "";
  state.selectedConsumerTopic = "";
  state.lastChainPayload = null;
  state.lastConsumerPayload = null;
  state.lastChainQueryString = "";
  state.topicMutationNotice = "";
  resetTopicMessageSendPanel();
  resetConsumerOffsetResetPanel();
  $("#topicRouteRows").innerHTML = `<tr><td colspan="6">选择一个 Topic 查看路由。</td></tr>`;
  $("#topicQueueRows").innerHTML = `<tr><td colspan="6">选择一个 Topic 查看队列水位。</td></tr>`;
  $("#topicMessageRows").innerHTML = `<tr><td colspan="8">选择一个 Topic 浏览消息。</td></tr>`;
  $("#systemTopicRows").innerHTML = `<tr><td colspan="5">等待刷新。</td></tr>`;
  $("#capabilityGrid").innerHTML = `<div class="empty-state">等待能力画像。</div>`;
  $("#nameServerConfigGroups").innerHTML = `<div class="empty-state">等待刷新。</div>`;
  $("#brokerConfigGroups").innerHTML = `<div class="empty-state">等待刷新。</div>`;
  $("#featureConfigSearch").value = "";
  $("#chainSource").textContent = "等待从消息列表选择，或使用高级查询。";
}

// resetTopicMessageSendPanel 在切换运行时后清空发送结果和自动值，避免跨集群误发。
function resetTopicMessageSendPanel() {
  state.topicMessageSendAutoTopic = "";
  state.topicMessageSendAutoBroker = "";
  state.lastTopicMessageSendResult = null;
  const form = $("#topicMessageSendForm");
  if (!form) {
    return;
  }
  form.reset();
  $("#topicMessageSendStatus").textContent = "等待选择";
  $("#topicMessageSendChainButton").disabled = true;
  $("#topicMessageSendResult").innerHTML = `
    <div><span>Message ID</span><strong>-</strong></div>
    <div><span>发送状态</span><strong>-</strong></div>
    <div><span>Broker</span><strong>-</strong></div>
    <div><span>队列</span><strong>-</strong></div>
  `;
}

// resetConsumerOffsetResetPanel 在切换运行时后清空消费点重置表单和结果。
function resetConsumerOffsetResetPanel() {
  state.consumerOffsetResetAutoGroup = "";
  state.consumerOffsetResetAutoTopic = "";
  state.lastConsumerOffsetResetResult = null;
  const form = $("#consumerOffsetResetForm");
  if (!form) {
    return;
  }
  form.reset();
  form.elements.timestamp.value = "now";
  form.elements.force.checked = true;
  $("#consumerOffsetResetStatus").textContent = "等待选择";
  $("#consumerOffsetResetResult").innerHTML = `
    <div><span>目标</span><strong>-</strong></div>
    <div><span>时间</span><strong>-</strong></div>
    <div><span>操作</span><strong>-</strong></div>
    <div><span>输出</span><strong>-</strong></div>
  `;
}

// renderCandidates 让用户直接点选候选 messageId，减少 key 查询后的二次输入。
function renderCandidates(candidates) {
  if (!candidates.length) {
    $("#candidateHint").textContent = "0 条";
    $("#candidateList").innerHTML = `<div class="empty-state">没有返回候选消息。</div>`;
    return;
  }

  $("#candidateHint").textContent = `${candidates.length} 条`;
  $("#candidateList").innerHTML = candidates.map((candidate, index) => `
    <button class="candidate-item" type="button" data-message-id="${escapeAttr(candidate.messageId)}">
      <span class="candidate-index">#${index + 1}</span>
      <span class="candidate-main">
        <strong>${escapeHTML(candidate.messageId)}</strong>
        <small>QID ${escapeHTML(candidate.queueId)} · Offset ${escapeHTML(candidate.queueOffset)}</small>
      </span>
    </button>
  `).join("");

  $("#candidateList").querySelectorAll("[data-message-id]").forEach((element) => {
    element.addEventListener("click", () => {
      $("#chainForm").elements.messageId.value = element.dataset.messageId;
      $("#chainForm").elements.key.value = "";
      $("#chainForm").requestSubmit();
      $("#chainStatus").textContent = `切换到 ${element.dataset.messageId}`;
    });
  });
}

// renderSteps 按时间顺序展示生命周期节点，包含发送、存储、消费和异常。
function renderSteps(steps) {
  if (!steps.length) {
    $("#timeline").innerHTML = `<div class="empty-state">没有可展示的轨迹节点。</div>`;
    return;
  }

  $("#timeline").innerHTML = steps.map((step) => `
    <article class="timeline-step">
      <div class="timeline-time">${formatTime(step.timestamp)}</div>
      <div class="timeline-body">
        <div class="timeline-head">
          <strong>${escapeHTML(step.label || step.stage)}</strong>
          <span class="pill pill-${escapeHTML(step.health || "ok")}">${escapeHTML(step.health || "ok")}</span>
        </div>
        <p>${escapeHTML(step.detail || "-")}</p>
        <small>${escapeHTML(step.group || "broker")} · ${escapeHTML(step.stage || "-")}</small>
      </div>
    </article>
  `).join("");
}

// firstBrokerVersion 从集群里提取第一条有效 broker 版本，作为总览主版本。
function firstBrokerVersion(clusters) {
  for (const cluster of clusters) {
    for (const broker of cluster.brokers || []) {
      if (broker.version) {
        return broker.version;
      }
    }
  }
  return "-";
}

// metaText 把快照状态和业务摘要合并成一句话，适合卡片副标题。
function metaText(payload, fallback) {
  return `${snapshotStatusText(payload)} · ${fallback}`;
}

// summariseSnapshot 用简短句子概括三个快照状态，落在总览区域。
function summariseSnapshot(...payloads) {
  if (payloads.some((payload) => payload.lastError)) {
    return "有错误";
  }
  if (payloads.some((payload) => payload.refreshing)) {
    return "刷新中";
  }
  if (payloads.some((payload) => payload.stale)) {
    return "有过期";
  }
  if (payloads.every((payload) => payload.cacheHit)) {
    return "已就绪";
  }
  return "已加载";
}

// snapshotStatusText 把快照响应压缩成一句短状态，便于扫读。
function snapshotStatusText(payload) {
  const latency = payload.latencyMillis != null ? `${payload.latencyMillis} ms` : "-";
  const parts = [];
  parts.push(payload.cacheHit ? "缓存命中" : "实时刷新");
  parts.push(payload.refreshing ? "刷新中" : "已就绪");
  if (payload.lastError) {
    parts.push("有错误");
  }
  parts.push(latency);
  return parts.join(" · ");
}

// snapshotPills 将快照元信息拆成胶囊标签，方便快速辨认状态变化。
function snapshotPills(payload) {
  const pills = [];
  pills.push(`<span class="pill ${payload.cacheHit ? "pill-ok" : "pill-info"}">${payload.cacheHit ? "cache hit" : "cache miss"}</span>`);
  pills.push(`<span class="pill ${payload.refreshing ? "pill-info" : "pill-muted"}">${payload.refreshing ? "refreshing" : "idle"}</span>`);
  pills.push(`<span class="pill ${payload.stale ? "pill-warn" : "pill-ok"}">${payload.stale ? "stale" : "fresh"}</span>`);
  if (payload.lastRefreshUnixMilli) {
    pills.push(`<span class="pill pill-muted">${formatTime(payload.lastRefreshUnixMilli)}</span>`);
  }
  return pills.join("");
}

// refreshTriggerText 将后台刷新启动结果压缩成一句话，便于运维判断按钮是否真的触发了线上拉取。
function refreshTriggerText(triggered) {
  const names = [
    ["clusters", "集群"],
    ["features", "配置"],
    ["topics", "Topic"],
    ["consumers", "Consumer"]
  ];
  const started = names.filter(([key]) => triggered[key]).map(([, label]) => label);
  if (started.length === 0) {
    return "已有刷新任务运行中";
  }
  return `已触发 ${started.join(" / ")} 后台刷新`;
}

// formatTime 统一把毫秒时间戳格式化成本地时间。
function formatTime(value) {
  if (!value) {
    return "-";
  }
  return new Date(value).toLocaleString("zh-CN", { hour12: false });
}

// formatDuration 将待决毫秒数压缩成适合面板扫描的中文时长。
function formatDuration(value) {
  const millis = Number(value || 0);
  if (!Number.isFinite(millis) || millis <= 0) {
    return "0 秒";
  }
  const totalSeconds = Math.floor(millis / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (days > 0) {
    return `${days} 天 ${hours} 小时`;
  }
  if (hours > 0) {
    return `${hours} 小时 ${minutes} 分`;
  }
  if (minutes > 0) {
    return `${minutes} 分 ${seconds} 秒`;
  }
  return `${seconds} 秒`;
}

// formatCount 把队列水位、样本数等整数统一格式化，避免大数在面板里难以扫描。
function formatCount(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) {
    return "0";
  }
  return number.toLocaleString("zh-CN", { maximumFractionDigits: 0 });
}

// optionalNonNegativeInteger 解析可空的非负整数字段，空字符串表示不向后端传该参数。
function optionalNonNegativeInteger(value, fieldName) {
  const raw = String(value || "").trim();
  if (!raw) {
    return null;
  }
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed < 0) {
    throw new Error(`${fieldName} 必须是非负整数`);
  }
  return parsed;
}

// escapeHTML 防止服务端返回文本被当作 HTML 执行。
function escapeHTML(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

// escapeAttr 用在 data-* 属性里，避免引号和反引号破坏 DOM。
function escapeAttr(value) {
  return escapeHTML(value).replaceAll("`", "&#096;");
}

bindTabs();
bindSubtabs();
$("#openNameServerDialog").addEventListener("click", openNameServerDialog);
$("#nameServerDialogClose").addEventListener("click", closeNameServerDialog);
$("#nameServerDialogCancel").addEventListener("click", closeNameServerDialog);
$("#nameServerForm").addEventListener("submit", handleNameServerSubmit);
$("#detailDialogClose").addEventListener("click", closeDetailDialog);
$("#refreshButton").addEventListener("click", forceRefreshSnapshots);
$("#topicSearchInput").addEventListener("input", handleTopicSearchInput);
$("#topicSearchClear").addEventListener("click", clearTopicSearch);
$("#featureConfigSearch").addEventListener("input", handleFeatureConfigSearchInput);
$("#featureConfigSearchClear").addEventListener("click", clearFeatureConfigSearch);
$("#consumerSearchInput").addEventListener("input", handleConsumerSearchInput);
$("#consumerSearchClear").addEventListener("click", clearConsumerSearch);
$("#topicRouteOpenDetail").addEventListener("click", openTopicRouteDetail);
$("#topicStatusOpenDetail").addEventListener("click", openTopicStatusDetail);
$("#topicMessagesOpenDetail").addEventListener("click", openTopicMessagesDetail);
$("#consumerConnectionsOpenDetail").addEventListener("click", openConsumerConnectionsDetail);
$("#consumerSubscriptionsOpenDetail").addEventListener("click", openConsumerSubscriptionsDetail);
$("#consumerProgressOpenDetail").addEventListener("click", openConsumerProgressDetail);
$("#topicMutationForm").addEventListener("submit", handleTopicMutationSubmit);
$("#topicMutationForm").addEventListener("input", handleTopicMutationFieldChange);
$("#topicMutationForm").addEventListener("change", handleTopicMutationFieldChange);
$("#topicMutationForm").querySelectorAll("[data-topic-mutation-scope]").forEach((button) => {
  button.addEventListener("click", handleTopicMutationScopeClick);
});
$("#topicDeleteButton").addEventListener("click", handleTopicDelete);
$("#topicMessageSendForm").addEventListener("submit", handleTopicMessageSendSubmit);
$("#topicMessageSendForm").addEventListener("input", handleTopicMessageSendFieldChange);
$("#topicMessageSendForm").addEventListener("change", handleTopicMessageSendFieldChange);
$("#topicMessageSendChainButton").addEventListener("click", openChainForSentMessage);
$("#consumerOffsetResetForm").addEventListener("submit", handleConsumerOffsetResetSubmit);
$("#consumerOffsetResetForm").addEventListener("input", handleConsumerOffsetResetFieldChange);
$("#consumerOffsetResetForm").addEventListener("change", handleConsumerOffsetResetFieldChange);
$("#chainForm").addEventListener("submit", handleChainSubmit);
$("#topicMessagesRefresh").addEventListener("click", () => {
  if (state.selectedTopicMessagesTopic) {
    loadTopicMessages(state.selectedTopicMessagesTopic, { force: true });
  }
});

loadConfig()
  .then(loadHealth)
  .then(loadSnapshots)
  .catch((error) => {
    $("#snapshotState").textContent = error.message;
    $("#clusterStatus").textContent = error.message;
    $("#featureStatus").textContent = error.message;
    $("#topicStatus").textContent = error.message;
    $("#consumerStatus").textContent = error.message;
  });
