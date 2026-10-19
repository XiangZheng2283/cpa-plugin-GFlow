// background.js - GFlow 登录助手后台服务
importScripts('common.js');

const ALARM = 'gflow-cookie-sync';
const FLOW_URL = 'https://flow.google.com/projects';
let activeSync = null;

// 日志系统
const Logger = {
  async log(level, message, details = null) {
    const timestamp = new Date().toISOString();
    const safeDetails = sanitizeLogDetails(details);
    const logEntry = { timestamp, level, message, details: safeDetails };
    console.log(`[${level}] ${message}`, safeDetails || '');
    try {
      const { logs = [] } = await chrome.storage.local.get(['logs']);
      const updatedLogs = [logEntry, ...logs].slice(0, 100);
      await chrome.storage.local.set({ logs: updatedLogs });
    } catch (_) {}
  },
  info(msg, d) { return this.log('INFO', msg, d); },
  error(msg, d) { return this.log('ERROR', msg, d); },
  success(msg, d) { return this.log('SUCCESS', msg, d); },
  async getLogs() {
    const { logs = [] } = await chrome.storage.local.get(['logs']);
    return logs;
  },
  async clearLogs() {
    await chrome.storage.local.set({ logs: [] });
  }
};

function sanitizeLogDetails(value, key = '') {
  if (value == null) return value;
  if (/^(authorization|connectionToken|google_cookies|session_token|password|secret)$/i.test(key)) {
    return '[REDACTED]';
  }
  if (Array.isArray(value)) return value.map(item => sanitizeLogDetails(item));
  if (typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([k, v]) => [k, sanitizeLogDetails(v, k)])
    );
  }
  if (typeof value === 'string') {
    return value
      .replace(/Bearer\s+[^\s]+/gi, 'Bearer [REDACTED]')
      .replace(/[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}/gi, email => email[0] + '***@' + email.split('@')[1])
      .replace(/((?:^|[;\s])(?:__Secure-)?(?:OSID|SID|HSID|SSID|APISID|SAPISID)=)[^;\s]+/gi, '$1[REDACTED]');
  }
  return value;
}

function safeServerDiagnostic(raw) {
  const result = {};
  if (['bootstrap', 'rpc'].includes(raw?.phase)) result.phase = raw.phase;
  if (['nzlxg', 'jHPbke'].includes(raw?.rpc)) result.rpc = raw.rpc;
  if (['redirect', 'http_auth', 'missing_metadata'].includes(raw?.reason)) result.reason = raw.reason;
  for (const key of ['http_status', 'rpc_code', 'browser_http_status', 'browser_rpc_code']) {
    if (Number.isInteger(raw?.[key]) && raw[key] >= 0 && raw[key] < 600) result[key] = raw[key];
  }
  for (const key of ['bootstrap_identity', 'xsrf_present', 'account_matches', 'browser_credit_ok', 'observed_cookie_header']) {
    if (typeof raw?.[key] === 'boolean') result[key] = raw[key];
  }
  for (const key of ['rpc_cookie_count', 'duplicate_cookie_names']) {
    if (Number.isInteger(raw?.[key]) && raw[key] >= 0 && raw[key] <= 1000) result[key] = raw[key];
  }
  return result;
}

async function publicState() {
  const state = await chrome.storage.local.get(['config', 'lastSync']);
  const config = state.config || {};
  return {
    configured: Boolean(config.connectionToken),
    loginAccount: config.loginAccount || '',
    apiUrl: config.apiUrl || '',
    autoSync: config.autoSync !== false,
    lastSync: state.lastSync || null
  };
}

async function restoreAlarm() {
  const { config } = await chrome.storage.local.get('config');
  await chrome.alarms.clear(ALARM);
  if (config?.connectionToken && config.autoSync) {
    await chrome.alarms.create(ALARM, { periodInMinutes: 60 });
  }
}

async function saveConfiguration(input) {
  const state = await chrome.storage.local.get('config');
  const connection = validateConnection(input.connection || state.config);
  const config = {
    ...connection,
    loginAccount: validateEmail(input.loginAccount),
    autoSync: Boolean(input.autoSync)
  };
  if (!await chrome.permissions.contains({ origins: [connectionPermission(config.apiUrl)] })) {
    throw new Error('请授权扩展连接 GFlow');
  }
  await chrome.storage.local.set({ config });
  await restoreAlarm();
  await Logger.info('GFlow 连接配置已保存', { apiUrl: config.apiUrl, loginAccount: config.loginAccount });
  return publicState();
}

async function waitForTabReady(tabId, maxWaitMs = 15000) {
  const startTime = Date.now();
  while (Date.now() - startTime < maxWaitMs) {
    try {
      const tab = await chrome.tabs.get(tabId);
      if (tab && tab.status === 'complete') return tab;
    } catch (_) {
      break;
    }
    await new Promise(r => setTimeout(r, 500));
  }
}

async function closeTemporaryTab(tab) {
  if (tab && typeof tab.id === 'number') {
    try { await chrome.tabs.remove(tab.id); } catch (_) {}
  }
}

// 监听范围只包含本次临时标签的 nzlxg 请求；Cookie 仅在内存中存在。
async function readObservedFlowCookies(tabId, email) {
  if (!chrome.webRequest?.onBeforeSendHeaders) throw new Error('请重新加载 GFlow 登录助手以启用请求读取权限');
  let cookieHeader='';
  const listener = details => {
    const url=new URL(details.url);
    if (details.tabId !== tabId || details.method !== 'POST' || url.origin !== 'https://flow.google.com'
        || url.pathname !== '/_/AiSandboxAngularFrontend/data/batchexecute' || url.searchParams.get('rpcids') !== 'nzlxg') return;
    cookieHeader=(details.requestHeaders || []).find(header=>header.name.toLowerCase()==='cookie')?.value || '';
  };
  chrome.webRequest.onBeforeSendHeaders.addListener(listener,
    {urls:['https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute*'],tabId}, ['requestHeaders','extraHeaders']);
  try {
    const diagnostic = await chrome.tabs.sendMessage(tabId, {action:'gflow-credit-check',email});
    // 保留浏览器的真实顺序和同名 Cookie；拒绝空值及报文控制字符。
    if (!cookieHeader || cookieHeader.length > 262144 || /[\x00-\x1f\x7f]/.test(cookieHeader)) {
      throw new Error('Chrome 未提供本次 Flow 请求的 Cookie，请确认扩展管理页已允许新权限后重试');
    }
    const safe = {success:diagnostic?.success === true};
    if (['identity','network','rpc'].includes(diagnostic?.phase)) safe.phase=diagnostic.phase;
    for (const key of ['http_status','rpc_code']) if (Number.isInteger(diagnostic?.[key]) && diagnostic[key]>=0 && diagnostic[key]<600) safe[key]=diagnostic[key];
    return {cookieHeader,diagnostic:safe};
  } finally {
    chrome.webRequest.onBeforeSendHeaders.removeListener(listener);
  }
}

async function executeSync() {
  const { config } = await chrome.storage.local.get('config');
  if (!config) throw new Error('请先从 GFlow 复制配置并保存');
  const connection = validateConnection(config);
  const loginAccount = validateEmail(config.loginAccount);

  await Logger.info('开始同步 GFlow 登录凭据...', { account: loginAccount });

  // 固定本次同步的窗口和 Cookie 存储区，避免后台标签与读取来自不同窗口。
  const sourceTabs = chrome.tabs?.query ? await chrome.tabs.query({ active: true, currentWindow: true }) : [];
  const sourceTab = sourceTabs[0];
  let storeId;
  if (sourceTab && chrome.cookies?.getAllCookieStores) {
    const stores = await chrome.cookies.getAllCookieStores();
    storeId = stores.find(item => item.tabIds.includes(sourceTab.id))?.id;
    if (!storeId) throw new Error('无法确定当前窗口的登录态，请在 Flow 所在窗口重新打开助手');
  }
  let tempTab = null;
  let rawCookies, frontend, observed;
  let pageUrl = FLOW_URL;
  try {
    const current = new URL(sourceTab?.url || '');
    if (current.origin === 'https://flow.google.com' && /^\/project\/[a-f0-9-]{36}$/.test(current.pathname)) pageUrl = current.origin + current.pathname;
  } catch (_) {}
  try {
    if (chrome.tabs?.create && chrome.tabs?.get) {
      await Logger.info('正在后台刷新 Flow 登录态…');
      tempTab = await chrome.tabs.create({ url: pageUrl, active: false,
        ...(Number.isInteger(sourceTab?.windowId) ? { windowId: sourceTab.windowId } : {}) });
      const loadedTab = await waitForTabReady(tempTab.id);
      if (!loadedTab) throw new Error('Flow 页面加载超时，请确认网页可以正常打开后重试');
      if (new URL(loadedTab.url || '').hostname !== 'flow.google.com') {
        throw new Error('Flow 页面要求重新登录，请先在当前浏览器完成登录');
      }
      await new Promise(resolve => setTimeout(resolve, 5000));
      if (chrome.tabs.sendMessage) {
        let session;
        try { session = await chrome.tabs.sendMessage(tempTab.id, {action:'gflow-page-session',email:loginAccount}); }
        catch (_) { throw new Error('无法读取 Flow 页面登录态，请确认助手已重新加载后重试'); }
        if (!session?.signed_in) throw new Error('Flow 页面没有返回已登录账号，请在当前浏览器重新登录 Flow 后同步');
        if (session.account_matches !== true) throw new Error('Flow 当前账号与助手填写的邮箱不一致，请核对邮箱或使用独立浏览器配置');
        if (!session.frontend) throw new Error('Flow 页面认证字段未加载完整，请刷新 Flow 后重试');
        frontend = session.frontend;
        observed = await readObservedFlowCookies(tempTab.id, loginAccount);
        await Logger.info('Flow 浏览器额度检查完成', observed.diagnostic);
      }
      // 根据实际预热标签确定存储区；定时同步没有活动标签时也保持一致。
      if (chrome.cookies?.getAllCookieStores) {
        const stores = await chrome.cookies.getAllCookieStores();
        const warmedStore = stores.find(item => item.tabIds.includes(tempTab.id));
        if (!warmedStore || (storeId && storeId !== warmedStore.id)) {
          throw new Error('Flow 页面与当前窗口登录态不一致，请从 Flow 所在窗口同步');
        }
        storeId = warmedStore.id;
      }
    }
    if (!observed) {
      // 兼容没有页面消息 API 的测试/旧浏览器；现用 Chrome 使用实际请求头。
      const scopes = [FLOW_URL, 'https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute'];
      const groups = [];
      for (const url of scopes) {
        groups.push(await chrome.cookies.getAll({ url, ...(storeId ? { storeId } : {}) }));
      }
      rawCookies = groups.flat();
    }
    const googleCookies = observed?.cookieHeader || serializeFlowCookies(rawCookies);
    await Logger.info('Cookie 提取成功，正在提交至本机 GFlow 内核...');

    const payload = {
      login_account: loginAccount,
      google_cookies: googleCookies,
      protocol_mode: 'protocol',
      user_agent: typeof navigator !== 'undefined' ? navigator.userAgent : '',
      ...(frontend ? {frontend} : {}),
      ...(observed ? {browser_check:observed.diagnostic,cookie_source:'browser_request'} : {})
    };

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 120000);
    let response;
    try {
      response = await fetch(connection.apiUrl, {
        method: 'POST',
        redirect: 'error',
        cache: 'no-store',
        credentials: 'omit',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': 'Bearer ' + connection.connectionToken
        },
        body: JSON.stringify(payload),
        signal: controller.signal
      });
    } catch (error) {
      const errText = error.name === 'AbortError' ? 'GFlow 同步超时，请检查代理设置后重试' : '无法连接本机 GFlow，请确认 EasyCPA 内核运行中';
      await Logger.error(errText);
      throw new Error(errText);
    } finally {
      clearTimeout(timer);
    }

    let result = null;
    try { result = await response.json(); } catch (_) {}
    if (!response.ok || result?.success !== true) {
      if (response.status === 401 || response.status === 403) {
        await Logger.error('连接凭据失效');
        throw new Error('连接凭据失效，请回到 GFlow 重新复制扩展配置');
      }
      if (response.status === 404) {
        await Logger.error('接口未找到');
        throw new Error('当前 GFlow 版本不支持扩展同步，请更新插件');
      }
      const detail = result?.error;
      const safeMessage = typeof detail?.message === 'string'
        && detail.message.length <= 500
        && !/(cookie\s*[:=]|bearer\s|connectionToken|access_token|https?:\/\/|[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})/i.test(detail.message)
        ? detail.message : null;
      const code = /^gflow_[a-z_]+$/.test(detail?.code || '') ? detail.code : null;
      const errorMsg = safeMessage
        ? safeMessage + (code ? ' [' + code + ']' : '')
        : 'GFlow 同步失败（HTTP ' + response.status + '）。请查看日志获取详细诊断。';
      await Logger.error('同步失败', { error: errorMsg, diagnostic: safeServerDiagnostic(detail?.diagnostic) });
      throw new Error(errorMsg);
    }

    const actionMsg = result.action === 'updated' ? 'GFlow 账号登录态已更新' : 'GFlow 账号已添加';
    await Logger.success(`✅ ${actionMsg}`, { action: result.action });
    return { success: true, action: result.action, message: actionMsg };
  } finally {
    await closeTemporaryTab(tempTab);
  }
}

function syncNow() {
  if (activeSync) return activeSync;
  activeSync = (async () => {
    let result;
    try {
      result = await executeSync();
    } catch (error) {
      result = { success: false, message: error.message };
    }
    await chrome.storage.local.set({
      lastSync: { success: result.success, message: result.message, timestamp: Date.now() }
    });
    if (chrome.action?.setBadgeText) {
      await chrome.action.setBadgeText({ text: result.success ? 'OK' : '!' });
      await chrome.action.setBadgeBackgroundColor({ color: result.success ? '#0d9488' : '#dc2626' });
    }
    return result;
  })().finally(() => { activeSync = null; });
  return activeSync;
}

chrome.runtime.onMessage.addListener((request, sender, respond) => {
  if (sender.id !== chrome.runtime.id) return false;
  const action = request.action;
  if (action === 'state' || action === 'getConfig') {
    publicState().then(result => respond({ ok: true, success: true, result, config: result }), () => respond({ ok: false, success: false }));
    return true;
  }
  if (action === 'save' || action === 'saveConfig') {
    saveConfiguration(request.config).then(result => respond({ ok: true, success: true, result, config: result }), e => respond({ ok: false, success: false, message: e.message, error: e.message }));
    return true;
  }
  if (action === 'sync' || action === 'testNow') {
    syncNow().then(result => respond({ ok: true, success: result.success, result, ...result }), e => respond({ ok: false, success: false, message: e.message, error: e.message }));
    return true;
  }
  if (action === 'forget') {
    chrome.storage.local.remove(['config', 'lastSync'])
      .then(restoreAlarm)
      .then(publicState)
      .then(result => respond({ ok: true, result }), () => respond({ ok: false }));
    return true;
  }
  if (action === 'getLogs') {
    Logger.getLogs().then(logs => respond({ ok: true, success: true, logs }), () => respond({ ok: false, logs: [] }));
    return true;
  }
  if (action === 'clearLogs') {
    Logger.clearLogs().then(() => respond({ ok: true, success: true }), () => respond({ ok: false }));
    return true;
  }
  return false;
});

if (chrome.alarms?.onAlarm) {
  chrome.alarms.onAlarm.addListener(alarm => { if (alarm.name === ALARM) void syncNow(); });
}
if (chrome.runtime?.onStartup) {
  chrome.runtime.onStartup.addListener(() => { void restoreAlarm(); });
}
if (chrome.runtime?.onInstalled) {
  chrome.runtime.onInstalled.addListener(() => { void restoreAlarm(); });
}