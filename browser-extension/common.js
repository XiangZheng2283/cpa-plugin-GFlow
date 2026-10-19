// common.js - GFlow 登录助手通用工具函数
const GFLOW_SYNC_PATH = '/v0/management/plugins/gflow/api/extension-sync';

function validateConnection(value) {
  if (!value || value.product !== 'GFlow') {
    throw new Error('请粘贴从 GFlow 添加账号窗口复制的配置');
  }
  let url;
  try {
    url = new URL(value.apiUrl);
  } catch (_) {
    throw new Error('GFlow 连接地址无效');
  }
  // 远程宿主会收到 Google Cookie，只允许 HTTPS；每个域名仍需用户在 Chrome 中单独授权。
  const local = ['127.0.0.1', 'localhost'].includes(url.hostname);
  if (!(local ? ['http:', 'https:'] : ['https:']).includes(url.protocol)
      || url.username || url.password || url.search || url.hash || url.pathname !== GFLOW_SYNC_PATH) {
    throw new Error('扩展仅连接本机或 HTTPS 远程 GFlow；请重新复制配置');
  }
  const connectionToken = String(value.connectionToken || '').trim();
  if (!connectionToken || connectionToken.length > 512 || /[\x00-\x1f\x7f]/.test(connectionToken)) {
    throw new Error('连接凭据无效，请重新复制 GFlow 扩展配置');
  }
  return { product: 'GFlow', apiUrl: url.href, connectionToken };
}

function validateEmail(value) {
  const email = String(value || '').trim().toLowerCase();
  if (email.length > 254 || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
    throw new Error('请填写当前 Google 账号邮箱');
  }
  return email;
}

function connectionPermission(apiUrl) {
  const url = new URL(apiUrl);
  return url.protocol + '//' + url.hostname + '/*';
}

function normalizeCookieDomain(domain) {
  return String(domain || '').replace(/^\./, '').toLowerCase();
}

function isGoogleAccountCookieDomain(domain) {
  return normalizeCookieDomain(domain) === 'google.com';
}

function deduplicateCookies(cookies) {
  const map = new Map();
  for (const c of cookies) {
    if (!c || !c.name || !c.value) continue;
    const partitionSite = c.partitionKey && c.partitionKey.topLevelSite ? c.partitionKey.topLevelSite : '';
    const key = [c.name, c.domain, c.path, c.storeId, partitionSite].join('\u0000');
    if (!map.has(key)) map.set(key, c);
  }
  return Array.from(map.values());
}

function serializeCookieStorage(cookies) {
  const payload = cookies
    .filter(cookie => cookie && cookie.name && cookie.value)
    .map(cookie => {
      const item = {
        name: cookie.name,
        value: cookie.value,
        domain: cookie.domain || '',
        path: cookie.path || '/',
        secure: Boolean(cookie.secure),
        httpOnly: Boolean(cookie.httpOnly),
        hostOnly: Boolean(cookie.hostOnly),
        session: Boolean(cookie.session)
      };
      if (Number.isFinite(cookie.expirationDate)) item.expirationDate = cookie.expirationDate;
      if (cookie.sameSite) item.sameSite = cookie.sameSite;
      if (cookie.partitionKey && typeof cookie.partitionKey === 'object') item.partitionKey = cookie.partitionKey;
      return item;
    });
  return JSON.stringify(payload);
}

function serializeFlowCookies(cookies) {
  const deduped = deduplicateCookies(cookies);
  const targets = [new URL('https://flow.google.com/projects'), new URL('https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute')];
  const selected = deduped.filter(cookie => {
    const domain = normalizeCookieDomain(cookie.domain);
    if (cookie.partitionKey || cookie.partitionKeyOpaque) return false;
    if (cookie.expirationDate && cookie.expirationDate <= Date.now() / 1000) return false;
    const path = cookie.path || '/';
    return targets.some(target => {
      const hostMatches = cookie.hostOnly ? target.hostname === domain
        : target.hostname === domain || target.hostname.endsWith('.' + domain);
      const pathMatches = target.pathname === path || target.pathname.startsWith(path.endsWith('/') ? path : path + '/');
      return hostMatches && pathMatches;
    });
  });
  const names = new Set(selected.map(cookie => cookie.name));
  if (!['OSID', '__Secure-OSID'].some(name => names.has(name))
      || !['SID', 'HSID', 'SSID', 'APISID', 'SAPISID', '__Secure-1PSID', '__Secure-3PSID'].some(name => names.has(name))) {
    throw new Error('未找到完整 Flow 登录态，请先在本浏览器登录 flow.google.com，再重试');
  }
  return serializeCookieStorage(selected);
}