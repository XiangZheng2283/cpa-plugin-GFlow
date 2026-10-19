"""模拟 Chrome 扩展 API，不安装扩展、不访问浏览器 Cookie。"""
import json
from pathlib import Path
import shutil
import subprocess
import unittest

EXTENSION = Path(__file__).resolve().parents[1] / "browser-extension"
HARNESS = r"""
const vm = require('node:vm'), assert = require('node:assert/strict');
const saved = {};
const listeners = {};
let uploads = 0, responseStatus = 200, responseBody = {success:true, action:"updated"}, cookieReads = 0;
const store = {
  async get(keys) { const result = {}; for (const key of Array.isArray(keys) ? keys : [keys]) result[key] = saved[key]; return result; },
  async set(values) { Object.assign(saved, structuredClone(values)); },
  async remove(keys) { for (const key of keys) delete saved[key]; }
};
function listener(name) { return { addListener(callback) { listeners[name] = callback; } }; }
const context = vm.createContext({
  URL, AbortController, setTimeout, clearTimeout, console,
  chrome: {
    storage: { local: store },
    permissions: { async contains(request) { assert.equal(request.origins[0], 'http://127.0.0.1/*'); return true; } },
    alarms: { async clear() {}, async create() {}, onAlarm: listener('alarm') },
    runtime: { id: 'gflow-test', onMessage: listener('message'), onStartup: listener('startup'), onInstalled: listener('installed') },
    action: { async setBadgeText() {}, async setBadgeBackgroundColor() {} },
    tabs: { async query() { return [{ id: 42 }]; } },
    cookies: { async getAllCookieStores() { return [{ id: "profile-test", tabIds: [42] }]; },
      async getAll(request) {
      assert.ok(['https://flow.google.com/projects', 'https://flow.google.com/_/AiSandboxAngularFrontend/data/batchexecute'].includes(request.url));
      assert.equal(request.storeId, "profile-test"); cookieReads++;
      return [
        { name:'SID', value:'fake-google', domain:'.google.com', path:'/' },
        { name:'__Secure-OSID', value:'fake-flow', domain:'flow.google.com', path:'/', hostOnly:true },
        { name:'unrelated', value:'never-upload', domain:'.unrelated.test', path:'/' }
      ];
    } }
  },
  fetch: async (url, options) => {
    uploads++;
    assert.equal(url, 'http://127.0.0.1:8317/v0/management/plugins/gflow/api/extension-sync');
    assert.equal(options.method, 'POST');
    assert.equal(options.headers.Authorization, 'Bearer fake-management');
    assert.equal(options.redirect, 'error');
    assert.equal(options.credentials, 'omit');
    const payload = JSON.parse(options.body);
    assert.equal(payload.login_account, 'example@example.test');
    assert.ok(!payload.google_cookies.includes('never-upload'));
    return { status:responseStatus, ok:responseStatus === 200, json:async () => responseBody };
  },
  importScripts() {}
});
vm.runInContext(COMMON, context);
vm.runInContext(BACKGROUND, context);
(async () => {
  const connection = {product:'GFlow', apiUrl:'http://127.0.0.1:8317/v0/management/plugins/gflow/api/extension-sync', connectionToken:'fake-management'};
  for (const apiUrl of ['http://cpa.example.test/v0/management/plugins/gflow/api/extension-sync','https://user:pw@cpa.example.test/v0/management/plugins/gflow/api/extension-sync','http://127.0.0.1:8317/v8/management/config','http://127.0.0.1:8317/v0/management/plugins/gflow/api/extension-sync?secret=x']) {
    assert.throws(() => context.validateConnection({...connection,apiUrl}));
  }
  const remote = context.validateConnection({...connection,apiUrl:'https://cpa.example.test/v0/management/plugins/gflow/api/extension-sync'});
  assert.equal(context.connectionPermission(remote.apiUrl), 'https://cpa.example.test/*');
  console.log('PASS plaintext remote destinations and unrelated management routes rejected; HTTPS remote host scoped');
  await context.saveConfiguration({connection,loginAccount:'Example@example.test',autoSync:true});
  const publicState = await context.publicState();
  assert.equal(publicState.configured,true);
  assert.ok(!JSON.stringify(publicState).includes('fake-management'));
  assert.equal(cookieReads,0);
  console.log('PASS setup stores connection only locally and does not read cookies');
  const results = await Promise.all([context.syncNow(),context.syncNow()]);
  assert.equal(uploads,1);
  assert.ok(results.every(result => result.success));
  assert.equal(cookieReads,2);
  assert.ok(!JSON.stringify(saved.lastSync).includes('fake-management'));
  assert.ok(!JSON.stringify(saved.lastSync).includes('fake-google'));
  console.log('PASS concurrent sync uses one request and only matching Flow cookies');
  responseStatus=401;
  const failure = await context.syncNow();
  assert.equal(failure.success,false);
  assert.match(failure.message,/凭据失效/);
  console.log('PASS rejected credentials produce a clear recovery instruction');
  responseStatus=422;
  responseBody={success:false,error:{code:"gflow_google_auth_rejected",message:"Google 未接受当前登录态，请刷新 Flow 项目页后再同步。"}};
  const rejected=await context.syncNow();
  assert.match(rejected.message,/Google 未接受/);
  assert.match(rejected.message,/gflow_google_auth_rejected/);
  responseStatus=200;
  const oldBackend=await context.syncNow();
  assert.equal(oldBackend.success,false);
  assert.match(oldBackend.message,/Google 未接受/);
  responseStatus=500;
  responseBody={error:{message:"Cookie=do-not-echo-access_token"}};
  const privateError=await context.syncNow();
  assert.ok(!privateError.message.includes("do-not-echo"));
  console.log("PASS server diagnostics are displayed without accepting false success or leaking cookies");
  const envelope = await new Promise(resolve => listeners.message({action:'sync'}, {id:'gflow-test'}, resolve));
  assert.equal(envelope.ok, true);
  assert.equal(envelope.success, false);
  assert.equal(envelope.result.success, false);
  assert.ok(!JSON.stringify(saved.logs).includes('example@example.test'));
  assert.equal(listeners.message({action:'sync'},{id:'another-extension'},()=>{}),false);
  console.log('PASS messages from other extensions are ignored');
})().catch(error => { console.error(error); process.exitCode=1; });
"""


class ExtensionBrowserTest(unittest.TestCase):
    def test_chrome_api_simulation(self):
        node = shutil.which("node")
        self.assertIsNotNone(node)
        common = (EXTENSION / "common.js").read_text(encoding="utf-8")
        background = (EXTENSION / "background.js").read_text(encoding="utf-8")
        script = "const COMMON = " + json.dumps(common) + "; const BACKGROUND = " + json.dumps(background) + ";\n" + HARNESS
        result = subprocess.run([node, "-"], input=script, text=True, capture_output=True, encoding="utf-8", timeout=20)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(result.stdout.count("PASS "), 6, result.stdout)
        print(result.stdout, end="")


if __name__ == "__main__":
    unittest.main()
