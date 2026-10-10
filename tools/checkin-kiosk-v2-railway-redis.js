// CLAUDE: prints the public Redis address of the Railway project linked to this folder.
// CLAUDE: Talks to Railway's API directly with the login token the railway CLI already saved,
// CLAUDE: so it does not depend on the railway variables command. Creates the public TCP proxy if it is missing.
// CLAUDE: Run from the project folder: node tools/checkin-kiosk-v2-railway-redis.js
const fs = require('fs');
const path = require('path');
const os = require('os');

const say = m => process.stderr.write(m + '\n');
const fail = m => { say(m); process.exit(1); };
const sleep = ms => new Promise(r => setTimeout(r, ms));

let cfg;
try { cfg = JSON.parse(fs.readFileSync(path.join(os.homedir(), '.railway', 'config.json'), 'utf8')); }
catch (e) { fail('Railway login file not found in ~/.railway/config.json'); }
const token = process.env.RAILWAY_API_TOKEN || (cfg.user && (cfg.user.accessToken || cfg.user.token));
if (!token) fail('No Railway login token found. The railway CLI is not logged in.');

const projects = cfg.projects || {};
const cwd = fs.realpathSync(process.cwd());
const link = projects[cwd] || projects[process.cwd()] || Object.values(projects).find(p => p.projectPath && fs.existsSync(p.projectPath) && fs.realpathSync(p.projectPath) === cwd);
if (!link || !link.project) fail('This folder is not linked to a Railway project.');

async function gql(query, variables) {
  const r = await fetch('https://backboard.railway.com/graphql/v2', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + token },
    body: JSON.stringify({ query, variables })
  });
  const j = await r.json().catch(() => ({}));
  if (j.errors && j.errors.length) throw new Error(j.errors.map(e => e.message).join('; '));
  if (!j.data) throw new Error('Railway API answered with status ' + r.status);
  return j.data;
}

(async () => {
  const projectId = link.project;
  const p = await gql(`query($id: String!) { project(id: $id) {
    environments { edges { node { id name } } }
    services { edges { node { id name } } } } }`, { id: projectId });
  const envs = p.project.environments.edges.map(e => e.node);
  const services = p.project.services.edges.map(e => e.node);
  const environmentId = link.environment || (envs.find(e => e.name === 'production') || envs[0] || {}).id;
  const redis = services.find(s => s.id === link.service && /redis/i.test(s.name)) || services.find(s => /redis/i.test(s.name));
  if (!environmentId) fail('No environment found in the Railway project.');
  if (!redis) fail('No Redis service found in the Railway project. Services: ' + services.map(s => s.name).join(', '));

  const readVars = () => gql(`query($p: String!, $e: String!, $s: String!) { variables(projectId: $p, environmentId: $e, serviceId: $s) }`,
    { p: projectId, e: environmentId, s: redis.id }).then(d => d.variables || {});

  let triedProxy = false;
  for (let i = 0; i < 30; i++) {
    const v = await readVars();
    if (v.REDIS_PUBLIC_URL && !/\$\{/.test(v.REDIS_PUBLIC_URL)) { process.stdout.write(v.REDIS_PUBLIC_URL); return; }
    const pw = v.REDIS_PASSWORD || v.REDISPASSWORD;
    const host = v.RAILWAY_TCP_PROXY_DOMAIN, port = v.RAILWAY_TCP_PROXY_PORT;
    if (pw && host && port) { process.stdout.write(`redis://default:${pw}@${host}:${port}`); return; }
    if (!host && !triedProxy) {
      triedProxy = true;
      try {
        await gql(`mutation($i: TCPProxyCreateInput!) { tcpProxyCreate(input: $i) { domain proxyPort } }`,
          { i: { serviceId: redis.id, environmentId, applicationPort: 6379 } });
        say('Turned on the public address for Redis.');
      } catch (e) { say('Public address request: ' + e.message); }
    }
    say('waiting for Railway Redis');
    await sleep(10000);
  }
  fail('Railway Redis did not report a public address after five minutes.');
})().catch(e => fail('Railway API error: ' + e.message));
