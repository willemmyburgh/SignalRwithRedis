const $ = id => document.getElementById(id);
const short = s => (s || "?").slice(0, 6);
const state = { id: null, server: null, rooms: new Set(), virtual: [], rx: 0, flood: null, burst: null, reactions: 0 };

// ---------- helpers ----------
// Each live server owns a colour slot (assigned by the cluster, see ClusterReporter), so colours are distinct and
// every device shows the same colour for the same server. Chip text also carries the slot number (colour-blind friendly).
const PALETTE = ["#e6194b", "#3b82f6", "#16a34a", "#f59e0b", "#9333ea", "#0891b2", "#db2777", "#78350f", "#65a30d", "#1e3a8a", "#ea580c", "#475569"];
const slots = {};
function slotOf(server) { return slots[server]; }
function color(server) {
  const i = slotOf(server);
  if (i === undefined) return "#8b95a1";
  return i < PALETTE.length ? PALETTE[i] : `hsl(${(i * 137) % 360} 60% 40%)`;
}
function chip(server, text) {
  const s = document.createElement("span"); s.className = "chip"; s.style.background = color(server);
  const i = slotOf(server); s.textContent = text ?? `${i === undefined ? "" : "#" + (i + 1) + " "}${short(server)}`; return s;
}
function log(msg) {
  const li = document.createElement("li"); li.textContent = `${new Date().toLocaleTimeString()}  ${msg}`;
  $("log").prepend(li); while ($("log").children.length > 80) $("log").lastChild.remove();
}
const retryPolicy = { nextRetryDelayInMilliseconds: c => Math.min(500 + c.previousRetryCount * 500, 5000) }; // never give up
const sleep = ms => new Promise(r => setTimeout(r, ms));
// Server-requested rebalance: drop the socket and reconnect; the load balancer picks the emptiest server.
async function bounce(c, reason) {
  await sleep(Math.random() * 1500); // jitter so a batch doesn't reconnect in lockstep
  if (c._closed) return;
  if (c === conn) { disconnectedAt = performance.now(); lostServer = state.server; rebalancing = reason === "drain" ? "drain" : "rebalance"; }
  try { await c.stop(); } catch { /* ignore */ }
  while (!c._closed) { try { await c.start(); return; } catch { await sleep(1500); } }
}
function build(url, quiet) {
  // WebSockets only + skipNegotiation: no sticky sessions needed, any LB algorithm works.
  const c = new signalR.HubConnectionBuilder()
    .withUrl(url, { skipNegotiation: true, transport: signalR.HttpTransportType.WebSockets })
    .withAutomaticReconnect(retryPolicy)
    .configureLogging(quiet ? signalR.LogLevel.None : signalR.LogLevel.Warning)
    .build();
  c.on("Reconnect", reason => bounce(c, reason));
  c.keepAliveIntervalInMilliseconds = 5000; c.serverTimeoutInMilliseconds = 15000; // notice dead servers sooner
  return c;
}
const pct = (arr, p) => arr.length ? arr[Math.min(arr.length - 1, Math.floor(p * arr.length))] : NaN;
const fmt = n => Number.isFinite(n) ? n.toFixed(1) : "–";

// ---------- connection ----------
// Browsers don't expose the real device name (privacy), so naming is best effort, in this order:
//   1. a name the user typed (saved in this browser)      2. the device's network name, looked up by the server
//   3. what the browser reveals (iPhone, Pixel 7, Windows, Mac …) + browser.
// The server numbers duplicates, so two browsers on one device show as "Mac · Chrome" and "Mac · Safari",
// two tabs of the same browser as "Mac · Chrome" and "Mac · Chrome #2".
function deviceInfo() {
  const ua = navigator.userAgent;
  let device = /iPhone/.test(ua) ? "iPhone" : /iPad/.test(ua) ? "iPad" : /Android/.test(ua) ? "Android" : /Windows/.test(ua) ? "Windows"
    : /Mac OS X/.test(ua) ? "Mac" : /CrOS/.test(ua) ? "Chromebook" : /Linux/.test(ua) ? "Linux" : "Device";
  const model = /Android[^;)]*; ([^;)]+)/.exec(ua)?.[1]?.trim();
  if (model && model !== "K" && !/^(Mobile|wv)$/.test(model)) device = model;
  const browser = /Edg\//.test(ua) ? "Edge" : /OPR\//.test(ua) ? "Opera" : /Firefox\//.test(ua) ? "Firefox" : /(Chrome|CriOS)\//.test(ua) ? "Chrome" : /Safari\//.test(ua) ? "Safari" : "Browser";
  return { device, browser };
}
let name = new URLSearchParams(location.search).get("name") || localStorage.getItem("customName") || "";
let cid = sessionStorage.getItem("cid"); // per tab; lets the server recognise this tab after a reconnect
if (!cid) { cid = Math.random().toString(36).slice(2, 12); sessionStorage.setItem("cid", cid); }
const { device, browser } = deviceInfo();
const nameQuery = name ? `name=${encodeURIComponent(name)}` : `auto=1&device=${encodeURIComponent(device)}&browser=${encodeURIComponent(browser)}`;
$("name").value = name || `${device} · ${browser}`;
const conn = build(`/chat?${nameQuery}&cid=${cid}`);
let disconnectedAt = 0, lostServer = null, rebalancing = "";

function setStatus(text, cls) { $("status").textContent = text; $("status").className = "pill " + (cls || ""); }

let myBuild = 0;
conn.on("Connected", async (server, id, build) => {
  // A server from a newer deployment: reload to pick up the new page (never reload for older/equal, so rolling upgrades can't loop).
  if (!myBuild) myBuild = build; else if (build > myBuild) { log("new version deployed, reloading…"); setTimeout(() => location.reload(), 500); }
  state.id = id; state.server = server;
  $("myServer").replaceChildren("on ", chip(server));
  setStatus("connected", "ok");
  if (disconnectedAt) {
    const secs = ((performance.now() - disconnectedAt) / 1000).toFixed(1);
    log(rebalancing ? `${rebalancing === "drain" ? "server draining" : "rebalanced"}: moved ${short(lostServer)} → ${short(server)} in ${secs}s` : `reconnected to ${short(server)} after ${secs}s (was on ${short(lostServer)})`);
    disconnectedAt = 0; rebalancing = "";
  }
  for (const r of [...state.rooms, ...(state.flood?.temp || [])]) await conn.invoke("JoinRoom", r); // groups are per-connection: re-join
});
conn.onreconnecting(() => { disconnectedAt = performance.now(); lostServer = state.server; setStatus("reconnecting…", ""); log(`lost connection to ${short(state.server)}, retrying…`); });
conn.onclose(() => { setStatus("disconnected", "bad"); });

async function start() {
  try { await conn.start(); } catch { setStatus("retrying…"); setTimeout(start, 2000); }
}

// ---------- rx counter ----------
setInterval(() => { $("rx").textContent = state.rx; state.rx = 0; $("reactRate").textContent = state.reactions; state.reactions = 0; }, 1000);

// ---------- name ----------
$("name").onchange = async () => {
  const typed = $("name").value.trim(); if (!typed) return;
  localStorage.setItem("customName", typed);
  if (conn.state === "Connected") name = $("name").value = await conn.invoke("SetName", typed);
};
conn.on("YourName", n => { name = n; $("name").value = n; });

// ---------- invite ----------
$("inviteBtn").onclick = () => {
  const box = $("invite"); box.hidden = !box.hidden; if (box.hidden) return;
  $("inviteUrl").textContent = location.origin;
  const qr = qrcode(0, "M"); qr.addData(location.origin); qr.make();
  const n = qr.getModuleCount(), px = 4, c = $("qr"); c.width = c.height = (n + 2) * px;
  const g = c.getContext("2d"); g.fillStyle = "#fff"; g.fillRect(0, 0, c.width, c.height); g.fillStyle = "#000";
  for (let r = 0; r < n; r++) for (let q = 0; q < n; q++) if (qr.isDark(r, q)) g.fillRect((q + 1) * px, (r + 1) * px, px, px);
};

// ---------- tabs ----------
document.querySelectorAll("#tabs button").forEach(b => b.onclick = () => {
  document.querySelectorAll("#tabs button").forEach(x => x.classList.toggle("on", x === b));
  document.querySelectorAll(".tab").forEach(t => t.hidden = t.id !== "tab-" + b.dataset.tab);
});

// ---------- cluster panel ----------
$("rebal").onchange = () => conn.invoke("SetRebalance", $("rebal").checked);
conn.on("ClusterStats", s => {
  for (const k in slots) delete slots[k];
  s.servers.forEach(x => slots[x.name] = x.slot);
  $("rebal").checked = s.rebalance;
  const total = s.servers.reduce((a, x) => a + x.connections, 0), virt = s.servers.reduce((a, x) => a + x.virtualConnections, 0);
  $("clusterSummary").textContent = `· ${total} connections (${virt} virtual) on ${s.servers.length} servers`;
  const max = Math.max(1, ...s.servers.map(x => x.connections));
  $("servers").replaceChildren(...s.servers.map(x => {
    const row = document.createElement("div"); row.className = "srvrow" + (x.name === state.server ? " mine" : "");
    const bar = document.createElement("div"); bar.className = "bar"; const i = document.createElement("i");
    i.style.width = (100 * x.connections / max) + "%"; i.style.background = color(x.name); bar.append(i);
    const txt = document.createElement("span"); txt.textContent = `${x.draining ? "🔧 draining · " : ""}${x.connections} conn · ${x.callsPerSec}/s`;
    row.append(chip(x.name), bar, txt); return row;
  }));
  $("redisLine").textContent = s.redis.up ? `Redis ${s.redis.version} primary ${s.redis.primary} (${s.redis.replicas} replicas): ${s.redis.opsPerSec} ops/s · ${s.redis.connectedClients} connections · ${s.redis.memory}` : "Redis: no primary reachable (failing over?)";
  $("clientsSummary").textContent = `Clients online (${s.clients.length})`;
  $("clients").replaceChildren(...s.clients.map(c => chip(c.server, c.name)));
});
conn.on("Notice", t => { log(t); });

// ---------- chat ----------
function addMsg(user, text, server, room) {
  state.rx++;
  const li = document.createElement("li");
  if (user === name) li.className = "mine";
  const aud = document.createElement("span"); aud.className = "aud" + (room ? " room" : "");
  aud.textContent = room ? `🔒 Room ${room} only` : "👥 Everyone";
  const who = document.createElement("span"); who.className = "who"; who.textContent = user;
  li.append(chip(server), aud, who, `: ${text}`); $("messages").prepend(li);
  while ($("messages").children.length > 100) $("messages").lastChild.remove();
}
conn.on("ReceiveMessage", (u, m, s) => addMsg(u, m, s));
conn.on("RoomMessage", (u, m, s, room) => addMsg(u, m, s, room));
for (const r of ["A", "B", "C"]) {
  const b = document.createElement("button"); b.textContent = "Room " + r; $("rooms").append(b);
  b.onclick = async () => {
    const joined = !state.rooms.has(r);
    await conn.invoke(joined ? "JoinRoom" : "LeaveRoom", r);
    joined ? state.rooms.add(r) : state.rooms.delete(r);
    b.classList.toggle("on", joined);
    $("sendTo").replaceChildren(new Option("👥 Everyone", ""), ...[...state.rooms].sort().map(x => new Option("🔒 Room " + x, x)));
    if (joined) $("sendTo").value = r; // joining a room switches the composer to it
  };
}
$("send").onclick = async () => {
  const m = $("message").value.trim(); if (!m) return;
  const room = $("sendTo").value;
  await (room ? conn.invoke("SendToRoom", room, name, m) : conn.invoke("SendMessage", name, m, 0));
  $("message").value = "";
};
$("message").onkeydown = e => { if (e.key === "Enter") $("send").click(); };

// ---------- canvas ----------
const board = $("board"), g = board.getContext("2d");
let pen = "#2563eb", drawing = false, last = null, buf = [];
// On touch screens a full-size drawing surface would swallow every swipe and make the page unscrollable,
// so drawing there is an explicit mode. Mouse users draw straight away.
let drawOn = !matchMedia("(pointer: coarse)").matches;
function applyDrawMode() {
  board.className = drawOn ? "on" : "off";
  $("drawMode").className = drawOn ? "" : "off";
  $("drawMode").textContent = drawOn ? "✏️ Drawing" : "✏️ Draw";
  $("drawHint").textContent = drawOn ? "Drawing is on: swipes on the canvas paint. Tap “Drawing” to scroll the page again." : "Scrolling is on. Tap “Draw” to paint on the canvas.";
}
$("drawMode").onclick = () => { drawOn = !drawOn; applyDrawMode(); };
for (const c of ["#2563eb", "#dc2626", "#16a34a", "#d97706", "#111111"]) {
  const b = document.createElement("button"); b.style.background = c; b.onclick = () => { pen = c; [...$("colors").children].forEach(x => x.classList.toggle("on", x === b)); };
  if (c === pen) b.classList.add("on"); $("colors").append(b);
}
function seg(c, x0, y0, x1, y1) {
  g.strokeStyle = c; g.lineWidth = 4; g.lineCap = "round"; g.beginPath();
  g.moveTo(x0 * board.width, y0 * board.height); g.lineTo(x1 * board.width, y1 * board.height); g.stroke();
}
const pos = e => { const r = board.getBoundingClientRect(); return [(e.clientX - r.left) / r.width, (e.clientY - r.top) / r.height]; };
board.onpointerdown = e => { if (!drawOn) return; drawing = true; last = pos(e); board.setPointerCapture(e.pointerId); };
board.onpointerup = board.onpointercancel = () => { drawing = false; };
board.onpointermove = e => {
  if (!drawing) return; const p = pos(e);
  seg(pen, ...last, ...p); buf.push(...last, ...p); last = p;
};
setInterval(() => { if (buf.length && conn.state === "Connected") { conn.send("Draw", pen, buf); buf = []; } }, 40);
conn.on("Draw", (c, s) => { state.rx++; for (let i = 0; i + 3 < s.length; i += 4) seg(c, s[i], s[i + 1], s[i + 2], s[i + 3]); });
conn.on("ClearCanvas", () => g.clearRect(0, 0, board.width, board.height));
applyDrawMode();
$("clear").onclick = () => conn.invoke("ClearCanvas");

// ---------- poll + reactions ----------
const options = ["🍕 Pizza", "🍔 Burger", "🌮 Taco", "🥗 Salad"];
options.forEach((o, i) => {
  const row = document.createElement("div"); row.className = "pollrow"; row.id = "p" + i;
  row.innerHTML = `<span>${o}</span><div class="bar"><i></i></div><b>0</b>`;
  row.onclick = () => conn.send("Vote", i); $("poll").append(row);
});
conn.on("PollUpdate", t => {
  state.rx++; const max = Math.max(1, ...t);
  t.forEach((v, i) => { const r = $("p" + i); r.querySelector("i").style.width = (100 * v / max) + "%"; r.querySelector("b").textContent = v; });
});
$("resetPoll").onclick = () => conn.invoke("ResetPoll");
for (const e of ["👍", "❤️", "🎉", "🔥", "😂", "🚀"]) {
  const b = document.createElement("button"); b.textContent = e; b.onclick = () => conn.send("React", e); $("emojis").append(b);
}
conn.on("Reaction", (emoji, server) => {
  state.rx++; state.reactions++;
  const d = document.createElement("div"); d.className = "float"; d.textContent = emoji;
  d.style.left = Math.random() * 90 + "vw"; d.style.filter = `drop-shadow(0 0 6px ${color(server)})`;
  $("fx").append(d); setTimeout(() => d.remove(), 3000);
});

// ---------- experiments: latency ----------
setInterval(async () => {
  if (conn.state !== "Connected") return;
  const t = performance.now(); await conn.invoke("Ping"); $("rtt").textContent = Math.round(performance.now() - t) + " ms";
}, 3000);
$("pingBtn").onclick = async () => {
  const xs = [];
  for (let i = 0; i < 20; i++) { const t = performance.now(); await conn.invoke("Ping"); xs.push(performance.now() - t); }
  xs.sort((a, b) => a - b);
  $("pingOut").textContent = `min ${fmt(xs[0])} · p50 ${fmt(pct(xs, .5))} · p95 ${fmt(pct(xs, .95))} · max ${fmt(xs.at(-1))} ms`;
};

// ---------- experiments: server burst ----------
const reports = {};
$("burstBtn").onclick = async () => {
  const r = await conn.invoke("StartBurst", +$("burstCount").value, +$("burstSize").value, +$("burstRate").value, $("burstAudience").value);
  if (r === "busy") log("a burst is already running on this server");
};
conn.on("BurstStart", async (id, count, server, by, bytes, audience) => {
  state.burst = { id, expected: count, got: 0, last: 0, ooo: 0, first: 0, lastAt: 0, audience, autoRoom: null };
  // Room audiences only reach members: a device in no room joins one for the duration of the burst.
  if (audience && state.rooms.size === 0) { const r = "ABC"[Math.floor(Math.random() * 3)]; state.burst.autoRoom = r; await conn.invoke("JoinRoom", r); }
  $("burstLive").textContent = `Burst ${id}: ${count} × ${bytes}B from ${short(server)}, started by ${by}…`;
  log(`burst ${id} started by ${by} on ${short(server)} (${count} msgs)`);
});
conn.on("BurstMsg", (id, seq) => {
  const b = state.burst; if (!b || b.id !== id) return;
  const now = performance.now(); b.first ||= now; b.lastAt = now; b.got++; state.rx++;
  if (seq < b.last) b.ooo++; b.last = Math.max(b.last, seq);
  if (b.got % 50 === 0) $("burstLive").textContent = `Burst ${id}: received ${b.got}/${b.expected}`;
});
conn.on("BurstEnd", (id, count, ms, perRoom) => setTimeout(() => {
  const b = state.burst; if (!b || b.id !== id) return;
  if (b.audience) {
    const mine = b.autoRoom ? new Set([b.autoRoom]) : state.rooms;
    b.expected = [...mine].reduce((n, r) => n + (perRoom?.[r] || 0), 0);
    if (b.autoRoom) conn.invoke("LeaveRoom", b.autoRoom);
    if (!b.expected) { $("burstLive").textContent = `Burst ${id}: none of your rooms were addressed`; return; }
  }
  const ms = Math.max(1, b.lastAt - b.first);
  $("burstLive").textContent = `Burst ${id} done: ${b.got}/${b.expected} received (${(100 * b.got / b.expected).toFixed(1)}%)`;
  conn.send("ReportBurst", id, b.got, b.expected, Math.round(ms), b.ooo);
}, 1000));
conn.on("BurstReport", (who, server, id, got, expected, ms, ooo) => {
  if (state.burst?.id !== id) { for (const k in reports) delete reports[k]; }
  reports[who] = { server, got, expected, ms, ooo };
  const body = $("burstTable").tBodies[0]; body.replaceChildren();
  for (const [n, r] of Object.entries(reports)) {
    const tr = body.insertRow(); tr.insertCell().textContent = n; tr.insertCell().append(chip(r.server));
    tr.insertCell().textContent = `${r.got}/${r.expected}`; tr.insertCell().textContent = `${Math.round(r.got * 1000 / r.ms)}/s`; tr.insertCell().textContent = r.ooo;
  }
});

// ---------- experiments: flood ----------
async function startFlood(rate, secs, audience = "everyone") {
  stopFlood();
  const f = state.flood = { sent: 0, got: 0, lat: [], bySrv: {}, timer: null, until: performance.now() + secs * 1000, audience, temp: [] };
  if (audience !== "everyone") {
    // join two random rooms for the test (rooms the user already joined are kept as they are)
    for (const r of ["A", "B", "C"].sort(() => Math.random() - .5).slice(0, 2)) if (!state.rooms.has(r)) { f.temp.push(r); await conn.invoke("JoinRoom", r); }
  }
  const per = Math.max(1, Math.round(rate / 10));
  f.timer = setInterval(() => {
    if (performance.now() > f.until || conn.state !== "Connected") return stopFlood(true);
    for (let i = 0; i < per; i++) {
      const toRoom = f.audience === "rooms" || (f.audience === "mixed" && Math.random() < .5);
      if (toRoom) conn.send("NoiseToRoom", "ABC"[Math.floor(Math.random() * 3)], state.id, performance.now());
      else conn.send("Noise", state.id, performance.now());
      f.sent++;
    }
  }, 1000 / (rate / per));
  log(`flood started: ${rate}/s for ${secs}s, audience ${audience}`);
}
function stopFlood(done) {
  const f = state.flood; if (!f?.timer) return;
  clearInterval(f.timer); f.timer = null; if (done) log("flood finished");
  for (const r of f.temp) conn.invoke("LeaveRoom", r).catch(() => {}); f.temp = [];
}
conn.on("Noise", (from, server, t) => {
  state.rx++; const f = state.flood; if (!f) return; f.got++;
  if (from === state.id) { f.lat.push(performance.now() - t); if (f.lat.length > 3000) f.lat.shift(); }
});
setInterval(() => {
  const f = state.flood; if (!f) return;
  const l = [...f.lat].sort((a, b) => a - b);
  $("floodOut").textContent = `${f.timer ? "running" : "stopped"} · sent ${f.sent} · received (from everyone) ${f.got} · own latency via Redis p50 ${fmt(pct(l, .5))} p95 ${fmt(pct(l, .95))} p99 ${fmt(pct(l, .99))} ms`;
}, 500);

// ---------- experiments: virtual users ----------
async function addVirtual(n) {
  n = Math.min(n, 1000 - state.virtual.length); log(`opening ${n} virtual connections`);
  for (let i = 0; i < n; i += 10) {
    await Promise.all(Array.from({ length: Math.min(10, n - i) }, async () => {
      const c = build(`/chat?virtual=1`, true);
      try { await c.start(); state.virtual.push(c); $("vuCount").textContent = state.virtual.length; } catch { /* ignore */ }
    }));
  }
}
async function closeVirtual() {
  const vs = state.virtual.splice(0); vs.forEach(v => v._closed = true); await Promise.all(vs.map(v => v.stop())); $("vuCount").textContent = 0;
}
document.querySelectorAll(".vu").forEach(b => b.onclick = () => $("onAll").checked ? conn.invoke("RunOnAll", "virtual", +b.dataset.n, 0) : addVirtual(+b.dataset.n));
$("vuStop").onclick = () => closeVirtual();

// ---------- remote commands ----------
$("floodBtn").onclick = () => $("onAll").checked ? conn.invoke("RunOnAll", "flood", +$("floodRate").value, +$("floodSecs").value, $("floodAudience").value) : startFlood(+$("floodRate").value, +$("floodSecs").value, $("floodAudience").value);
$("stopAll").onclick = () => conn.invoke("RunOnAll", "stop", 0, 0);
conn.on("Command", (cmd, a, b, by, arg) => {
  if (!$("accept").checked) return;
  log(`command from ${by}: ${cmd} ${a || ""} ${b || ""}`);
  if (cmd === "virtual") addVirtual(a);
  else if (cmd === "flood") startFlood(a, b, arg || "everyone");
  else if (cmd === "stop") { stopFlood(true); closeVirtual(); }
});

// ---------- chaos ----------
$("drainBtn").onclick = () => conn.send("GracefulShutdown");
$("redisBtn").onclick = async () => log("redis: " + await conn.invoke("FailoverRedis"));
$("crashBtn").onclick = () => { if (confirm("Crash the server you are connected to?")) conn.send("CrashServer"); };

start();
