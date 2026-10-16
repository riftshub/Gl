#!/usr/bin/env node
/**
 * Unified game host — ONE Node.js process, ONE port, no Java server needed.
 * ES module (import/export); runs on Node.js 22 ("type": "module" is set in server/package.json).
 *
 *   GET /             the built web client (../client/dist, with ../client/public overrides)
 *   GET /play         opens the client with this server pre-filled in the connect screen
 *   GET /config.json  client config, patched so this server is the featured one
 *   WS  /mc           Minecraft protocol bridge into the embedded world (flying-squid)
 *   WS  /room         JSON multiplayer room: sessions, PLAYER_SHOOT, cars, hit confirms
 *   GET /healthz      health check for hosting platforms
 *
 * Folder layout:
 *   client/   the game client project; `pnpm build` there produces client/dist (HTML/JS/CSS)
 *   server/   this file + its package.json
 *
 * Environment variables (all optional):
 *   CLIENT_DIR=../client      where the client project lives (its dist/ and public/ are served)
 *   CLIENT_DIST=<CLIENT_DIR>/dist   override just the built-assets folder
 *   PUBLIC_GAME_URL=wss://trueelement.shop   address players connect to (featured server + /play link);
 *                             unset = whatever address the page was opened on
 *   PORT=8080                 port to listen on (hosts like Render/Railway set this)
 *   SERVER_NAME="Our World"   name shown in the server list
 *   MC_VERSION=1.18           game version of the embedded world
 *   MAX_PLAYERS=20
 *   WORLD_FOLDER=./world      where the world is saved (needs a persistent disk to survive restarts)
 *   GAME_MODE=0               0 survival, 1 creative, 2 adventure
 *   VIEW_DISTANCE=4            chunks sent around each player (lower = less work for small servers)
 *   KICK_TIMEOUT=120           seconds a player can go without answering pings before being dropped
 *   ENABLE_PROXY=1            also let players join other Minecraft servers through this host
 *   MC_INTERNAL_PORT=25565    internal-only port of the embedded world (bound to 127.0.0.1)
 */

import http from 'node:http'
import net from 'node:net'
import path from 'node:path'
import fs from 'node:fs'
import crypto from 'node:crypto'
import { fileURLToPath } from 'node:url'
import tty from 'node:tty'
import { PassThrough } from 'node:stream'
import express from 'express'
import compression from 'compression'
import { WebSocketServer, WebSocket } from 'ws'
import vec3 from 'vec3'

const { Vec3 } = vec3
const __dirname = path.dirname(fileURLToPath(import.meta.url))

// ───────────────────────────── Config ─────────────────────────────

const numArg = process.argv.find(x => /^\d+$/.test(x))
const PORT = Number(process.env.PORT || numArg || 8080)
const isProd = process.argv.includes('--prod') || process.env.NODE_ENV === 'production'
const SERVER_NAME = process.env.SERVER_NAME || 'Our World'
const MC_VERSION = process.env.MC_VERSION || '1.18'
const MC_INTERNAL_PORT = Number(process.env.MC_INTERNAL_PORT || 25565)
const MAX_PLAYERS = Number(process.env.MAX_PLAYERS || 20)
const WORLD_FOLDER = process.env.WORLD_FOLDER || path.join(__dirname, 'world')
const GAME_MODE = Number(process.env.GAME_MODE || 0)
const VIEW_DISTANCE = Number(process.env.VIEW_DISTANCE || 4)
// Vanilla uses 30 s; longer survives background tabs and slow world generation on small hosts
const KICK_TIMEOUT_MS = Number(process.env.KICK_TIMEOUT || 120) * 1000
const ENABLE_PROXY = process.env.ENABLE_PROXY === '1'
const PUBLIC_GAME_URL = (process.env.PUBLIC_GAME_URL || '').replace(/\/+$/, '')
const CLIENT_DIR = path.resolve(__dirname, process.env.CLIENT_DIR || path.join('..', 'client'))
const CLIENT_DIST = path.resolve(__dirname, process.env.CLIENT_DIST || path.join(CLIENT_DIR, 'dist'))
const CLIENT_PUBLIC = path.join(CLIENT_DIR, 'public')

/**
 * Guns are ordinary Minecraft items with a custom name. The server decides damage from what the
 * shooter is actually holding, so a modified client can't fake a stronger weapon.
 * Keep item names, cooldowns, pellets and spread in sync with WEAPONS in client/src/carsAndGuns.ts.
 *
 *   damage     per pellet (players have 20 health)     pellets  bullets per shot (shotgun)
 *   cooldownMs time between shots                        spread   max random aim offset, radians
 */
const WEAPON_LIST = [
  { item: 'stone_hoe', id: 'pistol', displayName: 'Pistol', color: 'gray', damage: 4, range: 64, cooldownMs: 300, pellets: 1, spread: 0.012 },
  { item: 'iron_hoe', id: 'rifle', displayName: 'Rifle', color: 'gold', damage: 6, range: 128, cooldownMs: 450, pellets: 1, spread: 0.004 },
  { item: 'golden_hoe', id: 'smg', displayName: 'SMG', color: 'yellow', damage: 2, range: 48, cooldownMs: 100, pellets: 1, spread: 0.035 },
  { item: 'iron_shovel', id: 'shotgun', displayName: 'Shotgun', color: 'red', damage: 2, range: 24, cooldownMs: 900, pellets: 8, spread: 0.08 },
  { item: 'diamond_hoe', id: 'sniper', displayName: 'Sniper', color: 'aqua', damage: 16, range: 220, cooldownMs: 1400, pellets: 1, spread: 0 },
  { item: 'netherite_hoe', id: 'minigun', displayName: 'Minigun', color: 'dark_purple', damage: 2, range: 64, cooldownMs: 70, pellets: 1, spread: 0.05 },
]
const WEAPONS = Object.fromEntries(WEAPON_LIST.map(w => [w.item, w]))

/** Cars in the garage. Keep ids in sync with CAR_MODELS in client/src/carsAndGuns.ts. */
const CAR_MODELS = [
  { id: 'sports', name: 'Sports Car' },
  { id: 'police', name: 'Police Cruiser' },
  { id: 'taxi', name: 'Taxi' },
  { id: 'pickup', name: 'Pickup Truck' },
  { id: 'monster', name: 'Monster Truck', aliases: ['monstertruck', 'truck'] },
  { id: 'kart', name: 'Go-Kart', aliases: ['gokart', 'go-kart'] },
]
const findCarModel = name => {
  const key = String(name || '').trim().toLowerCase()
  return CAR_MODELS.find(m => m.id === key || m.aliases?.includes(key))
}
const WEAPONS_BY_ID = Object.fromEntries(WEAPON_LIST.map(w => [w.id, w]))

const ROOM_STATE_MIN_INTERVAL_MS = 45 // drop STATE spam above ~20/s per player
const SHOOT_LENIENCY_MS = 60 // network jitter allowance on the fire-rate check

// ───────────────────────────── HTTP app ─────────────────────────────

const app = express()
app.set('trust proxy', true) // correct https/wss detection behind Render, Railway, Fly, nginx...
app.use(compression())

const isLocalRequest = req => /^(localhost|127\.0\.0\.1)(:\d+)?$/i.test(req.get('host') || '')

/** World address to advertise: the public game URL, or the page's own address when testing locally */
const ownServerAddress = req => {
  if (PUBLIC_GAME_URL && !isLocalRequest(req)) return `${PUBLIC_GAME_URL}/mc`
  const wsProtocol = req.protocol === 'https' ? 'wss' : 'ws'
  return `${wsProtocol}://${req.get('host')}/mc`
}

app.get('/healthz', (req, res) => {
  res.json({
    ok: true,
    world: worldStatus,
    players: mcServer?.players?.length ?? 0,
    roomSessions: sessions.size,
  })
})

// /mc and /room are WebSocket addresses the game connects to; opening them as a page just explains that
const websocketOnly = (req, res) => {
  res.status(426).type('text').send(`This is the game's WebSocket address (${ownServerAddress(req).replace(/\/mc$/, req.path)}).\nIt can't be opened as a web page - the game connects to it by itself. Server status: /healthz`)
}
app.get('/mc', websocketOnly)
app.get('/room', websocketOnly)

app.get('/play', (req, res) => {
  const params = new URLSearchParams({ ip: ownServerAddress(req), version: MC_VERSION, name: SERVER_NAME })
  res.redirect(`/?${params.toString()}`)
})

/** Reads a JSON file fresh on every call (edits to config.json apply without a restart) */
const readJson = file => {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'))
  } catch {
    return undefined
  }
}

app.get('/config.json', (req, res) => {
  const config = readJson(path.join(CLIENT_DIST, 'config.json')) ?? readJson(path.join(CLIENT_DIR, 'config.json')) ?? {}
  const publicConfig = readJson(path.join(CLIENT_PUBLIC, 'config.json')) ?? {}
  res.json({
    ...config,
    ...ENABLE_PROXY ? { defaultProxy: '' } : {}, // '' = use this host as the proxy
    promoteServers: [
      {
        ip: ownServerAddress(req),
        name: SERVER_NAME,
        description: 'Cars & guns multiplayer world',
        version: MC_VERSION,
      },
      ...ENABLE_PROXY ? config.promoteServers ?? [] : [],
    ],
    allowAutoConnect: true,
    multiplayerRoom: true,
    ...publicConfig,
  })
})

/**
 * Optional proxy so players can also join other Minecraft servers through this host.
 * Its WebSockets live on a private http.Server; the upgrade router below forwards
 * /api/vm/net/* upgrades to it, so it never competes with /mc and /room.
 */
const proxyUpgradeServer = ENABLE_PROXY ? http.createServer() : null
if (ENABLE_PROXY) {
  const { default: netApi } = await import('net-browserify')
  const { default: cors } = await import('cors')
  app.use(cors())
  app.use(netApi({ allowOrigin: '*', log: process.env.LOG === 'true', timeout: 10_000, server: proxyUpgradeServer }))
}

if (isProd) {
  // Headers needed for SharedArrayBuffer (used by the renderer's workers)
  app.use((req, res, next) => {
    res.setHeader('Cross-Origin-Opener-Policy', 'same-origin')
    res.setHeader('Cross-Origin-Embedder-Policy', 'require-corp')
    if (req.path.endsWith('.wasm')) res.setHeader('Content-Type', 'application/wasm')
    next()
  })
}

// Serve the renderer's wasm mesher from an installed package if one is present
const rendererDir = [CLIENT_DIR, __dirname]
  .map(dir => path.join(dir, 'node_modules', 'minecraft-renderer'))
  .find(dir => fs.existsSync(path.join(dir, 'package.json')))
if (rendererDir) {
  app.get('/wasm_mesher.js', (req, res) => res.sendFile(path.join(rendererDir, 'public/wasm_mesher.js')))
  app.get('/wasm_mesher_bg.wasm', (req, res) => res.sendFile(path.join(rendererDir, 'public/wasm_mesher_bg.wasm')))
}
if (!isProd) app.use('/sounds', express.static(path.join(CLIENT_DIR, 'generated/sounds')))
app.use(express.static(CLIENT_PUBLIC))
app.use(express.static(CLIENT_DIST))

// When the game page is hosted elsewhere (e.g. a static site) there's no client build here:
// show a small status page at / instead of Express's bare "Cannot GET /"
const escapeHtml = text => String(text).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' })[c])
app.get('/', (req, res) => {
  const online = worldStatus === 'running'
  const players = mcServer?.players?.length ?? 0
  res.type('html').send(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>${escapeHtml(SERVER_NAME)} server</title>
<style>body{font:16px/1.5 system-ui,sans-serif;background:#16181d;color:#e8e8e8;display:grid;place-items:center;min-height:100vh;margin:0;padding:16px}
main{max-width:460px;width:100%;background:#20232a;border-radius:12px;padding:24px}h1{font-size:22px;margin:0 0 12px}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px;background:${online ? '#3ecf6e' : '#e5534b'}}
code{background:#2b2f37;padding:2px 6px;border-radius:4px;word-break:break-all}p{margin:8px 0}</style></head>
<body><main><h1>${escapeHtml(SERVER_NAME)}</h1>
<p><span class="dot"></span>World ${online ? 'online' : escapeHtml(worldStatus)} · ${players} player${players === 1 ? '' : 's'} on</p>
<p>World address: <code>${escapeHtml(ownServerAddress(req))}</code></p>
<p>This is the game server. Play from the game website.</p></main></body></html>`)
})
if (!fs.existsSync(path.join(CLIENT_DIST, 'index.html'))) {
  console.warn(`[web] no client build at ${CLIENT_DIST} — run \`pnpm build\` in the client folder (the room and world still run)`)
}

const server = http.createServer(app)

// ───────────────────────────── Embedded world (flying-squid) ─────────────────────────────

let mcServer = null
let worldStatus = 'starting'

/**
 * The world server opens an interactive command console on stdin. Hosts like Render start the
 * app with no stdin, so that console closes immediately and the next log line crashes the whole
 * process with ERR_USE_AFTER_CLOSE. When no terminal is attached, give it an input that simply
 * stays open. (In a local terminal you keep the real console and can type server commands.)
 */
const keepWorldConsoleOpen = () => {
  if (tty.isatty(0)) return
  Object.defineProperty(process, 'stdin', { value: new PassThrough(), configurable: true, writable: true })
}

// A stray async error inside the world server shouldn't take every player offline
process.on('unhandledRejection', err => {
  console.error('[server] unhandled async error (server kept running):', err)
})

const startWorld = async () => {
  try {
    keepWorldConsoleOpen()
    const flyingSquid = await import('flying-squid')
    const createMCServer = flyingSquid.createMCServer ?? flyingSquid.default?.createMCServer
    mcServer = createMCServer({
      motd: SERVER_NAME,
      host: '127.0.0.1', // only reachable through the /mc bridge
      port: MC_INTERNAL_PORT,
      'max-players': MAX_PLAYERS,
      'online-mode': false,
      gameMode: GAME_MODE,
      difficulty: 1,
      worldFolder: WORLD_FOLDER,
      generation: { name: 'diamond_square', options: {} },
      kickTimeout: KICK_TIMEOUT_MS, // ping timeout (and login timeout)
      checkTimeoutInterval: 5_000, // how often players are pinged
      plugins: {},
      pluginsFolder: false,
      modpe: false,
      'view-distance': VIEW_DISTANCE,
      'player-list-text': { header: SERVER_NAME, footer: 'Cars & guns' },
      'everybody-op': false,
      'max-entities': 100,
      version: MC_VERSION,
    })
    worldStatus = 'running'
    console.log(`[world] embedded ${MC_VERSION} world on 127.0.0.1:${MC_INTERNAL_PORT}, saving to ${WORLD_FOLDER}`)

    mcServer.on('error', err => console.error('[world] error', err?.message ?? err))

    // Log every join/leave with the reason, so kicks (e.g. KeepAliveTimeout) show up in the host's logs
    mcServer._server?.on('connection', client => {
      client.once('login', () => console.log(`[world] ${client.username} joined (${Object.keys(mcServer._server.clients).length} connected)`))
      client.once('end', reason => {
        if (client.username) console.log(`[world] ${client.username} disconnected: ${reason || 'left'}`)
      })
    })
    mcServer.on('newPlayer', player => {
      player.once('spawned', () => {
        setTimeout(() => {
          const given = giveKit(player)
          tell(player, `§6Welcome!§f ${given.length ? `You got: ${given.map(w => w.displayName).join(', ')}.` : 'Your guns are in your inventory.'}`)
          tell(player, '§fHold a gun and §eleft-click§f to shoot, §eR§f to reload. §eF§f near your car to drive, §eB§f to pick a car. Type §e/guns§f or §e/cars§f for more.')
        }, 800)
      })
      // Top up missing guns after respawning
      player._client?.on('client_command', data => {
        if ((data?.actionId ?? data?.payload) === 0) setTimeout(() => giveKit(player), 1000)
      })
    })

    registerGameCommands()
  } catch (err) {
    worldStatus = 'failed'
    console.error('[world] failed to start the embedded world:', err)
  }
}

const itemNameOf = item => (item && mcServer?.mcData?.items?.[item.type]?.name) || null

const tell = (player, message) => {
  try { player.chat(message) } catch {}
}

const makeGun = weapon => {
  const itemId = mcServer.mcData.itemsByName[weapon.item]?.id
  if (itemId === undefined) return null
  const gun = new mcServer.PrismarineItem(itemId, 1)
  try {
    gun.customName = JSON.stringify({ text: weapon.displayName, italic: false, color: weapon.color })
  } catch {}
  return gun
}

const hasGun = (player, weapon) => {
  const itemId = mcServer.mcData.itemsByName[weapon.item]?.id
  return player.inventory.slots.some(it => it && it.type === itemId)
}

/** Puts one gun in the first free slot (hotbar first). Returns false if the inventory is full. */
const giveGun = (player, weapon) => {
  const gun = makeGun(weapon)
  if (!gun || !player.inventory) return false
  const slot = player.inventory.firstEmptyInventorySlot?.(true)
  if (slot === null || slot === undefined) return false
  player.inventory.updateSlot(slot, gun)
  return true
}

/** Gives every gun the player doesn't have yet. Returns the guns given. */
function giveKit (player) {
  const given = []
  try {
    if (!player.inventory) return given
    for (const weapon of WEAPON_LIST) {
      if (hasGun(player, weapon)) continue
      if (!giveGun(player, weapon)) break // inventory full
      given.push(weapon)
    }
  } catch (err) {
    console.warn('[world] could not give guns:', err?.message ?? err)
  }
  return given
}

const gunList = () => WEAPON_LIST.map(w => `§e${w.id}§f`).join(', ')

const giveNamedGun = (player, name) => {
  const weapon = WEAPONS_BY_ID[String(name || '').trim().toLowerCase()]
  if (!weapon) return tell(player, `§cUnknown gun.§f Pick one of: ${gunList()} — e.g. §e/gun shotgun`)
  if (giveGun(player, weapon)) tell(player, `§aGave you a ${weapon.displayName}.`)
  else tell(player, '§cYour inventory is full.')
}

/**
 * Chat commands (anyone can use them):
 *   /guns            every gun you don't have yet
 *   /gun <name>      one specific gun, e.g. /gun sniper   (/gun alone lists them)
 *   /pistol /rifle /smg /shotgun /sniper /minigun   shortcuts for /gun <name>
 *   /car [type]      brings your car, or switches to another one (/car monster)
 *   /cars            lists the cars (B opens the garage menu in game)
 */
function registerGameCommands () {
  const { commands } = mcServer
  if (!commands?.add) return console.warn('[world] command registry missing; chat commands disabled')
  const playerOnly = handler => (args, ctx) => {
    if (!ctx?.player) return 'Only players can use this'
    handler(ctx.player, typeof args === 'string' ? args : '')
  }

  commands.add({
    base: 'guns',
    aliases: ['kit', 'weapons'],
    info: 'get every gun you are missing',
    usage: '/guns',
    action: playerOnly(player => {
      const given = giveKit(player)
      if (given.length) tell(player, `§aGave you: ${given.map(w => w.displayName).join(', ')}.`)
      else if (WEAPON_LIST.every(w => hasGun(player, w))) tell(player, `§fYou already have every gun: ${gunList()}. Use §e/gun <name>§f for an extra one.`)
      else tell(player, '§cYour inventory is full.')
    }),
  })

  commands.add({
    base: 'gun',
    info: 'get a specific gun',
    usage: '/gun <pistol|rifle|smg|shotgun|sniper|minigun>',
    action: playerOnly((player, args) => {
      if (!args.trim()) return tell(player, `Guns: ${gunList()} — e.g. §e/gun shotgun§f, or §e/guns§f for all of them.`)
      giveNamedGun(player, args)
    }),
  })

  for (const weapon of WEAPON_LIST) {
    commands.add({
      base: weapon.id,
      info: `get a ${weapon.displayName}`,
      usage: `/${weapon.id}`,
      action: playerOnly(player => giveNamedGun(player, weapon.id)),
    })
  }

  const carList = () => CAR_MODELS.map(m => `§e${m.id}§f`).join(', ')

  commands.add({
    base: 'car',
    info: 'bring your car, or switch car: /car monster',
    usage: '/car [sports|police|taxi|pickup|monster|kart]',
    action: playerOnly((player, args) => {
      const session = [...sessions.values()].find(v => v.username?.toLowerCase() === player.username?.toLowerCase())
      if (!session) return tell(player, '§cCars need the game website; this client is not connected to the car system.')
      if (!args.trim()) return send(session.ws, { type: 'CAR_CALL' })
      const model = findCarModel(args)
      if (!model) return tell(player, `§cUnknown car.§f Cars: ${carList()} — e.g. §e/car monster`)
      send(session.ws, { type: 'CAR_CALL', model: model.id })
    }),
  })

  commands.add({
    base: 'cars',
    aliases: ['garage'],
    info: 'list the cars',
    usage: '/cars',
    action: playerOnly(player => tell(player, `Cars: ${carList()}. Use §e/car <name>§f, or press §eB§f for the garage.`)),
  })
}

const findMcPlayer = username => {
  if (!username || !mcServer?.players) return undefined
  const lower = username.toLowerCase()
  return mcServer.players.find(p => p.username?.toLowerCase() === lower)
}

// ───────────────────────────── /mc  WebSocket ⇄ world bridge ─────────────────────────────

const mcWss = new WebSocketServer({ noServer: true, perMessageDeflate: false, maxPayload: 4 * 1024 * 1024 })

mcWss.on('connection', ws => {
  const tcp = net.connect(MC_INTERNAL_PORT, '127.0.0.1')
  tcp.setNoDelay(true)
  let tcpReady = false
  const pending = []

  tcp.on('connect', () => {
    tcpReady = true
    for (const chunk of pending) tcp.write(chunk)
    pending.length = 0
  })
  tcp.on('data', chunk => {
    if (ws.readyState !== WebSocket.OPEN) return
    ws.send(chunk)
    // Backpressure: stop reading the world while the browser is far behind
    if (ws.bufferedAmount > 8 * 1024 * 1024) {
      tcp.pause()
      const resume = setInterval(() => {
        if (ws.readyState !== WebSocket.OPEN || ws.bufferedAmount < 1024 * 1024) {
          clearInterval(resume)
          tcp.resume()
        }
      }, 50)
    }
  })
  ws.on('message', data => {
    const buf = Buffer.isBuffer(data) ? data : Buffer.from(data)
    if (tcpReady) tcp.write(buf)
    else pending.push(buf)
  })

  const closeBoth = () => {
    tcp.destroy()
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close()
  }
  tcp.on('close', closeBoth)
  tcp.on('error', err => {
    console.warn('[mc] bridge error:', err.message)
    closeBoth()
  })
  ws.on('close', closeBoth)
  ws.on('error', closeBoth)
})

// ───────────────────────────── /room  JSON multiplayer ─────────────────────────────

const roomWss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 })
/** @type {Map<string, any>} */
const sessions = new Map()

const isNum = n => typeof n === 'number' && Number.isFinite(n)
const readVec = v => (v && isNum(v.x) && isNum(v.y) && isNum(v.z) && Math.abs(v.x) < 3e7 && Math.abs(v.y) < 4096 && Math.abs(v.z) < 3e7)
  ? { x: v.x, y: v.y, z: v.z }
  : null
const readDir = v => {
  const d = readVec(v)
  if (!d) return null
  const len = Math.hypot(d.x, d.y, d.z)
  if (len < 1e-6) return null
  return { x: d.x / len, y: d.y / len, z: d.z / len }
}
const round = (n, p = 1000) => Math.round(n * p) / p
const roundVec = v => v && { x: round(v.x), y: round(v.y), z: round(v.z) }

const send = (ws, msg) => {
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg))
}
const broadcast = (msg, exceptId) => {
  const data = JSON.stringify(msg)
  for (const s of sessions.values()) {
    if (s.id !== exceptId && s.ws.readyState === WebSocket.OPEN) s.ws.send(data)
  }
}

const publicSession = s => ({
  id: s.id,
  username: s.username,
  position: s.position,
  rotation: s.rotation,
  heldItem: s.heldItem,
  car: s.car,
})

/** Distance along the ray to the first full block, checked against the real world. */
const blockRayDistance = async (world, o, d, maxDist) => {
  if (!world?.getBlockStateId) return maxDist
  const blocks = mcServer.mcData.blocksByStateId
  let x = Math.floor(o.x)
  let y = Math.floor(o.y)
  let z = Math.floor(o.z)
  const stepX = Math.sign(d.x)
  const stepY = Math.sign(d.y)
  const stepZ = Math.sign(d.z)
  const tDeltaX = d.x === 0 ? Infinity : Math.abs(1 / d.x)
  const tDeltaY = d.y === 0 ? Infinity : Math.abs(1 / d.y)
  const tDeltaZ = d.z === 0 ? Infinity : Math.abs(1 / d.z)
  let tMaxX = d.x > 0 ? (x + 1 - o.x) / d.x : d.x < 0 ? (x - o.x) / d.x : Infinity
  let tMaxY = d.y > 0 ? (y + 1 - o.y) / d.y : d.y < 0 ? (y - o.y) / d.y : Infinity
  let tMaxZ = d.z > 0 ? (z + 1 - o.z) / d.z : d.z < 0 ? (z - o.z) / d.z : Infinity
  let t = 0
  const pos = new Vec3(0, 0, 0)
  for (let i = 0; i < 1024; i++) {
    if (i > 0) {
      if (y < -64 || y > 320) return maxDist
      pos.set(x, y, z)
      const stateId = await world.getBlockStateId(pos)
      if (blocks[stateId]?.boundingBox === 'block') return t
    }
    if (tMaxX < tMaxY && tMaxX < tMaxZ) { t = tMaxX; tMaxX += tDeltaX; x += stepX } else if (tMaxY < tMaxZ) { t = tMaxY; tMaxY += tDeltaY; y += stepY } else { t = tMaxZ; tMaxZ += tDeltaZ; z += stepZ }
    if (t > maxDist) return maxDist
  }
  return maxDist
}

/** Ray vs axis-aligned box (slab method). Returns entry distance or null. */
const rayAabb = (o, d, min, max) => {
  let tMin = -Infinity
  let tMax = Infinity
  for (const axis of ['x', 'y', 'z']) {
    if (Math.abs(d[axis]) < 1e-9) {
      if (o[axis] < min[axis] || o[axis] > max[axis]) return null
      continue
    }
    let t1 = (min[axis] - o[axis]) / d[axis]
    let t2 = (max[axis] - o[axis]) / d[axis]
    if (t1 > t2) [t1, t2] = [t2, t1]
    tMin = Math.max(tMin, t1)
    tMax = Math.min(tMax, t2)
    if (tMin > tMax) return null
  }
  if (tMax < 0) return null
  return Math.max(tMin, 0)
}

/** Angle in radians between two unit vectors */
const angleBetween = (a, b) => Math.acos(Math.max(-1, Math.min(1, a.x * b.x + a.y * b.y + a.z * b.z)))

const handleShoot = async (s, msg) => {
  const now = Date.now()
  const shooter = findMcPlayer(s.username)
  const heldName = shooter ? itemNameOf(shooter.inventory?.slots?.[36 + (shooter.heldItemSlot ?? 0)]) : s.heldItem
  const weapon = WEAPONS[heldName]
  if (!weapon) return // not holding a gun
  // Fire-rate limit: enforce each gun's average rate, with a little slack for network jitter
  const slack = Math.min(SHOOT_LENIENCY_MS, weapon.cooldownMs * 0.3)
  const nextAt = s.nextShotAt.get(weapon.id) ?? 0 // each gun has its own cooldown
  if (now < nextAt - slack) return
  s.nextShotAt.set(weapon.id, Math.max(now, nextAt) + weapon.cooldownMs)

  let origin = readVec(msg.origin)
  const direction = readDir(msg.direction)
  if (!origin || !direction) return

  // Pellet directions (shotgun) must stay inside the weapon's spread cone around the aim
  const maxAngle = weapon.spread + 0.01
  let pelletDirs = Array.isArray(msg.directions) ? msg.directions.slice(0, weapon.pellets).map(readDir).filter(Boolean) : []
  pelletDirs = pelletDirs.filter(d => angleBetween(d, direction) <= maxAngle)
  if (!pelletDirs.length) pelletDirs = [direction]

  const shots = []
  /** @type {Map<any, number>} victim → total damage */
  const damageByVictim = new Map()

  if (shooter && mcServer) {
    // The aim origin must be near the shooter's head (third-person cameras sit a few blocks back)
    const eye = { x: shooter.position.x, y: shooter.position.y + 1.62, z: shooter.position.z }
    if (Math.hypot(origin.x - eye.x, origin.y - eye.y, origin.z - eye.z) > 6) origin = eye

    for (const dir of pelletDirs) {
      let distance = await blockRayDistance(shooter.world, origin, dir, weapon.range)
      let hit = distance < weapon.range ? 'block' : null
      let victim = null
      for (const p of mcServer.players) {
        if (p === shooter || !p.position || p.health <= 0 || p.gameMode === 3) continue
        if (p.world && shooter.world && p.world !== shooter.world) continue
        const min = { x: p.position.x - 0.3, y: p.position.y, z: p.position.z - 0.3 }
        const max = { x: p.position.x + 0.3, y: p.position.y + 1.8, z: p.position.z + 0.3 }
        const t = rayAabb(origin, dir, min, max)
        if (t !== null && t < distance) {
          distance = t
          hit = 'player'
          victim = p
        }
      }
      if (victim) damageByVictim.set(victim, (damageByVictim.get(victim) ?? 0) + weapon.damage)
      shots.push({ direction: dir, distance, hit })
    }
  } else {
    // Not linked to an in-world player (should be rare): visual-only relay with a capped distance
    const distance = isNum(msg.distance) ? Math.max(0, Math.min(weapon.range, msg.distance)) : weapon.range
    shots.push({ direction, distance, hit: msg.hit === 'block' ? 'block' : null })
  }

  const roundDir = d => ({ x: round(d.x, 1e5), y: round(d.y, 1e5), z: round(d.z, 1e5) })
  broadcast({
    type: 'PLAYER_SHOOT',
    id: s.id,
    username: s.username,
    weapon: weapon.id,
    origin: roundVec(origin),
    // first pellet, for simple clients
    direction: roundDir(shots[0].direction),
    distance: round(shots[0].distance),
    hit: shots[0].hit,
    shots: shots.map(sh => ({ direction: roundDir(sh.direction), distance: round(sh.distance), hit: sh.hit })),
  }, s.id)

  if (!damageByVictim.size || !shooter || shooter.health <= 0 || shooter.gameMode === 3) return

  for (const [victim, damage] of damageByVictim) {
    if (victim.gameMode === 1 || victim.invincible) continue
    const knockback = new Vec3(direction.x * 2.5, 0.35, direction.z * 2.5)
    try {
      victim.takeDamage({ damage, velocity: knockback })
    } catch (err) {
      console.warn('[room] damage failed:', err?.message ?? err)
      continue
    }
    const killed = victim.health <= 0
    send(s.ws, { type: 'HIT_CONFIRM', target: victim.username, damage, killed })
    const victimSession = [...sessions.values()].find(v => v.username?.toLowerCase() === victim.username?.toLowerCase())
    if (victimSession) send(victimSession.ws, { type: 'HIT_TAKEN', by: s.username, damage, killed })
    if (killed) {
      for (const p of mcServer.players) tell(p, `§c${victim.username}§f was shot by §6${s.username}§f (${weapon.displayName})`)
    }
  }
}

const handleRoomMessage = (s, raw) => {
  let msg
  try {
    msg = JSON.parse(raw.toString())
  } catch {
    return
  }
  if (!msg || typeof msg.type !== 'string') return

  switch (msg.type) {
    case 'HELLO': {
      if (s.username) return
      const username = typeof msg.username === 'string' ? msg.username.slice(0, 16).replace(/[^\w]/g, '') : ''
      if (!username) return s.ws.close(4000, 'username required')
      // One room session per player name
      for (const other of sessions.values()) {
        if (other !== s && other.username?.toLowerCase() === username.toLowerCase()) other.ws.close(4001, 'replaced by new session')
      }
      s.username = username
      send(s.ws, { type: 'WELCOME', id: s.id, players: [...sessions.values()].filter(o => o !== s && o.username).map(publicSession) })
      broadcast({ type: 'PLAYER_JOIN', player: publicSession(s) }, s.id)
      return
    }
    case 'STATE': {
      if (!s.username) return
      const now = Date.now()
      if (now - s.lastStateAt < ROOM_STATE_MIN_INTERVAL_MS) return
      s.lastStateAt = now
      const position = readVec(msg.position)
      if (position) s.position = roundVec(position)
      if (msg.rotation && isNum(msg.rotation.yaw) && isNum(msg.rotation.pitch)) {
        s.rotation = { yaw: round(msg.rotation.yaw), pitch: round(msg.rotation.pitch) }
      }
      s.heldItem = typeof msg.heldItem === 'string' ? msg.heldItem.slice(0, 64) : null
      broadcast({ type: 'PLAYER_STATE', id: s.id, position: s.position, rotation: s.rotation, heldItem: s.heldItem }, s.id)
      return
    }
    case 'PLAYER_SHOOT': {
      if (!s.username) return
      handleShoot(s, msg).catch(err => console.warn('[room] shoot error:', err?.message ?? err))
      return
    }
    case 'CAR_STATE': {
      if (!s.username) return
      const c = msg.car
      if (c === null) {
        s.car = null
      } else {
        const pos = readVec(c)
        if (!pos || !isNum(c.heading)) return
        s.car = {
          ...roundVec(pos),
          heading: round(c.heading),
          steer: isNum(c.steer) ? round(c.steer) : 0,
          speed: isNum(c.speed) ? round(c.speed) : 0,
          driving: !!c.driving,
          model: findCarModel(c.model)?.id ?? 'sports',
        }
      }
      broadcast({ type: 'CAR_STATE', id: s.id, car: s.car }, s.id)
      return
    }
    case 'PING':
      send(s.ws, { type: 'PONG', t: msg.t })
  }
}

roomWss.on('connection', ws => {
  const s = {
    id: crypto.randomUUID(),
    ws,
    username: null,
    position: null,
    rotation: null,
    heldItem: null,
    car: null,
    nextShotAt: new Map(),
    lastStateAt: 0,
    alive: true,
  }
  sessions.set(s.id, s)
  ws.on('pong', () => { s.alive = true })
  ws.on('message', data => handleRoomMessage(s, data))
  ws.on('close', () => {
    sessions.delete(s.id)
    if (s.username) broadcast({ type: 'PLAYER_LEAVE', id: s.id })
  })
  ws.on('error', () => ws.terminate())
})

// Drop dead connections
const heartbeat = setInterval(() => {
  for (const s of sessions.values()) {
    if (!s.alive) {
      s.ws.terminate()
      continue
    }
    s.alive = false
    try { s.ws.ping() } catch {}
  }
}, 15_000)

// ───────────────────────────── Upgrade routing ─────────────────────────────

server.on('upgrade', (req, socket, head) => {
  let pathname = '/'
  try {
    pathname = new URL(req.url, 'http://localhost').pathname
  } catch {}
  if (pathname === '/mc') {
    if (worldStatus !== 'running') return socket.destroy()
    mcWss.handleUpgrade(req, socket, head, ws => mcWss.emit('connection', ws, req))
  } else if (pathname === '/room') {
    roomWss.handleUpgrade(req, socket, head, ws => roomWss.emit('connection', ws, req))
  } else if (proxyUpgradeServer && pathname.startsWith('/api/vm/net/')) {
    proxyUpgradeServer.emit('upgrade', req, socket, head)
  } else {
    socket.destroy()
  }
})

// ───────────────────────────── Start / stop ─────────────────────────────

void startWorld()

server.listen(PORT, () => {
  console.log(`[web] game + multiplayer on http://localhost:${PORT}  (share http(s)://<your-domain>/play)`)
})

const shutdown = () => {
  console.log('Shutting down...')
  clearInterval(heartbeat)
  for (const s of sessions.values()) s.ws.close(1001, 'server restarting')
  try { mcServer?.quit?.('Server restarting') } catch {}
  server.close(() => process.exit(0))
  setTimeout(() => process.exit(0), 5000).unref()
}
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

export { app, server }
