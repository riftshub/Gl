#!/usr/bin/env node
/**
 * Unified game host — ONE Node.js process, ONE port, no Java server needed.
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
 *   PORT=8080                 port to listen on (hosts like Render/Railway set this)
 *   SERVER_NAME="Our World"   name shown in the server list
 *   MC_VERSION=1.18           game version of the embedded world
 *   MAX_PLAYERS=20
 *   WORLD_FOLDER=./world      where the world is saved (needs a persistent disk to survive restarts)
 *   GAME_MODE=0               0 survival, 1 creative, 2 adventure
 *   VIEW_DISTANCE=6
 *   ENABLE_PROXY=1            also let players join other Minecraft servers through this host
 *   MC_INTERNAL_PORT=25565    internal-only port of the embedded world (bound to 127.0.0.1)
 */

const http = require('http')
const net = require('net')
const path = require('path')
const fs = require('fs')
const crypto = require('crypto')
const express = require('express')
const compression = require('compression')
const { WebSocketServer, WebSocket } = require('ws')
const { Vec3 } = require('vec3')

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
const VIEW_DISTANCE = Number(process.env.VIEW_DISTANCE || 6)
const ENABLE_PROXY = process.env.ENABLE_PROXY === '1'
const CLIENT_DIR = path.resolve(__dirname, process.env.CLIENT_DIR || path.join('..', 'client'))
const CLIENT_DIST = path.resolve(__dirname, process.env.CLIENT_DIST || path.join(CLIENT_DIR, 'dist'))
const CLIENT_PUBLIC = path.join(CLIENT_DIR, 'public')

/**
 * Guns are ordinary Minecraft items. The server decides damage from what the shooter is
 * actually holding, so a modified client can't fake a stronger weapon.
 * Keep `item` names and cooldowns in sync with WEAPONS in client/src/carsAndGuns.ts.
 */
const WEAPONS = {
  iron_hoe: { id: 'rifle', displayName: 'Rifle', damage: 5, range: 128, cooldownMs: 500 },
}
const RIFLE_ITEM = 'iron_hoe'

const ROOM_STATE_MIN_INTERVAL_MS = 45 // drop STATE spam above ~20/s per player
const SHOOT_LENIENCY_MS = 60 // network jitter allowance on the fire-rate check

// ───────────────────────────── HTTP app ─────────────────────────────

const app = express()
app.set('trust proxy', true) // correct https/wss detection behind Render, Railway, Fly, nginx...
app.use(compression())

const ownServerAddress = req => {
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

app.get('/play', (req, res) => {
  const params = new URLSearchParams({ ip: ownServerAddress(req), version: MC_VERSION, name: SERVER_NAME })
  res.redirect(`/?${params.toString()}`)
})

const tryRequire = file => {
  try {
    return require(file)
  } catch {
    return undefined
  }
}

app.get('/config.json', (req, res) => {
  const config = tryRequire(path.join(CLIENT_DIST, 'config.json')) ?? tryRequire(path.join(CLIENT_DIR, 'config.json')) ?? {}
  const publicConfig = tryRequire(path.join(CLIENT_PUBLIC, 'config.json')) ?? {}
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

if (ENABLE_PROXY) {
  const netApi = require('net-browserify')
  const cors = require('cors')
  app.use(cors())
  app.use(netApi({ allowOrigin: '*', log: process.env.LOG === 'true', timeout: 10_000 }))
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

try {
  const rendererDir = path.dirname(require.resolve('minecraft-renderer/package.json', { paths: [CLIENT_DIR, __dirname] }))
  app.get('/wasm_mesher.js', (req, res) => res.sendFile(path.join(rendererDir, 'public/wasm_mesher.js')))
  app.get('/wasm_mesher_bg.wasm', (req, res) => res.sendFile(path.join(rendererDir, 'public/wasm_mesher_bg.wasm')))
} catch {
  console.warn('[web] minecraft-renderer package not found; wasm mesher assets come from dist/ only')
}
if (!isProd) app.use('/sounds', express.static(path.join(CLIENT_DIR, 'generated/sounds')))
app.use(express.static(CLIENT_PUBLIC))
app.use(express.static(CLIENT_DIST))
if (!fs.existsSync(path.join(CLIENT_DIST, 'index.html'))) {
  console.warn(`[web] no client build at ${CLIENT_DIST} — run \`pnpm build\` in the client folder (the room and world still run)`)
}

const server = http.createServer(app)

// ───────────────────────────── Embedded world (flying-squid) ─────────────────────────────

let mcServer = null
let worldStatus = 'starting'

const startWorld = () => {
  try {
    const { createMCServer } = require('flying-squid')
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
      kickTimeout: 30_000,
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
    mcServer.on('newPlayer', player => {
      player.once('spawned', () => {
        setTimeout(() => {
          giveRifle(player)
          try {
            player.chat('§6Welcome! §fYour §6Rifle§f is the iron hoe: left-click to fire, R to reload. Press F near your car to drive. Type §e/rifle§f for a new one.')
          } catch {}
        }, 800)
      })
    })

    // Command registry is created while the server's modules load; register once it exists
    const registerRifleCommand = (attempt = 0) => {
      if (!mcServer.commands?.add) {
        if (attempt < 50) setTimeout(() => registerRifleCommand(attempt + 1), 200)
        return
      }
      mcServer.commands.add({
        base: 'rifle',
        info: 'get a Rifle',
        usage: '/rifle',
        action (_str, ctx) {
          if (!ctx?.player) return 'Only players can use this'
          giveRifle(ctx.player)
        },
      })
    }
    registerRifleCommand()
  } catch (err) {
    worldStatus = 'failed'
    console.error('[world] failed to start the embedded world:', err)
  }
}

const itemNameOf = item => (item && mcServer?.mcData?.items?.[item.type]?.name) || null

function giveRifle (player) {
  try {
    const itemId = mcServer.mcData.itemsByName[RIFLE_ITEM]?.id
    if (itemId === undefined || !player.inventory) return
    const slots = player.inventory.slots
    if (slots.some(it => it && it.type === itemId)) return
    const rifle = new mcServer.PrismarineItem(itemId, 1)
    try {
      rifle.customName = JSON.stringify({ text: 'Rifle', italic: false, color: 'gold' })
    } catch {}
    const hotbarFirst = 36
    const slot = slots[hotbarFirst] ? player.inventory.firstEmptyInventorySlot?.() : hotbarFirst
    if (slot === null || slot === undefined) return
    player.inventory.updateSlot(slot, rifle)
  } catch (err) {
    console.warn('[world] could not give rifle:', err?.message ?? err)
  }
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

const handleShoot = async (s, msg) => {
  const now = Date.now()
  const shooter = findMcPlayer(s.username)
  const heldName = shooter ? itemNameOf(shooter.inventory?.slots?.[36 + (shooter.heldItemSlot ?? 0)]) : s.heldItem
  const weapon = WEAPONS[heldName]
  if (!weapon) return // not holding a gun
  if (now - s.lastShotAt < weapon.cooldownMs - SHOOT_LENIENCY_MS) return
  s.lastShotAt = now

  let origin = readVec(msg.origin)
  const direction = readDir(msg.direction)
  if (!origin || !direction) return

  let distance = weapon.range
  let hit = null
  let victim = null

  if (shooter && mcServer) {
    // The aim origin must be near the shooter's head (third-person cameras sit a few blocks back)
    const eye = { x: shooter.position.x, y: shooter.position.y + 1.62, z: shooter.position.z }
    if (Math.hypot(origin.x - eye.x, origin.y - eye.y, origin.z - eye.z) > 6) origin = eye

    distance = await blockRayDistance(shooter.world, origin, direction, weapon.range)
    if (distance < weapon.range) hit = 'block'

    for (const p of mcServer.players) {
      if (p === shooter || !p.position || p.health <= 0 || p.gameMode === 3) continue
      if (p.world && shooter.world && p.world !== shooter.world) continue
      const min = { x: p.position.x - 0.3, y: p.position.y, z: p.position.z - 0.3 }
      const max = { x: p.position.x + 0.3, y: p.position.y + 1.8, z: p.position.z + 0.3 }
      const t = rayAabb(origin, direction, min, max)
      if (t !== null && t < distance) {
        distance = t
        hit = 'player'
        victim = p
      }
    }
  } else {
    // Not linked to an in-world player (should be rare): visual-only relay with a capped distance
    if (isNum(msg.distance)) distance = Math.max(0, Math.min(weapon.range, msg.distance))
    hit = msg.hit === 'block' ? 'block' : null
  }

  broadcast({
    type: 'PLAYER_SHOOT',
    id: s.id,
    username: s.username,
    weapon: weapon.id,
    origin: roundVec(origin),
    direction: { x: round(direction.x, 1e5), y: round(direction.y, 1e5), z: round(direction.z, 1e5) },
    distance: round(distance),
    hit,
  }, s.id)

  if (!victim || !shooter || shooter.health <= 0 || shooter.gameMode === 3) return
  if (victim.gameMode === 1 || victim.invincible) return

  const knockback = new Vec3(direction.x * 2.5, 0.35, direction.z * 2.5)
  try {
    victim.takeDamage({ damage: weapon.damage, velocity: knockback })
  } catch (err) {
    console.warn('[room] damage failed:', err?.message ?? err)
    return
  }
  const killed = victim.health <= 0
  send(s.ws, { type: 'HIT_CONFIRM', target: victim.username, damage: weapon.damage, killed })
  const victimSession = [...sessions.values()].find(v => v.username?.toLowerCase() === victim.username?.toLowerCase())
  if (victimSession) send(victimSession.ws, { type: 'HIT_TAKEN', by: s.username, damage: weapon.damage, killed })
  if (killed) {
    try {
      for (const p of mcServer.players) p.chat(`§c${victim.username}§f was shot by §6${s.username}`)
    } catch {}
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
    lastShotAt: 0,
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
  } else if (server.listenerCount('upgrade') <= 1) {
    socket.destroy()
  }
})

// ───────────────────────────── Start / stop ─────────────────────────────

startWorld()

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

module.exports = { app, server }
