// db/poolManager.js
const mysql = require('mysql2/promise');
const { getFromRedis } = require('./redisClient');

const CONNECTION_LIMIT = 5;
const MAX_OPEN_POOLS = 10;
const IDLE_TTL_MS = 5 * 60_000;
const HOST_FIJO = 'bhsmysql1.lightdata.com.ar';

const pools = new Map(); // idempresa -> { pool, lastUsed }

async function getTenantConfig(idempresa) {
    const empresasData = await getFromRedis('empresasData');
    if (!empresasData) throw new Error('No se encontraron datos de empresas en Redis.');

    // Soporta objeto { [id]: cfg } o array
    let empresa = empresasData[String(idempresa)];
    if (!empresa && Array.isArray(empresasData)) {
        empresa = empresasData.find(e => String(e.id) === String(idempresa));
    }
    if (!empresa) throw new Error(`No se encontró la configuración para empresa ${idempresa}`);

    return {
        host: HOST_FIJO,
        database: empresa.dbname,
        user: empresa.dbuser,
        password: empresa.dbpass,
        // connectTimeout: 10000,
    };
}

function makePool(cfg) {
    return mysql.createPool({
        ...cfg,
        waitForConnections: true,
        connectionLimit: CONNECTION_LIMIT,
        queueLimit: 0,
        enableKeepAlive: true,
        keepAliveInitialDelay: 0,
    });
}

function evictIdle() {
    const now = Date.now();
    for (const [tenant, entry] of pools) {
        if (now - entry.lastUsed > IDLE_TTL_MS) {
            entry.pool.end().catch(() => { });
            pools.delete(tenant);
        }
    }
    if (pools.size > MAX_OPEN_POOLS) {
        const sorted = [...pools.entries()].sort((a, b) => a[1].lastUsed - b[1].lastUsed);
        for (let i = 0; i < pools.size - MAX_OPEN_POOLS; i++) {
            sorted[i][1].pool.end().catch(() => { });
            pools.delete(sorted[i][0]);
        }
    }
}
setInterval(evictIdle, 60_000).unref();

async function getPool(idempresa) {
    if (typeof idempresa !== 'string' && typeof idempresa !== 'number') {
        throw new Error(`idempresa debe ser string o number, es: ${typeof idempresa}`);
    }
    const key = String(idempresa);

    const existing = pools.get(key);
    if (existing) {
        existing.lastUsed = Date.now();
        return existing.pool;
    }

    const cfg = await getTenantConfig(idempresa);
    const pool = makePool(cfg);
    pools.set(key, { pool, lastUsed: Date.now() });
    evictIdle();
    return pool;
}

async function closeAllPools() {
    await Promise.all([...pools.values()].map(({ pool }) => pool.end().catch(() => { })));
    pools.clear();
}

module.exports = { getPool, closeAllPools };
