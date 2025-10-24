// db/redisClient.js
const redis = require('redis');

const redisClient = redis.createClient({
    socket: { host: '192.99.190.137', port: 50301 },
    password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg',
});

redisClient.on('error', (err) => {
    console.error('Error al conectar con Redis:', err);
});

(async () => {
    if (!redisClient.isOpen) await redisClient.connect();
})();

async function getFromRedis(key) {
    try {
        const value = await redisClient.get(key);
        return value ? JSON.parse(value) : null;
    } catch (error) {
        console.error(`Error obteniendo clave ${key} de Redis:`, error);
        throw { status: 500, response: { estado: false, error: -1 } };
    }
}

async function updateEstadoRedis(empresaId, envioId, estado) {
    let DWRTE = await redisClient.get('DWRTE');
    DWRTE = DWRTE ? JSON.parse(DWRTE) : {};

    const empresaKey = `e.${empresaId}`;
    const envioKey = `en.${envioId}`;
    const ahora = Date.now();

    if (!DWRTE[empresaKey]) DWRTE[empresaKey] = {};
    if (!DWRTE[empresaKey][envioKey]) {
        DWRTE[empresaKey][envioKey] = { estado, timestamp: ahora };
    } else {
        DWRTE[empresaKey][envioKey].estado = estado;
    }

    await redisClient.set('DWRTE', JSON.stringify(DWRTE));
}

module.exports = { redisClient, getFromRedis, updateEstadoRedis };
