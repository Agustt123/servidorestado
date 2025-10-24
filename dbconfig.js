// db/dbconfig.js
// Ya NO usamos 'mysql' clásico; todo va por mysql2/promise vía poolManager
const { getPool } = require('./poolManager');
const { redisClient, getFromRedis, updateEstadoRedis } = require('./redisClient');

// Conexión “prestada” del pool (acordate de release())
async function getConnection(idempresa) {
    try {
        const pool = await getPool(idempresa);
        const conn = await pool.getConnection();
        return conn;
    } catch (error) {
        console.error('Error al obtener la conexión:', error?.message || error);
        throw { status: 500, response: { estado: false, error: -1 } };
    }
}

// executeQuery compatible con mysql2/promise (y fallback a .query cb)
async function executeQuery(connection, query, values = [], { timeout = 15000 } = {}) {
    try {
        if (typeof connection.execute === 'function') {
            const [rows] = await connection.execute({ sql: query, timeout }, values);
            return rows;
        }
        // Fallback por si aún usás alguna conexión con callbacks
        return await new Promise((resolve, reject) => {
            connection.query(query, values, (err, results) => {
                if (err) return reject(err);
                resolve(results);
            });
        });
    } catch (error) {
        throw error;
    }
}

module.exports = {
    // DB
    getConnection,
    executeQuery,
    // Redis
    redisClient,
    getFromRedis,
    updateEstadoRedis,
};
