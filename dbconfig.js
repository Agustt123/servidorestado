const mysql = require('mysql');
const redis = require('redis');
const mysql = require('mysql2');

const redisClient = redis.createClient({
    socket: {
        host: '192.99.190.137',
        port: 50301,
    },
    password: 'sdJmdxXC8luknTrqmHceJS48NTyzExQg',
});

redisClient.on('error', (err) => {
    console.error('Error al conectar con Redis:', err);
});

(async () => {
    await redisClient.connect();
    //  console.log('Redis conectado');
})();

const pools = {}; // Cache de pools por empresa

async function getConnection(idempresa) {
    try {
        if (typeof idempresa !== 'string' && typeof idempresa !== 'number') {
            throw new Error(`idempresa debe ser un string o un número, pero es: ${typeof idempresa}`);
        }

        // Obtener empresas desde Redis
        const redisKey = 'empresasData';
        const empresasData = await getFromRedis(redisKey);
        if (!empresasData) {
            throw new Error(`No se encontraron datos de empresas en Redis.`);
        }

        const empresa = empresasData[String(idempresa)];
        if (!empresa) {
            throw new Error(`No se encontró la configuración de la empresa con ID: ${idempresa}`);
        }

        // Si ya hay un pool para esa empresa, lo devolvemos
        if (pools[idempresa]) {
            return pools[idempresa].promise();
        }

        // Crear pool de conexiones
        const config = {
            host: 'bhsmysql1.lightdata.com.ar',
            database: empresa.dbname,
            user: empresa.dbuser,
            password: empresa.dbpass,
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0
        };

        const pool = mysql.createPool(config);
        pools[idempresa] = pool; // Cachear el pool

        return pool.promise();
    } catch (error) {
        console.error(`Error al obtener la conexión:`, error.message);

        throw {
            status: 500,
            response: {
                estado: false,
                error: -1,
            },
        };
    }
}

// Función para obtener datos desde Redis
async function getFromRedis(key) {
    try {
        const value = await redisClient.get(key);
        return value ? JSON.parse(value) : null;
    } catch (error) {
        console.error(`Error obteniendo clave ${key} de Redis:`, error);
        throw {
            status: 500,
            response: {
                estado: false,

                error: -1

            },
        };
    }
}
async function updateEstadoRedis(empresaId, envioId, estado) {
    let DWRTE = await redisClient.get('DWRTE');
    DWRTE = DWRTE ? JSON.parse(DWRTE) : {};

    const empresaKey = `e.${empresaId}`;
    const envioKey = `en.${envioId}`;
    const ahora = Date.now(); // Guardamos el timestamp actual

    if (!DWRTE[empresaKey]) {
        DWRTE[empresaKey] = {};
    }

    if (!DWRTE[empresaKey][envioKey]) {
        DWRTE[empresaKey][envioKey] = { estado, timestamp: ahora };
    } else {
        DWRTE[empresaKey][envioKey].estado = estado;
        // No actualizamos el timestamp para no resetear la antigüedad
    }

    await redisClient.set('DWRTE', JSON.stringify(DWRTE));
}
async function executeQuery(connection, query, values, log = false) {
    if (log) {
        logYellow(`Ejecutando query: ${query} con valores: ${values}`);
    }
    try {
        return new Promise((resolve, reject) => {
            connection.query(query, values, (err, results) => {
                if (err) {
                    if (log) {
                        logRed(`Error en executeQuery: ${err.message}`);
                    }
                    reject(err);
                } else {
                    if (log) {
                        logYellow(`Query ejecutado con éxito: ${JSON.stringify(results)}`);
                    }
                    resolve(results);
                }
            });
        });
    } catch (error) {
        logRed(`Error en executeQuery: ${error.stack}`);
        throw error;
    }
}



module.exports = { getConnection, getFromRedis, redisClient, updateEstadoRedis, executeQuery };
