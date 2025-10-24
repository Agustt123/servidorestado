const amqp = require('amqplib');
const express = require('express');
const { redisClient, getConnection, updateEstadoRedis } = require('./dbconfig');
const mysql = require('mysql2/promise'); // Usar mysql2 con promesas
const moment = require('moment');
const { updateProducction, procesarEstadoIndividual } = require('./controller/updateProducction');
const cors = require('cors');
const RABBITMQ_URL = 'amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672';
const QUEUE_NAME = 'srvshipmltosrvstates';


// Función que genera el hash SHA-256 de la fecha actual



const newDbConfig = {
  //host: '149.56.182.49',
  host: 'localhost',
  user: 'userdata2',
  password: 'pt78pt79',
  database: 'dataestaos',
  //port: 44337
};

// Crear un pool de conexiones
const pool = mysql.createPool({
  ...newDbConfig,
  connectionLimit: 10, // Limitar a 10 conexiones
});

const app = express();

// Función para escuchar los mensajes de la cola
function logConsola(mensaje, tipo = 'info') {
  const timestamp = new Date().toISOString();
  const colores = {
    info: '\x1b[36m%s\x1b[0m',    // Cyan
    ok: '\x1b[32m%s\x1b[0m',      // Green
    warn: '\x1b[33m%s\x1b[0m',    // Yellow
    error: '\x1b[31m%s\x1b[0m'    // Red
  };
  console.log(colores[tipo] || '%s', `[${timestamp}] ${mensaje}`);
}

const listenToQueue2 = async () => {
  let connection;
  let channel;


  const connect = async () => {
    try {
      logConsola('Intentando conectar a RabbitMQ...', 'info');
      connection = await amqp.connect(RABBITMQ_URL);

      channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      await channel.prefetch(500);
      logConsola(`✅ Conectado a RabbitMQ y escuchando la cola "${QUEUE_NAME}"`, 'ok');
      logConsola(`Esperando mensajes en la cola ${QUEUE_NAME}...`, 'info');

      channel.consume(QUEUE_NAME, async (msg) => {
        if (msg !== null) {
          const jsonData = JSON.parse(msg.content.toString());
          logConsola(`📩 Mensaje recibido`, 'info');
          channel.ack(msg);

          try {
            await checkAndInsertData(jsonData);

            if (jsonData.operacion) {
              logConsola("⚙️  Procesando operación: " + jsonData.operacion, 'info');
              await updateProducction(jsonData);
            }

            // channel.ack(msg);
            logConsola('✅ Mensaje procesado correctamente', 'ok');
          } catch (error) {
            logConsola(`❌ Error procesando mensaje: ${error.message}`, 'error');
          }
        }
      });

      connection.on('error', (err) => {
        logConsola(`❌ Error en la conexión de RabbitMQ: ${err.message}`, 'error');
      });

      connection.on('close', () => {
        logConsola('⚠️  Conexión cerrada. Reintentando en 5s...', 'warn');
        setTimeout(connect, 5000);
      });

    } catch (error) {
      logConsola(`❌ Error conectando a RabbitMQ: ${error.message}`, 'error');
      setTimeout(connect, 5000); // Reintento
    }
  };

  await connect();
};


async function limpiarEnviosViejos() {
  let DWRTE = await redisClient.get('DWRTE');
  if (!DWRTE) return;

  DWRTE = JSON.parse(DWRTE);
  const ahora = Date.now();
  const limite = 14 * 24 * 60 * 60 * 1000; // 14 días en milisegundos

  let cambios = false;

  for (const empresaKey in DWRTE) {
    for (const envioKey in DWRTE[empresaKey]) {
      if (DWRTE[empresaKey][envioKey].timestamp < ahora - limite) {
        delete DWRTE[empresaKey][envioKey]; // Eliminamos el envío
        cambios = true;
      }
    }

    // Si la empresa ya no tiene envíos, eliminarla también
    if (Object.keys(DWRTE[empresaKey]).length === 0) {
      delete DWRTE[empresaKey];
    }
  }

  if (cambios) {
    await redisClient.set('DWRTE', JSON.stringify(DWRTE));
  }
}



// Función para insertar los datos en la nueva base de datos
const checkAndInsertData = async (jsonData, intento = 1) => {
  const { didempresa, didenvio, estado, subestado, estadoML, fecha, quien } = jsonData;
  const superado = jsonData.superado || 0;
  const elim = jsonData.elim || 0;
  const formattedFecha = moment(fecha).format('YYYY-MM-DD HH:mm:ss');
  const tableName = `estados_${didempresa}`;
  let latitud = jsonData.latitud || 0;
  let longitud = jsonData.longitud || 0;

  let conn;

  try {
    // asumo que getConnection(didempresa) devuelve una conexión del pool (mysql2/promise)
    conn = await getConnection(didempresa);

    // 1) OJO: mysql2/promise devuelve [rows, fields]
    const [choferRows] = await conn.query(
      `SELECT choferAsignado FROM envios WHERE elim = 0 AND superado = 0 AND did = ?`,
      [didenvio]
    );
    const choferAsignado = choferRows?.[0]?.choferAsignado ?? 0;

    // 2) Usar SIEMPRE la misma conexión en esta función
    const [tables] = await conn.query(`SHOW TABLES LIKE ?`, [tableName]);

    if (tables.length > 0) {
      const [existingResults] = await conn.query(
        `SELECT 1 FROM ${tableName} WHERE didEnvio = ? LIMIT 1`,
        [didenvio]
      );

      if (existingResults.length > 0) {
        await conn.query(
          `UPDATE ${tableName} SET superado = ? WHERE didEnvio = ?`,
          [1, didenvio]
        );
      }

      await conn.query(
        `INSERT INTO ${tableName}
          (didEnvio, operador, estado, estadoML, subestadoML, fecha, quien, superado, elim, latitud, longitud)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim, latitud, longitud]
      );
      crearLog(didempresa, '', '', jsonData, Date.now(), '', '', 1, conn);


    } else {
      // ⚠️ Si `didempresa` viene de entrada externa, validalo para evitar SQLi en el nombre de tabla
      await conn.query(`
        CREATE TABLE ${tableName} (
          id INT AUTO_INCREMENT PRIMARY KEY,
          didEnvio VARCHAR(255),
          operador VARCHAR(255),
          estado VARCHAR(255),
          estadoML VARCHAR(255),
          subestadoML VARCHAR(255),
          fecha DATETIME,
          autofecha TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          quien INT,
          superado INT,
          elim INT,
          latitud DOUBLE,
          longitud DOUBLE,
          INDEX(didEnvio),
          INDEX(operador),
          INDEX(fecha),
          INDEX(superado),
          INDEX(elim),
          INDEX(quien),
          INDEX(estadoML),
          INDEX(subestadoML)
        )
      `);

      await conn.query(
        `INSERT INTO ${tableName}
          (didEnvio, operador, estado, estadoML, subestadoML, fecha, quien, superado, elim, latitud, longitud)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim, latitud, longitud]
      );
      crearLog(didempresa, '', '', jsonData, Date.now(), '', '', 1, conn);
    }

  } catch (error) {
    console.error(`Error en checkAndInsertData (intento ${intento}):`, error);

    if (intento < 3) {
      console.log(`🔁 Reintentando checkAndInsertData (intento ${intento + 1})...`);
      await new Promise(res => setTimeout(res, 300));
      return checkAndInsertData(jsonData, intento + 1);
    }

    console.error(`❌ Falló definitivamente después de 3 intentos: didenvio ${didenvio}`);

  } finally {
    // ✅ liberar si viene del pool; cerrar si es una conexión suelta
    if (conn) {
      try {
        if (typeof conn.release === 'function') {
          conn.release();
        } else if (typeof conn.end === 'function') {
          await conn.end();
        }
      } catch (e) {
        console.error('Error liberando/cerrando la conexión:', e);
      }
    }
  }
};



app.use(cors());
app.use(express.json());

app.get('/test', (req, res) => {
  const currentDate = new Date();
  currentDate.setHours(currentDate.getHours()); // Resta 3 horas

  // Formatear la hora en el formato HH:MM:SS
  const hours = currentDate.getHours().toString().padStart(2, '0');
  const minutes = currentDate.getMinutes().toString().padStart(2, '0');
  const seconds = currentDate.getSeconds().toString().padStart(2, '0');

  const formattedTime = `${hours}:${minutes}:${seconds}`;

  res.status(200).json({
    hora: formattedTime,
    response: "hola barbie morocha"
  });
});
const crypto = require('crypto');
const { deleteProduction } = require('./controller/deleteProduction');
const { crearLog } = require('./funciones/crearLogs');


// Función que genera el hash SHA-256 de la fecha actual
function generarTokenFechaHoy() {
  const ahora = new Date();
  ahora.setHours(ahora.getHours() - 3); // Resta 3 horas

  console.log("📆 Fecha ajustada (UTC-3):", ahora);

  const dia = String(ahora.getDate()).padStart(2, '0');
  const mes = String(ahora.getMonth() + 1).padStart(2, '0');
  const anio = ahora.getFullYear();

  const fechaString = `${dia}${mes}${anio}`; // Ej: "11072025"
  const hash = crypto.createHash('sha256').update(fechaString).digest('hex');

  return hash;
}

app.post('/estados', async (req, res) => {
  const jsonData = req.body;
  // console.log("JSON recibido:", jsonData);

  // Validar token
  const tokenEsperado = generarTokenFechaHoy();
  //console.log("Token esperado:", tokenEsperado);


  if (jsonData.tkn !== tokenEsperado) {
    console.warn("⚠️ Token inválido:", jsonData.tkn);
    return res.status(401).json({ success: false, message: 'Token inválido' });
  }

  try {
    await checkAndInsertData(jsonData);

    if (jsonData.operacion) {
      const resultado = await updateProducction(jsonData);

      return res.status(200).json({ success: true, message: 'Estado procesado correctamente', id: resultado });
    }

    return res.status(200).json({ success: true, message: 'Estado procesado correctamente', id: resultado });
  } catch (error) {
    console.error('❌ Error en endpoint /api/estados:', error);
    return res.status(500).json({ success: false, message: 'Error interno al procesar el estado' });
  }
});

app.post('/estados/lote', async (req, res) => {


  const {
    didempresa,
    estado,
    subestado = null,
    estadoML = null,
    fecha = null,
    quien,
    latitud = 0,
    longitud = 0,
    operacion = "operador", // "ml" u otro
    didenvios,
  } = req.body || {};

  // Validaciones mínimas
  if (!didempresa) {
    return res.status(400).json({ success: false, message: 'Falta didempresa' });
  }
  if (!estado) {
    return res.status(400).json({ success: false, message: 'Falta estado' });
  }
  if (!Array.isArray(didenvios) || didenvios.length === 0) {
    return res.status(400).json({ success: false, message: 'didenvios debe ser un array con al menos un id' });
  }

  let dbConnection;
  const resultados = [];
  const fallidos = [];

  try {
    dbConnection = await getConnection(didempresa);

    // Procesa cada didEnvio de forma independiente
    for (const didenvio of didenvios) {
      try {
        const insertId = await procesarEstadoIndividual({
          dbConnection,
          didenvio,
          estado,
          subestado,
          estadoML,
          fecha,
          quien,
          latitud,
          longitud,
          operacion,
        });
        resultados.push({ didenvio, insertId });
      } catch (err) {
        console.error(`Error procesando didenvio ${didenvio}:`, err?.message || err);
        fallidos.push({ didenvio, error: err?.message || 'Error desconocido' });
      }
    }

    return res.status(200).json({
      success: true,
      message: 'Lote procesado',
      procesados: resultados.length,
      fallidos: fallidos.length,
      resultados,
      errores: fallidos,
    });
  } catch (error) {
    console.error('❌ Error general en /estados/lote:', error);
    return res.status(500).json({ success: false, message: 'Error interno en lote' });
  } finally {
    if (dbConnection) dbConnection.end();
  }
});
app.post('/estados/eliminar', async (req, res) => {
  const data = req.body;


  try {



    const resultado = await deleteProduction(data);


    return res.status(200).json(resultado);



  } catch (error) {
    console.error('❌ Error en endpoint /api/estados/elimianr:', error);
    return res.status(500).json({ success: false, message: 'Error interno al procesar el estado' });
  }
});


const PORT = 13000;
app.listen(PORT, () => {
  console.log(`Servidor escuchando en http://localhost:${PORT}`);
});

// Iniciar la escucha de la cola
listenToQueue2();
setInterval(limpiarEnviosViejos, 24 * 60 * 60 * 1000);


limpiarEnviosViejos();


module.exports = { listenToQueue2 };
