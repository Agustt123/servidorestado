const amqp = require('amqplib');
const express = require('express');
const { redisClient, getConnection, updateEstadoRedis } = require('./dbconfig');
const mysql = require('mysql2/promise'); // Usar mysql2 con promesas
const moment = require('moment');
const { updateProducction } = require('./controller/updateProducction');
const cors = require('cors');
const RABBITMQ_URL = 'amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672';
const QUEUE_NAME = 'srvshipmltosrvstates';


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

  /*************  ✨ Windsurf Command ⭐  *************/
  /**
   * Establishes a connection to RabbitMQ server and listens to a specified queue.
   * 
   * This function attempts to connect to RabbitMQ using the provided URL and queue name. 
   * If successful, it creates a channel, asserts the queue, and sets a prefetch limit. 
   * It also consumes messages from the queue, processes them, and acknowledges them. 
   * If any error occurs during connection or message processing, it logs the error 
   * and retries connection after a delay. Additionally, it logs and retries upon 
   * connection closure.
   */

  /*******  fb8688df-e659-4b9d-b862-ac2bb06a0fc9  *******/
  const connect = async () => {
    try {
      logConsola('Intentando conectar a RabbitMQ...', 'info');
      connection = await amqp.connect(RABBITMQ_URL);

      channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      await channel.prefetch(20000);
      logConsola(`✅ Conectado a RabbitMQ y escuchando la cola "${QUEUE_NAME}"`, 'ok');
      logConsola(`Esperando mensajes en la cola ${QUEUE_NAME}...`, 'info');

      channel.consume(QUEUE_NAME, async (msg) => {
        if (msg !== null) {
          const jsonData = JSON.parse(msg.content.toString());
          logConsola(`📩 Mensaje recibido`, 'info');

          try {
            await checkAndInsertData(jsonData);

            if (jsonData.operacion) {
              logConsola("⚙️  Procesando operación: " + jsonData.operacion, 'info');
              await updateProducction(jsonData);
            }

            channel.ack(msg);
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
const checkAndInsertData = async (jsonData) => {
  const { didempresa, didenvio, estado, subestado, estadoML, fecha, quien } = jsonData;
  const superado = jsonData.superado || 0;
  const elim = jsonData.elim || 0;
  const formattedFecha = moment(fecha).format('YYYY-MM-DD HH:mm:ss');
  const tableName = `estados_${didempresa}`;
  let latitud = jsonData.latitud || 0;
  let longitud = jsonData.longitud || 0;

  let dbConnection
  try {
    dbConnection = await getConnection(didempresa);
    // Conexión a la base de datos actual para obtener el chofer asignado
    const getChoferAsignadoQuery = `SELECT choferAsignado FROM envios WHERE elim = 0 AND superado = 0 AND did = ?`;

    const choferResults = await dbConnection.query(getChoferAsignadoQuery, [didenvio]);
    //console.log('Resultados de la consulta de chofer:', choferResults);

    // Asignar 0 si no se encuentran resultados
    const choferAsignado = (Array.isArray(choferResults) && choferResults.length > 0)
      ? choferResults[0].choferAsignado
      : 0;

    // Verificar si la tabla existe
    const [results] = await pool.query(`SHOW TABLES LIKE ?`, [tableName]);

    if (results.length > 0) {
      // Verificar si el didEnvio ya existe
      const [existingResults] = await pool.query(`SELECT * FROM ${tableName} WHERE didEnvio = ?`, [didenvio]);

      if (existingResults.length > 0) {
        // Actualizar solo el campo superado
        await pool.query(`UPDATE ${tableName} SET superado = ? WHERE didEnvio = ?`, [1, didenvio]);
        //  console.log(`Campo superado actualizado a 1 en la nueva base de datos: ${JSON.stringify(jsonData)}`);
      }

      // Insertar nuevo registro con los nuevos datos
      await pool.query(`
        INSERT INTO ${tableName} (didEnvio, operador, estado, estadoML, subestadoML, fecha, quien, superado, elim,latitud,longitud)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)
      `, [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim, latitud, longitud]);
      //console.log(`Nuevo registro insertado correctamente en la nueva base de datos: ${JSON.stringify(jsonData)}`);
    } else {
      // Crear tabla e insertar
      await pool.query(`CREATE TABLE ${tableName} (
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
      )`);

      await pool.query(`
        INSERT INTO ${tableName} (didEnvio, operador, estado, estadoML, subestadoML, fecha, quien, superado, elim,latitud,longitud)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim, latitud, longitud]);
      //console.log(`Tabla creada y datos insertados correctamente en la nueva base de datos: ${JSON.stringify(jsonData)}`);
    }
  } catch (error) {
    console.error('Error en checkAndInsertData:', error);
  } finally {

    dbConnection.end(); // Asegúrate de cerrar la conexión a la base de datos actual

  }
};


app.use(cors());

app.get('/ping', (req, res) => {
  const currentDate = new Date();
  currentDate.setHours(currentDate.getHours()); // Resta 3 horas

  // Formatear la hora en el formato HH:MM:SS
  const hours = currentDate.getHours().toString().padStart(2, '0');
  const minutes = currentDate.getMinutes().toString().padStart(2, '0');
  const seconds = currentDate.getSeconds().toString().padStart(2, '0');

  const formattedTime = `${hours}:${minutes}:${seconds}`;

  res.status(200).json({
    hora: formattedTime
  });
});
app.post('/estados', async (req, res) => {
  const jsonData = req.body;



  try {
    await checkAndInsertData(jsonData);

    if (jsonData.operacion) {
      await updateProducction(jsonData);
    }

    return res.status(200).json({ success: true, message: 'Estado procesado correctamente' });
  } catch (error) {
    console.error('❌ Error en endpoint /api/estados:', error);
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
