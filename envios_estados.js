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
 host: '149.56.182.49',
 //host: 'localhost',
  user: 'userdata2',
  password: 'pt78pt79',
  database: 'dataestaos',
 port: 44337
};

// Crear un pool de conexiones
const pool = mysql.createPool({
  ...newDbConfig,
  connectionLimit: 10, // Limitar a 10 conexiones
});

const app = express();

// Función para escuchar los mensajes de la cola
const listenToQueue2 = async () => {
  
  let connection;
  let channel;
  
  const connect = async () => {
    try {
      connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      // Configurar el prefetch para procesar hasta 25 mensajes
      await channel.prefetch(20000);
   console.log(`Esperando mensajes en la cola ${QUEUE_NAME}...`);

      channel.consume(QUEUE_NAME, async (msg) => {
        if (msg !== null) {
          const jsonData = JSON.parse(msg.content.toString());
       //  console.log('Datos recibidos:', jsonData);
          
          try {
           await checkAndInsertData(jsonData);
            //   await updateEstadoRedis(jsonData.didempresa,jsonData.didenvio,jsonData.estado)
          //     console.log("pase");
               
            
           if(jsonData.operacion)
             {
              console.log("pase operacion");
             
                 await updateProducction(jsonData);
                
                
              }
             // console.log('Mensaje procesado.');
            
            //console.log("holaaa");
            channel.ack(msg); // No confirmar el mensaje si hubo un error
          } catch (error) {
            console.log(error.message);
            
           // console.error('Error procesando el mensaje:', error);
          }
        }
      });

      connection.on('error', (err) => {
        //console.error('Error en la conexión de RabbitMQ:', err);
      });

      connection.on('close', () => {
        //console.error('Conexión a RabbitMQ cerrada, intentando reconectar...');
        setTimeout(connect, 5000); // Intentar reconectar cada 5 segundos
      });
    } catch (error) {
      //console.error('Error conectando a RabbitMQ:', error);
      setTimeout(connect, 5000); // Intentar reconectar cada 5 segundos
    }
  };

  await connect(); // Iniciar la conexión
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
  let latitud=jsonData.latitud || 0;
  let longitud=jsonData.longitud || 0;

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
      `, [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim,latitud,longitud]);
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
      `, [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim,latitud,longitud]);
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

const PORT = 13000;
app.listen(PORT, () => {
  console.log(`Servidor escuchando en http://localhost:${PORT}`);
});

// Iniciar la escucha de la cola
listenToQueue2();
setInterval(limpiarEnviosViejos, 24 * 60 * 60 * 1000);


limpiarEnviosViejos();


module.exports = { listenToQueue2 };
