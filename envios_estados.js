const amqp = require('amqplib');
const { redisClient, getConnection } = require('./dbconfig');
const mysql = require('mysql'); // Usar mysql normal
const moment = require('moment'); 

const RABBITMQ_URL = 'amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672';
const QUEUE_NAME = 'srvshipmltosrvstates';

const newDbConfig = {
  host: 'mysqlhc.lightdata.com.ar',
  user: 'serveres_udata',
  password: '5j2[2A[]D@jQ',
  database: 'serveres_data',
};

// Función para escuchar los mensajes de la cola
const listenToQueue2 = async () => {
  let connection;
  let channel;

  const connect = async () => {
    try {
      connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      console.log(`Esperando mensajes en la cola ${QUEUE_NAME}...`);

      channel.consume(QUEUE_NAME, async (msg) => {
        if (msg !== null) {
          const jsonData = JSON.parse(msg.content.toString());
          console.log('Datos recibidos:', jsonData);

          try {
            await checkAndInsertData(jsonData);
            channel.ack(msg);
            console.log('Mensaje procesado.');
          } catch (error) {
            console.error('Error procesando el mensaje:', error);
            channel.nack(msg); // No confirmar el mensaje si hubo un error
          }
        }
      });

      // Manejar la desconexión
      connection.on('error', (err) => {
        console.error('Error en la conexión de RabbitMQ:', err);
      });

      connection.on('close', () => {
        console.error('Conexión a RabbitMQ cerrada, intentando reconectar...');
        setTimeout(connect, 5000); // Intentar reconectar cada 5 segundos
      });
    } catch (error) {
      console.error('Error conectando a RabbitMQ:', error);
      setTimeout(connect, 5000); // Intentar reconectar cada 5 segundos
    }
  };

  await connect(); // Iniciar la conexión
};

// Función para insertar los datos en la nueva base de datos
const checkAndInsertData = async (jsonData) => {
  const { didempresa, didenvio, estado, subestado, estadoML, fecha } = jsonData;
  const superado = jsonData.superado || 0;
  const elim = jsonData.elim || 0;
  const quien = jsonData.quien || 0;
  const formattedFecha = moment(fecha).format('YYYY-MM-DD HH:mm:ss'); 
  const tableName = `estados_${didempresa}`;

  let dbConnection;

  try {
    // Conexión a la base de datos actual para obtener el chofer asignado
    dbConnection = await getConnection(didempresa);
    const getChoferAsignadoQuery = `SELECT choferAsignado FROM envios WHERE elim = 0 AND superado = 0 AND did = ?`;

    dbConnection.query(getChoferAsignadoQuery, [didenvio], (err, choferResults) => {
      if (err) {
        console.error('Error al obtener el choferAsignado:', err);
        return;
      }

      const choferAsignado = choferResults.length > 0 ? choferResults[0].choferAsignado : 0;

      // Conexión a la nueva base de datos
      const newDbConnection = mysql.createConnection(newDbConfig);
      newDbConnection.connect((err) => {
        if (err) {
          console.error('Error conectando a la nueva base de datos:', err);
          return;
        }

        const queryTableExist = `SHOW TABLES LIKE ?`;
        newDbConnection.query(queryTableExist, [tableName], (err, results) => {
          if (err) {
            console.error('Error al verificar la existencia de la tabla en la nueva base de datos:', err);
            newDbConnection.end(); // Cerrar conexión aquí
            return;
          }

          if (results.length > 0) {
            // Verificar si el didEnvio ya existe
            const checkExistingQuery = `SELECT * FROM ${tableName} WHERE didEnvio = ?`;
            newDbConnection.query(checkExistingQuery, [didenvio], (err, existingResults) => {
              if (err) {
                console.error('Error al verificar el didEnvio en la nueva base de datos:', err);
                newDbConnection.end(); // Cerrar conexión aquí
                return;
              }

              if (existingResults.length > 0) {
                // Actualizar solo el campo superado
                const updateQuery = `UPDATE ${tableName} SET superado = ? WHERE didEnvio = ?`;
                newDbConnection.query(updateQuery, [1, didenvio], (err) => {
                  if (err) {
                    console.error('Error al actualizar el campo superado en la nueva base de datos:', err);
                  } else {
                    console.log(`Campo superado actualizado a 1 en la nueva base de datos: ${JSON.stringify(jsonData)}`);
                  }
                });
              } 
              // Insertar nuevo registro con los nuevos datos
              const insertQuery = `
                INSERT INTO ${tableName} (didEnvio, operador, estado, estadoML, subestadoML, fecha, quien, superado, elim)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              `;
              newDbConnection.query(insertQuery, [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim], (err) => {
                if (err) {
                  console.error('Error al insertar los nuevos datos en la nueva base de datos:', err);
                } else {
                  console.log(`Nuevo registro insertado correctamente en la nueva base de datos: ${JSON.stringify(jsonData)}`);
                }
                newDbConnection.end(); // Cerrar conexión aquí
              });
            });
          } else {
            // Crear tabla e insertar
            const createTableQuery = `CREATE TABLE ${tableName} (
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
              elim INT
            )`;
            newDbConnection.query(createTableQuery, (err) => {
              if (err) {
                console.error('Error al crear la tabla en la nueva base de datos:', err);
                newDbConnection.end(); // Cerrar conexión aquí
                return;
              }

              const insertQuery = `
                INSERT INTO ${tableName} (didEnvio, operador, estado, estadoML, subestadoML, fecha, quien, superado, elim)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
              `;
              newDbConnection.query(insertQuery, [didenvio, choferAsignado, estado, estadoML, subestado, formattedFecha, quien, superado, elim], (err) => {
                if (err) {
                  console.error('Error al insertar los datos en la nueva base de datos:', err);
                } else {
                  console.log(`Tabla creada y datos insertados correctamente en la nueva base de datos: ${JSON.stringify(jsonData)}`);
                }
                newDbConnection.end(); // Cerrar conexión aquí
              });
            });
          }
        });
      });
    });
  } catch (error) {
    console.error('Error en checkAndInsertData:', error);
  }
};

// Iniciar la escucha de la cola
listenToQueue2();

module.exports = { listenToQueue2 };
