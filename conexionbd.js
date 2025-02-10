const mysql = require('mysql2/promise');

const newDbConfig = {
  host: 'localhost',
  user: 'userdata2',
  password: 'pt78pt79',
  database: 'dataestaos',
 // port: 44337
};

async function connectToDatabase() {
  let connection;

  try {
    // Intentar conectar a la base de datos
    connection = await mysql.createConnection(newDbConfig);
    console.log('¡Conexión exitosa a la base de datos!');

  } catch (error) {
    // Mostrar información detallada del error
    console.error('Error al conectar a la base de datos:');
    console.error(`Código: ${error.code}`);
    console.error(`Mensaje: ${error.message}`);
    console.error(`SQLSTATE: ${error.sqlState}`);
  } finally {
    // Cerrar la conexión si se estableció
    if (connection) {
      await connection.end();
    }
  }
}

// Llamar a la función para conectar
connectToDatabase();
