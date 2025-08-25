const { getConnection, executeQuery } = require("../dbconfig");

const updateProducction = async (jsonData) => {
  const {
    didempresa,
    didenvio,
    estado,
    subestado,
    estadoML,
    fecha,
    quien,
    latitud,
    longitud,
  } = jsonData;
  let dbConnection;
  console.log(jsonData, "jsonData");

  try {
    dbConnection = await getConnection(didempresa);

    const sqlSuperado = `
        UPDATE envios_historial 
        SET superado = 1 
        WHERE superado = 0 AND didEnvio = ?
    `;
    await executeQuery(dbConnection, sqlSuperado, [didenvio]);

    const sqlActualizarEnvios = `
        UPDATE envios 
        SET estado_envio = ? 
        WHERE superado = 0 AND did = ?
    `;
    await executeQuery(dbConnection, sqlActualizarEnvios, [estado, didenvio]);

    const sqlDidCadete = `
        SELECT operador 
        FROM envios_asignaciones 
        WHERE didEnvio = ? AND superado = 0 AND elim = 0
    `;
    const cadeteResults = await executeQuery(dbConnection, sqlDidCadete, [
      didenvio,
    ]);

    const didCadete = cadeteResults.length > 0 ? cadeteResults[0].operador : 0;
    const now = new Date(
      new Date().toLocaleString("en-US", {
        timeZone: "America/Argentina/Buenos_Aires",
      })
    );
    now.setHours(now.getHours() - 3);
    const fechaT = fecha || now.toISOString().slice(0, 19).replace("T", " ");

    if (jsonData.operacion == "ml") {
      const sqlInsertHistorial = `
        INSERT INTO envios_historial (didEnvio, estado, quien, fecha, didCadete,estadoML, subEstadoML)
        VALUES (?, ?, ?, ?, ?, ?, ?) 
  
    `;
      await executeQuery(dbConnection, sqlInsertHistorial, [
        didenvio,
        estado,
        quien,
        fechaT,
        didCadete,
        estadoML,
        subestado,
      ]);
    } else {
      let lat = latitud;
      let long = longitud;
      if (
        lat == undefined ||
        long == undefined ||
        lat == null ||
        long == null
      ) {
        lat = 0;
        long = 0;
      }

      const sqlInsertHistorial = `
           INSERT INTO envios_historial (didEnvio, estado, quien, fecha, didCadete,latitud,longitud) 
           VALUES (?, ?, ?, ?, ?,?,?)
       `;

      const resultado = await executeQuery(dbConnection, sqlInsertHistorial, [
        didenvio,
        estado,
        quien,
        fechaT,
        didCadete,
        lat,
        long,
      ]);
      console.log(estado);
      return resultado.insertId;

    }
  } catch (error) {
    console.log(`Error en updateLastShipmentState: ${error.stack}`);
    throw error;
  } finally {
    if (dbConnection) {
      dbConnection.end();
    }
  }
};
async function procesarEstadoIndividual({
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
}) {
  // 1) Marcar historiales previos como superados
  const sqlSuperado = `
    UPDATE envios_historial 
    SET superado = 1 
    WHERE superado = 0 AND didEnvio = ?
  `;
  await executeQuery(dbConnection, sqlSuperado, [didenvio]);

  // 2) Actualizar estado en envios
  const sqlActualizarEnvios = `
    UPDATE envios 
    SET estado_envio = ? 
    WHERE superado = 0 AND did = ?
  `;
  await executeQuery(dbConnection, sqlActualizarEnvios, [estado, didenvio]);

  // 3) Obtener cadete asignado
  const sqlDidCadete = `
    SELECT operador 
    FROM envios_asignaciones 
    WHERE didEnvio = ? AND superado = 0 AND elim = 0
  `;
  const cadeteResults = await executeQuery(dbConnection, sqlDidCadete, [didenvio]);
  const didCadete = cadeteResults.length > 0 ? cadeteResults[0].operador : 0;

  // 4) Fecha en AR si no viene
  const now = new Date(
    new Date().toLocaleString('en-US', { timeZone: 'America/Argentina/Buenos_Aires' })
  );
  // Mantengo tu ajuste -3hs por compatibilidad
  now.setHours(now.getHours() - 3);
  const fechaT = fecha || now.toISOString().slice(0, 19).replace('T', ' ');

  // 5) Insertar historial según operacion
  if (operacion === 'ml') {
    const sqlInsertML = `
      INSERT INTO envios_historial
        (didEnvio, estado, quien, fecha, didCadete, estadoML, subEstadoML)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `;
    const r = await executeQuery(dbConnection, sqlInsertML, [
      didenvio, estado, quien, fechaT, didCadete, estadoML, subestado,
    ]);
    return r.insertId;
  } else {
    // Normalizo lat/long
    const lat = (latitud ?? 0) || 0;
    const long = (longitud ?? 0) || 0;

    const sqlInsert = `
      INSERT INTO envios_historial
        (didEnvio, estado, quien, fecha, didCadete, latitud, longitud)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `;
    const r = await executeQuery(dbConnection, sqlInsert, [
      didenvio, estado, quien, fechaT, didCadete, lat, long,
    ]);
    return r.insertId;
  }
}


module.exports = { updateProducction, procesarEstadoIndividual };
