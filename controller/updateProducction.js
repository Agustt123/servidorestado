const { getConnection, executeQuery } = require("../dbconfig");

// Helper para timestamp AR (mantengo tu -3h por compatibilidad)
function ahoraARISO() {
  const now = new Date(
    new Date().toLocaleString("en-US", { timeZone: "America/Argentina/Buenos_Aires" })
  );
  now.setHours(now.getHours() - 3);
  return now.toISOString().slice(0, 19).replace("T", " ");
}

// Requiere: getConnection(didempresa) -> conn del pool (con .beginTransaction/.commit/.rollback/.release)
//           executeQuery(conn, sql, params) -> wrapper de conn.query que devuelve { affectedRows, insertId, ... }
//           ahoraARISO() -> fecha/hora en ISO (zona AR) si no viene en jsonData

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
    desde = "",
  } = jsonData;

  let conn;
  conn = await getConnection(didempresa);
  try {
    await conn.beginTransaction();

    // 1) Resolver cadete asignado antes del insert
    const sqlDidCadete = `
      SELECT operador 
      FROM envios_asignaciones 
      WHERE didEnvio = ? AND superado = 0 AND elim = 0
    `;
    const cadeteResults = await executeQuery(conn, sqlDidCadete, [didenvio]);
    const didCadete = cadeteResults?.length > 0 ? cadeteResults[0].operador : 0;

    // 2) Fecha efectiva
    const fechaT = fecha || ahoraARISO();

    // 3) INSERTAR historial nuevo (superado = 0 por default)
    let insertSql, insertParams;

    if (jsonData.operacion === "ml") {
      insertSql = `
        INSERT INTO envios_historial 
          (didEnvio, estado, quien, fecha, didCadete, estadoML, subEstadoML, desde)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `;
      insertParams = [didenvio, estado, quien, fechaT, didCadete, estadoML, subestado, desde];
    } else {
      const lat = Number.isFinite(+latitud) ? +latitud : 0;
      const long = Number.isFinite(+longitud) ? +longitud : 0;

      insertSql = `
        INSERT INTO envios_historial 
          (didEnvio, estado, quien, fecha, didCadete, latitud, longitud, desde)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `;
      insertParams = [didenvio, estado, quien, fechaT, didCadete, lat, long, desde];
    }

    const r = await executeQuery(conn, insertSql, insertParams);

    if (!r || r.affectedRows !== 1 || !r.insertId) {
      try { await conn.rollback(); } catch (_) { } //viendo si dejarlo
      return null;
    }

    const newId = r.insertId;

    // 4) Marcar historiales previos como superados (excluyendo el recién insertado)
    const sqlSuperarPrevios = `
      UPDATE envios_historial
      SET superado = 1
      WHERE didEnvio = ? AND superado = 0 AND id <> ?
    `;
    await executeQuery(conn, sqlSuperarPrevios, [didenvio, newId]);

    // 5) Actualizar estado en envios (sin filtro superado en esta tabla)
    const sqlActualizarEnvio = `
      UPDATE envios
      SET estado_envio = ?
      WHERE did = ?
    `;
    await executeQuery(conn, sqlActualizarEnvio, [estado, didenvio]);

    // 6) Commit final
    await conn.commit();
    return newId;

  } catch (error) {
    // Asegurar rollback ante cualquier error
    try { if (conn) await conn.rollback(); } catch (_) { }
    console.log(`Error en updateProduction: ${JSON.stringify(error) || error?.stack || error}`);
    throw error;
  } finally {
    // Liberar conexión al pool
    if (conn) {
      try { conn.release(); } catch (_) { }
    }
  }
};



// Si querés reusar esta función fuera, también la dejo con release()
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
  desde = "",
}) {
  // Asumo que dbConnection viene de getConnection() y el que llama maneja begin/commit si quiere
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

  // 3) Obtener cadete
  const sqlDidCadete = `
    SELECT operador 
    FROM envios_asignaciones 
    WHERE didEnvio = ? AND superado = 0 AND elim = 0
  `;
  const cadeteResults = await executeQuery(dbConnection, sqlDidCadete, [didenvio]);
  const didCadete = cadeteResults.length > 0 ? cadeteResults[0].operador : 0;

  // 4) Fecha
  const fechaT = fecha || ahoraARISO();

  // 5) Insert según operación
  if (operacion === "ml") {
    const sqlInsertML = `
      INSERT INTO envios_historial
        (didEnvio, estado, quien, fecha, didCadete, estadoML, subEstadoML, desde)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;
    const r = await executeQuery(dbConnection, sqlInsertML, [
      didenvio, estado, quien, fechaT, didCadete, estadoML, subestado, desde,
    ]);
    return r.insertId;
  } else {
    const lat = (latitud ?? 0) || 0;
    const long = (longitud ?? 0) || 0;

    const sqlInsert = `
      INSERT INTO envios_historial
        (didEnvio, estado, quien, fecha, didCadete, latitud, longitud, desde)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;
    const r = await executeQuery(dbConnection, sqlInsert, [
      didenvio, estado, quien, fechaT, didCadete, lat, long, desde,
    ]);
    return r.insertId;
  }
}

module.exports = { updateProducction, procesarEstadoIndividual };
