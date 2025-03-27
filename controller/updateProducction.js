const { getConnection, executeQuery } = require("../dbconfig");

const updateProducction = async (jsonData) => {
    const { didempresa, didenvio, estado, subestado, estadoML, fecha, quien } = jsonData;
    let dbConnection;

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
        SELECT quien 
        FROM envios_asignaciones 
        WHERE didEnvio = ? AND superado = 0 AND elim = 0
    `;
    const cadeteResults = await executeQuery(dbConnection, sqlDidCadete, [didenvio]);



    const didCadete = cadeteResults.length > 0 ? cadeteResults[0].quien : 0;
    const fechaT = fecha || new Date().toISOString().slice(0, 19).replace('T', ' ');

    // Convertir fechaT a un objeto Date
    const fechaDate = new Date(fechaT);
    
    // Sumar 3 horas
    fechaDate.setHours(fechaDate.getHours() );
    
    // Convertir de nuevo a formato ISO (si es necesario) o a tu formato deseado
    const nuevaFechaT = fechaDate.toISOString().slice(0, 19).replace('T', ' ');
    
console.log(nuevaFechaT, "fdssdasd");

    const sqlInsertHistorial = `
        INSERT INTO envios_historial (didEnvio, estado, quien, fecha, didCadete) 
        VALUES (?, ?, ?, ?, ?)
    `;
    await executeQuery(dbConnection, sqlInsertHistorial, [didenvio, estado, didCadete, nuevaFechaT, didCadete]);

} catch (error) {
    logRed(`Error en updateLastShipmentState: ${error.stack}`);
    throw error;
} finally {
    dbConnection.end();
}
};

module.exports = { updateProducction };