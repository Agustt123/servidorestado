const { getConnection, executeQuery } = require("../dbconfig");

const updateProducction = async (jsonData) => {
    const { didempresa, didenvio, estado, subestado, estadoML, fecha, quien } = jsonData;
    let dbConnection;
    //console.log(dbConnection,"sdadasdsadssadsad");
    

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

    const now = new Date();
    now.setHours(now.getHours() );
    const fechaT = fecha || now.toISOString().slice(0, 19).replace('T', ' ');
    
   
    

    const sqlInsertHistorial = `
        INSERT INTO envios_historial (didEnvio, estado, quien, fecha, didCadete) 
        VALUES (?, ?, ?, ?, ?)
    `;
    await executeQuery(dbConnection, sqlInsertHistorial, [didenvio, estado, didCadete, fechaT, didCadete]);


} catch (error) {
    logRed(`Error en updateLastShipmentState: ${error.stack}`);
    throw error;
} finally { 
    if (dbConnection){


        dbConnection.end();
    }
}
};

module.exports = { updateProducction };