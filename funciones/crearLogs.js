const { executeQuery } = require("../dbconfig.js");



async function crearLog(empresa, usuario, perfil, body, tiempo, resultado, endpoint, exito, db) {
    try {
        // Si 'resultado' vino como JSON string, conviértelo a objeto
        let resultadoObj = typeof resultado === 'string'
            ? JSON.parse(resultado)
            : resultado;
        let bodyObj = typeof body === 'string'
            ? JSON.parse(body)
            : body;

        // Normaliza el endpoint (quita comillas sobrantes)
        const endpointClean = endpoint.replace(/"/g, '');

        if (endpointClean === '/company-identification' && exito == 1) {
            resultadoObj.image = 'Imagen eliminada por logs';
        }
        if (endpointClean === '/upload-image' || endpointClean === '/change-profile-picture') {
            bodyObj.image = 'Imagen eliminada por logs';
        }

        const sqlLog = `
            INSERT INTO logs_v2
                (empresa, usuario, perfil, body, tiempo, resultado, endpoint, exito)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `;

        // Siempre stringify para que el driver reciba un string
        const values = [
            empresa,
            usuario,
            perfil,
            JSON.stringify(bodyObj),
            tiempo,
            JSON.stringify(resultadoObj),
            endpointClean,
            exito ? 1 : 0
        ];

        await executeQuery(db, sqlLog, values);

    } catch (error) {
        console.error('Error al crear el log:', error);
        throw error;
    }
}

module.exports = { crearLog };