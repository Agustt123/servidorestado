const { executeQuery } = require("../dbconfig.js");



async function crearLog(empresa, usuario, perfil, body, tiempo, resultado, exito, db) {
    try {

        // Enmascarados según endpoint

        const sqlLog = `
      INSERT INTO logs_v2
        (empresa, usuario, perfil, body, tiempo, resultado, exito)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `;

        const values = [
            empresa,
            usuario,
            perfil ?? null,
            JSON.stringify(body ?? {}),          // siempre string
            tiempo ?? Date.now(),
            JSON.stringify(resultado ?? {}),     // siempre strin
            exito ? 1 : 0
        ];
        console.log(values);


        await executeQuery(db, sqlLog, values);
    } catch (error) {
        console.error('Error al crear el log:', error);
        throw error;
    }
}

module.exports = { crearLog };
