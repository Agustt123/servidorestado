const { executeQuery } = require("../dbconfig.js");

function safeParseMaybeJSON(val, fallback = null) {
    if (val == null) return fallback;
    if (typeof val !== 'string') return val;        // ya es objeto/num/etc
    const s = val.trim();
    if (!s) return '';                               // string vacía → dejala vacía
    const looksJSON = s.startsWith('{') || s.startsWith('[') || s === 'true' || s === 'false' || /^-?\d+(\.\d+)?$/.test(s);
    if (!looksJSON) return val;                      // no parece JSON → conservar string
    try {
        return JSON.parse(s);
    } catch {
        return val;                                    // si falla el parseo, conservar string original
    }
}

async function crearLog(empresa, usuario, perfil, body, tiempo, resultado, endpoint, exito, db) {
    try {
        const bodyObj = safeParseMaybeJSON(body, {});
        const resultadoObj = safeParseMaybeJSON(resultado, {});
        const endpointClean = (endpoint || '').replace(/"/g, '');

        // Enmascarados según endpoint
        if (endpointClean === '/company-identification' && exito == 1 && bodyObj && typeof bodyObj === 'object') {
            // (en tu código original enmascarabas en resultado; aclaro por si era intencional)
            if (resultadoObj && typeof resultadoObj === 'object') {
                resultadoObj.image = 'Imagen eliminada por logs';
            }
        }
        if (endpointClean === '/upload-image' || endpointClean === '/change-profile-picture') {
            if (bodyObj && typeof bodyObj === 'object') {
                bodyObj.image = 'Imagen eliminada por logs';
            }
        }

        const sqlLog = `
      INSERT INTO logs_v2
        (empresa, usuario, perfil, body, tiempo, resultado, endpoint, exito)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `;

        const values = [
            empresa ?? null,
            usuario ?? null,
            perfil ?? null,
            JSON.stringify(bodyObj ?? {}),          // siempre string
            tiempo ?? Date.now(),
            JSON.stringify(resultadoObj ?? {}),     // siempre string
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
