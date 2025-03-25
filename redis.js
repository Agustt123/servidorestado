const { redisClient } = require("./dbconfig");

async function limpiarEnviosViejos() {
    let DWRTE = await redisClient.get('DWRTE');
    if (!DWRTE) return;
    
    DWRTE = JSON.parse(DWRTE);
    const ahora = Date.now();
    const limite = 1 * 60 * 1000; // 5 minutos en milisegundos

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

// Ejecutar cada 30 segundos
setInterval(limpiarEnviosViejos, 30 * 1000);
