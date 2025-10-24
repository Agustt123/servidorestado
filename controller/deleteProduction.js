const { getConnection, executeQuery } = require("../dbconfig");

const deleteProduction = async (data) => {
    const { didEmpresa, didEnvio, didHistorial } = data;
    let dbConnection;

    try {
        dbConnection = await getConnection(didEmpresa);
        await dbConnection.beginTransaction();

        // 1) Soft delete del historial objetivo
        const sqlEliminar = `
      UPDATE envios_historial
      SET elim = 7
      WHERE didEnvio = ? AND id = ?
    `;
        await executeQuery(dbConnection, sqlEliminar, [didEnvio, didHistorial]);

        // 2) Marcar TODOS los no eliminados como superados (1)
        //    (no tocamos los que tienen elim!=0; así el eliminado puede quedar con superado=0, como tu ejemplo)
        const sqlMarcarNoEliminadosSuperados = `
      UPDATE envios_historial
      SET superado = 1
      WHERE didEnvio = ? AND elim = 0
    `;
        await executeQuery(dbConnection, sqlMarcarNoEliminadosSuperados, [didEnvio]);

        // 3) Elegir el nuevo vigente = último NO eliminado
        const sqlElegirVigente = `
      SELECT id, estado
      FROM envios_historial
      WHERE didEnvio = ? AND elim = 0
      ORDER BY id DESC
      LIMIT 1
    `;
        const vigenteRows = await executeQuery(dbConnection, sqlElegirVigente, [didEnvio]);
        const vigente = Array.isArray(vigenteRows) && vigenteRows.length ? vigenteRows[0] : null;

        let nuevoEstado = 0;

        if (vigente) {
            // 4) Des-superar SOLO el vigente
            const sqlDesuperarVigente = `
        UPDATE envios_historial
        SET superado = 0
        WHERE id = ? AND didEnvio = ? AND elim = 0
      `;
            await executeQuery(dbConnection, sqlDesuperarVigente, [vigente.id, didEnvio]);
            nuevoEstado = vigente.estado ?? 0;
        } else {
            // Si no quedan no eliminados, estado_envio a 0 (o el que definas como default)
            nuevoEstado = 0;
        }

        // 5) Actualizar envíos con el estado del nuevo vigente
        const sqlActualizarEnvio = `
      UPDATE envios
      SET estado_envio = ?
      WHERE did = ?
    `;
        await executeQuery(dbConnection, sqlActualizarEnvio, [nuevoEstado, didEnvio]);

        await dbConnection.commit();
        return {
            estado: true,
            error: 0,
            menssage: `Historial eliminado y estado actualizado (envíos.estado_envio=${nuevoEstado}) para el envío ${didEnvio}`,
        };

    } catch (error) {
        try { await dbConnection?.rollback(); } catch { }
        console.log(`Error en deleteProduction: ${error.stack || error.message}`);
        throw error;
    } finally {
        try {
            if (dbConnection?.release) dbConnection.release();   // ✅ devolver al pool
            // si algún día no fuera de pool:
            else if (dbConnection?.end) await dbConnection.end();
        } catch { }
    }

};

module.exports = { deleteProduction };
