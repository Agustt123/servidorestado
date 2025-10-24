// controllers/setOrderStatusController.js
// npm i axios
const axios = require('axios');

async function setOrderStatusController(data) {
    try {
        const {
            platform,
            orderId,
            status,
            tracking = {},
            // credenciales vienen en el body:
            credentials = {},
            // en caso de que quieras pasar extras específicos por plataforma:
            prestashop = {},
            shopify = {}
        } = data || {};

        if (!platform || !orderId || !status) {
            throw new Error('Faltan platform, orderId o status');
        }

        const normalized = normalizeStatus(status);
        const p = String(platform).toLowerCase();

        let result;
        if (p === 'woocommerce') {
            result = await wooUpdate(credentials, orderId, normalized, tracking);
        } else if (p === 'prestashop') {
            result = await prestaUpdate({ ...credentials, ...prestashop }, orderId, normalized, tracking);
        } else if (p === 'tiendanube') {
            result = await tiendaNubeUpdate(credentials, orderId, normalized, tracking);
        } else if (p === 'shopify') {
            result = await shopifyUpdate({ ...credentials, ...shopify }, orderId, normalized, tracking);
        } else {
            throw new Error(`Plataforma no soportada: ${platform}`);
        }

        return { ok: true, data: result };
    } catch (err) {
        return { ok: false, error: err?.response?.data || err?.message || String(err) };
    }
}

/* ===================== Helpers comunes ===================== */
function normalizeStatus(s) {
    const key = String(s).toLowerCase().trim();
    if (['en_camino', 'en camino', 'in_transit', 'shipped'].includes(key)) return 'en_camino';
    if (['entregado', 'delivered', 'completed', 'fulfilled'].includes(key)) return 'entregado';
    throw new Error(`Estado no reconocido: ${s}. Usa "en_camino" o "entregado".`);
}

function stripSlash(u) {
    return String(u || '').replace(/\/+$/, '');
}

function makeTrackingNote(tr, status) {
    const parts = [
        `Estado: ${status === 'en_camino' ? 'En camino' : 'Entregado'}`,
        tr.carrier ? `Carrier: ${tr.carrier}` : null,
        tr.code ? `Tracking: ${tr.code}` : null,
        tr.url ? `URL: ${tr.url}` : null,
    ].filter(Boolean);
    return parts.join(' | ');
}

/* ===================== WooCommerce ===================== */
// en_camino → processing, entregado → completed
async function wooUpdate({ url, consumerKey, consumerSecret }, orderId, status, tracking) {
    if (!url || !consumerKey || !consumerSecret) throw new Error('Woo: faltan credenciales');

    // nota opcional con tracking (no bloquea)
    if (tracking && (tracking.code || tracking.url || tracking.carrier)) {
        try {
            await axios.post(
                `${stripSlash(url)}/wp-json/wc/v3/orders/${orderId}/notes`,
                { note: makeTrackingNote(tracking, status), customer_note: false },
                { auth: { username: consumerKey, password: consumerSecret } }
            );
        } catch (_) { }
    }

    const map = { en_camino: 'processing', entregado: 'completed' };
    const { data } = await axios.put(
        `${stripSlash(url)}/wp-json/wc/v3/orders/${orderId}`,
        { status: map[status] },
        { auth: { username: consumerKey, password: consumerSecret } }
    );
    return data;
}

/* ===================== PrestaShop ===================== */
// crea un order_history con el id_order_state (tenés que pasar IDs)
async function prestaUpdate({ url, apiKey, orderStateIds }, orderId, status, tracking) {
    if (!url || !apiKey) throw new Error('Presta: faltan credenciales');
    if (!orderStateIds || (!orderStateIds.shipped && !orderStateIds.delivered)) {
        throw new Error('Presta: faltan orderStateIds { shipped, delivered }');
    }

    const order_state = status === 'en_camino' ? orderStateIds.shipped : orderStateIds.delivered;
    const xml = `<?xml version="1.0" encoding="UTF-8"?>
<prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
  <order_history>
    <id_order><![CDATA[${orderId}]]></id_order>
    <id_order_state><![CDATA[${order_state}]]></id_order_state>
  </order_history>
</prestashop>`;

    const config = { headers: { 'Content-Type': 'text/xml' }, auth: { username: apiKey, password: '' } };

    try { await axios.get(`${stripSlash(url)}/api/order_histories?schema=blank`, config); } catch (_) { }

    const { data } = await axios.post(`${stripSlash(url)}/api/order_histories`, xml, config);

    // mensaje/nota opcional
    if (tracking && (tracking.code || tracking.url || tracking.carrier)) {
        const noteXml = `<?xml version="1.0" encoding="UTF-8"?>
<prestashop xmlns:xlink="http://www.w3.org/1999/xlink">
  <message>
    <id_order><![CDATA[${orderId}]]></id_order>
    <message><![CDATA[${makeTrackingNote(tracking, status)}]]></message>
  </message>
</prestashop>`;
        try {
            await axios.post(`${stripSlash(url)}/api/messages`, noteXml, config);
        } catch (_) { }
    }

    return data;
}

/* ===================== Tienda Nube ===================== */
// en_camino → shipped, entregado → delivered
async function tiendaNubeUpdate({ storeId, accessToken, userAgentEmail }, orderId, status, tracking) {
    if (!storeId || !accessToken) throw new Error('TN: faltan credenciales');

    const map = { en_camino: 'shipped', entregado: 'delivered' };
    const body = {
        shipping_status: map[status],
        ...(tracking?.code ? { shipping_tracking_number: tracking.code } : {}),
        ...(tracking?.url ? { shipping_tracking_url: tracking.url } : {}),
        ...(tracking?.carrier ? { shipping_carrier: tracking.carrier } : {}),
    };
    const headers = {
        'Authentication': `Bearer ${accessToken}`,
        'User-Agent': `idi-integration (${userAgentEmail || 'contact@example.com'})`,
        'Content-Type': 'application/json',
    };

    const { data } = await axios.put(
        `https://api.tiendanube.com/v1/${storeId}/orders/${orderId}`,
        body,
        { headers }
    );
    return data;
}

/* ===================== Shopify ===================== */
// usa Fulfillments + Fulfillment Events
async function shopifyUpdate({ shop, accessToken, locationId }, orderId, status, tracking) {
    if (!shop || !accessToken) throw new Error('Shopify: faltan credenciales');

    const base = `https://${shop}/admin/api/2024-07`;
    const headers = { 'X-Shopify-Access-Token': accessToken, 'Content-Type': 'application/json' };

    // 1) buscar fulfillments
    const fulfRes = await axios.get(`${base}/orders/${orderId}/fulfillments.json`, { headers });
    let fulfillment = (fulfRes.data.fulfillments || [])[0];

    // 2) si no hay, crear uno (muchas tiendas exigen location_id)
    if (!fulfillment) {
        const createBody = {
            fulfillment: {
                notify_customer: false,
                tracking_number: tracking?.code || undefined,
                tracking_urls: tracking?.url ? [tracking.url] : undefined,
                tracking_company: tracking?.carrier || undefined,
                location_id: locationId || undefined
            }
        };
        const createRes = await axios.post(`${base}/orders/${orderId}/fulfillments.json`, createBody, { headers });
        fulfillment = createRes.data.fulfillment;
    }

    // 3) registrar el evento
    const eventMap = { en_camino: 'in_transit', entregado: 'delivered' };
    const eventBody = {
        fulfillment_event: {
            status: eventMap[status],
            happened_at: new Date().toISOString(),
            tracking_number: tracking?.code || undefined,
            tracking_url: tracking?.url || undefined,
            message: tracking?.message || (status === 'en_camino' ? 'Pedido en tránsito' : 'Pedido entregado'),
        }
    };
    const { data } = await axios.post(
        `${base}/fulfillments/${fulfillment.id}/events.json`,
        eventBody,
        { headers }
    );
    return data;
}

module.exports = { setOrderStatusController };
