import express from "express";
import axios from "axios";
import crypto from "crypto";
import Redis from "ioredis";

// =========================
// ENV
// =========================
const PORT = process.env.PORT || 3000;

const WA_TOKEN = process.env.WA_TOKEN || "";
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID || "";
const VERIFY_TOKEN = process.env.VERIFY_TOKEN || "";
const META_APP_SECRET = process.env.META_APP_SECRET || "";

const BUSINESS_NAME =
  process.env.BUSINESS_NAME ||
  process.env.AGENCY_NAME ||
  process.env.CLINIC_NAME ||
  "Cavenasam Travel & Tour Group SRL";

const BUSINESS_ADDRESS =
  process.env.BUSINESS_ADDRESS ||
  process.env.CLINIC_ADDRESS ||
  "Punta Cana, República Dominicana";

const MARKET_CONTACT_TEXT =
  (
    process.env.MARKET_CONTACT_TEXT ||
    "📍 Base operativa: Punta Cana, República Dominicana.\n📲 Escríbenos por este WhatsApp y un asesor te ayuda con tu reserva."
  ).trim();

const FOLLOWUP_ENABLED = (process.env.FOLLOWUP_ENABLED || "1") === "1";
const FOLLOWUP_AFTER_MIN = parseInt(process.env.FOLLOWUP_AFTER_MIN || "180", 10);
const FOLLOWUP_MAX_AGE_HOURS = parseInt(process.env.FOLLOWUP_MAX_AGE_HOURS || "72", 10);

const PERSONAL_WA_TO = (process.env.PERSONAL_WA_TO || "").trim();

// =========================
// BOTHUB
// =========================
const BOTHUB_WEBHOOK_URL = (process.env.BOTHUB_WEBHOOK_URL || "").trim();
const BOTHUB_WEBHOOK_SECRET = (process.env.BOTHUB_WEBHOOK_SECRET || "").trim();
const BOTHUB_TIMEOUT_MS = Number(process.env.BOTHUB_TIMEOUT_MS || 6000);

const BOT_PUBLIC_BASE_URL = (process.env.BOT_PUBLIC_BASE_URL || "").replace(/\/$/, "");
const HUB_MEDIA_SECRET =
  (process.env.HUB_MEDIA_SECRET || BOTHUB_WEBHOOK_SECRET || VERIFY_TOKEN || "").trim();
const HUB_MEDIA_TTL_SEC = parseInt(process.env.HUB_MEDIA_TTL_SEC || "900", 10);
const META_GRAPH_VERSION =
  process.env.WHATSAPP_GRAPH_VERSION || process.env.META_GRAPH_VERSION || "v23.0";

// =========================
// REDIS
// =========================
const REDIS_URL_RAW = (process.env.REDIS_URL || "").trim();
const SESSION_TTL_SEC = parseInt(process.env.SESSION_TTL_SEC || String(60 * 60 * 24 * 14), 10);
const SESSION_PREFIX = process.env.SESSION_PREFIX || "tekko:travel:rd:sess:";

function normalizeRedisUrl(url) {
  const u = String(url || "").trim();
  if (!u) return "";
  if (u.startsWith("redis://")) return "rediss://" + u.slice("redis://".length);
  return u;
}

const redisUrl = normalizeRedisUrl(REDIS_URL_RAW);
const redis = redisUrl
  ? new Redis(redisUrl, {
      tls: redisUrl.startsWith("rediss://") ? { rejectUnauthorized: false } : undefined,
      maxRetriesPerRequest: 2,
      enableReadyCheck: true,
    })
  : null;

const sessions = new Map();

// =========================
// HELPERS
// =========================
function safeJson(str, fallback) {
  try {
    return JSON.parse(str);
  } catch {
    return fallback;
  }
}

function normalizeText(t) {
  return String(t || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizePhoneDigits(raw) {
  return String(raw || "").replace(/[^\d]/g, "");
}

function toE164DigitsRD(phoneDigits) {
  const d = normalizePhoneDigits(phoneDigits);
  if (d.length === 10) return "1" + d;
  if (d.length === 11 && d.startsWith("1")) return d;
  return d;
}

function buildPhoneVariants(raw) {
  const d = normalizePhoneDigits(raw);
  if (!d) return [];
  const set = new Set([d]);
  const e164 = toE164DigitsRD(d);
  if (e164) set.add(e164);
  if (d.length === 11 && d.startsWith("1")) set.add(d.slice(1));
  if (d.length === 10) set.add("1" + d);
  return Array.from(set).filter(Boolean);
}

function waRowTitle(title, max = 24) {
  const clean = String(title || "").replace(/\s+/g, " ").trim();
  if (!clean) return "";
  return clean.length <= max ? clean : clean.slice(0, max).trim();
}

function chunkRows(rows, size = 10) {
  const out = [];
  for (let i = 0; i < rows.length; i += size) {
    out.push(rows.slice(i, i + size));
  }
  return out;
}

function sanitizeFileName(name, fallback = "file") {
  const raw = String(name || fallback).trim() || fallback;
  return raw.replace(/[\\/:*?"<>|]+/g, "_");
}

function isGreeting(textNorm) {
  const t = textNorm || "";
  const greetings = [
    "hola",
    "buen dia",
    "buen día",
    "buenos dias",
    "buenos días",
    "buenas",
    "buenas tardes",
    "buenas noches",
    "saludos",
    "hey",
    "hi",
  ];

  return greetings.some((g) => t === g || t.startsWith(g + " ")) || /^(hola+|buenas+)\b/.test(t);
}

function isThanks(textNorm) {
  return ["gracias", "ok", "okay", "listo", "perfecto", "dale", "bien", "genial"].some(
    (k) => textNorm === k || textNorm.includes(k)
  );
}

function isChoice(textNorm, n) {
  const t = (textNorm || "").trim();
  return t === String(n) || t === `${n}.` || t.startsWith(`${n} `);
}

function wantsAdvisor(textNorm) {
  return ["asesor", "agente", "humano", "persona", "ayuda", "cotizacion", "cotización", "precio", "reservar"].some(
    (k) => (textNorm || "").includes(k)
  );
}

function mainMenuText() {
  return (
    `👋 ¡Hola! Soy el asistente de *${BUSINESS_NAME}*.\n\n` +
    `Puedo ayudarte con:\n` +
    `🌴 Tours en República Dominicana\n` +
    `✈️ Boletos aéreos\n` +
    `🏨 Solo hoteles\n` +
    `🛡️ Seguros de viaje\n` +
    `🚕 Traslados\n` +
    `🎒 Paquetes vacacionales\n` +
    `👤 Hablar con un asesor\n` +
    `📍 Ubicación y contacto\n\n` +
    `También puedes escribirme cosas como *"Tours desde Punta Cana"*, *"Tours de Marzo"* o *"Semana Santa"* y te muestro las opciones reales cargadas.`
  );
}

function quickHelpText() {
  return (
    `¡Hola! 😊\n` +
    `Puedo ayudarte con tours, boletos aéreos, hoteles, seguros, traslados y paquetes.\n\n` +
    `Escribe *"menú"* para ver las opciones o *"tours"* para ver el submenú de tours.`
  );
}

function buildLocationContactText() {
  const addressLine = BUSINESS_ADDRESS ? `📍 Dirección: ${BUSINESS_ADDRESS}\n` : "";
  return (`📍 *Ubicación y contacto*\n\n${addressLine}${MARKET_CONTACT_TEXT}`).trim();
}

function buildLeadSummary(title, fields = []) {
  const lines = [`📌 *${title}*`, ""];
  for (const f of fields) {
    lines.push(`${f.label}: ${f.value || "—"}`);
  }
  return lines.join("\n");
}

function defaultLead() {
  return {
    topic: "",
    followupSent: false,
    lastInteractionAt: "",
    quotePreview: "",
    converted: false,
  };
}

function defaultSession() {
  return {
    messages: [],
    state: "idle",
    greeted: false,
    lastMsgId: null,

    pendingServiceLine: null,
    pendingCollection: null,
    pendingTourKey: null,
    pendingLeadTopic: null,

    pendingName: null,
    pendingDestination: null,
    pendingDepartureCity: null,
    pendingTravelDateText: null,
    pendingPassengers: null,
    pendingTripDays: null,
    pendingTravelerAgesText: null,
    pendingHotelStars: null,
    pendingNights: null,
    pendingTransferRoute: null,
    pendingAdvisorTopic: null,

    lead: defaultLead(),
  };
}

function sanitizeSession(session) {
  if (!session || typeof session !== "object") return defaultSession();

  if (!Array.isArray(session.messages)) session.messages = [];
  session.messages = session.messages.slice(-20);

  if (!session.lead || typeof session.lead !== "object") {
    session.lead = defaultLead();
  } else {
    if (typeof session.lead.topic !== "string") session.lead.topic = "";
    if (typeof session.lead.followupSent !== "boolean") session.lead.followupSent = false;
    if (typeof session.lead.lastInteractionAt !== "string") session.lead.lastInteractionAt = "";
    if (typeof session.lead.quotePreview !== "string") session.lead.quotePreview = "";
    if (typeof session.lead.converted !== "boolean") session.lead.converted = false;
  }

  if (typeof session.state !== "string") session.state = "idle";
  if (typeof session.greeted !== "boolean") session.greeted = false;
  if (typeof session.lastMsgId !== "string" && session.lastMsgId !== null) session.lastMsgId = null;

  const maybeStringOrNull = [
    "pendingServiceLine",
    "pendingCollection",
    "pendingTourKey",
    "pendingLeadTopic",
    "pendingName",
    "pendingDestination",
    "pendingDepartureCity",
    "pendingTravelDateText",
    "pendingTravelerAgesText",
    "pendingHotelStars",
    "pendingTransferRoute",
    "pendingAdvisorTopic",
  ];

  for (const k of maybeStringOrNull) {
    if (typeof session[k] !== "string" && session[k] !== null) session[k] = null;
  }

  const maybeNumberOrNull = ["pendingPassengers", "pendingTripDays", "pendingNights"];
  for (const k of maybeNumberOrNull) {
    if (typeof session[k] !== "number" && session[k] !== null) session[k] = null;
  }

  return session;
}

async function getSession(userId) {
  if (!userId) return sanitizeSession(defaultSession());

  if (!redis) {
    if (!sessions.has(userId)) sessions.set(userId, defaultSession());
    return sanitizeSession(sessions.get(userId));
  }

  const key = `${SESSION_PREFIX}${userId}`;
  const raw = await redis.get(key);
  const s = raw ? safeJson(raw, defaultSession()) : defaultSession();
  return sanitizeSession(s);
}

async function saveSession(userId, session) {
  if (!userId || !session) return;
  session = sanitizeSession(session);

  if (!redis) {
    sessions.set(userId, session);
    return;
  }

  const key = `${SESSION_PREFIX}${userId}`;
  await redis.set(key, JSON.stringify(session), "EX", SESSION_TTL_SEC);
}

async function listAllSessionIds() {
  if (!redis) return Array.from(sessions.keys());

  const ids = [];
  let cursor = "0";

  do {
    const out = await redis.scan(cursor, "MATCH", `${SESSION_PREFIX}*`, "COUNT", 200);
    cursor = out[0];
    const keys = out[1] || [];
    for (const k of keys) ids.push(String(k).replace(SESSION_PREFIX, ""));
  } while (cursor !== "0");

  return ids;
}

function clearIntakeFlow(session) {
  session.state = "idle";
  session.pendingServiceLine = null;
  session.pendingCollection = null;
  session.pendingTourKey = null;
  session.pendingLeadTopic = null;

  session.pendingName = null;
  session.pendingDestination = null;
  session.pendingDepartureCity = null;
  session.pendingTravelDateText = null;
  session.pendingPassengers = null;
  session.pendingTripDays = null;
  session.pendingTravelerAgesText = null;
  session.pendingHotelStars = null;
  session.pendingNights = null;
  session.pendingTransferRoute = null;
  session.pendingAdvisorTopic = null;
}

function updateLead(session, patch = {}) {
  session.lead = {
    ...defaultLead(),
    ...(session.lead || {}),
    ...patch,
    lastInteractionAt: new Date().toISOString(),
  };
}

// =========================
// BOTHUB / SIGNATURES
// =========================
function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return `[${obj.map(stableStringify).join(",")}]`;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k])).join(",")}}`;
}

function removeUndefinedDeep(value) {
  if (Array.isArray(value)) return value.map(removeUndefinedDeep);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, v]) => v !== undefined)
        .map(([k, v]) => [k, removeUndefinedDeep(v)])
    );
  }
  return value;
}

function bothubHmacStable(payload, secret) {
  const raw = stableStringify(payload);
  return crypto.createHmac("sha256", secret).update(raw).digest("hex");
}

function bothubHmacJson(payload, secret) {
  return crypto.createHmac("sha256", secret).update(JSON.stringify(payload)).digest("hex");
}

function getHubSignature(req) {
  const h =
    req.get("X-HUB-SIGNATURE") ||
    req.get("x-hub-signature") ||
    req.get("X-Hub-Signature") ||
    req.get("X-HUB-SIGNATURE-256") ||
    req.get("X-Hub-Signature-256") ||
    req.get("x-hub-signature-256") ||
    "";

  const sig = String(h || "").trim();
  if (!sig) return "";
  return sig.startsWith("sha256=") ? sig.slice("sha256=".length) : sig;
}

function timingSafeEqualHex(aHex, bHex) {
  const a = Buffer.from(String(aHex || ""), "utf8");
  const b = Buffer.from(String(bHex || ""), "utf8");
  if (!a.length || a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function verifyHubSignature(reqBody, signatureHex, secret) {
  if (!signatureHex || !secret) return false;

  const expectedStable = bothubHmacStable(reqBody, secret);
  if (timingSafeEqualHex(signatureHex, expectedStable)) return true;

  const expectedJson = bothubHmacJson(reqBody, secret);
  if (timingSafeEqualHex(signatureHex, expectedJson)) return true;

  return false;
}

async function bothubReportMessage(payload) {
  if (!BOTHUB_WEBHOOK_URL || !BOTHUB_WEBHOOK_SECRET) return;

  try {
    const cleanPayload = removeUndefinedDeep(payload);
    const raw = stableStringify(cleanPayload);
    const sig = crypto.createHmac("sha256", BOTHUB_WEBHOOK_SECRET).update(raw).digest("hex");

    await axios.post(BOTHUB_WEBHOOK_URL, raw, {
      headers: {
        "Content-Type": "application/json",
        "X-HUB-SIGNATURE": sig,
      },
      timeout: BOTHUB_TIMEOUT_MS,
      transformRequest: [(data) => data],
    });
  } catch (e) {
    console.error("Bothub report failed:", e?.response?.data || e?.message || e);
  }
}

function verifyMetaSignature(req) {
  if (!META_APP_SECRET) return true;
  const signature = req.get("X-Hub-Signature-256");
  if (!signature) return false;

  const expected =
    "sha256=" +
    crypto.createHmac("sha256", META_APP_SECRET).update(req.rawBody || Buffer.from("")).digest("hex");

  try {
    return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected));
  } catch {
    return false;
  }
}

// =========================
// MEDIA PROXY
// =========================
function getRequestBaseUrl(req) {
  const proto = String(req.headers["x-forwarded-proto"] || req.protocol || "https")
    .split(",")[0]
    .trim();
  const host = String(req.headers["x-forwarded-host"] || req.get("host") || "")
    .split(",")[0]
    .trim();

  if (!host) return BOT_PUBLIC_BASE_URL || "";
  return `${proto}://${host}`;
}

function getBotPublicBaseUrl(req) {
  return BOT_PUBLIC_BASE_URL || getRequestBaseUrl(req);
}

function signHubMediaToken(mediaId, ts) {
  if (!HUB_MEDIA_SECRET) return "";
  return crypto.createHmac("sha256", HUB_MEDIA_SECRET).update(`${String(mediaId)}:${String(ts)}`).digest("hex");
}

function verifyHubMediaToken(mediaId, ts, sig) {
  if (!HUB_MEDIA_SECRET) return false;
  if (!mediaId || !ts || !sig) return false;

  const tsNum = Number(ts);
  if (!Number.isFinite(tsNum)) return false;

  const ageMs = Math.abs(Date.now() - tsNum);
  if (ageMs > HUB_MEDIA_TTL_SEC * 1000) return false;

  const expected = signHubMediaToken(mediaId, ts);
  return timingSafeEqualHex(sig, expected);
}

function buildHubMediaUrl(req, mediaId) {
  if (!mediaId || !HUB_MEDIA_SECRET) return "";
  const base = getBotPublicBaseUrl(req);
  if (!base) return "";

  const ts = String(Date.now());
  const sig = signHubMediaToken(mediaId, ts);

  return `${base.replace(/\/$/, "")}/hub_media/${encodeURIComponent(mediaId)}?ts=${encodeURIComponent(ts)}&sig=${encodeURIComponent(sig)}`;
}

function extractInboundMeta(msg) {
  if (!msg) return {};

  if (msg?.type === "audio") {
    return {
      kind: "AUDIO",
      mediaId: msg?.audio?.id,
      mimeType: msg?.audio?.mime_type,
      voice: msg?.audio?.voice,
    };
  }

  if (msg?.type === "location") {
    return {
      kind: "LOCATION",
      latitude: msg?.location?.latitude,
      longitude: msg?.location?.longitude,
      name: msg?.location?.name,
      address: msg?.location?.address,
    };
  }

  if (msg?.type === "image") {
    return {
      kind: "IMAGE",
      mediaId: msg?.image?.id,
      mimeType: msg?.image?.mime_type,
      caption: msg?.image?.caption,
    };
  }

  if (msg?.type === "video") {
    return {
      kind: "VIDEO",
      mediaId: msg?.video?.id,
      mimeType: msg?.video?.mime_type,
      caption: msg?.video?.caption,
    };
  }

  if (msg?.type === "document") {
    return {
      kind: "DOCUMENT",
      mediaId: msg?.document?.id,
      mimeType: msg?.document?.mime_type,
      filename: msg?.document?.filename,
    };
  }

  if (msg?.type === "sticker") {
    return { kind: "STICKER", mediaId: msg?.sticker?.id, mimeType: msg?.sticker?.mime_type };
  }

  if (msg?.type === "contacts") return { kind: "CONTACTS", count: msg?.contacts?.length || 0 };

  if (msg?.type === "reaction") {
    return { kind: "REACTION", emoji: msg?.reaction?.emoji, messageId: msg?.reaction?.message_id };
  }

  if (msg?.type === "order") {
    return {
      kind: "ORDER",
      itemCount: Array.isArray(msg?.order?.product_items) ? msg.order.product_items.length : 0,
    };
  }

  return { kind: msg?.type ? String(msg.type).toUpperCase() : "UNKNOWN" };
}

function extFromMimeType(mime) {
  const m = String(mime || "").toLowerCase();
  if (m.includes("audio/ogg")) return ".ogg";
  if (m.includes("audio/mpeg") || m.includes("audio/mp3")) return ".mp3";
  if (m.includes("audio/wav")) return ".wav";
  if (m.includes("audio/webm")) return ".webm";
  if (m.includes("image/jpeg")) return ".jpg";
  if (m.includes("image/png")) return ".png";
  if (m.includes("image/gif")) return ".gif";
  if (m.includes("image/webp")) return ".webp";
  if (m.includes("video/mp4")) return ".mp4";
  if (m.includes("application/pdf")) return ".pdf";
  return "";
}

async function getMetaMediaInfo(mediaId) {
  if (!WA_TOKEN) throw new Error("WA_TOKEN not configured");
  const res = await axios.get(
    `https://graph.facebook.com/${META_GRAPH_VERSION}/${encodeURIComponent(mediaId)}`,
    {
      headers: { Authorization: `Bearer ${WA_TOKEN}` },
      timeout: 30000,
      validateStatus: () => true,
    }
  );

  if (res.status < 200 || res.status >= 300) {
    throw new Error(
      res?.data?.error?.message ||
        res?.data?.error?.error_user_msg ||
        `Meta media lookup failed (${res.status})`
    );
  }

  return res.data || {};
}

async function downloadMetaMedia(mediaId) {
  const info = await getMetaMediaInfo(mediaId);
  const mediaUrl = info?.url;
  const mimeType = info?.mime_type || "application/octet-stream";

  if (!mediaUrl) throw new Error("Meta respondió sin url para ese mediaId");

  const bin = await axios.get(mediaUrl, {
    headers: { Authorization: `Bearer ${WA_TOKEN}` },
    responseType: "arraybuffer",
    timeout: 60000,
    validateStatus: () => true,
  });

  if (bin.status < 200 || bin.status >= 300) {
    throw new Error(typeof bin.data === "string" ? bin.data : `Meta media download failed (${bin.status})`);
  }

  return {
    buffer: Buffer.from(bin.data),
    mimeType,
  };
}

function attachHubMediaUrl(req, meta) {
  const out = { ...(meta || {}) };
  const kind = String(out?.kind || "").toUpperCase();

  if (out?.mediaId && ["AUDIO", "IMAGE", "VIDEO", "DOCUMENT", "STICKER"].includes(kind)) {
    const mediaUrl = buildHubMediaUrl(req, out.mediaId);
    if (mediaUrl) out.mediaUrl = mediaUrl;
  }

  return out;
}

// =========================
// MENÚS
// =========================
const SERVICE_LINES = [
  { key: "tours_rd", id: "svc_tours_rd", title: "Tours en República Dominicana" },
  { key: "boletos_aereos", id: "svc_boletos_aereos", title: "Boletos aéreos" },
  { key: "solo_hoteles", id: "svc_solo_hoteles", title: "Solo hoteles" },
  { key: "seguros_viaje", id: "svc_seguros_viaje", title: "Seguros de viaje" },
  { key: "traslados", id: "svc_traslados", title: "Traslados" },
  { key: "paquetes_vacacionales", id: "svc_paquetes_vacacionales", title: "Paquetes vacacionales" },
  { key: "hablar_asesor", id: "svc_hablar_asesor", title: "Hablar con un asesor" },
  { key: "ubicacion_contacto", id: "svc_ubicacion_contacto", title: "Ubicación y contacto" },
];

const SERVICE_LINE_ID_TO_KEY = Object.fromEntries(SERVICE_LINES.map((s) => [s.id, s.key]));

const TOUR_ORIGINS = [
  { key: "santo_domingo", id: "org_santo_domingo", title: "Santo Domingo" },
  { key: "punta_cana", id: "org_punta_cana", title: "Punta Cana" },
  { key: "las_terrenas", id: "org_las_terrenas", title: "Las Terrenas" },
];

const TOUR_ORIGIN_ID_TO_KEY = Object.fromEntries(TOUR_ORIGINS.map((o) => [o.id, o.key]));

const PACKAGE_DESTINATIONS = [
  { key: "peru", id: "pkg_peru", title: "Perú" },
  { key: "bogota", id: "pkg_bogota", title: "Bogotá" },
  { key: "miami", id: "pkg_miami", title: "Miami" },
  { key: "italia", id: "pkg_italia", title: "Italia" },
  { key: "otro_destino", id: "pkg_otro_destino", title: "Otro destino" },
];

const PACKAGE_DESTINATION_ID_TO_KEY = Object.fromEntries(PACKAGE_DESTINATIONS.map((p) => [p.id, p.key]));

const TOUR_SUBMENU_OPTIONS = [
  { key: "tours_punta_cana", id: "toursmenu_punta_cana", title: "Tours desde Punta Cana" },
  { key: "tours_marzo", id: "toursmenu_marzo", title: "Tours de Marzo" },
  { key: "tours_semana_santa", id: "toursmenu_semana_santa", title: "Tours Semana Santa" },
  { key: "ver_por_origen", id: "toursmenu_origen", title: "Ver por origen" },
  { key: "hablar_asesor_tours", id: "toursmenu_asesor", title: "Hablar con asesor" },
];

const TOUR_SUBMENU_ID_TO_KEY = Object.fromEntries(TOUR_SUBMENU_OPTIONS.map((o) => [o.id, o.key]));

// =========================
// TOURS REALES ENVIADOS POR EL CLIENTE
// =========================
const VISUAL_TOUR_COLLECTIONS = [
  {
    key: "tours_punta_cana",
    id: "col_tours_punta_cana",
    title: "Tours desde Punta Cana",
    triggers: ["tours desde punta cana", "tour desde punta cana", "punta cana tours"],
    originKeys: ["punta_cana"],
    tours: [
      {
        key: "pc_scoobadoo",
        id: "tour_pc_scoobadoo",
        title: "Scoobadoo",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427504/Scoobadoo_vjqbif.jpg",
      },
      {
        key: "pc_polaris",
        id: "tour_pc_polaris",
        title: "Polaris",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427504/Polaris_hgbvqi.jpg",
      },
      {
        key: "pc_maroca",
        id: "tour_pc_maroca",
        title: "Maroca",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427503/Maroca_hzzyps.jpg",
      },
      {
        key: "pc_jet_ski",
        id: "tour_pc_jet_ski",
        title: "Jet Ski",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427503/Jet-ski_kfxska.jpg",
      },
      {
        key: "pc_jet_cars",
        id: "tour_pc_jet_cars",
        title: "Jet Cars",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427502/Jet-cars_pu2p3w.jpg",
      },
      {
        key: "pc_isla_catalina",
        id: "tour_pc_isla_catalina",
        title: "Isla Catalina",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427501/Isla_Catalina_kavssn.jpg",
      },
      {
        key: "pc_horseback_riding",
        id: "tour_pc_horseback_riding",
        title: "Horseback Riding",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427501/Horseback_Riding_fwojde.jpg",
      },
      {
        key: "pc_fourwheel",
        id: "tour_pc_fourwheel",
        title: "Fourwheel",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Fourwheel_cixu6i.jpg",
      },
      {
        key: "pc_dorado_park",
        id: "tour_pc_dorado_park",
        title: "Dorado Park",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Dorado_Park_p8unjz.jpg",
      },
      {
        key: "pc_dolphin_ocean_aventure",
        id: "tour_pc_dolphin_ocean_aventure",
        title: "Dolphin Ocean Aventure",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Dolphin_ocean_aventure_tzzspl.jpg",
      },
      {
        key: "pc_coco_bongo",
        id: "tour_pc_coco_bongo",
        title: "Coco Bongo",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Coco_Bongo_dknp2w.jpg",
      },
      {
        key: "pc_cayo_new",
        id: "tour_pc_cayo_new",
        title: "Cayo New",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Cayo_New_m0ke20.jpg",
      },
      {
        key: "pc_buggies",
        id: "tour_pc_buggies",
        title: "Buggies",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/Buggies_d3s2th.jpg",
      },
      {
        key: "pc_jet_ski_aqua_kart_polaris",
        id: "tour_pc_jet_ski_aqua_kart_polaris",
        title: "Jet Ski + Aqua Kart + Polaris",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/3-_Jet-sky_Aqua-kart_Polaris_lm2sht.jpg",
      },
      {
        key: "pc_jet_ski_aqua_kart",
        id: "tour_pc_jet_ski_aqua_kart",
        title: "Jet Ski + Aqua Kart",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/2_-Jet-skit_Aqua-kart_cxpyzj.jpg",
      },
      {
        key: "pc_boat_party",
        id: "tour_pc_boat_party",
        title: "Boat Party",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/Boat_Party_g3iycw.jpg",
      },
    ],
  },
  {
    key: "tours_marzo",
    id: "col_tours_marzo",
    title: "Tours de Marzo",
    triggers: ["tours de marzo", "tour de marzo", "marzo"],
    originKeys: [],
    tours: [
      {
        key: "mar_santa_fe_full_day",
        id: "tour_mar_santa_fe_full_day",
        title: "Santa Fe Full Day",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428140/Santa_Fe_full_day_k2twpq.jpg",
      },
      {
        key: "mar_rio_y_playas_san_juan",
        id: "tour_mar_rio_y_playas_san_juan",
        title: "Río y Playas San Juan",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428139/Rio_y_playas_san_juan_ivhnev.jpg",
      },
      {
        key: "mar_parapente_jarabacoa",
        id: "tour_mar_parapente_jarabacoa",
        title: "Parapente Jarabacoa",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428138/Parapente_Jarabacoa_itfvyv.jpg",
      },
      {
        key: "mar_ocean_world_confresi_punta_cana",
        id: "tour_mar_ocean_world_confresi_punta_cana",
        title: "Ocean World Confresí Punta Cana",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428138/Ocean_world_confresi_punta_cana_wdbjq8.jpg",
      },
      {
        key: "mar_jarabacoa_fourwheel",
        id: "tour_mar_jarabacoa_fourwheel",
        title: "Jarabacoa Fourwheel",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428136/Jarabacoa_Fourwheel_doakpy.jpg",
      },
      {
        key: "mar_jarabacoa_city_tours",
        id: "tour_mar_jarabacoa_city_tours",
        title: "Jarabacoa City Tours",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428135/Jarabacoa_City_Tours_lzxkux.jpg",
      },
      {
        key: "mar_jarabacoa_city_polaris",
        id: "tour_mar_jarabacoa_city_polaris",
        title: "Jarabacoa City Polaris",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428134/Jarabacoa_city_polaris_y7aea1.jpg",
      },
      {
        key: "mar_isla_saona",
        id: "tour_mar_isla_saona",
        title: "Isla Saona",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428133/Isla_Saona_mcvfid.jpg",
      },
      {
        key: "mar_fourwheel_punta_cana",
        id: "tour_mar_fourwheel_punta_cana",
        title: "Fourwheel Punta Cana",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428133/Fourwheel_punta_cana_v8lw1l.jpg",
      },
      {
        key: "mar_cayo_arena",
        id: "tour_mar_cayo_arena",
        title: "Cayo Arena",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428132/Cayo_arena_twyhw9.jpg",
      },
      {
        key: "mar_ballenas_jorobada",
        id: "tour_mar_ballenas_jorobada",
        title: "Ballenas Jorobada",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428131/Ballenas_Jorobada_rv0ioc.jpg",
      },
      {
        key: "mar_cayo_levantado",
        id: "tour_mar_cayo_levantado",
        title: "Cayo Levantado",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428131/Cayo_levantado_mxh6gv.jpg",
      },
      {
        key: "mar_buggies_punta_cana",
        id: "tour_mar_buggies_punta_cana",
        title: "Buggies Punta Cana",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428130/Buggies_punta_cana_wcqwdl.jpg",
      },
    ],
  },
  {
    key: "tours_semana_santa",
    id: "col_tours_semana_santa",
    title: "Tours Semana Santa",
    triggers: ["semana santa", "tours semana santa", "tour semana santa"],
    originKeys: [],
    tours: [
      {
        key: "ss_polaris",
        id: "tour_ss_polaris",
        title: "Polaris",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427803/Polaris_mlhvmz.jpg",
      },
      {
        key: "ss_playa_dominicus",
        id: "tour_ss_playa_dominicus",
        title: "Playa Dominicus",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427803/Playa_Dominicus_uj7pg0.jpg",
      },
      {
        key: "ss_jet_ski",
        id: "tour_ss_jet_ski",
        title: "Jet Ski",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427802/Jet-ski_wr0dk5.jpg",
      },
      {
        key: "ss_isla_saona_2",
        id: "tour_ss_isla_saona_2",
        title: "Isla Saona 2",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427802/Isla_Saona2_z0kre2.jpg",
      },
      {
        key: "ss_isla_saona",
        id: "tour_ss_isla_saona",
        title: "Isla Saona",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427801/Isla_Saona_sndbbm.jpg",
      },
      {
        key: "ss_aqua_kart",
        id: "tour_ss_aqua_kart",
        title: "Aqua Kart",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427801/Aqua-kart_krqxuj.jpg",
      },
      {
        key: "ss_isla_catalina",
        id: "tour_ss_isla_catalina",
        title: "Isla Catalina",
        imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427800/Isla_Catalina_hxfyjm.jpg",
      },
    ],
  },
];

const VISUAL_COLLECTION_ID_TO_KEY = Object.fromEntries(
  VISUAL_TOUR_COLLECTIONS.map((c) => [c.id, c.key])
);

const VISUAL_TOURS = VISUAL_TOUR_COLLECTIONS.flatMap((c) =>
  c.tours.map((t) => ({
    ...t,
    collectionKey: c.key,
    collectionTitle: c.title,
  }))
);

const VISUAL_TOUR_ID_TO_KEY = Object.fromEntries(VISUAL_TOURS.map((t) => [t.id, t.key]));

function getCollectionByKey(key) {
  return VISUAL_TOUR_COLLECTIONS.find((c) => c.key === key) || null;
}

function getVisualTourByKey(key) {
  return VISUAL_TOURS.find((t) => t.key === key) || null;
}

function detectServiceLineFromUser(text) {
  const t = normalizeText(text);

  if (SERVICE_LINE_ID_TO_KEY[text]) return SERVICE_LINE_ID_TO_KEY[text];

  if (
    t === "tours" ||
    t === "tour" ||
    t.includes("tours en republica dominicana") ||
    t.includes("tours en república dominicana")
  ) {
    return "tours_rd";
  }

  if (
    t.includes("boleto") ||
    t.includes("vuelo") ||
    t.includes("vuelos") ||
    t.includes("aereo") ||
    t.includes("aéreo") ||
    t.includes("aerolinea") ||
    t.includes("aerolínea")
  ) {
    return "boletos_aereos";
  }

  if (t.includes("hotel") || t.includes("hospedaje") || t.includes("alojamiento")) {
    return "solo_hoteles";
  }

  if (
    t.includes("seguro de viaje") ||
    t.includes("seguros de viaje") ||
    t.includes("seguro") ||
    t.includes("asistencia de viaje")
  ) {
    return "seguros_viaje";
  }

  if (
    t.includes("traslado") ||
    t.includes("traslados") ||
    t.includes("aeropuerto") ||
    t.includes("transfer")
  ) {
    return "traslados";
  }

  if (
    t.includes("paquete vacacional") ||
    t.includes("paquetes vacacionales") ||
    t.includes("paquete") ||
    t.includes("paquetes")
  ) {
    return "paquetes_vacacionales";
  }

  if (
    t.includes("asesor") ||
    t.includes("agente") ||
    t.includes("humano") ||
    t.includes("persona")
  ) {
    return "hablar_asesor";
  }

  if (
    t.includes("ubicacion") ||
    t.includes("ubicación") ||
    t.includes("direccion") ||
    t.includes("dirección") ||
    t.includes("contacto") ||
    t.includes("oficina")
  ) {
    return "ubicacion_contacto";
  }

  return null;
}

function matchesOriginText(textNorm, label) {
  const v = normalizeText(label);
  const variants = [
    v,
    `desde ${v}`,
    `salgo de ${v}`,
    `salimos de ${v}`,
    `origen ${v}`,
    `salida ${v}`,
    `voy desde ${v}`,
    `me voy desde ${v}`,
    `quiero salir desde ${v}`,
  ];

  return variants.includes(textNorm);
}

function detectOriginKeyFromUser(text) {
  const t = normalizeText(text);
  if (TOUR_ORIGIN_ID_TO_KEY[text]) return TOUR_ORIGIN_ID_TO_KEY[text];

  for (const o of TOUR_ORIGINS) {
    if (matchesOriginText(t, o.title)) return o.key;
  }

  if (matchesOriginText(t, "bavaro") || matchesOriginText(t, "bávaro")) {
    return "punta_cana";
  }

  return null;
}

function detectPackageDestinationKeyFromUser(text) {
  const t = normalizeText(text);
  if (PACKAGE_DESTINATION_ID_TO_KEY[text]) return PACKAGE_DESTINATION_ID_TO_KEY[text];

  for (const p of PACKAGE_DESTINATIONS) {
    const norm = normalizeText(p.title);
    if (t === norm || t.includes(norm)) return p.key;
  }

  if (t.includes("peru") || t.includes("perú")) return "peru";
  if (t.includes("bogota") || t.includes("bogotá")) return "bogota";
  if (t.includes("miami")) return "miami";
  if (t.includes("italia")) return "italia";
  if (t.includes("otro destino")) return "otro_destino";

  return null;
}

function detectTourSubmenuAction(text) {
  const t = normalizeText(text);
  if (TOUR_SUBMENU_ID_TO_KEY[text]) return TOUR_SUBMENU_ID_TO_KEY[text];

  if (t.includes("tours desde punta cana")) return "tours_punta_cana";
  if (t.includes("tours de marzo") || t === "marzo") return "tours_marzo";
  if (t.includes("semana santa")) return "tours_semana_santa";
  if (t.includes("origen")) return "ver_por_origen";
  if (t.includes("asesor")) return "hablar_asesor_tours";

  return null;
}

function detectVisualCollectionFromUser(text) {
  const t = normalizeText(text);
  if (VISUAL_COLLECTION_ID_TO_KEY[text]) return VISUAL_COLLECTION_ID_TO_KEY[text];

  for (const c of VISUAL_TOUR_COLLECTIONS) {
    const titleNorm = normalizeText(c.title);
    if (t === titleNorm) return c.key;

    if (Array.isArray(c.triggers) && c.triggers.some((k) => t === normalizeText(k) || t.includes(normalizeText(k)))) {
      return c.key;
    }
  }

  return null;
}

function detectVisualTourFromUser(text, preferredCollectionKey = null) {
  const t = normalizeText(text);

  if (VISUAL_TOUR_ID_TO_KEY[text]) {
    return getVisualTourByKey(VISUAL_TOUR_ID_TO_KEY[text]);
  }

  const pools = [];
  if (preferredCollectionKey) {
    const preferred = VISUAL_TOURS.filter((x) => x.collectionKey === preferredCollectionKey);
    pools.push(preferred);
  }
  pools.push(VISUAL_TOURS);

  for (const pool of pools) {
    for (const tour of pool) {
      const titleNorm = normalizeText(tour.title);
      if (t === titleNorm || t.includes(titleNorm)) return tour;
    }
  }

  return null;
}

function parsePassengerCount(text) {
  const t = normalizeText(text);
  const digits = t.match(/\d+/);
  if (digits) return parseInt(digits[0], 10);

  const words = {
    uno: 1,
    una: 1,
    dos: 2,
    tres: 3,
    cuatro: 4,
    cinco: 5,
    seis: 6,
    siete: 7,
    ocho: 8,
    nueve: 9,
    diez: 10,
    ninguno: 0,
    ningun: 0,
    ningún: 0,
    cero: 0,
  };

  for (const [k, v] of Object.entries(words)) {
    if (t === k || t.includes(` ${k}`) || t.startsWith(k + " ")) return v;
  }

  return null;
}

// =========================
// WHATSAPP SEND HELPERS
// =========================
async function sendWhatsAppText(to, text, reportSource = "BOT") {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "text",
      text: { body: text },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: String(text),
    source: reportSource,
    kind: "TEXT",
  });
}

async function sendWhatsAppImage(to, imageUrl, caption = "", reportSource = "BOT") {
  if (!imageUrl) throw new Error("imageUrl is required");

  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "image",
      image: {
        link: imageUrl,
        caption: caption || undefined,
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: caption || "Imagen enviada",
    source: reportSource,
    kind: "IMAGE",
    meta: {
      link: imageUrl,
    },
  });
}

async function sendInteractiveList(to, { headerText, bodyText, buttonText, sections }, reportMeta = {}) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: headerText },
        body: { text: bodyText },
        footer: { text: BUSINESS_NAME },
        action: {
          button: buttonText,
          sections,
        },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: `${headerText}\n${bodyText}`,
    source: "BOT",
    kind: "LIST",
    meta: reportMeta,
  });
}

async function sendServiceLinesList(to) {
  const rows = SERVICE_LINES.map((s) => ({
    id: s.id,
    title: waRowTitle(s.title),
    description: "",
  }));

  await sendInteractiveList(
    to,
    {
      headerText: "Servicios disponibles",
      bodyText: "Selecciona el servicio que te interesa 👇",
      buttonText: "Ver opciones",
      sections: [{ title: "Servicios", rows }],
    },
    { rows }
  );
}

async function sendToursSubmenu(to) {
  const rows = TOUR_SUBMENU_OPTIONS.map((o) => ({
    id: o.id,
    title: waRowTitle(o.title),
    description: "",
  }));

  await sendInteractiveList(
    to,
    {
      headerText: "Submenú de tours",
      bodyText:
        "Aquí están las opciones reales de tours cargadas con las imágenes del cliente 👇",
      buttonText: "Ver tours",
      sections: [{ title: "Tours", rows }],
    },
    { rows }
  );
}

async function sendTourOriginsList(to) {
  const rows = TOUR_ORIGINS.map((o) => ({
    id: o.id,
    title: waRowTitle(o.title),
    description: "",
  }));

  await sendInteractiveList(
    to,
    {
      headerText: "Origen del tour",
      bodyText: "¿Desde dónde deseas salir? 👇",
      buttonText: "Elegir origen",
      sections: [{ title: "Salidas", rows }],
    },
    { rows }
  );
}

async function sendPackageDestinationsList(to) {
  const rows = PACKAGE_DESTINATIONS.map((d) => ({
    id: d.id,
    title: waRowTitle(d.title),
    description: "",
  }));

  await sendInteractiveList(
    to,
    {
      headerText: "Paquetes vacacionales",
      bodyText: "Elige el destino que te interesa 👇",
      buttonText: "Ver destinos",
      sections: [{ title: "Destinos", rows }],
    },
    { rows }
  );
}

async function sendVisualCollectionList(to, collectionKey) {
  const collection = getCollectionByKey(collectionKey);
  if (!collection || !Array.isArray(collection.tours) || !collection.tours.length) {
    await sendWhatsAppText(to, "No encontré tours en esa colección ahora mismo 🙏");
    return;
  }

  const rows = collection.tours.map((t) => ({
    id: t.id,
    title: waRowTitle(t.title),
    description: "",
  }));

  const sections = chunkRows(rows, 10).map((chunk, idx) => ({
    title: `${waRowTitle(collection.title, 20)} ${idx + 1}`,
    rows: chunk,
  }));

  await sendWhatsAppText(
    to,
    `🌴 *${collection.title}*\n\nEstas son las opciones visuales reales cargadas del cliente.\nSelecciona el tour que deseas ver 👇`
  );

  await sendInteractiveList(
    to,
    {
      headerText: collection.title,
      bodyText: "Elige un tour para ver su imagen e información visual 👇",
      buttonText: "Ver tours",
      sections,
    },
    { collectionKey, rows }
  );
}

async function sendVisualTourCard(to, tour) {
  const caption =
    `🌴 *${tour.title}*\n` +
    `Colección: *${tour.collectionTitle}*\n\n` +
    `Esta es la pieza visual real enviada por el cliente.\n\n` +
    `Si deseas *precio, disponibilidad o reservar*, responde:\n` +
    `1) Hablar con asesor\n` +
    `2) Ver más tours`;

  await sendWhatsAppImage(to, tour.imageUrl, caption, "BOT");
}

async function notifyPersonalWhatsAppLeadSummary(summaryText, customerPhone = "") {
  try {
    if (!PERSONAL_WA_TO) return;

    const myTo = String(PERSONAL_WA_TO).replace(/[^\d]/g, "");
    const leadPhone = String(customerPhone || "").replace(/[^\d]/g, "");
    if (!myTo) return;
    if (leadPhone && myTo === leadPhone) return;

    await sendWhatsAppText(myTo, summaryText, "BOT");
  } catch (e) {
    console.error("notifyPersonalWhatsAppLeadSummary error:", e?.response?.data || e?.message || e);
  }
}

// =========================
// EXPRESS APP
// =========================
const app = express();
app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);

// =========================
// WEBHOOK VERIFY
// =========================
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];

  if (mode === "subscribe" && token === VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }

  return res.sendStatus(403);
});

function extractIncomingText(msg) {
  if (!msg) return "";
  if (msg?.text?.body) return msg.text.body;

  if (msg?.type === "interactive" && msg?.interactive?.list_reply) {
    const lr = msg.interactive.list_reply;
    return lr.id || lr.title || "";
  }

  if (msg?.type === "interactive" && msg?.interactive?.button_reply) {
    const br = msg.interactive.button_reply;
    return br.id || br.title || "";
  }

  if (msg?.type === "interactive" && msg?.interactive?.product_reply) {
    const pr = msg.interactive.product_reply;
    return pr?.product_retailer_id || pr?.title || "";
  }

  if (msg?.type === "order" && Array.isArray(msg?.order?.product_items) && msg.order.product_items.length) {
    const first = msg.order.product_items[0];
    return first?.product_retailer_id || first?.name || first?.title || "";
  }

  if (msg?.type === "audio" && msg?.audio?.id) return "[AUDIO]";
  if (msg?.type === "location" && msg?.location) {
    const { latitude, longitude, name, address } = msg.location;
    return `📍 Ubicación: ${name || ""} ${address || ""} (${latitude}, ${longitude})`.trim();
  }
  if (msg?.type === "image" && msg?.image?.id) return "[IMAGE]";
  if (msg?.type === "video" && msg?.video?.id) return "[VIDEO]";
  if (msg?.type === "document" && msg?.document?.id) return "[DOCUMENT]";
  if (msg?.type === "sticker" && msg?.sticker?.id) return "[STICKER]";
  if (msg?.type === "contacts" && msg?.contacts?.length) return "[CONTACTS]";
  if (msg?.type === "reaction" && msg?.reaction) return `[REACTION] ${msg.reaction.emoji || ""}`.trim();

  return `[${(msg?.type || "UNKNOWN").toUpperCase()}]`;
}

// =========================
// AGENT MESSAGE
// =========================
app.post("/agent_message", async (req, res) => {
  try {
    if (!BOTHUB_WEBHOOK_SECRET) {
      return res.status(400).json({ error: "BOTHUB_WEBHOOK_SECRET not configured" });
    }

    const signature = getHubSignature(req);
    const okSig = verifyHubSignature(req.body, signature, BOTHUB_WEBHOOK_SECRET);

    if (!signature || !okSig) {
      return res.status(401).json({ error: "Invalid signature" });
    }

    const { waTo, text } = req.body || {};
    if (!waTo || !String(waTo).trim()) return res.status(400).json({ error: "waTo is required" });
    if (!text || !String(text).trim()) return res.status(400).json({ error: "text is required" });

    await sendWhatsAppText(String(waTo), String(text), "AGENT");
    return res.json({ ok: true });
  } catch (e) {
    console.error("agent_message error:", e?.response?.data || e?.message || e);
    return res.status(500).json({ error: "Internal error" });
  }
});

// =========================
// HUB MEDIA
// =========================
app.get("/hub_media/:mediaId", async (req, res) => {
  try {
    const { mediaId } = req.params || {};
    const ts = String(req.query?.ts || "");
    const sig = String(req.query?.sig || "");

    if (!mediaId) return res.status(400).json({ error: "mediaId is required" });
    if (!verifyHubMediaToken(mediaId, ts, sig)) {
      return res.status(401).json({ error: "Invalid or expired media signature" });
    }
    if (!WA_TOKEN) return res.status(500).json({ error: "WA_TOKEN not configured in bot" });

    const info = await getMetaMediaInfo(mediaId);
    const mimeType = String(info?.mime_type || "application/octet-stream");
    const filename = sanitizeFileName(
      info?.filename || `media-${mediaId}${extFromMimeType(mimeType)}`,
      `media-${mediaId}${extFromMimeType(mimeType)}`
    );

    const downloaded = await downloadMetaMedia(mediaId);

    res.setHeader("Content-Type", downloaded.mimeType || mimeType);
    res.setHeader("Cache-Control", "private, max-age=300");
    res.setHeader("Content-Disposition", `inline; filename="${String(filename).replace(/"/g, "")}"`);
    return res.status(200).send(downloaded.buffer);
  } catch (e) {
    console.error("hub_media error:", e?.response?.data || e?.message || e);
    return res.status(500).json({
      error: "hub_media_failed",
      detail: e?.response?.data || e?.message || "unknown",
    });
  }
});

// =========================
// MAIN WEBHOOK
// =========================
app.post("/webhook", async (req, res) => {
  let from = "";
  let session = null;

  try {
    if (!verifyMetaSignature(req)) return res.sendStatus(403);

    const entry = req.body.entry?.[0];
    const change = entry?.changes?.[0];
    const value = change?.value;

    const incomingPhoneNumberId = String(value?.metadata?.phone_number_id || "").trim();
    const expectedPhoneNumberId = String(PHONE_NUMBER_ID || "").trim();

    if (incomingPhoneNumberId && expectedPhoneNumberId && incomingPhoneNumberId !== expectedPhoneNumberId) {
      return res.sendStatus(200);
    }

    const msg = value?.messages?.[0];
    if (!msg) return res.sendStatus(200);

    from = msg.from;
    if (!from) return res.sendStatus(200);

    session = await getSession(from);

    const msgId = msg?.id;
    if (msgId && session.lastMsgId === msgId) return res.sendStatus(200);
    if (msgId) session.lastMsgId = msgId;

    const userTextRaw = extractIncomingText(msg);
    const userText = (userTextRaw || "").trim();
    const tNorm = normalizeText(userText);
    if (!userText) return res.sendStatus(200);

    const inboundMeta = extractInboundMeta(msg);
    const inboundMetaWithMediaUrl = attachHubMediaUrl(req, inboundMeta);

    await bothubReportMessage({
      direction: "INBOUND",
      from: String(from),
      body: String(userText),
      source: "WHATSAPP",
      waMessageId: msg?.id,
      name: value?.contacts?.[0]?.profile?.name,
      kind: inboundMetaWithMediaUrl?.kind || (msg?.type ? String(msg.type).toUpperCase() : "UNKNOWN"),
      meta: inboundMetaWithMediaUrl,
      mediaUrl: inboundMetaWithMediaUrl?.mediaUrl || undefined,
    });

    // =========================
    // SALUDO
    // =========================
    if (!session.greeted && isGreeting(tNorm) && session.state === "idle") {
      session.greeted = true;
      await sendWhatsAppText(from, mainMenuText());
      await sendServiceLinesList(from);
      return res.sendStatus(200);
    }

    if (session.greeted && isGreeting(tNorm) && session.state === "idle") {
      await sendWhatsAppText(from, quickHelpText());
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle") session.greeted = true;

    // =========================
    // MENÚS RÁPIDOS
    // =========================
    if (
      tNorm.includes("menu") ||
      tNorm.includes("menú") ||
      tNorm === "inicio" ||
      tNorm === "servicios"
    ) {
      clearIntakeFlow(session);
      await sendWhatsAppText(from, mainMenuText());
      await sendServiceLinesList(from);
      return res.sendStatus(200);
    }

    if (
      tNorm === "tours" ||
      tNorm === "tour" ||
      tNorm === "menu tours" ||
      tNorm === "menú tours"
    ) {
      clearIntakeFlow(session);
      session.pendingServiceLine = "tours_rd";
      session.state = "await_tour_menu";
      await sendWhatsAppText(
        from,
        `🌴 *Tours en República Dominicana*\n\nAquí verás únicamente las opciones reales cargadas con las imágenes enviadas por el cliente.`
      );
      await sendToursSubmenu(from);
      return res.sendStatus(200);
    }

    // =========================
    // TOUR SUBMENÚ
    // =========================
    if (session.state === "await_tour_menu") {
      const action = detectTourSubmenuAction(userText) || detectVisualCollectionFromUser(userText);

      if (action === "tours_punta_cana") {
        session.pendingServiceLine = "tours_rd";
        session.pendingCollection = "tours_punta_cana";
        session.state = "await_visual_tour_select";
        await sendVisualCollectionList(from, "tours_punta_cana");
        return res.sendStatus(200);
      }

      if (action === "tours_marzo") {
        session.pendingServiceLine = "tours_rd";
        session.pendingCollection = "tours_marzo";
        session.state = "await_visual_tour_select";
        await sendVisualCollectionList(from, "tours_marzo");
        return res.sendStatus(200);
      }

      if (action === "tours_semana_santa") {
        session.pendingServiceLine = "tours_rd";
        session.pendingCollection = "tours_semana_santa";
        session.state = "await_visual_tour_select";
        await sendVisualCollectionList(from, "tours_semana_santa");
        return res.sendStatus(200);
      }

      if (action === "ver_por_origen") {
        session.pendingServiceLine = "tours_rd";
        session.state = "await_tour_origin";
        await sendWhatsAppText(
          from,
          `Perfecto 🌴\nDime desde dónde sales o elige una opción del menú.`
        );
        await sendTourOriginsList(from);
        return res.sendStatus(200);
      }

      if (action === "hablar_asesor_tours") {
        session.pendingServiceLine = "tours_rd";
        session.pendingLeadTopic = "Consulta de tours";
        session.state = "await_advisor_name";
        await sendWhatsAppText(from, `Perfecto 👤\nAntes de pasarte con el asesor, dime tu *nombre completo*.`);
        return res.sendStatus(200);
      }

      await sendWhatsAppText(from, `Selecciona una opción del submenú de tours 👇`);
      await sendToursSubmenu(from);
      return res.sendStatus(200);
    }

    // =========================
    // ORIGEN TOURS
    // =========================
    if (session.state === "await_tour_origin") {
      const originKey = detectOriginKeyFromUser(userText);

      if (!originKey) {
        await sendWhatsAppText(
          from,
          `No logré identificar el origen 🙏\nOpciones: Santo Domingo, Punta Cana o Las Terrenas.`
        );
        await sendTourOriginsList(from);
        return res.sendStatus(200);
      }

      if (originKey === "punta_cana") {
        session.pendingCollection = "tours_punta_cana";
        session.state = "await_visual_tour_select";
        await sendVisualCollectionList(from, "tours_punta_cana");
        return res.sendStatus(200);
      }

      session.pendingLeadTopic = `Consulta de tours desde ${TOUR_ORIGINS.find((o) => o.key === originKey)?.title || originKey}`;
      session.state = "await_advisor_name";
      await sendWhatsAppText(
        from,
        `Ahora mismo la información real cargada en imágenes es de *Punta Cana*, *Tours de Marzo* y *Semana Santa*.\n\nSi deseas, te paso con un asesor para ayudarte con *${TOUR_ORIGINS.find((o) => o.key === originKey)?.title || originKey}*.\n\nDime tu *nombre completo*.`
      );
      return res.sendStatus(200);
    }

    // =========================
    // SELECCIÓN DE TOUR VISUAL
    // =========================
    if (session.state === "await_visual_tour_select") {
      const selectedTour = detectVisualTourFromUser(userText, session.pendingCollection);

      if (!selectedTour) {
        await sendWhatsAppText(from, `Selecciona uno de los tours de la lista para mostrarte la imagen correcta 🙏`);
        await sendVisualCollectionList(from, session.pendingCollection || "tours_punta_cana");
        return res.sendStatus(200);
      }

      session.pendingTourKey = selectedTour.key;
      session.state = "await_tour_post_action";

      updateLead(session, {
        topic: `Interés en tour: ${selectedTour.title}`,
        quotePreview: `${selectedTour.title} - ${selectedTour.collectionTitle}`,
        converted: false,
        followupSent: false,
      });

      await sendVisualTourCard(from, selectedTour);
      return res.sendStatus(200);
    }

    // =========================
    // POST TOUR VISUAL
    // =========================
    if (session.state === "await_tour_post_action" && session.pendingTourKey) {
      const selectedTour = getVisualTourByKey(session.pendingTourKey);

      if (!selectedTour) {
        clearIntakeFlow(session);
        await sendWhatsAppText(from, `Vamos a empezar de nuevo con los tours.`);
        await sendToursSubmenu(from);
        return res.sendStatus(200);
      }

      if (isChoice(tNorm, 1) || wantsAdvisor(tNorm)) {
        session.pendingLeadTopic = `Interés en tour: ${selectedTour.title} (${selectedTour.collectionTitle})`;
        session.state = "await_advisor_name";
        await sendWhatsAppText(
          from,
          `Perfecto 👤\nTe ayudo con *${selectedTour.title}*.\n\nDime tu *nombre completo* para pasarte con el asesor.`
        );
        return res.sendStatus(200);
      }

      if (isChoice(tNorm, 2) || tNorm.includes("mas tours") || tNorm.includes("más tours")) {
        session.state = "await_visual_tour_select";
        await sendVisualCollectionList(from, selectedTour.collectionKey);
        return res.sendStatus(200);
      }

      if (isThanks(tNorm)) {
        await sendWhatsAppText(
          from,
          `Con gusto 😊\nSi deseas *precio, disponibilidad o reservar*, responde *1* para hablar con un asesor.\n\nTambién puedes responder *2* para ver más tours.`
        );
        return res.sendStatus(200);
      }

      await sendWhatsAppText(
        from,
        `Responde:\n1) Hablar con asesor\n2) Ver más tours\n\nO escribe *menú* para volver al inicio.`
      );
      return res.sendStatus(200);
    }

    // =========================
    // ATAJOS DIRECTOS A COLECCIONES
    // =========================
    const directCollection = detectVisualCollectionFromUser(userText);
    if (directCollection) {
      clearIntakeFlow(session);
      session.pendingServiceLine = "tours_rd";
      session.pendingCollection = directCollection;
      session.state = "await_visual_tour_select";
      await sendVisualCollectionList(from, directCollection);
      return res.sendStatus(200);
    }

    // =========================
    // TOUR DIRECTO POR NOMBRE
    // =========================
    const directTour = detectVisualTourFromUser(userText);
    if (directTour) {
      clearIntakeFlow(session);
      session.pendingServiceLine = "tours_rd";
      session.pendingCollection = directTour.collectionKey;
      session.pendingTourKey = directTour.key;
      session.state = "await_tour_post_action";

      updateLead(session, {
        topic: `Interés en tour: ${directTour.title}`,
        quotePreview: `${directTour.title} - ${directTour.collectionTitle}`,
        converted: false,
        followupSent: false,
      });

      await sendVisualTourCard(from, directTour);
      return res.sendStatus(200);
    }

    // =========================
    // MENÚ PRINCIPAL -> SERVICIOS
    // =========================
    const serviceLineKey = detectServiceLineFromUser(userText);
    if (serviceLineKey) {
      clearIntakeFlow(session);
      session.pendingServiceLine = serviceLineKey;

      if (serviceLineKey === "ubicacion_contacto") {
        await sendWhatsAppText(from, buildLocationContactText());
        return res.sendStatus(200);
      }

      if (serviceLineKey === "hablar_asesor") {
        session.pendingLeadTopic = "Consulta general con asesor";
        session.state = "await_advisor_name";
        await sendWhatsAppText(
          from,
          `Perfecto 👤\nVamos a pasarte con un asesor.\n\nDime tu *nombre completo*.`
        );
        return res.sendStatus(200);
      }

      if (serviceLineKey === "tours_rd") {
        session.state = "await_tour_menu";
        await sendWhatsAppText(
          from,
          `🌴 *Tours en República Dominicana*\n\nAquí verás únicamente las opciones reales cargadas con las imágenes enviadas por el cliente.`
        );
        await sendToursSubmenu(from);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "boletos_aereos") {
        session.state = "await_flight_origin";
        await sendWhatsAppText(
          from,
          `Perfecto ✈️\nVamos con *boletos aéreos*.\n\n¿Desde dónde deseas salir?\nEj: Santo Domingo, Punta Cana o Santiago.`
        );
        return res.sendStatus(200);
      }

      if (serviceLineKey === "solo_hoteles") {
        session.state = "await_hotel_destination";
        await sendWhatsAppText(
          from,
          `Perfecto 🏨\nVamos con *solo hoteles*.\n\n¿En qué *destino o ciudad* deseas hospedarte?`
        );
        return res.sendStatus(200);
      }

      if (serviceLineKey === "seguros_viaje") {
        session.state = "await_insurance_destination";
        await sendWhatsAppText(
          from,
          `Perfecto 🛡️\nVamos con *seguros de viaje*.\n\n¿A qué *país o destino* viajas?`
        );
        return res.sendStatus(200);
      }

      if (serviceLineKey === "traslados") {
        session.state = "await_transfer_route";
        await sendWhatsAppText(
          from,
          `Perfecto 🚕\nVamos con *traslados*.\n\nDime la *ruta* que necesitas.\nEj: aeropuerto → hotel / hotel → aeropuerto / ciudad → ciudad.`
        );
        return res.sendStatus(200);
      }

      if (serviceLineKey === "paquetes_vacacionales") {
        session.state = "await_package_destination";
        await sendWhatsAppText(
          from,
          `Perfecto 🎒\nVamos con *paquetes vacacionales*.\n\nDime el destino que te interesa o elige uno del menú.`
        );
        await sendPackageDestinationsList(from);
        return res.sendStatus(200);
      }
    }

    // =========================
    // FLIGHTS FLOW
    // =========================
    if (session.state === "await_flight_origin") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Indícame desde dónde deseas salir. Ej: Santo Domingo, Punta Cana o Santiago.`);
        return res.sendStatus(200);
      }

      session.pendingDepartureCity = userText;
      session.state = "await_flight_destination";
      await sendWhatsAppText(from, `Perfecto ✈️\nAhora dime el *destino* o ciudad/país a donde quieres viajar.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_flight_destination") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame el *destino* del vuelo.`);
        return res.sendStatus(200);
      }

      session.pendingDestination = userText;
      session.state = "await_flight_date";
      await sendWhatsAppText(from, `Gracias. Ahora dime la *fecha aproximada* del viaje.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_flight_date") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame la *fecha aproximada* del vuelo.`);
        return res.sendStatus(200);
      }

      session.pendingTravelDateText = userText;
      session.state = "await_flight_people";
      await sendWhatsAppText(from, `Perfecto. ¿Para cuántas *personas* sería el boleto aéreo?`);
      return res.sendStatus(200);
    }

    if (session.state === "await_flight_people") {
      const pax = parsePassengerCount(userText);
      if (pax === null || pax < 1) {
        await sendWhatsAppText(from, `Indícame cuántas *personas* viajarían. Ej: 1, 2, 3...`);
        return res.sendStatus(200);
      }

      session.pendingPassengers = pax;
      session.state = "await_flight_name";
      await sendWhatsAppText(from, `Perfecto 👍\nAhora dime tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_flight_name") {
      if (tNorm.length < 3) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }

      session.pendingName = userText;
      session.state = "await_flight_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_flight_phone") {
      const phoneDigits = normalizePhoneDigits(userText);
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de boletos aéreos", [
        { label: "🧩 Servicio", value: "Boletos aéreos" },
        { label: "🛫 Salida / origen", value: session.pendingDepartureCity || "—" },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📅 Fecha", value: session.pendingTravelDateText || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, {
        topic: "Boletos aéreos",
        quotePreview: summaryText,
        converted: true,
        followupSent: true,
      });

      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *boletos aéreos* y un asesor te contactará pronto.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // HOTELS FLOW
    // =========================
    if (session.state === "await_hotel_destination") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Dime el *destino* o ciudad donde deseas reservar hotel.`);
        return res.sendStatus(200);
      }

      session.pendingDestination = userText;
      session.state = "await_hotel_date";
      await sendWhatsAppText(from, `Perfecto 🏨\nAhora dime la *fecha aproximada* del check-in o temporada.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_hotel_date") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame la *fecha aproximada* del hotel.`);
        return res.sendStatus(200);
      }

      session.pendingTravelDateText = userText;
      session.state = "await_hotel_nights";
      await sendWhatsAppText(from, `¿Cuántas *noches* deseas reservar?`);
      return res.sendStatus(200);
    }

    if (session.state === "await_hotel_nights") {
      const nights = parsePassengerCount(userText);
      if (nights === null || nights < 1) {
        await sendWhatsAppText(from, `Indícame cuántas *noches* serían. Ej: 2, 3, 5...`);
        return res.sendStatus(200);
      }

      session.pendingNights = nights;
      session.state = "await_hotel_people";
      await sendWhatsAppText(from, `Perfecto. ¿Para cuántas *personas* sería la reserva del hotel?`);
      return res.sendStatus(200);
    }

    if (session.state === "await_hotel_people") {
      const pax = parsePassengerCount(userText);
      if (pax === null || pax < 1) {
        await sendWhatsAppText(from, `Indícame cuántas *personas* se hospedarían. Ej: 2`);
        return res.sendStatus(200);
      }

      session.pendingPassengers = pax;
      session.state = "await_hotel_stars";
      await sendWhatsAppText(from, `Perfecto. ¿Qué tipo de hotel prefieres?\nEj: *3 estrellas*, *4 estrellas* o *5 estrellas*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_hotel_stars") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Indícame si prefieres *3 estrellas*, *4 estrellas* o *5 estrellas*.`);
        return res.sendStatus(200);
      }

      session.pendingHotelStars = userText;
      session.state = "await_hotel_name";
      await sendWhatsAppText(from, `Gracias 👍\nAhora dime tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_hotel_name") {
      if (tNorm.length < 3) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }

      session.pendingName = userText;
      session.state = "await_hotel_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_hotel_phone") {
      const phoneDigits = normalizePhoneDigits(userText);
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de solo hoteles", [
        { label: "🧩 Servicio", value: "Solo hoteles" },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📅 Fecha", value: session.pendingTravelDateText || "—" },
        { label: "🌙 Noches", value: session.pendingNights || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "🏨 Categoría hotel", value: session.pendingHotelStars || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, {
        topic: "Solo hoteles",
        quotePreview: summaryText,
        converted: true,
        followupSent: true,
      });

      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *solo hoteles* y un asesor te contactará pronto.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // INSURANCE FLOW
    // =========================
    if (session.state === "await_insurance_destination") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Indícame el *país o destino* para tu seguro de viaje.`);
        return res.sendStatus(200);
      }

      session.pendingDestination = userText;
      session.state = "await_insurance_days";
      await sendWhatsAppText(from, `Perfecto 🛡️\n¿Cuántos *días* durará el viaje?`);
      return res.sendStatus(200);
    }

    if (session.state === "await_insurance_days") {
      const days = parsePassengerCount(userText);
      if (days === null || days < 1) {
        await sendWhatsAppText(from, `Indícame cuántos *días* durará el viaje. Ej: 5, 8, 12...`);
        return res.sendStatus(200);
      }

      session.pendingTripDays = days;
      session.state = "await_insurance_people";
      await sendWhatsAppText(from, `Gracias. Ahora dime cuántas *personas* viajan.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_insurance_people") {
      const pax = parsePassengerCount(userText);
      if (pax === null || pax < 1) {
        await sendWhatsAppText(from, `Indícame cuántas *personas* necesitan el seguro. Ej: 1, 2, 3...`);
        return res.sendStatus(200);
      }

      session.pendingPassengers = pax;
      session.state = "await_insurance_ages";
      await sendWhatsAppText(from, `Perfecto. Ahora dime las *edades* de los viajeros.\nEj: 34 y 29 / 40, 12 y 8`);
      return res.sendStatus(200);
    }

    if (session.state === "await_insurance_ages") {
      if (tNorm.length < 1) {
        await sendWhatsAppText(from, `Por favor, indícame las *edades* de los viajeros.`);
        return res.sendStatus(200);
      }

      session.pendingTravelerAgesText = userText;
      session.state = "await_insurance_name";
      await sendWhatsAppText(from, `Perfecto 👍\nAhora dime tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_insurance_name") {
      if (tNorm.length < 3) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }

      session.pendingName = userText;
      session.state = "await_insurance_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_insurance_phone") {
      const phoneDigits = normalizePhoneDigits(userText);
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de seguro de viaje", [
        { label: "🧩 Servicio", value: "Seguros de viaje" },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📆 Días de viaje", value: session.pendingTripDays || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "🎂 Edades", value: session.pendingTravelerAgesText || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, {
        topic: "Seguros de viaje",
        quotePreview: summaryText,
        converted: true,
        followupSent: true,
      });

      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *seguro de viaje* y un asesor te contactará pronto.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // TRANSFERS FLOW
    // =========================
    if (session.state === "await_transfer_route") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Dime la *ruta del traslado*.\nEj: aeropuerto → hotel / hotel → aeropuerto / ciudad → ciudad.`);
        return res.sendStatus(200);
      }

      session.pendingTransferRoute = userText;
      session.state = "await_transfer_date";
      await sendWhatsAppText(from, `Perfecto 🚕\nAhora dime la *fecha aproximada* del traslado.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_transfer_date") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame la *fecha aproximada* del traslado.`);
        return res.sendStatus(200);
      }

      session.pendingTravelDateText = userText;
      session.state = "await_transfer_people";
      await sendWhatsAppText(from, `Gracias. ¿Para cuántas *personas* sería el traslado?`);
      return res.sendStatus(200);
    }

    if (session.state === "await_transfer_people") {
      const pax = parsePassengerCount(userText);
      if (pax === null || pax < 1) {
        await sendWhatsAppText(from, `Indícame cuántas *personas* viajarían. Ej: 2`);
        return res.sendStatus(200);
      }

      session.pendingPassengers = pax;
      session.state = "await_transfer_name";
      await sendWhatsAppText(from, `Perfecto 👍\nAhora dime tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_transfer_name") {
      if (tNorm.length < 3) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }

      session.pendingName = userText;
      session.state = "await_transfer_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_transfer_phone") {
      const phoneDigits = normalizePhoneDigits(userText);
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de traslado", [
        { label: "🧩 Servicio", value: "Traslados" },
        { label: "🚕 Ruta", value: session.pendingTransferRoute || "—" },
        { label: "📅 Fecha", value: session.pendingTravelDateText || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, {
        topic: "Traslados",
        quotePreview: summaryText,
        converted: true,
        followupSent: true,
      });

      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *traslado* y un asesor te contactará pronto.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // PACKAGES FLOW
    // =========================
    if (session.state === "await_package_destination") {
      const packageKey = detectPackageDestinationKeyFromUser(userText);

      if (packageKey && packageKey !== "otro_destino") {
        const pkg = PACKAGE_DESTINATIONS.find((p) => p.key === packageKey);
        session.pendingDestination = pkg?.title || userText;
      } else if (packageKey === "otro_destino") {
        session.pendingDestination = "Otro destino";
      } else if (tNorm.length >= 2) {
        session.pendingDestination = userText;
      } else {
        await sendWhatsAppText(from, `Dime el *país o destino* que te interesa para el paquete vacacional.`);
        await sendPackageDestinationsList(from);
        return res.sendStatus(200);
      }

      session.state = "await_package_date";
      await sendWhatsAppText(from, `Perfecto 🎒\nAhora dime la *fecha* o *temporada* que te interesa.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_package_date") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame la *fecha* o *temporada* que te interesa.`);
        return res.sendStatus(200);
      }

      session.pendingTravelDateText = userText;
      session.state = "await_package_people";
      await sendWhatsAppText(from, `Gracias. ¿Para cuántas *personas* sería el paquete?`);
      return res.sendStatus(200);
    }

    if (session.state === "await_package_people") {
      const pax = parsePassengerCount(userText);
      if (pax === null || pax < 1) {
        await sendWhatsAppText(from, `Indícame cuántas *personas* viajarían. Ej: 2`);
        return res.sendStatus(200);
      }

      session.pendingPassengers = pax;
      session.state = "await_package_stars";
      await sendWhatsAppText(from, `Perfecto. ¿Qué tipo de hotel prefieres dentro del paquete?\nEj: *3 estrellas*, *4 estrellas* o *5 estrellas*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_package_stars") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Indícame si prefieres *3 estrellas*, *4 estrellas* o *5 estrellas*.`);
        return res.sendStatus(200);
      }

      session.pendingHotelStars = userText;
      session.state = "await_package_name";
      await sendWhatsAppText(from, `Perfecto 👍\nAhora dime tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_package_name") {
      if (tNorm.length < 3) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }

      session.pendingName = userText;
      session.state = "await_package_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_package_phone") {
      const phoneDigits = normalizePhoneDigits(userText);
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de paquete vacacional", [
        { label: "🧩 Servicio", value: "Paquetes vacacionales" },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📅 Fecha / temporada", value: session.pendingTravelDateText || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "🏨 Categoría hotel", value: session.pendingHotelStars || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, {
        topic: "Paquetes vacacionales",
        quotePreview: summaryText,
        converted: true,
        followupSent: true,
      });

      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *paquete vacacional* y un asesor te contactará pronto.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // ADVISOR FLOW
    // =========================
    if (session.state === "await_advisor_name") {
      if (tNorm.length < 3) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }

      session.pendingName = userText;
      session.state = "await_advisor_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* para que un asesor te contacte.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_advisor_phone") {
      const phoneDigits = normalizePhoneDigits(userText);
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Solicitud para hablar con un asesor", [
        { label: "📝 Tema", value: session.pendingLeadTopic || session.pendingAdvisorTopic || "Consulta general" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, {
        topic: session.pendingLeadTopic || session.pendingAdvisorTopic || "Consulta general",
        quotePreview: summaryText,
        converted: true,
        followupSent: true,
      });

      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nYa pasé tu caso para que un asesor te contacte pronto.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // FALLBACK
    // =========================
    await sendWhatsAppText(
      from,
      `Puedo ayudarte con tours, boletos, hoteles, seguros, traslados y paquetes.\n\nEscribe *"menú"* para ver opciones o *"tours"* para ir directo al submenú de tours.`
    );
    return res.sendStatus(200);
  } catch (e) {
    console.error("Webhook error:", e?.response?.data || e?.message || e);

    try {
      if (from) {
        await sendWhatsAppText(
          from,
          `Tuve un inconveniente momentáneo procesando tu mensaje 🙏\n\nEscríbeme *"menú"* para continuar o vuelve a intentar en unos segundos.`
        );
      }
    } catch {}

    return res.sendStatus(200);
  } finally {
    try {
      if (from && session) await saveSession(from, session);
    } catch (e) {
      console.error("saveSession error:", e?.message || e);
    }
  }
});

// =========================
// FOLLOWUP
// =========================
async function followupLeadsLoop() {
  try {
    if (!FOLLOWUP_ENABLED) return;

    const ids = await listAllSessionIds();
    const now = Date.now();
    const maxAgeMs = FOLLOWUP_MAX_AGE_HOURS * 60 * 60 * 1000;
    const minAgeMs = FOLLOWUP_AFTER_MIN * 60 * 1000;

    for (const id of ids) {
      const s = await getSession(id);
      const lead = s?.lead || {};

      if (!lead.topic || lead.followupSent || lead.converted) continue;
      if (!lead.lastInteractionAt) continue;

      const ageMs = now - new Date(lead.lastInteractionAt).getTime();
      if (!Number.isFinite(ageMs) || ageMs < minAgeMs || ageMs > maxAgeMs) continue;

      const msg =
        `Hola 👋 Quedó pendiente tu solicitud sobre *${lead.topic}*.\n\n` +
        `Si deseas, sigo ayudándote por aquí o te paso con un asesor 😊`;

      try {
        await sendWhatsAppText(id, msg, "BOT");
        s.lead.followupSent = true;
        s.lead.lastInteractionAt = new Date().toISOString();
        await saveSession(id, s);
      } catch (e) {
        console.error("followup send error:", id, e?.response?.data || e?.message || e);
      }
    }
  } catch (e) {
    console.error("followupLeadsLoop error:", e?.response?.data || e?.message || e);
  }
}

// =========================
// HEALTH
// =========================
app.get("/", (_req, res) => res.send("OK"));
app.get("/health", (_req, res) => res.status(200).send("ok"));

app.get("/tick", async (_req, res) => {
  try {
    await followupLeadsLoop();
  } catch {}
  return res.status(200).send("tick ok");
});

app.listen(PORT, () => console.log(`Bot running on :${PORT}`));
