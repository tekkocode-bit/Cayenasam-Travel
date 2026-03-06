import express from "express";
import axios from "axios";
import crypto from "crypto";
import { google } from "googleapis";
import Redis from "ioredis";

// =========================
// ENV
// =========================
const PORT = process.env.PORT || 3000;

const WA_TOKEN = process.env.WA_TOKEN;
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID;
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const META_APP_SECRET = process.env.META_APP_SECRET;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

const GOOGLE_CALENDAR_ID = process.env.GOOGLE_CALENDAR_ID;
const BUSINESS_NAME =
  process.env.BUSINESS_NAME || process.env.AGENCY_NAME || process.env.CLINIC_NAME || "Agencia de Tours";
const BUSINESS_ADDRESS =
  process.env.BUSINESS_ADDRESS || process.env.CLINIC_ADDRESS || "";
const BUSINESS_TIMEZONE =
  process.env.BUSINESS_TIMEZONE || process.env.CLINIC_TIMEZONE || "America/Santo_Domingo";

const SLOT_STEP_MIN = parseInt(process.env.SLOT_STEP_MIN || "15", 10);
const MIN_BOOKING_LEAD_MIN = parseInt(process.env.MIN_BOOKING_LEAD_MIN || "120", 10);
const MAX_SLOTS_RETURN = parseInt(process.env.MAX_SLOTS_RETURN || "80", 10);
const DISPLAY_SLOTS_LIMIT = parseInt(process.env.DISPLAY_SLOTS_LIMIT || "12", 10);

const REMINDER_24H = (process.env.REMINDER_24H || "1") === "1";
const REMINDER_2H = (process.env.REMINDER_2H || "1") === "1";
const FOLLOWUP_ENABLED = (process.env.FOLLOWUP_ENABLED || "1") === "1";
const FOLLOWUP_AFTER_MIN = parseInt(process.env.FOLLOWUP_AFTER_MIN || "180", 10);
const FOLLOWUP_MAX_AGE_HOURS = parseInt(process.env.FOLLOWUP_MAX_AGE_HOURS || "72", 10);

const PERSONAL_WA_TO = (process.env.PERSONAL_WA_TO || "").trim();
const PRICE_CURRENCY = (process.env.PRICE_CURRENCY || "US$").trim();

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
const SESSION_PREFIX = process.env.SESSION_PREFIX || "tekko:tour:sess:";

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

function defaultLead() {
  return {
    tour_key: "",
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
    lastSlots: [],
    lastDisplaySlots: [],
    selectedSlot: null,
    pendingCategory: null,
    pendingTour: null,
    pendingRange: null,
    pendingAdults: null,
    pendingChildren: null,
    pendingPickup: null,
    pendingCity: null,
    pendingName: null,
    lastBooking: null,
    greeted: false,
    lastMsgId: null,
    lead: defaultLead(),
    reschedule: {
      active: false,
      reservation_id: "",
      phone: "",
      passenger_name: "",
      tour_key: "",
      adults: 0,
      children: 0,
      pickup: "",
      city: "",
    },
  };
}

function sanitizeSession(session) {
  if (!session || typeof session !== "object") return defaultSession();

  if (!Array.isArray(session.messages)) session.messages = [];
  session.messages = session.messages.slice(-20);

  if (!Array.isArray(session.lastSlots)) session.lastSlots = [];
  session.lastSlots = session.lastSlots.slice(0, MAX_SLOTS_RETURN);

  if (!Array.isArray(session.lastDisplaySlots)) session.lastDisplaySlots = [];
  session.lastDisplaySlots = session.lastDisplaySlots.slice(0, DISPLAY_SLOTS_LIMIT);

  if (!session.lead || typeof session.lead !== "object") {
    session.lead = defaultLead();
  } else {
    if (typeof session.lead.tour_key !== "string") session.lead.tour_key = "";
    if (typeof session.lead.followupSent !== "boolean") session.lead.followupSent = false;
    if (typeof session.lead.lastInteractionAt !== "string") session.lead.lastInteractionAt = "";
    if (typeof session.lead.quotePreview !== "string") session.lead.quotePreview = "";
    if (typeof session.lead.converted !== "boolean") session.lead.converted = false;
  }

  if (!session.reschedule || typeof session.reschedule !== "object") {
    session.reschedule = defaultSession().reschedule;
  } else {
    if (typeof session.reschedule.active !== "boolean") session.reschedule.active = false;
    if (typeof session.reschedule.reservation_id !== "string") session.reschedule.reservation_id = "";
    if (typeof session.reschedule.phone !== "string") session.reschedule.phone = "";
    if (typeof session.reschedule.passenger_name !== "string") session.reschedule.passenger_name = "";
    if (typeof session.reschedule.tour_key !== "string") session.reschedule.tour_key = "";
    if (typeof session.reschedule.adults !== "number") session.reschedule.adults = 0;
    if (typeof session.reschedule.children !== "number") session.reschedule.children = 0;
    if (typeof session.reschedule.pickup !== "string") session.reschedule.pickup = "";
    if (typeof session.reschedule.city !== "string") session.reschedule.city = "";
  }

  if (typeof session.state !== "string") session.state = "idle";
  if (typeof session.greeted !== "boolean") session.greeted = false;

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
    for (const k of keys) {
      ids.push(String(k).replace(SESSION_PREFIX, ""));
    }
  } while (cursor !== "0");

  return ids;
}

// =====================================================
// Stable stringify
// =====================================================
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
  if (m.includes("word")) return ".docx";
  if (m.includes("sheet")) return ".xlsx";
  if (m.includes("presentation")) return ".pptx";
  return "";
}

function sanitizeFileName(name, fallback = "file") {
  const raw = String(name || fallback).trim() || fallback;
  return raw.replace(/[\\/:*?"<>|]+/g, "_");
}

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

function attachHubMediaUrl(req, meta) {
  const out = { ...(meta || {}) };
  const kind = String(out?.kind || "").toUpperCase();

  if (out?.mediaId && ["AUDIO", "IMAGE", "VIDEO", "DOCUMENT", "STICKER"].includes(kind)) {
    const mediaUrl = buildHubMediaUrl(req, out.mediaId);
    if (mediaUrl) out.mediaUrl = mediaUrl;
  }

  return out;
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

const app = express();
app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);

// =========================
// Tours / categories
// =========================
function safeJson(str, fallback) {
  try {
    return JSON.parse(str);
  } catch {
    return fallback;
  }
}

const TOUR_CATEGORIES = [
  { key: "tours_diarios", id: "cat_tours_diarios", title: "Tours diarios" },
  { key: "playas", id: "cat_playas", title: "Playas" },
  { key: "montanas", id: "cat_montanas", title: "Montañas" },
  { key: "excursiones_especiales", id: "cat_excursiones_especiales", title: "Excursiones especiales" },
  { key: "paquetes_temporada", id: "cat_paquetes_temporada", title: "Paquetes de temporada" },
];

const CATEGORY_ID_TO_KEY = Object.fromEntries(TOUR_CATEGORIES.map((c) => [c.id, c.key]));

function defaultTourCatalog() {
  return [
    {
      key: "city_tour_santo_domingo",
      id: "tour_city_tour_santo_domingo",
      title: "City Tour Santo Domingo",
      category: "tours_diarios",
      description: "Recorrido guiado por la Zona Colonial y puntos icónicos de Santo Domingo.",
      durationMin: 240,
      durationLabel: "4 horas",
      basePriceAdult: 35,
      basePriceChild: 25,
      capacity: 18,
      meetingPoint: "Parque Colón / punto coordinado",
      pickupOptions: "Santo Domingo Centro, Zona Colonial y Gazcue",
      paymentMethods: "Transferencia, efectivo y pago por link",
      reservationPolicy: "Reserva con al menos 24 horas de anticipación.",
      paymentPolicy: "Separa con avance para confirmar tu cupo.",
      includes: ["Transporte interno", "Guía", "Paradas fotográficas"],
      schedule: {
        mon: ["09:00", "15:00"],
        tue: ["09:00", "15:00"],
        wed: ["09:00", "15:00"],
        thu: ["09:00", "15:00"],
        fri: ["09:00", "15:00"],
        sat: ["09:00", "15:00"],
        sun: ["09:00"],
      },
    },
    {
      key: "isla_saona",
      id: "tour_isla_saona",
      title: "Isla Saona",
      category: "playas",
      description: "Excursión de día completo con playa, lancha/catamarán y ambiente caribeño.",
      durationMin: 720,
      durationLabel: "Día completo",
      basePriceAdult: 95,
      basePriceChild: 75,
      capacity: 24,
      meetingPoint: "Punto de salida coordinado según zona",
      pickupOptions: "Santo Domingo, Boca Chica, La Romana",
      paymentMethods: "Transferencia, efectivo y pago por link",
      reservationPolicy: "Reserva con 48 horas de anticipación.",
      paymentPolicy: "Requiere avance para bloquear espacios.",
      includes: ["Transporte", "Guía", "Almuerzo", "Bebidas"],
      schedule: {
        mon: ["06:00"],
        tue: ["06:00"],
        wed: ["06:00"],
        thu: ["06:00"],
        fri: ["06:00"],
        sat: ["06:00"],
        sun: ["06:00"],
      },
    },
    {
      key: "isla_catalina",
      id: "tour_isla_catalina",
      title: "Isla Catalina",
      category: "playas",
      description: "Tour ideal para disfrutar de playa, snorkeling y día relajado.",
      durationMin: 660,
      durationLabel: "Día completo",
      basePriceAdult: 89,
      basePriceChild: 69,
      capacity: 20,
      meetingPoint: "Punto de salida coordinado según zona",
      pickupOptions: "Santo Domingo y La Romana",
      paymentMethods: "Transferencia, efectivo y pago por link",
      reservationPolicy: "Reserva con 48 horas de anticipación.",
      paymentPolicy: "Avance obligatorio para confirmar.",
      includes: ["Transporte", "Guía", "Snorkeling", "Almuerzo"],
      schedule: {
        mon: ["06:30"],
        wed: ["06:30"],
        fri: ["06:30"],
        sat: ["06:30"],
      },
    },
    {
      key: "jarabacoa_aventura",
      id: "tour_jarabacoa_aventura",
      title: "Jarabacoa Aventura",
      category: "montanas",
      description: "Ruta de montaña con paisajes, río y paradas fotográficas.",
      durationMin: 720,
      durationLabel: "Día completo",
      basePriceAdult: 70,
      basePriceChild: 55,
      capacity: 16,
      meetingPoint: "Santo Domingo / Santiago según grupo",
      pickupOptions: "Santo Domingo, Santiago",
      paymentMethods: "Transferencia y efectivo",
      reservationPolicy: "Reserva con 48 horas de anticipación.",
      paymentPolicy: "Separa con avance.",
      includes: ["Transporte", "Guía", "Paradas", "Hidratación"],
      schedule: {
        sat: ["06:00"],
        sun: ["06:00"],
      },
    },
    {
      key: "buggies_macao",
      id: "tour_buggies_macao",
      title: "Buggies Macao",
      category: "excursiones_especiales",
      description: "Aventura en buggies con playa y recorrido guiado.",
      durationMin: 300,
      durationLabel: "5 horas",
      basePriceAdult: 65,
      basePriceChild: 50,
      capacity: 12,
      meetingPoint: "Punta Cana / punto coordinado",
      pickupOptions: "Punta Cana, Bávaro, Uvero Alto",
      paymentMethods: "Transferencia, efectivo y link de pago",
      reservationPolicy: "Reserva con 24 horas de anticipación.",
      paymentPolicy: "Avance para confirmar.",
      includes: ["Transporte", "Guía", "Equipo básico"],
      schedule: {
        mon: ["08:00", "13:00"],
        tue: ["08:00", "13:00"],
        wed: ["08:00", "13:00"],
        thu: ["08:00", "13:00"],
        fri: ["08:00", "13:00"],
        sat: ["08:00", "13:00"],
      },
    },
    {
      key: "samana_temporada",
      id: "tour_samana_temporada",
      title: "Samaná Temporada",
      category: "paquetes_temporada",
      description: "Paquete especial de temporada con transporte y experiencia guiada.",
      durationMin: 900,
      durationLabel: "Día completo",
      basePriceAdult: 120,
      basePriceChild: 95,
      capacity: 22,
      meetingPoint: "Punto coordinado según ciudad",
      pickupOptions: "Santo Domingo, San Pedro, La Romana",
      paymentMethods: "Transferencia, efectivo y pago por link",
      reservationPolicy: "Sujeto a temporada y cupos disponibles.",
      paymentPolicy: "Avance obligatorio para confirmar cupo.",
      includes: ["Transporte", "Guía", "Almuerzo", "Actividad principal"],
      activeMonths: [1, 2, 3],
      schedule: {
        fri: ["05:30"],
        sat: ["05:30"],
        sun: ["05:30"],
      },
    },
  ];
}

const TOURS = safeJson(process.env.TOUR_CATALOG_JSON, null) || defaultTourCatalog();
const TOUR_ID_TO_KEY = Object.fromEntries(TOURS.map((t) => [t.id, t.key]));

function getCategoryByKey(key) {
  return TOUR_CATEGORIES.find((c) => c.key === key) || null;
}

function getTourByKey(key) {
  return TOURS.find((t) => t.key === key) || null;
}

function getToursByCategory(categoryKey) {
  return TOURS.filter((t) => t.category === categoryKey);
}

function normalizeText(t) {
  return (t || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/\s+/g, " ")
    .trim();
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

function addMinutes(date, minutes) {
  return new Date(date.getTime() + minutes * 60000);
}

function weekdayKeyFromISOWeekday(isoWeekday) {
  return ["", "mon", "tue", "wed", "thu", "fri", "sat", "sun"][isoWeekday];
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

  const isOnlyGreeting = greetings.some((g) => t === g || t.startsWith(g + " ")) || /^(hola+|buenas+)\b/.test(t);
  const hasTravelIntent =
    t.includes("tour") ||
    t.includes("excursion") ||
    t.includes("excursión") ||
    t.includes("reserva") ||
    t.includes("reservar") ||
    t.includes("paquete") ||
    t.includes("viaje") ||
    t.includes("playa") ||
    t.includes("montana") ||
    t.includes("montaña");

  return isOnlyGreeting && !hasTravelIntent && t.length <= 40;
}

function quickHelpText() {
  return (
    `¡Hola! 😊\n` +
    `Puedo ayudarte a cotizar y reservar tours.\n\n` +
    `Escribe el destino que te interesa o escribe *"categorías"* para ver el menú.`
  );
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

function looksLikeConfirm(textNorm) {
  return ["confirmar", "confirmo", "confirmada", "confirmado", "confirmacion", "confirmación"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function looksLikeCancel(textNorm) {
  return ["cancelar", "cancela", "anular", "anula", "ya no", "cancelacion", "cancelación"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function looksLikeReschedule(textNorm) {
  return ["reprogramar", "reprograma", "cambiar", "cambio", "mover", "otro horario", "otra fecha"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function looksLikeNewReservation(textNorm) {
  return ["nueva reserva", "otra reserva", "reservar", "reserva nueva", "quiero reservar"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function wantsQuote(textNorm) {
  return ["precio", "cuanto cuesta", "cuánto cuesta", "cotizacion", "cotización", "tarifa", "valor"].some(
    (k) => (textNorm || "").includes(k)
  );
}

function wantsIncludes(textNorm) {
  return ["que incluye", "qué incluye", "incluye", "incluido"].some((k) => (textNorm || "").includes(k));
}

function wantsSchedule(textNorm) {
  return ["horario", "hora", "horas", "sale", "salida", "salen", "punto de encuentro"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function wantsPayments(textNorm) {
  return ["forma de pago", "formas de pago", "pago", "pagos", "transferencia", "tarjeta"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function wantsPolicies(textNorm) {
  return ["politica", "política", "reserva", "cancelacion", "cancelación"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function getZonedParts(date, timeZone) {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = fmt.formatToParts(date);
  const obj = {};
  for (const p of parts) obj[p.type] = p.value;
  return {
    year: parseInt(obj.year, 10),
    month: parseInt(obj.month, 10),
    day: parseInt(obj.day, 10),
    hour: parseInt(obj.hour, 10),
    minute: parseInt(obj.minute, 10),
    second: parseInt(obj.second, 10),
  };
}

function getOffsetMinutes(date, timeZone) {
  const p = getZonedParts(date, timeZone);
  const asUTC = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
  return (asUTC - date.getTime()) / 60000;
}

function zonedTimeToUtc({ year, month, day, hour, minute, second = 0 }, timeZone) {
  const guess = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  const offsetMin = getOffsetMinutes(guess, timeZone);
  return new Date(Date.UTC(year, month - 1, day, hour, minute, second) - offsetMin * 60000);
}

function formatTimeInTZ(iso, timeZone) {
  const d = new Date(iso);
  return new Intl.DateTimeFormat("es-DO", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  }).format(d);
}

function formatDateInTZ(iso, timeZone) {
  const d = new Date(iso);
  return new Intl.DateTimeFormat("es-DO", {
    timeZone,
    weekday: "long",
    day: "2-digit",
    month: "long",
    year: "numeric",
  }).format(d);
}

function getNowPlusLeadUTC() {
  const now = new Date();
  const lead = Math.max(0, Number.isFinite(MIN_BOOKING_LEAD_MIN) ? MIN_BOOKING_LEAD_MIN : 120);
  return addMinutes(now, lead);
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

async function sendWhatsAppText(to, text, reportSource = "BOT") {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  await axios.post(
    url,
    { messaging_product: "whatsapp", to, type: "text", text: { body: text } },
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

async function sendReminderWhatsAppToBestTarget(priv, fallbackPhoneDigits, text) {
  const candidates = [];

  if (priv?.wa_id) candidates.push(String(priv.wa_id).trim());
  if (priv?.wa_phone) candidates.push(toE164DigitsRD(priv.wa_phone));
  if (fallbackPhoneDigits) candidates.push(toE164DigitsRD(fallbackPhoneDigits));

  const tried = [];
  let lastErr = null;

  for (const c of candidates) {
    const to = String(c || "").replace(/[^\d]/g, "");
    if (!to) continue;
    if (tried.includes(to)) continue;
    tried.push(to);

    try {
      await sendWhatsAppText(to, text, "BOT");
      return { ok: true, to };
    } catch (e) {
      lastErr = e;
      console.error("[reminder] send failed for:", to, e?.response?.data || e?.message || e);
    }
  }

  return { ok: false, tried, error: lastErr?.response?.data || lastErr?.message || lastErr };
}

function categoriesEmojiText() {
  return (
    `👋 ¡Hola! Soy el asistente de *${BUSINESS_NAME}*.\n\n` +
    `Te ayudo con *cotización + reserva* de tours.\n\n` +
    `Puedes escribirme el tour o destino que buscas, o elegir una categoría:\n` +
    `🏙️ Tours diarios\n` +
    `🏝️ Playas\n` +
    `⛰️ Montañas\n` +
    `✨ Excursiones especiales\n` +
    `🎒 Paquetes de temporada\n\n` +
    `También puedo responder sobre precio, horarios, punto de salida, formas de pago y políticas.`
  );
}

async function sendCategoriesList(to) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  const rows = TOUR_CATEGORIES.map((c) => ({ id: c.id, title: c.title, description: "" }));

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: "Categorías de tours" },
        body: { text: "Selecciona una categoría para ver opciones disponibles 👇" },
        footer: { text: BUSINESS_NAME },
        action: { button: "Ver categorías", sections: [{ title: "Categorías", rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  const rendered =
    `*Categorías de tours*\nSelecciona una categoría para ver opciones 👇\n\n` +
    rows.map((r) => `• [${r.id}] ${r.title}`).join("\n");

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: rendered,
    source: "BOT",
    kind: "LIST",
    meta: { rows },
  });
}

async function sendToursListByCategory(to, categoryKey) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  const category = getCategoryByKey(categoryKey);
  const tours = getToursByCategory(categoryKey);

  if (!category || !tours.length) {
    await sendWhatsAppText(to, "No encontré tours en esa categoría ahora mismo 🙏");
    return;
  }

  const rows = tours.slice(0, 10).map((t) => ({
    id: t.id,
    title: t.title.slice(0, 24),
    description: `${PRICE_CURRENCY}${t.basePriceAdult} adulto • ${t.durationLabel}`.slice(0, 72),
  }));

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: category.title },
        body: { text: "Elige un tour para ver precio, detalles y reservar 👇" },
        footer: { text: BUSINESS_NAME },
        action: { button: "Ver tours", sections: [{ title: category.title, rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  const rendered =
    `*${category.title}*\nElige un tour para ver precio, detalles y reservar 👇\n\n` +
    rows.map((r) => `• [${r.id}] ${r.title} — ${r.description}`).join("\n");

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: rendered,
    source: "BOT",
    kind: "LIST",
    meta: { rows, category: categoryKey },
  });
}

function currency(n) {
  return `${PRICE_CURRENCY}${Number(n || 0).toFixed(0)}`;
}

function buildTourInfoText(tour) {
  if (!tour) return "";

  return (
    `🌴 *${tour.title}*\n` +
    `${tour.description}\n\n` +
    `💵 Desde *${currency(tour.basePriceAdult)}* adultos y *${currency(tour.basePriceChild)}* niños\n` +
    `⏳ Duración: ${tour.durationLabel}\n` +
    `📍 Punto de encuentro: ${tour.meetingPoint}\n` +
    `🚐 Salida / pickup: ${tour.pickupOptions}\n` +
    `✅ Incluye: ${Array.isArray(tour.includes) ? tour.includes.join(", ") : String(tour.includes || "Consultar")}\n` +
    `💳 Pago: ${tour.paymentMethods}\n` +
    `📌 Reserva: ${tour.reservationPolicy}`
  );
}

function buildTourFaqReply(tour, textNorm) {
  if (!tour) return "";

  const parts = [`🌴 *${tour.title}*`];

  if (wantsQuote(textNorm)) {
    parts.push(`💵 Precio base: Adultos *${currency(tour.basePriceAdult)}* / Niños *${currency(tour.basePriceChild)}*`);
  }

  if (wantsIncludes(textNorm)) {
    parts.push(`✅ Incluye: ${Array.isArray(tour.includes) ? tour.includes.join(", ") : String(tour.includes || "Consultar")}`);
  }

  if (wantsSchedule(textNorm)) {
    parts.push(`📍 Punto de encuentro: ${tour.meetingPoint}`);
    parts.push(`🚐 Pickup / salida: ${tour.pickupOptions}`);
    parts.push(`⏳ Duración aproximada: ${tour.durationLabel}`);
  }

  if (wantsPayments(textNorm)) {
    parts.push(`💳 Formas de pago: ${tour.paymentMethods}`);
    parts.push(`🧾 Política de pago: ${tour.paymentPolicy}`);
  }

  if (wantsPolicies(textNorm)) {
    parts.push(`📌 Política de reserva: ${tour.reservationPolicy}`);
    parts.push(`🧾 Política de pago: ${tour.paymentPolicy}`);
  }

  if (parts.length === 1) {
    parts.push(buildTourInfoText(tour));
  }

  parts.push(`\nSi deseas, dime la fecha y te comparto disponibilidad real.`);
  return parts.join("\n");
}

function buildQuotePreview(tour, adults, children) {
  const a = Number(adults || 0);
  const c = Number(children || 0);
  const total = a * Number(tour?.basePriceAdult || 0) + c * Number(tour?.basePriceChild || 0);

  return (
    `💵 *Cotización estimada*\n` +
    `Tour: *${tour?.title || "—"}*\n` +
    `Adultos: ${a} x ${currency(tour?.basePriceAdult || 0)}\n` +
    `Niños: ${c} x ${currency(tour?.basePriceChild || 0)}\n` +
    `Total estimado: *${currency(total)}*\n\n` +
    `*Incluye:* ${Array.isArray(tour?.includes) ? tour.includes.join(", ") : String(tour?.includes || "Consultar")}\n` +
    `*Pago:* ${tour?.paymentMethods || "Consultar"}`
  );
}

function updateLead(session, patch = {}) {
  session.lead = {
    ...defaultLead(),
    ...(session.lead || {}),
    ...patch,
    lastInteractionAt: new Date().toISOString(),
  };
}

function clearLeadOnBooking(session) {
  session.lead = {
    ...defaultLead(),
    tour_key: session.pendingTour || session.lead?.tour_key || "",
    converted: true,
    followupSent: true,
    lastInteractionAt: new Date().toISOString(),
  };
}

// =========================
// Google Calendar
// =========================
function getCalendarClient() {
  const json = safeJson(process.env.GOOGLE_SERVICE_ACCOUNT_JSON, null);
  if (!json?.client_email || !json?.private_key) {
    throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON");
  }

  const auth = new google.auth.JWT({
    email: json.client_email,
    key: json.private_key,
    scopes: ["https://www.googleapis.com/auth/calendar"],
  });

  return google.calendar({ version: "v3", auth });
}

async function listReservationEvents(calendar, timeMinISO, timeMaxISO) {
  const list = await calendar.events.list({
    calendarId: GOOGLE_CALENDAR_ID,
    timeMin: timeMinISO,
    timeMax: timeMaxISO,
    singleEvents: true,
    orderBy: "startTime",
    maxResults: 250,
  });

  return list.data.items || [];
}

function countReservedSeatsByKey(events) {
  const map = new Map();

  for (const ev of events) {
    const priv = ev.extendedProperties?.private || {};
    if (priv.status === "cancelled") continue;

    const tourKey = String(priv.tour_key || "").trim();
    const start = String(ev.start?.dateTime || "").trim();
    if (!tourKey || !start) continue;

    const passengers = parseInt(priv.passengers_total || "1", 10) || 1;
    const key = `${tourKey}|${start}`;
    map.set(key, (map.get(key) || 0) + passengers);
  }

  return map;
}

function buildCandidateDeparturesForTour({ tour, fromISO, toISO }) {
  const from = new Date(fromISO);
  const to = new Date(toISO);

  const fromP = getZonedParts(from, BUSINESS_TIMEZONE);
  const toP = getZonedParts(to, BUSINESS_TIMEZONE);

  let curUTC = zonedTimeToUtc(
    { year: fromP.year, month: fromP.month, day: fromP.day, hour: 0, minute: 0 },
    BUSINESS_TIMEZONE
  );
  const endUTC = zonedTimeToUtc(
    { year: toP.year, month: toP.month, day: toP.day, hour: 23, minute: 59 },
    BUSINESS_TIMEZONE
  );

  const departures = [];

  while (curUTC <= endUTC) {
    const curLocal = getZonedParts(curUTC, BUSINESS_TIMEZONE);
    const js = new Date(Date.UTC(curLocal.year, curLocal.month - 1, curLocal.day, 12, 0, 0));
    const isoWeekday = ((js.getUTCDay() + 6) % 7) + 1;
    const weekdayKey = weekdayKeyFromISOWeekday(isoWeekday);

    const allowedMonths = Array.isArray(tour.activeMonths) ? tour.activeMonths : null;
    if (!allowedMonths || allowedMonths.includes(curLocal.month)) {
      const dayTimes = Array.isArray(tour.schedule?.[weekdayKey]) ? tour.schedule[weekdayKey] : [];

      for (const hhmm of dayTimes) {
        const [h, m] = String(hhmm).split(":").map((n) => parseInt(n, 10));
        if (!Number.isFinite(h) || !Number.isFinite(m)) continue;

        const startUTC = zonedTimeToUtc(
          { year: curLocal.year, month: curLocal.month, day: curLocal.day, hour: h, minute: m },
          BUSINESS_TIMEZONE
        );
        const endUTCSlot = new Date(startUTC.getTime() + Number(tour.durationMin || 0) * 60000);

        if (startUTC >= from && endUTCSlot <= to) {
          departures.push({
            slot_id: `slot_${tour.key}_${startUTC.getTime()}`,
            tour_key: tour.key,
            start: startUTC.toISOString(),
            end: endUTCSlot.toISOString(),
            capacity: Number(tour.capacity || 0),
          });
        }
      }
    }

    const nextDayUTC = zonedTimeToUtc(
      { year: curLocal.year, month: curLocal.month, day: curLocal.day, hour: 0, minute: 0 },
      BUSINESS_TIMEZONE
    );
    curUTC = new Date(nextDayUTC.getTime() + 24 * 60 * 60000);
  }

  return departures;
}

async function getAvailableSlotsTool({ tour_key, from, to }) {
  const tour = getTourByKey(tour_key);
  if (!tour) throw new Error("Tour inválido");

  const calendar = getCalendarClient();
  const events = await listReservationEvents(calendar, from, to);
  const reservedMap = countReservedSeatsByKey(events);
  const candidates = buildCandidateDeparturesForTour({ tour, fromISO: from, toISO: to });
  const nowPlusLead = getNowPlusLeadUTC();

  const available = candidates
    .map((c) => {
      const key = `${tour.key}|${c.start}`;
      const reserved = reservedMap.get(key) || 0;
      const remainingSeats = Math.max(0, Number(c.capacity || 0) - reserved);
      return { ...c, reservedSeats: reserved, remainingSeats };
    })
    .filter((c) => new Date(c.start) >= nowPlusLead && c.remainingSeats > 0)
    .sort((a, b) => new Date(a.start).getTime() - new Date(b.start).getTime());

  return available.slice(0, MAX_SLOTS_RETURN);
}

async function getRemainingSeatsForSlot({ tour_key, slot_start, slot_end }) {
  const tour = getTourByKey(tour_key);
  if (!tour) throw new Error("Tour inválido");

  const calendar = getCalendarClient();
  const events = await listReservationEvents(calendar, slot_start, slot_end);
  const reservedMap = countReservedSeatsByKey(events);
  const reserved = reservedMap.get(`${tour_key}|${slot_start}`) || 0;
  return Math.max(0, Number(tour.capacity || 0) - reserved);
}

async function createReservationTool({
  passenger_name,
  phone,
  slot_id,
  tour_key,
  adults,
  children,
  city,
  pickup,
  notes,
  slot_start,
  slot_end,
  quote_total,
  wa_id,
}) {
  const calendar = getCalendarClient();
  const tour = getTourByKey(tour_key);
  if (!tour) throw new Error("Tour inválido");
  if (!slot_start || !slot_end) throw new Error("Missing slot_start/slot_end");

  const passengersTotal = Number(adults || 0) + Number(children || 0);
  if (passengersTotal <= 0) throw new Error("Debe haber al menos 1 pasajero");

  const remainingSeats = await getRemainingSeatsForSlot({ tour_key, slot_start, slot_end });
  if (passengersTotal > remainingSeats) {
    throw new Error(`Solo quedan ${remainingSeats} espacios para esa salida`);
  }

  const event = await calendar.events.insert({
    calendarId: GOOGLE_CALENDAR_ID,
    requestBody: {
      summary: `Reserva - ${tour.title} - ${passenger_name}`,
      location: BUSINESS_ADDRESS || tour.meetingPoint || undefined,
      description:
        `Cliente: ${passenger_name}\n` +
        `Tel: ${phone}\n` +
        `Tour: ${tour.title}\n` +
        `Adultos: ${adults}\n` +
        `Niños: ${children}\n` +
        `Ciudad: ${city}\n` +
        `Punto de salida: ${pickup}\n` +
        `Cotización estimada: ${quote_total ? currency(quote_total) : "—"}\n` +
        `Notas: ${notes || ""}\n` +
        `SlotId: ${slot_id}`,
      start: { dateTime: slot_start, timeZone: BUSINESS_TIMEZONE },
      end: { dateTime: slot_end, timeZone: BUSINESS_TIMEZONE },
      extendedProperties: {
        private: {
          wa_phone: phone,
          wa_id: wa_id || "",
          passenger_name,
          tour_key,
          adults: String(adults || 0),
          children: String(children || 0),
          passengers_total: String(passengersTotal),
          city: city || "",
          pickup: pickup || "",
          quote_total: String(quote_total || 0),
          slot_id,
          reminder24hSent: "false",
          reminder2hSent: "false",
          payment_status: "pending",
          status: "active",
        },
      },
    },
  });

  return {
    reservation_id: event.data.id,
    start: slot_start,
    end: slot_end,
    tour_key,
    passenger_name,
    phone,
    adults: Number(adults || 0),
    children: Number(children || 0),
    city,
    pickup,
    quote_total: Number(quote_total || 0),
  };
}

async function rescheduleReservationTool({
  reservation_id,
  new_slot_id,
  new_start,
  new_end,
  tour_key,
  passenger_name,
  phone,
  adults,
  children,
  city,
  pickup,
  wa_id,
}) {
  const calendar = getCalendarClient();
  const tour = getTourByKey(tour_key);
  if (!tour) throw new Error("Tour inválido");
  if (!new_start || !new_end) throw new Error("Missing new_start/new_end");

  const passengersTotal = Number(adults || 0) + Number(children || 0);
  const current = await calendar.events.get({ calendarId: GOOGLE_CALENDAR_ID, eventId: reservation_id });
  const priv = current.data.extendedProperties?.private || {};
  const oldStart = current.data.start?.dateTime;

  const remainingSeats = await getRemainingSeatsForSlot({ tour_key, slot_start: new_start, slot_end: new_end });
  const alreadyReservedOnSameSlot = oldStart === new_start ? passengersTotal : 0;
  if (passengersTotal > remainingSeats + alreadyReservedOnSameSlot) {
    throw new Error(`Solo quedan ${remainingSeats} espacios para esa salida`);
  }

  const nextPriv = {
    ...priv,
    tour_key,
    slot_id: new_slot_id,
    passenger_name: passenger_name || priv.passenger_name || "",
    wa_phone: phone || priv.wa_phone || "",
    wa_id: wa_id || priv.wa_id || "",
    adults: String(adults || priv.adults || 0),
    children: String(children || priv.children || 0),
    passengers_total: String(passengersTotal || priv.passengers_total || 1),
    city: city || priv.city || "",
    pickup: pickup || priv.pickup || "",
    reminder24hSent: "false",
    reminder2hSent: "false",
  };

  const updated = await calendar.events.patch({
    calendarId: GOOGLE_CALENDAR_ID,
    eventId: reservation_id,
    requestBody: {
      summary: `Reserva - ${tour.title} - ${passenger_name || priv.passenger_name || "Cliente"}`,
      location: BUSINESS_ADDRESS || tour.meetingPoint || undefined,
      start: { dateTime: new_start, timeZone: BUSINESS_TIMEZONE },
      end: { dateTime: new_end, timeZone: BUSINESS_TIMEZONE },
      extendedProperties: { private: nextPriv },
    },
  });

  return { ok: true, reservation_id: updated.data.id, new_start, new_end };
}

async function cancelReservationTool({ reservation_id, reason }) {
  const calendar = getCalendarClient();
  const event = await calendar.events.get({ calendarId: GOOGLE_CALENDAR_ID, eventId: reservation_id });
  const summary = event.data.summary || "Reserva";

  await calendar.events.patch({
    calendarId: GOOGLE_CALENDAR_ID,
    eventId: reservation_id,
    requestBody: {
      summary: `CANCELADA - ${summary}`,
      description: (event.data.description || "") + `\n\nCancelación: ${reason || ""}`,
      extendedProperties: {
        private: { ...(event.data.extendedProperties?.private || {}), status: "cancelled" },
      },
    },
  });

  return { ok: true, reservation_id };
}

async function handoffToHumanTool({ summary }) {
  return { ok: true, routed: true, summary };
}

async function findUpcomingReservationByPhone(phone, windowDays = 180) {
  try {
    const phoneDigits = String(phone || "").replace(/[^\d]/g, "");
    if (!phoneDigits) return null;

    const calendar = getCalendarClient();
    const now = new Date();
    const end = addMinutes(now, windowDays * 24 * 60);

    const events = await listReservationEvents(calendar, now.toISOString(), end.toISOString());

    for (const ev of events) {
      const priv = ev.extendedProperties?.private || {};
      if (priv.status === "cancelled") continue;

      const wa = String(priv.wa_phone || "").replace(/[^\d]/g, "");
      if (!wa || wa !== phoneDigits) continue;

      const start = ev.start?.dateTime;
      const endDT = ev.end?.dateTime;
      if (!start || !endDT) continue;

      return {
        reservation_id: ev.id,
        start,
        end: endDT,
        tour_key: String(priv.tour_key || "").trim() || inferTourFromSummary(ev.summary || ""),
        passenger_name: String(priv.passenger_name || "").trim() || "",
        phone: phoneDigits,
        adults: Number(priv.adults || 0),
        children: Number(priv.children || 0),
        city: String(priv.city || "").trim() || "",
        pickup: String(priv.pickup || "").trim() || "",
        quote_total: Number(priv.quote_total || 0),
      };
    }

    return null;
  } catch (e) {
    console.error("findUpcomingReservationByPhone error:", e?.response?.data || e?.message || e);
    return null;
  }
}

function inferTourFromSummary(summary) {
  const s = normalizeText(summary || "");
  for (const t of TOURS) {
    const title = normalizeText(t.title || "");
    if (title && s.includes(title)) return t.key;
  }
  return "";
}

// =========================
// Date parsing
// =========================
const DOW = {
  lunes: 1,
  martes: 2,
  miercoles: 3,
  miércoles: 3,
  jueves: 4,
  viernes: 5,
  sabado: 6,
  sábado: 6,
  domingo: 7,
};

const MONTHS = {
  enero: 1,
  febrero: 2,
  marzo: 3,
  abril: 4,
  mayo: 5,
  junio: 6,
  julio: 7,
  agosto: 8,
  septiembre: 9,
  setiembre: 9,
  octubre: 10,
  noviembre: 11,
  diciembre: 12,
};

function startOfLocalDayUTC(date, tz) {
  const p = getZonedParts(date, tz);
  return zonedTimeToUtc({ year: p.year, month: p.month, day: p.day, hour: 0, minute: 0 }, tz);
}

function addLocalDaysUTC(dateUTC, days, tz) {
  const p = getZonedParts(dateUTC, tz);
  const base = new Date(Date.UTC(p.year, p.month - 1, p.day, 12, 0, 0));
  base.setUTCDate(base.getUTCDate() + days);
  return zonedTimeToUtc(
    { year: base.getUTCFullYear(), month: base.getUTCMonth() + 1, day: base.getUTCDate(), hour: 0, minute: 0 },
    tz
  );
}

function nextWeekdayFromTodayUTC(targetIsoDow, tz, isNext = false) {
  const now = new Date();
  const todayLocal = startOfLocalDayUTC(now, tz);

  const p = getZonedParts(todayLocal, tz);
  const mid = zonedTimeToUtc({ year: p.year, month: p.month, day: p.day, hour: 12, minute: 0 }, tz);
  const js = new Date(mid.toISOString());
  const isoToday = ((js.getUTCDay() + 6) % 7) + 1;

  let diff = targetIsoDow - isoToday;
  if (diff < 0) diff += 7;
  if (diff === 0 && isNext) diff = 7;

  return addLocalDaysUTC(todayLocal, diff, tz);
}

function rangeForWholeMonth(year, month) {
  const from = zonedTimeToUtc({ year, month, day: 1, hour: 0, minute: 0 }, BUSINESS_TIMEZONE);
  const toMonth = month === 12 ? { year: year + 1, month: 1 } : { year, month: month + 1 };
  const to = zonedTimeToUtc(
    { year: toMonth.year, month: toMonth.month, day: 1, hour: 0, minute: 0 },
    BUSINESS_TIMEZONE
  );
  return { from: from.toISOString(), to: to.toISOString() };
}

function parseDateRangeFromText(userText) {
  const t = normalizeText(userText);

  if (t.includes("hoy")) {
    const from = startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "hoy" };
  }
  if (t.includes("pasado manana") || t.includes("pasado mañana")) {
    const from = addLocalDaysUTC(startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE), 2, BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "pasado mañana" };
  }
  if (t.includes("manana") || t.includes("mañana")) {
    const from = addLocalDaysUTC(startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE), 1, BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "mañana" };
  }

  if (t.includes("semana que viene") || t.includes("la semana que viene") || t.includes("siguiente semana")) {
    const from = addLocalDaysUTC(startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE), 1, BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 7, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "la semana que viene" };
  }

  for (const [mname, mnum] of Object.entries(MONTHS)) {
    if (t === mname || t.includes(`en ${mname}`) || t.includes(`para ${mname}`)) {
      const nowP = getZonedParts(new Date(), BUSINESS_TIMEZONE);
      let year = nowP.year;
      if (mnum < nowP.month) year += 1;
      const r = rangeForWholeMonth(year, mnum);
      return { ...r, label: mname };
    }
  }

  for (const [name, iso] of Object.entries(DOW)) {
    if (t.includes(name)) {
      const isNext =
        t.includes("proximo") || t.includes("próximo") || t.includes("que viene") || t.includes("siguiente");
      const fromDay = nextWeekdayFromTodayUTC(iso, BUSINESS_TIMEZONE, isNext);
      const toDay = addLocalDaysUTC(fromDay, 1, BUSINESS_TIMEZONE);
      return { from: fromDay.toISOString(), to: toDay.toISOString(), label: name };
    }
  }

  const m1 = t.match(/(\d{1,2})\s+de\s+([a-záéíóú]+)(\s+de\s+(\d{4}))?/);
  if (m1) {
    const day = parseInt(m1[1], 10);
    const monthName = normalizeText(m1[2]);
    const month = MONTHS[monthName];
    if (month) {
      const now = new Date();
      const nowP = getZonedParts(now, BUSINESS_TIMEZONE);
      let year = m1[4] ? parseInt(m1[4], 10) : nowP.year;

      if (!m1[4]) {
        const candidateUTC = zonedTimeToUtc({ year, month, day, hour: 0, minute: 0 }, BUSINESS_TIMEZONE);
        if (candidateUTC < startOfLocalDayUTC(now, BUSINESS_TIMEZONE)) year += 1;
      }

      const from = zonedTimeToUtc({ year, month, day, hour: 0, minute: 0 }, BUSINESS_TIMEZONE);
      const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
      return { from: from.toISOString(), to: to.toISOString(), label: `${day} de ${monthName}` };
    }
  }

  return null;
}

function formatSlotsList(tourKey, slots, session) {
  if (!slots?.length) return null;
  const tour = getTourByKey(tourKey);
  const dateLabel = formatDateInTZ(slots[0].start, BUSINESS_TIMEZONE);
  const view = slots.slice(0, Math.max(1, DISPLAY_SLOTS_LIMIT));
  if (session) session.lastDisplaySlots = view;

  const lines = view.map((s, i) => {
    const a = formatTimeInTZ(s.start, BUSINESS_TIMEZONE);
    return `${i + 1}. ${a} • ${s.remainingSeats} espacios`;
  });

  return (
    `Estos son los horarios disponibles para *${tour?.title || tourKey}* el *${dateLabel}*:\n\n` +
    `${lines.join("\n")}\n\n` +
    `Responde con el *número* (1,2,3...) o escribe la *hora* (ej: 9:00 am).`
  );
}

function parseUserTimeTo24h(userText) {
  const raw = String(userText || "").trim().toLowerCase();
  if (!raw) return null;

  let compact = raw.replace(/\./g, "").replace(/\s+/g, " ").trim();
  compact = compact.replace(/\b([ap])\s*m\b/g, "$1m");

  if (/^\d{1,2}$/.test(compact)) return null;

  const m = compact.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i);
  if (!m) return null;

  let hh = parseInt(m[1], 10);
  const mm = m[2] ? parseInt(m[2], 10) : 0;
  const mer = m[3] ? String(m[3]).toLowerCase() : "";

  if (Number.isNaN(hh) || Number.isNaN(mm)) return null;
  if (mm < 0 || mm > 59) return null;

  if (mer === "am" || mer === "pm") {
    if (hh < 1 || hh > 12) return null;
    if (mer === "pm" && hh !== 12) hh += 12;
    if (mer === "am" && hh === 12) hh = 0;
  } else {
    if (hh < 0 || hh > 23) return null;
  }

  return { hh, mm, meridian: mer || null };
}

function tryPickSlotFromUserText(session, userText) {
  const t = normalizeText(userText);

  if (/^\d+$/.test(t)) {
    const num = parseInt(t, 10);
    if (!Number.isNaN(num) && num >= 1 && num <= (session.lastDisplaySlots?.length || 0)) {
      return session.lastDisplaySlots[num - 1] || null;
    }
  }

  const parsed = parseUserTimeTo24h(userText);
  if (parsed) {
    const { hh, mm } = parsed;
    const found = session.lastSlots.find((s) => {
      const d = new Date(s.start);
      const parts = getZonedParts(d, BUSINESS_TIMEZONE);
      return parts.hour === hh && parts.minute === mm;
    });
    if (found) return found;
  }

  const m = t.match(/^(\d{1,2})(?::(\d{2}))?$/);
  if (m) {
    const hh = parseInt(m[1], 10);
    const mm = m[2] ? parseInt(m[2], 10) : 0;
    const found = session.lastSlots.find((s) => {
      const d = new Date(s.start);
      const parts = getZonedParts(d, BUSINESS_TIMEZONE);
      return parts.hour === hh && parts.minute === mm;
    });
    if (found) return found;
  }

  return null;
}

function detectCategoryKeyFromUser(text) {
  const t = normalizeText(text);
  if (CATEGORY_ID_TO_KEY[text]) return CATEGORY_ID_TO_KEY[text];

  if (t.includes("tour diario") || t.includes("tours diarios")) return "tours_diarios";
  if (t.includes("playa") || t.includes("isla")) return "playas";
  if (t.includes("montana") || t.includes("montaña") || t.includes("jarabacoa")) return "montanas";
  if (t.includes("especial")) return "excursiones_especiales";
  if (t.includes("temporada") || t.includes("paquete")) return "paquetes_temporada";

  for (const c of TOUR_CATEGORIES) {
    const n = normalizeText(c.title);
    if (t === n || t.includes(n)) return c.key;
  }

  return null;
}

function detectTourKeyFromUser(text) {
  const t = normalizeText(text);
  if (TOUR_ID_TO_KEY[text]) return TOUR_ID_TO_KEY[text];

  for (const tour of TOURS) {
    const nt = normalizeText(tour.title);
    if (t === nt || t.includes(nt)) return tour.key;
    const keyNorm = normalizeText(tour.key.replace(/_/g, " "));
    if (t.includes(keyNorm)) return tour.key;
  }

  if (t.includes("saona")) return "isla_saona";
  if (t.includes("catalina")) return "isla_catalina";
  if (t.includes("city tour") || t.includes("zona colonial") || t.includes("santo domingo")) return "city_tour_santo_domingo";
  if (t.includes("jarabacoa")) return "jarabacoa_aventura";
  if (t.includes("buggies") || t.includes("macao")) return "buggies_macao";
  if (t.includes("samana") || t.includes("samaná")) return "samana_temporada";

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

async function notifyPersonalWhatsAppBookingSummary(booking) {
  try {
    if (!PERSONAL_WA_TO) return;

    const myTo = String(PERSONAL_WA_TO).replace(/[^\d]/g, "");
    if (!myTo) return;

    const passengerPhone = String(booking?.phone || "").replace(/[^\d]/g, "");
    if (passengerPhone && myTo === passengerPhone) return;

    const tour = getTourByKey(booking.tour_key);

    const summary =
      `📌 *Nueva reserva turística*\n\n` +
      `🏢 Agencia: *${BUSINESS_NAME}*\n` +
      `🌴 Tour: *${tour?.title || booking.tour_key}*\n` +
      `👤 Cliente: *${booking.passenger_name}*\n` +
      `📞 Tel: *${passengerPhone || "—"}*\n` +
      `👥 Pax: *${Number(booking.adults || 0) + Number(booking.children || 0)}* (${booking.adults || 0} adultos / ${booking.children || 0} niños)\n` +
      `📍 Ciudad: ${booking.city || "—"}\n` +
      `🚐 Salida: ${booking.pickup || "—"}\n` +
      `📅 Fecha: *${formatDateInTZ(booking.start, BUSINESS_TIMEZONE)}*\n` +
      `⏰ Hora: *${formatTimeInTZ(booking.start, BUSINESS_TIMEZONE)}*\n` +
      `💵 Estimado: *${currency(booking.quote_total || 0)}*\n` +
      `🆔 ID: ${booking.reservation_id || "—"}`;

    await sendWhatsAppText(myTo, summary, "BOT");
  } catch (e) {
    console.error("notifyPersonalWhatsAppBookingSummary error:", e?.response?.data || e?.message || e);
  }
}

// =========================
// OpenAI fallback
// =========================
async function callOpenAI({ session, userText, userPhone, extraSystem = "" }) {
  const today = new Date();
  const tzParts = getZonedParts(today, BUSINESS_TIMEZONE);
  const todayStr = `${tzParts.year}-${String(tzParts.month).padStart(2, "0")}-${String(tzParts.day).padStart(2, "0")}`;

  const system = {
    role: "system",
    content: `
Eres un asistente de WhatsApp de ${BUSINESS_NAME} para cotizar y reservar tours.
Reglas:
- No inventes disponibilidad. Solo ofrece salidas reales usando get_available_departures.
- Para reservar, debes llamar a create_reservation con slot_start y slot_end EXACTOS de la salida elegida.
- Responde corto, claro y orientado a cerrar la reserva.
- Responde preguntas frecuentes: precio, qué incluye, horarios, punto de salida, formas de pago y políticas.
- Fecha actual (zona ${BUSINESS_TIMEZONE}): ${todayStr}. Interpreta "mañana", "viernes", "próximo martes", etc. correctamente.
- No ofrezcas salidas que inicien en menos de ${MIN_BOOKING_LEAD_MIN} minutos desde ahora.
- Si el usuario quiere una reserva, necesitas: tour, fecha, cantidad de adultos, cantidad de niños, punto de salida, ciudad, nombre y teléfono.

Tours disponibles:
${TOURS.map((t) => `- ${t.title} (${t.category})`).join("\n")}

${extraSystem}
Tel usuario: ${userPhone}.
`,
  };

  session.messages.push({ role: "user", content: userText });
  const messages = [system, ...session.messages].slice(-14);

  const tools = [
    {
      type: "function",
      function: {
        name: "get_available_departures",
        description: "Obtiene salidas reales disponibles para un tour en un rango de fechas.",
        parameters: {
          type: "object",
          properties: {
            tour_key: { type: "string" },
            from: { type: "string" },
            to: { type: "string" },
          },
          required: ["tour_key", "from", "to"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "create_reservation",
        description: "Crea una reserva turística en el calendario usando la salida elegida.",
        parameters: {
          type: "object",
          properties: {
            passenger_name: { type: "string" },
            phone: { type: "string" },
            slot_id: { type: "string" },
            tour_key: { type: "string" },
            adults: { type: "number" },
            children: { type: "number" },
            city: { type: "string" },
            pickup: { type: "string" },
            notes: { type: "string" },
            slot_start: { type: "string" },
            slot_end: { type: "string" },
            quote_total: { type: "number" },
          },
          required: [
            "passenger_name",
            "phone",
            "slot_id",
            "tour_key",
            "adults",
            "children",
            "city",
            "pickup",
            "slot_start",
            "slot_end",
          ],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "reschedule_reservation",
        description: "Reagenda una reserva a una nueva salida.",
        parameters: {
          type: "object",
          properties: {
            reservation_id: { type: "string" },
            new_slot_id: { type: "string" },
            new_start: { type: "string" },
            new_end: { type: "string" },
            tour_key: { type: "string" },
            passenger_name: { type: "string" },
            phone: { type: "string" },
            adults: { type: "number" },
            children: { type: "number" },
            city: { type: "string" },
            pickup: { type: "string" },
          },
          required: ["reservation_id", "new_slot_id", "new_start", "new_end", "tour_key"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "cancel_reservation",
        description: "Cancela una reserva por id.",
        parameters: {
          type: "object",
          properties: {
            reservation_id: { type: "string" },
            reason: { type: "string" },
          },
          required: ["reservation_id"],
        },
      },
    },
    {
      type: "function",
      function: {
        name: "handoff_to_human",
        description: "Deriva a humano si el caso requiere revisión manual.",
        parameters: {
          type: "object",
          properties: { summary: { type: "string" } },
          required: ["summary"],
        },
      },
    },
  ];

  const resp = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    {
      model: "gpt-4.1-mini",
      messages,
      tools,
      tool_choice: "auto",
      temperature: 0.2,
    },
    { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` } }
  );

  const msg = resp.data.choices?.[0]?.message;

  if (msg?.tool_calls?.length) {
    const toolResults = [];
    for (const tc of msg.tool_calls) {
      const name = tc.function.name;
      const args = JSON.parse(tc.function.arguments || "{}");

      if (name === "get_available_departures") {
        const slots = await getAvailableSlotsTool(args);
        toolResults.push({ tool_call_id: tc.id, role: "tool", name, content: JSON.stringify({ slots }) });
      }

      if (name === "create_reservation") {
        const booked = await createReservationTool(args);
        toolResults.push({ tool_call_id: tc.id, role: "tool", name, content: JSON.stringify({ booked }) });
      }

      if (name === "reschedule_reservation") {
        const out = await rescheduleReservationTool(args);
        toolResults.push({ tool_call_id: tc.id, role: "tool", name, content: JSON.stringify(out) });
      }

      if (name === "cancel_reservation") {
        const out = await cancelReservationTool(args);
        toolResults.push({ tool_call_id: tc.id, role: "tool", name, content: JSON.stringify(out) });
      }

      if (name === "handoff_to_human") {
        const out = await handoffToHumanTool(args);
        toolResults.push({ tool_call_id: tc.id, role: "tool", name, content: JSON.stringify(out) });
      }
    }

    const resp2 = await axios.post(
      "https://api.openai.com/v1/chat/completions",
      { model: "gpt-4.1-mini", messages: [...messages, msg, ...toolResults], temperature: 0.2 },
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` } }
    );

    const finalText = resp2.data.choices?.[0]?.message?.content?.trim() || "";
    session.messages.push({ role: "assistant", content: finalText });
    return finalText || "¿Qué tour te interesa? También puedo mostrarte las categorías.";
  }

  const text = msg?.content?.trim() || "Hola 👋 ¿Qué tour te interesa? También puedo mostrarte las categorías.";
  session.messages.push({ role: "assistant", content: text });
  return text;
}

// =========================
// Webhooks helpers
// =========================
app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];
  if (mode === "subscribe" && token === VERIFY_TOKEN) return res.status(200).send(challenge);
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

app.post("/agent_message", async (req, res) => {
  try {
    if (!BOTHUB_WEBHOOK_SECRET) {
      return res.status(400).json({ error: "BOTHUB_WEBHOOK_SECRET not configured" });
    }

    const signature = getHubSignature(req);
    const okSig = verifyHubSignature(req.body, signature, BOTHUB_WEBHOOK_SECRET);

    if (!signature || !okSig) {
      console.warn("[agent_message] Invalid signature", {
        hasSignature: Boolean(signature),
        sigLen: signature ? String(signature).length : 0,
      });
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

app.post("/webhook", async (req, res) => {
  let from = "";
  let session = null;

  try {
    if (!verifyMetaSignature(req)) return res.sendStatus(403);

    const entry = req.body.entry?.[0];
    const change = entry?.changes?.[0];
    const value = change?.value;
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

    const wantsCancel = looksLikeCancel(tNorm) || isChoice(tNorm, 3);
    const wantsReschedule = looksLikeReschedule(tNorm) || isChoice(tNorm, 2);
    const wantsConfirm = looksLikeConfirm(tNorm) || isChoice(tNorm, 1);

    if ((wantsCancel || wantsReschedule || wantsConfirm) && !session.lastBooking) {
      const found = await findUpcomingReservationByPhone(from);
      if (found) {
        session.lastBooking = found;
        session.state = "post_booking";
      }
    }

    const detectedCategoryEarly = detectCategoryKeyFromUser(userText);
    const detectedTourEarly = detectTourKeyFromUser(userText);
    const detectedRangeEarly = parseDateRangeFromText(userText);
    const hasEarlyIntent =
      !!detectedCategoryEarly ||
      !!detectedTourEarly ||
      !!detectedRangeEarly ||
      tNorm.includes("tour") ||
      tNorm.includes("excursion") ||
      tNorm.includes("excursión") ||
      tNorm.includes("reserva") ||
      tNorm.includes("reservar") ||
      tNorm.includes("cotizacion") ||
      tNorm.includes("cotización");

    if (session.greeted && session.state === "idle" && isGreeting(tNorm) && !hasEarlyIntent) {
      await sendWhatsAppText(from, quickHelpText());
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle" && isGreeting(tNorm) && !hasEarlyIntent) {
      session.greeted = true;
      await sendWhatsAppText(from, categoriesEmojiText());
      await sendCategoriesList(from);
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle") session.greeted = true;

    // POST BOOKING
    if (session.state === "post_booking" && session.lastBooking) {
      const booking = session.lastBooking;
      const tour = getTourByKey(booking.tour_key);

      if (wantsConfirm) {
        await sendWhatsAppText(
          from,
          `✅ ¡Reserva confirmada!\n\n🌴 Tour: ${tour?.title || booking.tour_key}\n📅 Fecha: ${formatDateInTZ(
            booking.start,
            BUSINESS_TIMEZONE
          )}\n⏰ Hora: ${formatTimeInTZ(booking.start, BUSINESS_TIMEZONE)}\n\nResponde:\n2) Reprogramar\n3) Cancelar`
        );
        return res.sendStatus(200);
      }

      if (wantsCancel) {
        await cancelReservationTool({ reservation_id: booking.reservation_id, reason: userText });
        await sendWhatsAppText(from, `✅ Listo. Tu reserva fue cancelada.\n\nSi deseas una nueva, escribe *"Nueva reserva"* o dime el tour.`);

        session.state = "idle";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingCategory = null;
        session.pendingTour = null;
        session.pendingRange = null;
        session.pendingAdults = null;
        session.pendingChildren = null;
        session.pendingPickup = null;
        session.pendingCity = null;
        session.pendingName = null;
        session.lastBooking = null;
        session.reschedule = defaultSession().reschedule;
        return res.sendStatus(200);
      }

      if (wantsReschedule) {
        session.reschedule.active = true;
        session.reschedule.reservation_id = booking.reservation_id;
        session.reschedule.phone = booking.phone || String(from).replace(/[^\d]/g, "");
        session.reschedule.passenger_name = booking.passenger_name || "";
        session.reschedule.tour_key = booking.tour_key || "";
        session.reschedule.adults = Number(booking.adults || 0);
        session.reschedule.children = Number(booking.children || 0);
        session.reschedule.city = booking.city || "";
        session.reschedule.pickup = booking.pickup || "";

        session.pendingTour = booking.tour_key || session.pendingTour;
        session.state = "await_day";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingRange = null;
        session.pendingName = null;

        await sendWhatsAppText(
          from,
          `Perfecto ✅ Vamos a reprogramar tu reserva.\nTour: *${tour?.title || booking.tour_key}*\n\n¿Para qué día?\nEj: "mañana", "viernes", "próximo martes".`
        );
        return res.sendStatus(200);
      }

      if (looksLikeNewReservation(tNorm)) {
        session.state = "idle";
        session.reschedule = defaultSession().reschedule;
        await sendWhatsAppText(from, `Claro ✅ Vamos con una nueva reserva.`);
        await sendWhatsAppText(from, categoriesEmojiText());
        await sendCategoriesList(from);
        return res.sendStatus(200);
      }

      if (isThanks(tNorm)) {
        await sendWhatsAppText(
          from,
          `¡Perfecto! ✅\nTu reserva queda registrada.\n\n🌴 Tour: ${tour?.title || booking.tour_key}\n📅 Fecha: ${formatDateInTZ(
            booking.start,
            BUSINESS_TIMEZONE
          )}\n⏰ Hora: ${formatTimeInTZ(booking.start, BUSINESS_TIMEZONE)}\n\nSi necesitas *reprogramar* o *cancelar*, escríbelo aquí.`
        );
        return res.sendStatus(200);
      }

      await sendWhatsAppText(
        from,
        `Estoy aquí ✅\nSi deseas *reprogramar* o *cancelar* tu reserva, responde:\n2) Reprogramar\n3) Cancelar\n\nSi deseas una *nueva reserva*, escribe "Nueva reserva".`
      );
      return res.sendStatus(200);
    }

    // AWAIT SLOT CHOICE
    if (session.state === "await_slot_choice" && session.lastSlots?.length) {
      if (["reiniciar", "reset", "resetear", "empezar", "inicio"].some((k) => tNorm.includes(k))) {
        session.state = "idle";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingCategory = null;
        session.pendingTour = null;
        session.pendingRange = null;
        session.pendingAdults = null;
        session.pendingChildren = null;
        session.pendingPickup = null;
        session.pendingCity = null;
        session.pendingName = null;
        session.reschedule = defaultSession().reschedule;

        await sendWhatsAppText(from, `Listo ✅ Reinicié el proceso.`);
        await sendWhatsAppText(from, categoriesEmojiText());
        await sendCategoriesList(from);
        return res.sendStatus(200);
      }

      if (["reprogramar", "cambiar", "otro dia", "otro día", "otra fecha"].some((k) => tNorm.includes(k))) {
        session.state = "await_day";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingRange = null;

        const tour = getTourByKey(session.pendingTour);
        await sendWhatsAppText(
          from,
          `Perfecto ✅ Vamos a elegir *otro día* para *${tour?.title || session.pendingTour}*.\n\n¿Para qué día?\nEj: "mañana", "viernes", "próximo martes", "la semana que viene" o "14 de junio".`
        );
        return res.sendStatus(200);
      }

      const picked = tryPickSlotFromUserText(session, userText);
      if (!picked) {
        if (/^\d+$/.test(tNorm)) {
          await sendWhatsAppText(from, `Ese número no corresponde a una salida disponible 🙏\nResponde con uno de los números que ves en la lista, o escribe una hora como "9:00 am".`);
          return res.sendStatus(200);
        }

        const parsed = parseUserTimeTo24h(userText);
        if (parsed) {
          await sendWhatsAppText(from, `Entendí *${userText}* ✅\nPero ese horario no está disponible.\n\nResponde con el *número* o una *hora disponible*.`);
          return res.sendStatus(200);
        }

        await sendWhatsAppText(from, `No entendí la salida 🙏\nResponde con el *número* (1,2,3...) o la *hora* (ej: 9:00 am).`);
        return res.sendStatus(200);
      }

      session.selectedSlot = picked;

      if (session.reschedule?.active && session.reschedule.reservation_id) {
        await rescheduleReservationTool({
          reservation_id: session.reschedule.reservation_id,
          new_slot_id: picked.slot_id,
          new_start: picked.start,
          new_end: picked.end,
          tour_key: session.pendingTour || session.reschedule.tour_key,
          passenger_name: session.reschedule.passenger_name,
          phone: session.reschedule.phone || from,
          adults: session.reschedule.adults,
          children: session.reschedule.children,
          city: session.reschedule.city,
          pickup: session.reschedule.pickup,
          wa_id: from,
        });

        session.lastBooking = {
          reservation_id: session.reschedule.reservation_id,
          start: picked.start,
          end: picked.end,
          tour_key: session.pendingTour || session.reschedule.tour_key,
          passenger_name: session.reschedule.passenger_name || session.lastBooking?.passenger_name || "",
          phone: session.reschedule.phone || String(from).replace(/[^\d]/g, ""),
          adults: session.reschedule.adults,
          children: session.reschedule.children,
          city: session.reschedule.city,
          pickup: session.reschedule.pickup,
          quote_total: session.lastBooking?.quote_total || 0,
        };

        const tour = getTourByKey(session.lastBooking.tour_key);
        session.state = "post_booking";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingRange = null;
        session.pendingName = null;
        session.reschedule = defaultSession().reschedule;

        await sendWhatsAppText(
          from,
          `✅ *Reserva reprogramada*\n\n🌴 Tour: *${tour?.title || session.lastBooking.tour_key}*\n📅 Fecha: *${formatDateInTZ(
            picked.start,
            BUSINESS_TIMEZONE
          )}*\n⏰ Hora: *${formatTimeInTZ(picked.start, BUSINESS_TIMEZONE)}*\n\nResponde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`
        );
        return res.sendStatus(200);
      }

      session.state = "await_adults";
      await sendWhatsAppText(
        from,
        `Perfecto ✅ Queda seleccionada la salida de las ${formatTimeInTZ(picked.start, BUSINESS_TIMEZONE)}.\n\n¿Cuántos *adultos* viajan?`
      );
      return res.sendStatus(200);
    }

    // AWAIT ADULTS
    if (session.state === "await_adults" && session.selectedSlot) {
      const count = parsePassengerCount(userText);
      if (count === null || count < 1) {
        await sendWhatsAppText(from, `Por favor, indícame cuántos *adultos* viajan. Ej: 2`);
        return res.sendStatus(200);
      }

      session.pendingAdults = count;
      session.state = "await_children";
      await sendWhatsAppText(from, `Perfecto 👍\nAhora dime cuántos *niños* viajan. Si no van niños, responde *0*.`);
      return res.sendStatus(200);
    }

    // AWAIT CHILDREN
    if (session.state === "await_children" && session.selectedSlot) {
      const count = parsePassengerCount(userText);
      if (count === null || count < 0) {
        await sendWhatsAppText(from, `Indícame cuántos *niños* viajan. Si no van niños, responde *0*.`);
        return res.sendStatus(200);
      }

      const total = Number(session.pendingAdults || 0) + Number(count || 0);
      const remainingSeats = Number(session.selectedSlot.remainingSeats || 0);
      if (total <= 0) {
        await sendWhatsAppText(from, `Debe viajar al menos *1 persona* para continuar.`);
        return res.sendStatus(200);
      }
      if (total > remainingSeats) {
        session.pendingAdults = null;
        session.pendingChildren = null;
        session.state = "await_slot_choice";
        await sendWhatsAppText(
          from,
          `Ahora mismo esa salida solo tiene *${remainingSeats} espacios*.\n\nTe devolví al paso anterior para elegir otro horario o salida.`
        );
        return res.sendStatus(200);
      }

      session.pendingChildren = count;
      const tour = getTourByKey(session.pendingTour);
      const quoteText = buildQuotePreview(tour, session.pendingAdults, session.pendingChildren);
      updateLead(session, { tour_key: session.pendingTour, quotePreview: quoteText });

      session.state = "await_pickup";
      await sendWhatsAppText(from, `${quoteText}\n\nAhora dime tu *punto de salida o pickup*. Ej: Santo Domingo Este / Bávaro / Punto de encuentro.`);
      return res.sendStatus(200);
    }

    // AWAIT PICKUP
    if (session.state === "await_pickup" && session.selectedSlot) {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame tu *punto de salida o pickup*.`);
        return res.sendStatus(200);
      }

      session.pendingPickup = userText;
      session.state = "await_city";
      await sendWhatsAppText(from, `Gracias. Ahora dime la *ciudad* donde te encuentras.`);
      return res.sendStatus(200);
    }

    // AWAIT CITY
    if (session.state === "await_city" && session.selectedSlot) {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame tu *ciudad*.`);
        return res.sendStatus(200);
      }

      session.pendingCity = userText;
      session.state = "await_name";
      await sendWhatsAppText(from, `Perfecto ✅\nAhora indícame tu *nombre completo* para dejar la reserva casi lista.`);
      return res.sendStatus(200);
    }

    // AWAIT NAME
    if (session.state === "await_name" && session.selectedSlot) {
      if (tNorm.length < 3 || ["si", "sí", "ok", "listo"].includes(tNorm)) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }
      session.pendingName = userText;
      session.state = "await_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* (ej: 829XXXXXXX) para completar la reserva.`);
      return res.sendStatus(200);
    }

    // AWAIT PHONE -> BOOK
    if (session.state === "await_phone" && session.selectedSlot && session.pendingName) {
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíame el teléfono así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const tour = getTourByKey(session.pendingTour);
      const quoteTotal =
        Number(session.pendingAdults || 0) * Number(tour?.basePriceAdult || 0) +
        Number(session.pendingChildren || 0) * Number(tour?.basePriceChild || 0);

      const booked = await createReservationTool({
        passenger_name: session.pendingName,
        phone: phoneDigits,
        slot_id: session.selectedSlot.slot_id,
        tour_key: session.pendingTour || session.selectedSlot.tour_key,
        adults: session.pendingAdults || 0,
        children: session.pendingChildren || 0,
        city: session.pendingCity || "",
        pickup: session.pendingPickup || "",
        notes: "",
        slot_start: session.selectedSlot.start,
        slot_end: session.selectedSlot.end,
        quote_total: quoteTotal,
        wa_id: from,
      });

      await sendWhatsAppText(
        from,
        `✅ *Reserva registrada*\n\n🌴 Tour: *${tour?.title || booked.tour_key}*\n👤 Cliente: *${booked.passenger_name}*\n📞 Teléfono: *${phoneDigits}*\n👥 Pasajeros: *${Number(booked.adults || 0) + Number(booked.children || 0)}* (${booked.adults || 0} adultos / ${booked.children || 0} niños)\n📍 Ciudad: ${booked.city || "—"}\n🚐 Salida: ${booked.pickup || "—"}\n📅 Fecha: *${formatDateInTZ(booked.start, BUSINESS_TIMEZONE)}*\n⏰ Hora: *${formatTimeInTZ(booked.start, BUSINESS_TIMEZONE)}*\n💵 Total estimado: *${currency(booked.quote_total || 0)}*\n📌 Pago: ${tour?.paymentPolicy || "El equipo confirmará los detalles de pago."}\n\nResponde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`
      );

      await notifyPersonalWhatsAppBookingSummary(booked);

      session.lastBooking = booked;
      session.state = "post_booking";
      session.lastSlots = [];
      session.lastDisplaySlots = [];
      session.selectedSlot = null;
      session.pendingRange = null;
      session.pendingAdults = null;
      session.pendingChildren = null;
      session.pendingPickup = null;
      session.pendingCity = null;
      session.pendingName = null;
      session.reschedule = defaultSession().reschedule;
      clearLeadOnBooking(session);

      return res.sendStatus(200);
    }

    // categories ask
    if (
      tNorm.includes("categorias") ||
      tNorm.includes("categorías") ||
      tNorm.includes("menu") ||
      tNorm.includes("menú") ||
      tNorm.includes("ver tours")
    ) {
      await sendWhatsAppText(from, categoriesEmojiText());
      await sendCategoriesList(from);
      return res.sendStatus(200);
    }

    // category selected
    const categoryKey = detectCategoryKeyFromUser(userText);
    if (categoryKey) {
      session.pendingCategory = categoryKey;
      updateLead(session, { tour_key: session.pendingTour || "" });
      await sendToursListByCategory(from, categoryKey);
      return res.sendStatus(200);
    }

    // tour selected or typed
    const tourKey = detectTourKeyFromUser(userText) || session.pendingTour;
    const directDetectedTour = detectTourKeyFromUser(userText);

    if (directDetectedTour) {
      session.pendingTour = directDetectedTour;
      const tour = getTourByKey(directDetectedTour);
      updateLead(session, { tour_key: directDetectedTour });

      if (wantsQuote(tNorm) || wantsIncludes(tNorm) || wantsSchedule(tNorm) || wantsPayments(tNorm) || wantsPolicies(tNorm)) {
        await sendWhatsAppText(from, buildTourFaqReply(tour, tNorm));
        return res.sendStatus(200);
      }

      const range = parseDateRangeFromText(userText);
      if (!range) {
        await sendWhatsAppText(
          from,
          `${buildTourInfoText(tour)}\n\nSi deseas reservar, dime la *fecha* o el *día* que te interesa.\nEj: "mañana", "viernes" o "14 de junio".`
        );
        session.state = "await_day";
        return res.sendStatus(200);
      }

      const slots = await getAvailableSlotsTool({ tour_key: directDetectedTour, from: range.from, to: range.to });
      if (!slots.length) {
        await sendWhatsAppText(from, `No veo salidas disponibles para ese rango 🙏\nDime otro día o mes y te comparto más opciones.`);
        session.state = "await_day";
        return res.sendStatus(200);
      }

      session.pendingRange = range;
      session.lastSlots = slots;
      session.state = "await_slot_choice";
      const listText = formatSlotsList(directDetectedTour, slots, session);
      await sendWhatsAppText(from, listText);
      return res.sendStatus(200);
    }

    // generic FAQ for pending tour
    if (session.pendingTour && (wantsQuote(tNorm) || wantsIncludes(tNorm) || wantsSchedule(tNorm) || wantsPayments(tNorm) || wantsPolicies(tNorm))) {
      const tour = getTourByKey(session.pendingTour);
      await sendWhatsAppText(from, buildTourFaqReply(tour, tNorm));
      return res.sendStatus(200);
    }

    // explicit reserve but no tour
    if (!tourKey && (tNorm.includes("reservar") || tNorm.includes("reserva") || tNorm.includes("cotizacion") || tNorm.includes("cotización"))) {
      await sendWhatsAppText(from, `Claro ✅ Primero elige una categoría o dime el tour que te interesa.`);
      await sendWhatsAppText(from, categoriesEmojiText());
      await sendCategoriesList(from);
      return res.sendStatus(200);
    }

    // pending tour + date
    if (session.pendingTour) {
      const range = parseDateRangeFromText(userText);
      if (range) {
        const slots = await getAvailableSlotsTool({ tour_key: session.pendingTour, from: range.from, to: range.to });
        if (!slots.length) {
          await sendWhatsAppText(from, `No veo salidas disponibles para ese rango 🙏\nDime otro día o un mes y te comparto más opciones.`);
          session.state = "await_day";
          return res.sendStatus(200);
        }

        session.pendingRange = range;
        session.lastSlots = slots;
        session.state = "await_slot_choice";
        const listText = formatSlotsList(session.pendingTour, slots, session);
        await sendWhatsAppText(from, listText);
        return res.sendStatus(200);
      }

      if (session.state === "await_day") {
        await sendWhatsAppText(from, `Para elegir la fecha, puedes escribir: "mañana", "viernes", "próximo martes", "14 de junio" o "en julio".`);
        return res.sendStatus(200);
      }
    }

    // Fallback OpenAI
    const reply = await callOpenAI({
      session,
      userText,
      userPhone: from,
      extraSystem: session.pendingTour ? `Nota: el tour actual pendiente es ${session.pendingTour}.` : "",
    });

    if (normalizeText(reply).includes("categor") || normalizeText(reply).includes("tour")) {
      await sendWhatsAppText(from, reply);
      return res.sendStatus(200);
    }

    await sendWhatsAppText(from, reply);
    return res.sendStatus(200);
  } catch (e) {
    console.error("Webhook error:", e?.response?.data || e?.message || e);
    return res.sendStatus(200);
  } finally {
    try {
      if (from && session) await saveSession(from, session);
    } catch (e) {
      console.error("saveSession error:", e?.message || e);
    }
  }
});

app.get("/", (_req, res) => res.send("OK"));
app.get("/health", (_req, res) => res.status(200).send("ok"));

// =========================
// Follow-up + reminders
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
      if (!lead.tour_key || lead.followupSent || lead.converted) continue;
      if (!lead.lastInteractionAt) continue;
      if (s?.state === "post_booking" || s?.lastBooking) continue;

      const ageMs = now - new Date(lead.lastInteractionAt).getTime();
      if (!Number.isFinite(ageMs) || ageMs < minAgeMs || ageMs > maxAgeMs) continue;

      const tour = getTourByKey(lead.tour_key);
      if (!tour) continue;

      const msg =
        `Hola 👋 Quedó pendiente tu reserva para *${tour.title}*.\n\n` +
        `Si deseas, te comparto disponibilidad y cotización para la fecha que prefieras. Solo responde con el día o fecha que te interesa 😊`;

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

async function reminderLoop() {
  try {
    const calendar = getCalendarClient();
    const now = new Date();
    const in26h = addMinutes(now, 26 * 60);
    const events = await listReservationEvents(calendar, now.toISOString(), in26h.toISOString());

    for (const ev of events) {
      const priv = ev.extendedProperties?.private || {};
      if (priv.status === "cancelled") continue;

      const phone = priv.wa_phone;
      const startISO = ev.start?.dateTime;
      if (!phone || !startISO) continue;

      const start = new Date(startISO);
      const minutesToStart = Math.round((start.getTime() - now.getTime()) / 60000);
      const tour = getTourByKey(String(priv.tour_key || "").trim());
      const pickup = String(priv.pickup || "").trim() || tour?.meetingPoint || BUSINESS_ADDRESS || "Punto coordinado";

      const in24hWindow = minutesToStart <= 25 * 60 && minutesToStart >= 23 * 60;
      const in2hWindow = minutesToStart <= 135 && minutesToStart >= 90;

      if (REMINDER_24H && in24hWindow && priv.reminder24hSent !== "true") {
        const msg =
          `Recordatorio 🌴: mañana tienes reserva para *${tour?.title || "tu tour"}* a las ${formatTimeInTZ(startISO, BUSINESS_TIMEZONE)}.\n` +
          `🚐 Salida / pickup: ${pickup}\n\n` +
          `Responde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`;

        const sendRes = await sendReminderWhatsAppToBestTarget(priv, phone, msg);
        if (sendRes.ok) {
          await calendar.events.patch({
            calendarId: GOOGLE_CALENDAR_ID,
            eventId: ev.id,
            requestBody: { extendedProperties: { private: { ...priv, reminder24hSent: "true" } } },
          });
        }
      }

      if (REMINDER_2H && in2hWindow && priv.reminder2hSent !== "true") {
        const msg =
          `Recordatorio 🌴: tu salida para *${tour?.title || "tu tour"}* es hoy a las ${formatTimeInTZ(startISO, BUSINESS_TIMEZONE)}.\n` +
          `🚐 Punto de salida: ${pickup}\n\n` +
          `Responde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`;

        const sendRes = await sendReminderWhatsAppToBestTarget(priv, phone, msg);
        if (sendRes.ok) {
          await calendar.events.patch({
            calendarId: GOOGLE_CALENDAR_ID,
            eventId: ev.id,
            requestBody: { extendedProperties: { private: { ...priv, reminder2hSent: "true" } } },
          });
        }
      }
    }
  } catch (e) {
    console.error("Reminder loop error:", e?.response?.data || e?.message || e);
  }
}

app.get("/tick", async (_req, res) => {
  try {
    await reminderLoop();
    await followupLeadsLoop();
  } catch {}
  return res.status(200).send("tick ok");
});

app.listen(PORT, () => console.log(`Bot running on :${PORT}`));
