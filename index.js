import express from "express";
import axios from "axios";
import crypto from "crypto";
import fs from "fs/promises";
import os from "os";
import path from "path";
import { spawn } from "child_process";
import { google } from "googleapis";
import Redis from "ioredis";
// =========================
// CATÁLOGOS IMPORTADOS
// =========================
import {
  SERVICE_LINES,
  TOUR_ORIGINS,
  PACKAGE_DESTINATIONS,
} from "./catalogo-servicios.js";
import {
  REAL_TOUR_GROUPS,
  buildRealToursCatalog,
  REAL_TOUR_TEXT_OVERRIDES,
} from "./catalogo-real.js";
import {
  TOUR_CATEGORIES,
  defaultTourCatalog,
} from "./catalogo-legacy.js";

const SERVICE_LINE_ID_TO_KEY = Object.fromEntries(SERVICE_LINES.map((s) => [s.id, s.key]));
const TOUR_ORIGIN_ID_TO_KEY = Object.fromEntries(TOUR_ORIGINS.map((o) => [o.id, o.key]));
const PACKAGE_DESTINATION_ID_TO_KEY = Object.fromEntries(PACKAGE_DESTINATIONS.map((p) => [p.id, p.key]));
const REAL_TOUR_GROUP_ID_TO_KEY = Object.fromEntries(REAL_TOUR_GROUPS.map((g) => [g.id, g.key]));
const REAL_TOURS = safeJson(process.env.REAL_TOUR_CATALOG_JSON, null) || buildRealToursCatalog();
const REAL_TOUR_ID_TO_KEY = Object.fromEntries(REAL_TOURS.map((t) => [t.id, t.key]));


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
  process.env.BUSINESS_NAME || process.env.AGENCY_NAME || process.env.CLINIC_NAME || "Cavenasam Travel & Tour Group SRL";
const BUSINESS_ADDRESS =
  process.env.BUSINESS_ADDRESS || process.env.CLINIC_ADDRESS || "Punta Cana, República Dominicana";
const BUSINESS_TIMEZONE =
  process.env.BUSINESS_TIMEZONE || process.env.CLINIC_TIMEZONE || "America/Santo_Domingo";

const MARKET_CONTACT_TEXT =
  (process.env.MARKET_CONTACT_TEXT ||
    "📍 Base operativa: Punta Cana, República Dominicana.\n📲 Escríbenos por este WhatsApp y un asesor te ayuda con tu reserva.")
    .trim();

const SLOT_STEP_MIN = parseInt(process.env.SLOT_STEP_MIN || "15", 10);
const MIN_BOOKING_LEAD_MIN = parseInt(process.env.MIN_BOOKING_LEAD_MIN || "120", 10);
const MAX_SLOTS_RETURN = parseInt(process.env.MAX_SLOTS_RETURN || "80", 10);
const DISPLAY_SLOTS_LIMIT = parseInt(process.env.DISPLAY_SLOTS_LIMIT || "12", 10);

const REMINDER_24H = (process.env.REMINDER_24H || "1") === "1";
const REMINDER_2H = (process.env.REMINDER_2H || "1") === "1";
const FOLLOWUP_ENABLED = (process.env.FOLLOWUP_ENABLED || "1") === "1";
const FOLLOWUP_AFTER_MIN = parseInt(process.env.FOLLOWUP_AFTER_MIN || "180", 10);
const FOLLOWUP_MAX_AGE_HOURS = parseInt(process.env.FOLLOWUP_MAX_AGE_HOURS || "72", 10);
const MENU_INACTIVITY_REMINDER_ENABLED = (process.env.MENU_INACTIVITY_REMINDER_ENABLED || "1") === "1";
const MENU_REMINDER_1_AFTER_MIN = parseInt(process.env.MENU_REMINDER_1_AFTER_MIN || "30", 10);
const MENU_REMINDER_2_AFTER_MIN = parseInt(process.env.MENU_REMINDER_2_AFTER_MIN || "1440", 10);
const MENU_REMINDER_MAX_AGE_HOURS = parseInt(process.env.MENU_REMINDER_MAX_AGE_HOURS || "48", 10);


const PERSONAL_WA_TO = (process.env.PERSONAL_WA_TO || "").trim();
const ADMIN_WA_TO = (process.env.ADMIN_WA_TO || process.env.ADMIN_PHONE || process.env.PERSONAL_WA_TO || "").trim();
const PRICE_CURRENCY = (process.env.PRICE_CURRENCY || "US$").trim();

const CATALOG_DOCUMENT_URL = (process.env.CATALOG_DOCUMENT_URL || "").trim();
const CATALOG_DOCUMENT_FILENAME = (process.env.CATALOG_DOCUMENT_FILENAME || "catalogo-servicios.pdf").trim();
const CATALOG_DOCUMENT_CAPTION =
  (process.env.CATALOG_DOCUMENT_CAPTION || "Aquí tienes el catálogo informativo 📄").trim();

// =========================
// BOTHUB
// =========================
const BOTHUB_WEBHOOK_URL = (process.env.BOTHUB_WEBHOOK_URL || "").trim();
const BOTHUB_WEBHOOK_SECRET = (process.env.BOTHUB_WEBHOOK_SECRET || "").trim();
const BOTHUB_TIMEOUT_MS = Number(process.env.BOTHUB_TIMEOUT_MS || 6000);

const BOTHUB_API_BASE_URL = (process.env.BOTHUB_API_BASE_URL || process.env.CRM_API_BASE_URL || "").trim();
const BOTHUB_JWT_TOKEN = (process.env.BOTHUB_JWT_TOKEN || process.env.CRM_JWT_TOKEN || "").trim();
const BOTHUB_BOT_ID = (process.env.BOTHUB_BOT_ID || "").trim();
const HUB_QUEUE_SERVICE_TOURS = (process.env.HUB_QUEUE_SERVICE_TOURS || "Servicio al cliente / Tours").trim();
const HUB_QUEUE_COMMERCIAL = (process.env.HUB_QUEUE_COMMERCIAL || "Asesoría comercial").trim();

const BOT_PUBLIC_BASE_URL = (process.env.BOT_PUBLIC_BASE_URL || "").replace(/\/$/, "");
const HUB_MEDIA_SECRET =
  (process.env.HUB_MEDIA_SECRET || BOTHUB_WEBHOOK_SECRET || VERIFY_TOKEN || "").trim();
const HUB_MEDIA_TTL_SEC = parseInt(process.env.HUB_MEDIA_TTL_SEC || "900", 10);
const META_GRAPH_VERSION =
  process.env.WHATSAPP_GRAPH_VERSION || process.env.META_GRAPH_VERSION || "v23.0";
const MEDIA_DOWNLOAD_TIMEOUT_MS = Number(process.env.MEDIA_DOWNLOAD_TIMEOUT_MS || 90000);
const MEDIA_UPLOAD_TIMEOUT_MS = Number(process.env.MEDIA_UPLOAD_TIMEOUT_MS || 120000);
const FFMPEG_BIN = (process.env.FFMPEG_BIN || "ffmpeg").trim();
const HUMAN_MODE_NOTIFY_USER = (process.env.HUMAN_MODE_NOTIFY_USER || "0") === "1";

// =========================
// HELPERS CONFIG
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

function truncateText(text, max = 800) {
  const raw = String(text || "").trim();
  if (!raw) return "";
  if (raw.length <= max) return raw;
  return `${raw.slice(0, Math.max(0, max - 1)).trim()}…`;
}

function isHumanModeDisableCommand(tNorm) {
  return [
    "modo bot",
    "activar bot",
    "quitar modo humano",
    "salir modo humano",
    "bot activo",
    "volver al bot",
    "bot"
  ].includes(String(tNorm || "").trim());
}

function isHumanModeEnableCommand(tNorm) {
  return [
    "modo humano",
    "activar humano",
    "hablar humano",
    "desactivar bot"
  ].includes(String(tNorm || "").trim());
}



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

function defaultMenuReminder() {
  return {
    active: false,
    menuShownAt: "",
    reminder1Sent: false,
    reminder2Sent: false,
  };
}

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

    pendingServiceLine: null,
    pendingOrigin: null,
    pendingCategory: null,
    pendingTour: null,
    pendingRange: null,

    pendingRealTourGroup: null,
    pendingRealTourKey: null,
    pendingDesiredDate: null,
    lastRealTours: [],

    pendingAdults: null,
    pendingChildren: null,
    pendingChildrenAges: null,
    pendingPickup: null,
    pendingCity: null,
    pendingName: null,
    pendingEmail: null,

    pendingDestination: null,
    pendingDepartureCity: null,
    pendingTravelDateText: null,
    pendingTravelEndDateText: null,
    pendingPassengers: null,
    pendingNotes: null,
    pendingTripDays: null,
    pendingTravelerAgesText: null,
    pendingHotelStars: null,
    pendingNights: null,
    pendingTransferRoute: null,
    pendingAdvisorTopic: null,

    lastBooking: null,
    greeted: false,
    lastMsgId: null,
    lastUserMessageAt: "",
    menuReminder: defaultMenuReminder(),
    conversationMode: "bot",

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

  if (!Array.isArray(session.lastRealTours)) session.lastRealTours = [];
  session.lastRealTours = session.lastRealTours.slice(0, 50);

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
if (typeof session.lastUserMessageAt !== "string") session.lastUserMessageAt = "";
if (session.conversationMode !== "human" && session.conversationMode !== "bot") session.conversationMode = "bot";

if (!session.menuReminder || typeof session.menuReminder !== "object") {
  session.menuReminder = defaultMenuReminder();
} else {
  if (typeof session.menuReminder.active !== "boolean") session.menuReminder.active = false;
  if (typeof session.menuReminder.menuShownAt !== "string") session.menuReminder.menuShownAt = "";
  if (typeof session.menuReminder.reminder1Sent !== "boolean") session.menuReminder.reminder1Sent = false;
  if (typeof session.menuReminder.reminder2Sent !== "boolean") session.menuReminder.reminder2Sent = false;
}

  const maybeStringOrNull = [
    "pendingServiceLine",
    "pendingOrigin",
    "pendingCategory",
    "pendingTour",
    "pendingPickup",
    "pendingCity",
    "pendingName",
    "pendingEmail",
    "pendingDestination",
    "pendingDepartureCity",
    "pendingTravelDateText",
    "pendingTravelEndDateText",
    "pendingNotes",
    "pendingTravelerAgesText",
    "pendingHotelStars",
    "pendingTransferRoute",
    "pendingAdvisorTopic",
    "pendingRealTourGroup",
    "pendingRealTourKey",
    "pendingDesiredDate",
    "pendingChildrenAges",
  ];

  for (const k of maybeStringOrNull) {
    if (typeof session[k] !== "string" && session[k] !== null) session[k] = null;
  }

  const maybeNumberOrNull = [
    "pendingAdults",
    "pendingChildren",
    "pendingPassengers",
    "pendingTripDays",
    "pendingNights",
  ];

  for (const k of maybeNumberOrNull) {
    if (typeof session[k] !== "number" && session[k] !== null) session[k] = null;
  }

  if (typeof session.pendingRange !== "object" && session.pendingRange !== null) session.pendingRange = null;
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

function clearIntakeFlow(session) {
  session.state = "idle";
  session.lastSlots = [];
  session.lastDisplaySlots = [];
  session.selectedSlot = null;

  session.pendingServiceLine = null;
  session.pendingOrigin = null;
  session.pendingCategory = null;
  session.pendingTour = null;
  session.pendingRange = null;

  session.pendingRealTourGroup = null;
  session.pendingRealTourKey = null;
  session.pendingDesiredDate = null;
  session.lastRealTours = [];

  session.pendingAdults = null;
  session.pendingChildren = null;
  session.pendingChildrenAges = null;
  session.pendingPickup = null;
  session.pendingCity = null;
  session.pendingName = null;
  session.pendingEmail = null;

  session.pendingDestination = null;
  session.pendingDepartureCity = null;
  session.pendingTravelDateText = null;
  session.pendingTravelEndDateText = null;
  session.pendingPassengers = null;
  session.pendingNotes = null;
  session.pendingTripDays = null;
  session.pendingTravelerAgesText = null;
  session.pendingHotelStars = null;
  session.pendingNights = null;
  session.pendingTransferRoute = null;
  session.pendingAdvisorTopic = null;
}

function disableMenuInactivityReminder(session) {
  session.menuReminder = defaultMenuReminder();
}

function armMenuInactivityReminder(session) {
  session.menuReminder = {
    active: true,
    menuShownAt: new Date().toISOString(),
    reminder1Sent: false,
    reminder2Sent: false,
  };
}

function markUserActivity(session) {
  session.lastUserMessageAt = new Date().toISOString();
}

function hasUserRespondedAfterMenu(session) {
  const shownAt = Date.parse(session?.menuReminder?.menuShownAt || "");
  const lastUserAt = Date.parse(session?.lastUserMessageAt || "");
  if (!Number.isFinite(shownAt) || !Number.isFinite(lastUserAt)) return false;
  return lastUserAt > shownAt;
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
  if (!BOTHUB_WEBHOOK_URL || !BOTHUB_WEBHOOK_SECRET) return null;

  try {
    const cleanPayload = removeUndefinedDeep(payload);
    const raw = stableStringify(cleanPayload);
    const sig = crypto.createHmac("sha256", BOTHUB_WEBHOOK_SECRET).update(raw).digest("hex");

    const res = await axios.post(BOTHUB_WEBHOOK_URL, raw, {
      headers: {
        "Content-Type": "application/json",
        "X-HUB-SIGNATURE": sig,
      },
      timeout: BOTHUB_TIMEOUT_MS,
      transformRequest: [(data) => data],
    });
    return res?.data || null;
  } catch (e) {
    console.error("Bothub report failed:", e?.response?.data || e?.message || e);
    return null;
  }
}

function deriveBothubApiBaseUrl() {
  const explicit = String(BOTHUB_API_BASE_URL || "").trim();
  if (explicit) return explicit.replace(/\/$/, "");
  const raw = String(BOTHUB_WEBHOOK_URL || "").trim();
  if (!raw) return "";
  try {
    const u = new URL(raw);
    const p = u.pathname.replace(/\/$/, "");
    const candidates = [
      /\/api\/webhooks\/[^/]+$/,
      /\/api\/webhooks\/webhook\/[^/]+$/,
      /\/webhooks\/[^/]+$/,
      /\/webhooks\/webhook\/[^/]+$/,
    ];
    let next = p;
    for (const rgx of candidates) {
      if (rgx.test(next)) {
        next = next.replace(rgx, "");
        break;
      }
    }
    u.pathname = next || "";
    u.search = "";
    u.hash = "";
    return u.toString().replace(/\/$/, "");
  } catch {
    return "";
  }
}

function deriveBothubBotId() {
  const explicit = String(BOTHUB_BOT_ID || "").trim();
  if (explicit) return explicit;
  const raw = String(BOTHUB_WEBHOOK_URL || "").trim();
  if (!raw) return "";
  const m = raw.match(/\/(?:api\/)?webhooks(?:\/webhook)?\/([^/?#]+)\/?(?:\?.*)?$/i);
  return m?.[1] ? String(m[1]).trim() : "";
}

function buildBothubAuthHeaders() {
  const token = String(BOTHUB_JWT_TOKEN || "").trim();
  if (!token) return {};
  return { Authorization: `Bearer ${token}` };
}

function phoneMatchesConversation(convo, phone) {
  const targetDigits = normalizePhoneDigits(phone);
  if (!targetDigits) return false;
  const candidates = [
    convo?.contact?.phone,
    convo?.phone,
    convo?.waFrom,
    convo?.waTo,
  ]
    .map((v) => normalizePhoneDigits(v))
    .filter(Boolean);
  return candidates.includes(targetDigits);
}

async function findBothubConversationIdByPhone(phone) {
  const baseUrl = deriveBothubApiBaseUrl();
  const botId = deriveBothubBotId();
  if (!baseUrl || !botId) return "";
  try {
    const res = await axios.get(`${baseUrl}/api/conversations`, {
      params: { botId },
      headers: buildBothubAuthHeaders(),
      timeout: Math.max(BOTHUB_TIMEOUT_MS, 8000),
    });
    const items = Array.isArray(res?.data) ? res.data : [];
    const found = items.find((c) => phoneMatchesConversation(c, phone));
    return String(found?.id || "").trim();
  } catch (e) {
    console.error("findBothubConversationIdByPhone failed:", e?.response?.data || e?.message || e);
    return "";
  }
}

async function routeConversationToQueue({ session, phone, queueName, reason = "service_selection" }) {
  const nextQueue = String(queueName || "").trim();
  if (!nextQueue) return { ok: false, reason: "missing_queue" };

  if (!String(BOTHUB_JWT_TOKEN || "").trim()) {
    return { ok: false, reason: "missing_bothub_jwt" };
  }

  if (session?.lastQueueRouted === nextQueue) {
    return { ok: true, skipped: true, queue: nextQueue };
  }

  const baseUrl = deriveBothubApiBaseUrl();
  if (!baseUrl) return { ok: false, reason: "missing_api_base" };

  const conversationId = session?.hubConversationId || await findBothubConversationIdByPhone(phone);
  if (!conversationId) return { ok: false, reason: "conversation_not_found" };

  try {
    await axios.patch(
      `${baseUrl}/api/conversations/${encodeURIComponent(conversationId)}`,
      {
        queue: nextQueue,
        assigneeId: null,
        transferReason: reason,
      },
      {
        headers: {
          "Content-Type": "application/json",
          ...buildBothubAuthHeaders(),
        },
        timeout: Math.max(BOTHUB_TIMEOUT_MS, 8000),
      }
    );
    if (session) {
      session.hubConversationId = conversationId;
      session.lastQueueRouted = nextQueue;
    }
    return { ok: true, conversationId, queue: nextQueue };
  } catch (e) {
    console.error("routeConversationToQueue failed:", e?.response?.data || e?.message || e);
    return { ok: false, reason: "patch_failed" };
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
  if (m.includes("audio/aac")) return ".aac";
  if (m.includes("audio/mp4")) return ".m4a";
  if (m.includes("audio/amr")) return ".amr";
  if (m.includes("image/jpeg")) return ".jpg";
  if (m.includes("image/png")) return ".png";
  if (m.includes("image/gif")) return ".gif";
  if (m.includes("image/webp")) return ".webp";
  if (m.includes("video/mp4")) return ".mp4";
  if (m.includes("video/webm")) return ".webm";
  if (m.includes("video/quicktime")) return ".mov";
  if (m.includes("video/3gpp")) return ".3gp";
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

function buildHubMediaUrlFromBase(base, mediaId) {
  if (!mediaId || !HUB_MEDIA_SECRET) return "";
  const normalizedBase = String(base || "").trim();
  if (!normalizedBase) return "";

  const ts = String(Date.now());
  const sig = signHubMediaToken(mediaId, ts);

  return `${normalizedBase.replace(/\/$/, "")}/hub_media/${encodeURIComponent(mediaId)}?ts=${encodeURIComponent(ts)}&sig=${encodeURIComponent(sig)}`;
}

function buildHubMediaUrl(req, mediaId) {
  return buildHubMediaUrlFromBase(getBotPublicBaseUrl(req), mediaId);
}

function buildBotHubMediaUrl(mediaId) {
  return buildHubMediaUrlFromBase(BOT_PUBLIC_BASE_URL, mediaId);
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

function filenameFromDisposition(disposition, fallback = "file") {
  const raw = String(disposition || "");
  const starMatch = raw.match(/filename\*=UTF-8''([^;]+)/i);
  if (starMatch?.[1]) {
    try {
      return sanitizeFileName(decodeURIComponent(starMatch[1]), fallback);
    } catch {
      return sanitizeFileName(starMatch[1], fallback);
    }
  }

  const match = raw.match(/filename="?([^";]+)"?/i);
  if (match?.[1]) return sanitizeFileName(match[1], fallback);
  return sanitizeFileName(fallback, fallback);
}

function filenameFromUrl(url, fallback = "file") {
  try {
    const parsed = new URL(String(url || ""));
    const last = parsed.pathname.split("/").filter(Boolean).pop() || "";
    if (!last) return sanitizeFileName(fallback, fallback);
    return sanitizeFileName(decodeURIComponent(last), fallback);
  } catch {
    const clean = String(url || "").split("?")[0].split("#")[0].split("/").filter(Boolean).pop() || fallback;
    return sanitizeFileName(clean, fallback);
  }
}

function mimeTypeFromFilename(name) {
  const lower = String(name || "").toLowerCase().split("?")[0].trim();
  if (lower.endsWith(".jpg") || lower.endsWith(".jpeg")) return "image/jpeg";
  if (lower.endsWith(".png")) return "image/png";
  if (lower.endsWith(".gif")) return "image/gif";
  if (lower.endsWith(".webp")) return "image/webp";
  if (lower.endsWith(".pdf")) return "application/pdf";
  if (lower.endsWith(".doc")) return "application/msword";
  if (lower.endsWith(".docx")) return "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
  if (lower.endsWith(".xls")) return "application/vnd.ms-excel";
  if (lower.endsWith(".xlsx")) return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  if (lower.endsWith(".ppt")) return "application/vnd.ms-powerpoint";
  if (lower.endsWith(".pptx")) return "application/vnd.openxmlformats-officedocument.presentationml.presentation";
  if (lower.endsWith(".mp3")) return "audio/mpeg";
  if (lower.endsWith(".ogg") || lower.endsWith(".opus")) return "audio/ogg";
  if (lower.endsWith(".wav")) return "audio/wav";
  if (lower.endsWith(".aac")) return "audio/aac";
  if (lower.endsWith(".m4a")) return "audio/mp4";
  if (lower.endsWith(".amr")) return "audio/amr";
  if (lower.endsWith(".webm")) return "video/webm";
  if (lower.endsWith(".mp4")) return "video/mp4";
  if (lower.endsWith(".mov")) return "video/quicktime";
  if (lower.endsWith(".3gp")) return "video/3gpp";
  return "";
}

function ensureFilenameExtension(name, mimeType) {
  const clean = sanitizeFileName(name || "file", "file");
  if (/\.[a-z0-9]{2,5}$/i.test(clean)) return clean;
  const ext = extFromMimeType(mimeType);
  return ext ? `${clean}${ext}` : clean;
}

function normalizeOutboundKind(kind, mimeType = "") {
  const raw = String(kind || "").trim().toLowerCase();
  if (["image", "photo", "picture", "img"].includes(raw)) return "IMAGE";
  if (["video", "movie", "clip"].includes(raw)) return "VIDEO";
  if (["audio", "voice", "voice_note", "voicenote", "ptt", "note", "nota_de_voz", "nota-voz"].includes(raw)) return "AUDIO";
  if (["document", "doc", "file", "archivo", "attachment", "adjunto", "pdf"].includes(raw)) return "DOCUMENT";
  if (["location", "ubicacion", "ubication", "geo", "map", "maps"].includes(raw)) return "LOCATION";
  if (["text", "texto", "message", "mensaje"].includes(raw)) return "TEXT";

  const lowerMime = String(mimeType || "").toLowerCase();
  if (lowerMime.startsWith("image/")) return "IMAGE";
  if (lowerMime.startsWith("video/")) return "VIDEO";
  if (lowerMime.startsWith("audio/")) return "AUDIO";
  if (lowerMime) return "DOCUMENT";
  return raw ? raw.toUpperCase() : "";
}

function firstDefined(...values) {
  for (const value of values) {
    if (value === undefined || value === null) continue;
    if (typeof value === "string" && !value.trim()) continue;
    return value;
  }
  return undefined;
}

function toNumberOrUndefined(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
}

function parseBoolLike(value) {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return false;
  return ["1", "true", "yes", "si", "sí", "y"].includes(raw);
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
    filename: info?.filename || `media-${mediaId}${extFromMimeType(mimeType)}`,
    info,
  };
}

async function downloadRemoteAsset(url, preferredFilename = "", preferredMimeType = "") {
  const res = await axios.get(String(url), {
    responseType: "arraybuffer",
    timeout: MEDIA_DOWNLOAD_TIMEOUT_MS,
    maxRedirects: 5,
    validateStatus: () => true,
    headers: {
      "User-Agent": "TekkoMediaFetcher/1.0",
    },
  });

  if (res.status < 200 || res.status >= 300) {
    throw new Error(typeof res.data === "string" ? res.data : `Media download failed (${res.status})`);
  }

  const disposition = res.headers?.["content-disposition"] || "";
  const headerMime = String(res.headers?.["content-type"] || "").split(";")[0].trim();
  const rawFilename =
    preferredFilename ||
    filenameFromDisposition(disposition, "") ||
    filenameFromUrl(url, "file");
  const mimeType = preferredMimeType || headerMime || mimeTypeFromFilename(rawFilename) || "application/octet-stream";
  const filename = ensureFilenameExtension(rawFilename, mimeType);

  return {
    buffer: Buffer.from(res.data),
    mimeType,
    filename,
    url: String(url),
  };
}

async function runFfmpeg(args) {
  return await new Promise((resolve, reject) => {
    const child = spawn(FFMPEG_BIN, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk || "");
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) return resolve(stderr);
      reject(new Error(stderr || `ffmpeg exited with code ${code}`));
    });
  });
}

async function convertMediaBuffer({ buffer, inputExt = "", outputExt, ffmpegArgs = [] }) {
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "tekko-wa-media-"));
  try {
    const inputPath = path.join(tempDir, `input${inputExt || ""}`);
    const outputPath = path.join(tempDir, `output${outputExt}`);
    await fs.writeFile(inputPath, buffer);
    await runFfmpeg(["-y", "-i", inputPath, ...ffmpegArgs, outputPath]);
    return await fs.readFile(outputPath);
  } finally {
    await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  }
}

async function prepareRemoteMediaForWhatsApp({ kind, url, mimeType = "", filename = "", voice = false }) {
  const downloaded = await downloadRemoteAsset(url, filename, mimeType);
  const normalizedKind = normalizeOutboundKind(kind, mimeType || downloaded.mimeType);

  let finalBuffer = downloaded.buffer;
  let finalMimeType = mimeType || downloaded.mimeType || mimeTypeFromFilename(downloaded.filename) || "application/octet-stream";
  let finalFilename = filename || downloaded.filename || `media${extFromMimeType(finalMimeType)}`;

  const lowerMime = String(finalMimeType || "").toLowerCase();
  const lowerFile = String(finalFilename || "").toLowerCase();

  try {
    if (normalizedKind === "AUDIO") {
      const supportedAudio = ["audio/ogg", "audio/mpeg", "audio/mp3", "audio/aac", "audio/mp4", "audio/amr", "audio/wav"];
      const needsAudioConversion =
        lowerMime.includes("webm") ||
        lowerFile.endsWith(".webm") ||
        lowerFile.endsWith(".oga") ||
        !supportedAudio.includes(lowerMime);

      if (voice || needsAudioConversion) {
        const outputExt = voice ? ".ogg" : ".mp3";
        finalBuffer = await convertMediaBuffer({
          buffer: finalBuffer,
          inputExt: extFromMimeType(finalMimeType) || path.extname(finalFilename) || ".bin",
          outputExt,
          ffmpegArgs: voice
            ? ["-vn", "-ac", "1", "-c:a", "libopus", "-b:a", "64k"]
            : ["-vn", "-c:a", "libmp3lame", "-b:a", "128k"],
        });
        finalMimeType = voice ? "audio/ogg" : "audio/mpeg";
        finalFilename = ensureFilenameExtension(path.parse(finalFilename).name || "audio", finalMimeType);
      }
    } else if (normalizedKind === "VIDEO") {
      const needsVideoConversion = lowerMime.includes("webm") || lowerFile.endsWith(".webm") || !["video/mp4", "video/3gpp"].includes(lowerMime);
      if (needsVideoConversion) {
        finalBuffer = await convertMediaBuffer({
          buffer: finalBuffer,
          inputExt: extFromMimeType(finalMimeType) || path.extname(finalFilename) || ".bin",
          outputExt: ".mp4",
          ffmpegArgs: [
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-c:a", "aac",
            "-b:a", "128k",
          ],
        });
        finalMimeType = "video/mp4";
        finalFilename = ensureFilenameExtension(path.parse(finalFilename).name || "video", finalMimeType);
      }
    } else if (normalizedKind === "IMAGE") {
      if (lowerMime.includes("webp") || lowerFile.endsWith(".webp")) {
        finalBuffer = await convertMediaBuffer({
          buffer: finalBuffer,
          inputExt: extFromMimeType(finalMimeType) || path.extname(finalFilename) || ".webp",
          outputExt: ".jpg",
          ffmpegArgs: ["-frames:v", "1"],
        });
        finalMimeType = "image/jpeg";
        finalFilename = ensureFilenameExtension(path.parse(finalFilename).name || "image", finalMimeType);
      }
    }
  } catch (conversionError) {
    console.error("Media conversion fallback:", conversionError?.message || conversionError);
  }

  return {
    kind: normalizedKind,
    originalUrl: String(url || ""),
    originalMimeType: downloaded.mimeType,
    originalFilename: downloaded.filename,
    buffer: finalBuffer,
    mimeType: finalMimeType,
    filename: ensureFilenameExtension(finalFilename, finalMimeType),
    voice: !!voice,
  };
}

async function uploadMediaBufferToMeta({ buffer, mimeType, filename }) {
  if (!WA_TOKEN) throw new Error("WA_TOKEN not configured");
  if (!PHONE_NUMBER_ID) throw new Error("PHONE_NUMBER_ID not configured");

  const form = new FormData();
  form.append("messaging_product", "whatsapp");
  form.append("type", mimeType || "application/octet-stream");
  form.append(
    "file",
    new Blob([buffer], { type: mimeType || "application/octet-stream" }),
    ensureFilenameExtension(filename || "file", mimeType || "application/octet-stream")
  );

  const response = await fetch(`https://graph.facebook.com/${META_GRAPH_VERSION}/${PHONE_NUMBER_ID}/media`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${WA_TOKEN}`,
    },
    body: form,
    signal: AbortSignal.timeout(MEDIA_UPLOAD_TIMEOUT_MS),
  });

  const raw = await response.text();
  const data = safeJson(raw, null) || {};
  if (!response.ok || !data?.id) {
    throw new Error(data?.error?.message || data?.error?.error_user_msg || `Meta media upload failed (${response.status})`);
  }

  return data;
}

async function postWhatsAppMessage(payload) {
  const url = `https://graph.facebook.com/${META_GRAPH_VERSION}/${PHONE_NUMBER_ID}/messages`;
  const res = await axios.post(url, payload, {
    headers: { Authorization: `Bearer ${WA_TOKEN}` },
    timeout: 60000,
    validateStatus: () => true,
  });

  if (res.status < 200 || res.status >= 300) {
    const detail = res?.data?.error?.message || res?.data?.error?.error_user_msg || `WhatsApp send failed (${res.status})`;
    const err = new Error(detail);
    err.responseData = res.data;
    throw err;
  }

  return res.data || {};
}

function buildMediaFallbackText(kind, mediaUrl, caption = "", filename = "") {
  const lead = caption || filename || (kind === "IMAGE" ? "Te comparto una imagen" : kind === "VIDEO" ? "Te comparto un video" : kind === "AUDIO" ? "Te comparto un audio" : "Te comparto un archivo");
  if (!mediaUrl) return lead;
  return `${lead}
${mediaUrl}`;
}

function buildMediaMessagePayload({ to, kind, mediaId = "", mediaUrl = "", caption = "", filename = "", latitude, longitude, name, address }) {
  const payload = {
    messaging_product: "whatsapp",
    to: String(to),
    type: String(kind || "").toLowerCase(),
  };

  if (kind === "IMAGE") {
    payload.image = mediaId
      ? { id: mediaId, caption: caption || undefined }
      : { link: mediaUrl, caption: caption || undefined };
    return payload;
  }

  if (kind === "VIDEO") {
    payload.video = mediaId
      ? { id: mediaId, caption: caption || undefined }
      : { link: mediaUrl, caption: caption || undefined };
    return payload;
  }

  if (kind === "DOCUMENT") {
    payload.document = mediaId
      ? { id: mediaId, filename: filename || undefined, caption: caption || undefined }
      : { link: mediaUrl, filename: filename || undefined, caption: caption || undefined };
    return payload;
  }

  if (kind === "AUDIO") {
    payload.audio = mediaId ? { id: mediaId } : { link: mediaUrl };
    return payload;
  }

  if (kind === "LOCATION") {
    payload.location = {
      latitude: Number(latitude),
      longitude: Number(longitude),
      name: name || undefined,
      address: address || undefined,
    };
    return payload;
  }

  throw new Error(`Unsupported media kind: ${kind}`);
}

async function reportOutboundMedia({ to, kind, reportSource = "BOT", body = "", mediaId = "", mediaUrl = "", originalUrl = "", mimeType = "", filename = "", caption = "", voice = false, waMessageId = "", sendMethod = "", latitude, longitude, name, address }) {
  const resolvedHubMediaUrl = mediaId ? buildBotHubMediaUrl(mediaId) : "";
  const resolvedMediaUrl = resolvedHubMediaUrl || mediaUrl || originalUrl || "";

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: String(body || caption || filename || `${kind} enviado`),
    source: reportSource,
    waMessageId: waMessageId || undefined,
    kind,
    mediaUrl: resolvedMediaUrl || undefined,
    meta: {
      kind,
      mediaId: mediaId || undefined,
      mediaUrl: resolvedMediaUrl || undefined,
      hubMediaUrl: resolvedHubMediaUrl || undefined,
      directUrl: originalUrl || mediaUrl || undefined,
      originalUrl: originalUrl || mediaUrl || undefined,
      mimeType: mimeType || undefined,
      filename: filename || undefined,
      caption: caption || undefined,
      voice: voice || undefined,
      sendMethod: sendMethod || undefined,
      latitude: latitude !== undefined ? Number(latitude) : undefined,
      longitude: longitude !== undefined ? Number(longitude) : undefined,
      name: name || undefined,
      address: address || undefined,
    },
  });
}

async function sendWhatsAppMediaMessage({ to, kind, mediaId = "", mediaUrl = "", mimeType = "", filename = "", caption = "", voice = false, latitude, longitude, name = "", address = "", reportSource = "BOT", textFallback = "" }) {
  const normalizedKind = normalizeOutboundKind(kind, mimeType);
  if (!normalizedKind) throw new Error("Invalid media kind");

  if (normalizedKind === "LOCATION") {
    const response = await postWhatsAppMessage(
      buildMediaMessagePayload({ to, kind: normalizedKind, latitude, longitude, name, address })
    );
    await reportOutboundMedia({
      to,
      kind: normalizedKind,
      reportSource,
      body: name || address || "Ubicación enviada",
      waMessageId: response?.messages?.[0]?.id,
      sendMethod: "location",
      latitude,
      longitude,
      name,
      address,
    });
    return { ok: true, sendMethod: "location", messageId: response?.messages?.[0]?.id };
  }

  const resolvedOriginalUrl = String(mediaUrl || "").trim();
  let resolvedMimeType = String(mimeType || "").trim();
  let resolvedFilename = String(filename || "").trim();
  let resolvedMediaId = String(mediaId || "").trim();
  let uploadedMediaId = resolvedMediaId;
  let sendMethod = resolvedMediaId ? "media_id" : "";
  let response = null;
  let lastError = null;

  if (!resolvedMediaId && resolvedOriginalUrl) {
    try {
      const prepared = await prepareRemoteMediaForWhatsApp({
        kind: normalizedKind,
        url: resolvedOriginalUrl,
        mimeType: resolvedMimeType,
        filename: resolvedFilename,
        voice,
      });
      resolvedMimeType = prepared.mimeType || resolvedMimeType;
      resolvedFilename = prepared.filename || resolvedFilename;
      const uploaded = await uploadMediaBufferToMeta(prepared);
      uploadedMediaId = String(uploaded?.id || "").trim();
      resolvedMediaId = uploadedMediaId;
      sendMethod = "media_id";
    } catch (uploadError) {
      lastError = uploadError;
      console.error(`Media upload fallback (${normalizedKind}):`, uploadError?.message || uploadError);
    }
  }

  try {
    if (resolvedMediaId) {
      response = await postWhatsAppMessage(
        buildMediaMessagePayload({
          to,
          kind: normalizedKind,
          mediaId: resolvedMediaId,
          mediaUrl: resolvedOriginalUrl,
          caption,
          filename: resolvedFilename,
        })
      );
      sendMethod = sendMethod || "media_id";
    } else if (resolvedOriginalUrl) {
      response = await postWhatsAppMessage(
        buildMediaMessagePayload({
          to,
          kind: normalizedKind,
          mediaUrl: resolvedOriginalUrl,
          caption,
          filename: resolvedFilename,
        })
      );
      sendMethod = "link";
    } else {
      throw new Error(`No mediaId or mediaUrl available for ${normalizedKind}`);
    }
  } catch (sendError) {
    lastError = sendError;
    if (resolvedMediaId && resolvedOriginalUrl) {
      try {
        response = await postWhatsAppMessage(
          buildMediaMessagePayload({
            to,
            kind: normalizedKind,
            mediaUrl: resolvedOriginalUrl,
            caption,
            filename: resolvedFilename,
          })
        );
        sendMethod = "link_fallback";
      } catch (fallbackError) {
        lastError = fallbackError;
      }
    }

    if (!response && textFallback && resolvedOriginalUrl) {
      await sendWhatsAppText(String(to), textFallback, reportSource);
      return {
        ok: false,
        fallback: "text",
        mediaId: uploadedMediaId || undefined,
        mediaUrl: buildBotHubMediaUrl(uploadedMediaId) || resolvedOriginalUrl || undefined,
        error: lastError?.message || String(lastError || "Media send failed"),
      };
    }

    if (!response) throw lastError;
  }

  await reportOutboundMedia({
    to,
    kind: normalizedKind,
    reportSource,
    body: caption || resolvedFilename || `${normalizedKind} enviado`,
    mediaId: uploadedMediaId || resolvedMediaId || undefined,
    mediaUrl: buildBotHubMediaUrl(uploadedMediaId || resolvedMediaId) || resolvedOriginalUrl || undefined,
    originalUrl: resolvedOriginalUrl || undefined,
    mimeType: resolvedMimeType || undefined,
    filename: resolvedFilename || undefined,
    caption: caption || undefined,
    voice,
    waMessageId: response?.messages?.[0]?.id,
    sendMethod,
  });

  return {
    ok: true,
    sendMethod,
    mediaId: uploadedMediaId || resolvedMediaId || undefined,
    mediaUrl: buildBotHubMediaUrl(uploadedMediaId || resolvedMediaId) || resolvedOriginalUrl || undefined,
    messageId: response?.messages?.[0]?.id,
  };
}

function normalizeAgentAttachment(candidate, fallbackCaption = "") {
  if (!candidate || typeof candidate !== "object") return null;

  const mimeType = String(
    firstDefined(
      candidate?.mimeType,
      candidate?.mimetype,
      candidate?.mime,
      candidate?.contentType,
      candidate?.content_type,
      candidate?.meta?.mimeType,
      candidate?.meta?.mimetype
    ) || ""
  ).trim();

  const kind = normalizeOutboundKind(
    firstDefined(
      candidate?.kind,
      candidate?.type,
      candidate?.messageType,
      candidate?.message_type,
      candidate?.mediaType,
      candidate?.media_type,
      candidate?.attachmentType,
      candidate?.attachment_type,
      candidate?.meta?.kind,
      candidate?.meta?.type
    ),
    mimeType
  );

  const latitude = toNumberOrUndefined(
    firstDefined(candidate?.latitude, candidate?.lat, candidate?.location?.latitude, candidate?.location?.lat)
  );
  const longitude = toNumberOrUndefined(
    firstDefined(candidate?.longitude, candidate?.lng, candidate?.lon, candidate?.location?.longitude, candidate?.location?.lng, candidate?.location?.lon)
  );

  if (kind === "LOCATION" || (latitude !== undefined && longitude !== undefined)) {
    if (latitude === undefined || longitude === undefined) return null;
    return {
      kind: "LOCATION",
      latitude,
      longitude,
      name: String(firstDefined(candidate?.name, candidate?.location?.name) || "").trim(),
      address: String(firstDefined(candidate?.address, candidate?.location?.address) || "").trim(),
    };
  }

  const mediaUrl = String(
    firstDefined(
      candidate?.mediaUrl,
      candidate?.media_url,
      candidate?.url,
      candidate?.link,
      candidate?.href,
      candidate?.src,
      candidate?.downloadUrl,
      candidate?.download_url,
      candidate?.attachmentUrl,
      candidate?.attachment_url,
      candidate?.fileUrl,
      candidate?.file_url,
      candidate?.assetUrl,
      candidate?.asset_url,
      candidate?.meta?.mediaUrl,
      candidate?.meta?.url,
      candidate?.meta?.link
    ) || ""
  ).trim();

  const mediaId = String(
    firstDefined(candidate?.mediaId, candidate?.media_id, candidate?.metaMediaId, candidate?.meta_media_id, candidate?.meta?.mediaId) || ""
  ).trim();

  const filename = String(
    firstDefined(candidate?.filename, candidate?.fileName, candidate?.file_name, candidate?.name, candidate?.title, candidate?.meta?.filename) || ""
  ).trim();

  const caption = String(
    firstDefined(candidate?.caption, candidate?.body, candidate?.text, candidate?.message, candidate?.meta?.caption, fallbackCaption) || ""
  ).trim();

  const voice = parseBoolLike(
    firstDefined(candidate?.voice, candidate?.ptt, candidate?.isVoiceNote, candidate?.is_voice_note, candidate?.audio?.voice, candidate?.meta?.voice)
  );

  if (!["IMAGE", "VIDEO", "AUDIO", "DOCUMENT"].includes(kind)) return null;
  if (!mediaUrl && !mediaId) return null;

  return {
    kind,
    mediaUrl,
    mediaId,
    mimeType,
    filename,
    caption,
    voice,
  };
}

function parseAgentMessagePayload(body) {
  const raw = body && typeof body === "object" ? body : {};
  const waToRaw = String(
    firstDefined(
      raw?.waTo,
      raw?.wa_to,
      raw?.to,
      raw?.phone,
      raw?.waId,
      raw?.wa_id,
      raw?.chatId,
      raw?.chat_id,
      raw?.contact?.wa_id,
      raw?.contact?.phone
    ) || ""
  ).trim();
  const waTo = toE164DigitsRD(waToRaw || "");

  const primaryText = String(firstDefined(raw?.text, raw?.body, raw?.message, raw?.content) || "").trim();
  const attachmentsRaw = [];

  const topLevelHasMedia =
    firstDefined(
      raw?.type,
      raw?.kind,
      raw?.messageType,
      raw?.message_type,
      raw?.mediaType,
      raw?.media_type,
      raw?.mediaUrl,
      raw?.media_url,
      raw?.url,
      raw?.link,
      raw?.mediaId,
      raw?.media_id,
      raw?.latitude,
      raw?.longitude,
      raw?.lat,
      raw?.lng,
      raw?.location
    ) !== undefined;

  if (topLevelHasMedia) attachmentsRaw.push(raw);

  for (const key of ["attachment", "media", "file", "asset", "location"]) {
    const value = raw?.[key];
    if (!value) continue;
    if (Array.isArray(value)) attachmentsRaw.push(...value);
    else if (typeof value === "object") attachmentsRaw.push(value);
  }

  if (Array.isArray(raw?.attachments)) attachmentsRaw.push(...raw.attachments);

  const mediaItems = [];
  const seen = new Set();
  for (const candidate of attachmentsRaw) {
    const item = normalizeAgentAttachment(candidate, attachmentsRaw.length === 1 ? primaryText : "");
    if (!item) continue;
    const signature = stableStringify(item);
    if (seen.has(signature)) continue;
    seen.add(signature);
    mediaItems.push(item);
  }

  let text = primaryText;
  if (mediaItems.length === 1 && mediaItems[0]?.caption && mediaItems[0].caption === primaryText && !parseBoolLike(raw?.sendTextSeparately || raw?.send_text_separately)) {
    text = "";
  }

  return { waTo, text, mediaItems };
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
// TOURS RD LEGACY \(CATÁLOGO ANTERIOR, SIN FLUJO ACTIVO\)
// =========================
const CATEGORY_ID_TO_KEY = Object.fromEntries(TOUR_CATEGORIES.map((c) => [c.id, c.key]));
const TOURS = safeJson(process.env.TOUR_CATALOG_JSON, null) || defaultTourCatalog();
const TOUR_ID_TO_KEY = Object.fromEntries(TOURS.map((t) => [t.id, t.key]));

function getServiceLineByKey(key) {
  return SERVICE_LINES.find((s) => s.key === key) || null;
}

function getOriginByKey(key) {
  return TOUR_ORIGINS.find((o) => o.key === key) || null;
}

function getPackageDestinationByKey(key) {
  return PACKAGE_DESTINATIONS.find((p) => p.key === key) || null;
}

function getCategoryByKey(key) {
  return TOUR_CATEGORIES.find((c) => c.key === key) || null;
}

function getTourByKey(key) {
  return TOURS.find((t) => t.key === key) || null;
}

function getToursByCategory(categoryKey) {
  return TOURS.filter((t) => t.category === categoryKey);
}

function getToursByOrigin(originKey) {
  return TOURS.filter((t) => !Array.isArray(t.origins) || t.origins.includes(originKey));
}

function getRealTourGroupByKey(key) {
  return REAL_TOUR_GROUPS.find((g) => g.key === key) || null;
}

function getRealTourByKey(key) {
  return REAL_TOURS.find((t) => t.key === key) || null;
}

function getRealToursByGroup(groupKey) {
  return REAL_TOURS.filter((t) => t.groupKey === groupKey);
}

function getVisibleRealTourGroups() {
  return REAL_TOUR_GROUPS.filter((g) => !g.hidden);
}

function formatVisibleRealTourGroupsInline() {
  return getVisibleRealTourGroups().map((g) => `*"${g.title}"*`).join(", ");
}

function getRealTourGroupEmoji(groupKey) {
  const map = {
    tours_punta_cana: "🏝️",
    tours_santo_domingo: "🏙️",
    tours_santiago: "⛰️",
    tours_las_terrenas: "🌊",
    tours_semana_santa: "⛪",
  };
  return map[groupKey] || "🌴";
}

function formatVisibleRealTourGroupsBulletText() {
  return getVisibleRealTourGroups()
    .map((g) => `${getRealTourGroupEmoji(g.key)} ${g.title}`)
    .join("\n");
}

function getAnyTourByKey(key) {
  return getRealTourByKey(key) || getTourByKey(key);
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
    t.includes("montaña") ||
    t.includes("boleto") ||
    t.includes("vuelo") ||
    t.includes("seguro") ||
    t.includes("hotel") ||
    t.includes("traslado");

  return isOnlyGreeting && !hasTravelIntent && t.length <= 40;
}

function quickHelpText() {
  return (
    `¡Hola! 😊\n` +
    `Puedo ayudarte con *Tours en República Dominicana*, *Boletos aéreos*, *Hoteles*, *Seguros de viaje* y *Traslados*.\n\n` +
    `También puedes escribirme ${formatVisibleRealTourGroupsInline()} para mostrarte las excursiones disponibles.`
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

function currency(n) {
  return `${PRICE_CURRENCY}${Number(n || 0).toFixed(0)}`;
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

function serviceLineRowTitle(service) {
  const map = {
    tours_rd: "Tours RD",
    boletos_aereos: "Boletos aéreos",
    solo_hoteles: "Hoteles",
    seguros_viaje: "Seguros de viaje",
    traslados: "Traslados",
    paquetes_vacacionales: "Paquete vacacional",
    hablar_asesor: "Hablar con asesor",
    ubicacion_contacto: "Ubicación/contacto",
    catalogo_pdf: "Catálogo PDF",
  };
  return waRowTitle(map[service?.key] || service?.title || "");
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

function matchesCategoryText(textNorm, label) {
  const v = normalizeText(label);
  const variants = [
    v,
    `ver ${v}`,
    `quiero ${v}`,
    `categoria ${v}`,
    `categoría ${v}`,
    `ver categoria ${v}`,
    `ver categoría ${v}`,
  ];
  return variants.includes(textNorm);
}

function detectCatalogRequest(textNorm) {
  const t = textNorm || "";
  return (
    t.includes("catalogo") ||
    t.includes("catálogo") ||
    t.includes("pdf") ||
    t.includes("brochure") ||
    t.includes("documento")
  );
}

function detectServiceLineFromUser(text) {
  const t = normalizeText(text);

  if (SERVICE_LINE_ID_TO_KEY[text]) return SERVICE_LINE_ID_TO_KEY[text];

  if (
    t.includes("tour") ||
    t.includes("excursion") ||
    t.includes("excursión") ||
    t.includes("playa") ||
    t.includes("isla") ||
    t.includes("buggies") ||
    t.includes("jarabacoa")
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

  if (detectCatalogRequest(t)) return "catalogo_pdf";
  return null;
}

function detectOriginKeyFromUser(text) {
  const t = normalizeText(text);
  if (TOUR_ORIGIN_ID_TO_KEY[text]) return TOUR_ORIGIN_ID_TO_KEY[text];

  for (const o of TOUR_ORIGINS) {
    if (matchesOriginText(t, o.title)) return o.key;
  }

  if (matchesOriginText(t, "bavaro") || matchesOriginText(t, "bávaro")) return "punta_cana";
  return null;
}

function detectPackageDestinationKeyFromUser(text) {
  const t = normalizeText(text);
  if (PACKAGE_DESTINATION_ID_TO_KEY[text]) return PACKAGE_DESTINATION_ID_TO_KEY[text];

  for (const p of PACKAGE_DESTINATIONS) {
    const norm = normalizeText(p.title);
    if (t === norm || t.includes(norm)) return p.key;
  }

  if (t.includes("celebra con mama") || t.includes("celebra con mamá")) return "celebra_con_mama_en_medellin";
  if (t.includes("octubre en medellin") || t.includes("octubre en medellín")) return "octubre_en_medellin";
  if (t.includes("semana santa en medellin") || t.includes("semana santa en medellín")) return "semana_santa_en_medellin";
  if (t.includes("medellin a tu alcance") || t.includes("medellín a tu alcance")) return "medellin_a_tu_alcance_2026";
  return null;
}

function detectCategoryKeyFromUser(text) {
  const t = normalizeText(text);
  if (CATEGORY_ID_TO_KEY[text]) return CATEGORY_ID_TO_KEY[text];

  if (matchesCategoryText(t, "Tours diarios") || matchesCategoryText(t, "Tour diario")) return "tours_diarios";
  if (matchesCategoryText(t, "Playas") || matchesCategoryText(t, "Playa")) return "playas";

  if (
    matchesCategoryText(t, "Montañas") ||
    matchesCategoryText(t, "Montana") ||
    matchesCategoryText(t, "Montaña")
  ) {
    return "montanas";
  }

  if (
    matchesCategoryText(t, "Excursiones especiales") ||
    matchesCategoryText(t, "Excursion especial") ||
    matchesCategoryText(t, "Excursión especial")
  ) {
    return "excursiones_especiales";
  }

  if (
    matchesCategoryText(t, "Paquetes de temporada") ||
    matchesCategoryText(t, "Paquete de temporada")
  ) {
    return "paquetes_temporada";
  }

  for (const c of TOUR_CATEGORIES) {
    const n = normalizeText(c.title);
    if (t === n) return c.key;
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
  if (t.includes("city tour") || t.includes("zona colonial")) return "city_tour_santo_domingo";
  if (t.includes("jarabacoa")) return "jarabacoa_aventura";
  if (t.includes("buggies") || t.includes("macao")) return "buggies_macao";
  if (t.includes("samana") || t.includes("samaná")) return "samana_temporada";

  return null;
}

function detectRealTourGroupFromUser(text, { allowBareOrigin = false } = {}) {
  const t = normalizeText(text);
  if (REAL_TOUR_GROUP_ID_TO_KEY[text]) return REAL_TOUR_GROUP_ID_TO_KEY[text];

  if (
    t.includes("tours desde punta cana") ||
    t.includes("tour desde punta cana") ||
    t.includes("tours punta cana") ||
    t.includes("tour punta cana") ||
    (allowBareOrigin && t === "punta cana")
  ) {
    return "tours_punta_cana";
  }

  if (
    t.includes("tours desde santo domingo") ||
    t.includes("tour desde santo domingo") ||
    t.includes("tours santo domingo") ||
    t.includes("tour santo domingo") ||
    (allowBareOrigin && (t === "santo domingo" || t === "sd"))
  ) {
    return "tours_santo_domingo";
  }

  if (
    t.includes("tours desde las terrenas") ||
    t.includes("tour desde las terrenas") ||
    t.includes("tours las terrenas") ||
    t.includes("tour las terrenas") ||
    (allowBareOrigin && t === "las terrenas")
  ) {
    return "tours_las_terrenas";
  }

  if (t.includes("tours semana santa") || t.includes("tour semana santa") || t.includes("semana santa")) {
    return "tours_semana_santa";
  }

  return null;
}

function detectRealTourKeyFromUser(text) {
  const t = normalizeText(text);
  if (REAL_TOUR_ID_TO_KEY[text]) return REAL_TOUR_ID_TO_KEY[text];

  const exact = REAL_TOURS.filter((tour) => normalizeText(tour.title) === t);
  if (exact.length === 1) return exact[0].key;

  const contains = REAL_TOURS.filter((tour) => t && normalizeText(tour.title).includes(t));
  if (contains.length === 1) return contains[0].key;

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

  if (parts.length === 1) parts.push(buildTourInfoText(tour));
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
    tour_key: session.pendingTour || session.pendingRealTourKey || session.lead?.tour_key || "",
    converted: true,
    followupSent: true,
    lastInteractionAt: new Date().toISOString(),
  };
}

function isValidEmail(text) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(text || "").trim());
}

function getRealTourGroupContext(groupKey) {
  switch (groupKey) {
    case "tours_santo_domingo":
      return { defaultCity: "Santo Domingo", fixedPickup: "Sambil", askPickup: false, pickupPrompt: "", locationLabel: "Salida" };
    case "tours_punta_cana":
      return { defaultCity: "Punta Cana", fixedPickup: "", askPickup: true, pickupPrompt: `Perfecto.
Ahora dime en qué *hotel* estás o desde qué *ubicación* saldrías.`, locationLabel: "Hotel / ubicación" };
    case "tours_santiago":
      return { defaultCity: "Santiago", fixedPickup: "", askPickup: true, pickupPrompt: `Perfecto.
Ahora dime desde qué *ubicación* saldrías en Santiago.`, locationLabel: "Ubicación" };
    case "tours_las_terrenas":
      return { defaultCity: "Las Terrenas", fixedPickup: "", askPickup: true, pickupPrompt: `Perfecto.
Ahora dime en qué *hotel* estás o desde qué *ubicación* saldrías en Las Terrenas.`, locationLabel: "Hotel / ubicación" };
    default:
      return { defaultCity: "", fixedPickup: "", askPickup: true, pickupPrompt: `Perfecto.
Ahora dime tu *ubicación* o *punto de salida*.`, locationLabel: "Ubicación" };
  }
}

function formatLocalDateOnly(dateLike, timeZone = BUSINESS_TIMEZONE) {
  const p = getZonedParts(new Date(dateLike), timeZone);
  return `${String(p.year).padStart(4, "0")}-${String(p.month).padStart(2, "0")}-${String(p.day).padStart(2, "0")}`;
}

function getEasterSundayUTC(year, timeZone = BUSINESS_TIMEZONE) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return zonedTimeToUtc({ year, month, day, hour: 0, minute: 0 }, timeZone);
}

function getNextHolyWeekStartUTC(now = new Date(), timeZone = BUSINESS_TIMEZONE) {
  const currentYear = getZonedParts(now, timeZone).year;
  for (const year of [currentYear, currentYear + 1]) {
    const easter = getEasterSundayUTC(year, timeZone);
    const holyWeekMonday = addLocalDaysUTC(easter, -6, timeZone);
    if (holyWeekMonday >= startOfLocalDayUTC(now, timeZone)) return holyWeekMonday;
  }
  const fallbackEaster = getEasterSundayUTC(currentYear + 1, timeZone);
  return addLocalDaysUTC(fallbackEaster, -6, timeZone);
}

function getRequestedDateMeta(rawText) {
  const raw = String(rawText || "").trim();
  const normalized = normalizeText(raw);
  const fallbackStart = startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE);
  const fallbackEnd = addLocalDaysUTC(fallbackStart, 1, BUSINESS_TIMEZONE);

  if (!raw) {
    return { raw, display: "—", startDate: formatLocalDateOnly(fallbackStart), endDate: formatLocalDateOnly(fallbackEnd), parsed: false };
  }

  if (normalized.includes("semana santa")) {
    const startUTC = getNextHolyWeekStartUTC(new Date(), BUSINESS_TIMEZONE);
    const endUTC = addLocalDaysUTC(startUTC, 1, BUSINESS_TIMEZONE);
    return { raw, display: `Semana Santa (${formatDateInTZ(startUTC.toISOString(), BUSINESS_TIMEZONE)})`, startDate: formatLocalDateOnly(startUTC), endDate: formatLocalDateOnly(endUTC), parsed: true };
  }

  const parsed = parseDateRangeFromText(raw);
  if (parsed?.from) {
    const startUTC = new Date(parsed.from);
    const endUTC = addLocalDaysUTC(startUTC, 1, BUSINESS_TIMEZONE);
    return { raw, display: formatDateInTZ(parsed.from, BUSINESS_TIMEZONE), startDate: formatLocalDateOnly(startUTC), endDate: formatLocalDateOnly(endUTC), parsed: true };
  }

  return { raw, display: raw, startDate: formatLocalDateOnly(fallbackStart), endDate: formatLocalDateOnly(fallbackEnd), parsed: false };
}

function isoWeekdayFromDateLike(dateLike, timeZone = BUSINESS_TIMEZONE) {
  const p = getZonedParts(new Date(dateLike), timeZone);
  const mid = zonedTimeToUtc({ year: p.year, month: p.month, day: p.day, hour: 12, minute: 0 }, timeZone);
  return ((mid.getUTCDay() + 6) % 7) + 1;
}

function formatWeekdayList(isoWeekdays = []) {
  const names = {
    1: "lunes",
    2: "martes",
    3: "miércoles",
    4: "jueves",
    5: "viernes",
    6: "sábado",
    7: "domingo",
  };
  const pluralNames = {
    1: "lunes",
    2: "martes",
    3: "miércoles",
    4: "jueves",
    5: "viernes",
    6: "sábados",
    7: "domingos",
  };
  const unique = Array.from(new Set((isoWeekdays || []).map((n) => Number(n)).filter((n) => names[n])));
  if (!unique.length) return "";
  if (unique.length === 1) return `los ${pluralNames[unique[0]]}`;
  if (unique.length === 2) return `${names[unique[0]]} y ${names[unique[1]]}`;
  return unique.map((n) => names[n]).join(", ");
}

function formatAllowedDatesList(allowedDates = []) {
  return allowedDates
    .map((d) => {
      const iso = `${String(d).trim()}T12:00:00Z`;
      return new Intl.DateTimeFormat("es-DO", {
        timeZone: BUSINESS_TIMEZONE,
        day: "2-digit",
        month: "long",
        year: "numeric",
      }).format(new Date(iso));
    })
    .join(", ");
}

function getHolyWeekRangeForYear(year, timeZone = BUSINESS_TIMEZONE) {
  const easter = getEasterSundayUTC(year, timeZone);
  const start = addLocalDaysUTC(easter, -6, timeZone);
  const end = addLocalDaysUTC(start, 7, timeZone);
  return { start, end };
}

function isDateInsideHolyWeek(dateLike, timeZone = BUSINESS_TIMEZONE) {
  const date = new Date(dateLike);
  const year = getZonedParts(date, timeZone).year;
  const { start, end } = getHolyWeekRangeForYear(year, timeZone);
  return date >= start && date < end;
}

function parseRealTourDateInput(rawText) {
  const raw = String(rawText || "").trim();
  const normalized = normalizeText(raw);

  if (!raw) return { kind: "empty", raw, normalized };
  if (normalized.includes("semana santa")) return { kind: "holy_week", raw, normalized };

  for (const [name, iso] of Object.entries(DOW)) {
    const normName = normalizeText(name);
    if (normalized === normName || normalized.startsWith(normName + " ") || normalized.includes(" " + normName)) {
      return { kind: "weekday", raw, normalized, isoWeekday: iso };
    }
  }

  const parsed = parseDateRangeFromText(raw);
  if (parsed?.from) {
    return {
      kind: "date",
      raw,
      normalized,
      parsed,
      isoWeekday: isoWeekdayFromDateLike(parsed.from, BUSINESS_TIMEZONE),
      localDate: formatLocalDateOnly(parsed.from, BUSINESS_TIMEZONE),
    };
  }

  return { kind: "unparsed", raw, normalized };
}

function getRealTourAvailability(tour) {
  const details = getRealTourTextDetails(tour) || {};
  const configured = details?.availability;
  const rawDateText = normalizeText(details?.dateText || "");
  const titleNorm = normalizeText(tour?.title || "");

  if (configured && typeof configured === "object" && configured.type) {
    return configured;
  }

  if (tour?.groupKey === "tours_semana_santa" || rawDateText.includes("semana santa")) {
    return { type: "holy_week", label: details?.dateText || "Semana Santa" };
  }

  if (tour?.groupKey === "tours_santo_domingo") {
    return { type: "weekdays", allowedWeekdays: [6, 7], label: details?.dateText || "Sábados y domingos." };
  }

  if (tour?.groupKey === "tours_las_terrenas") {
    return { type: "daily", label: details?.dateText || "Todos los días." };
  }

  if (tour?.groupKey === "tours_punta_cana") {
    if (titleNorm.includes("santo domingo") && titleNorm.includes("3 ojos")) {
      return { type: "weekdays", allowedWeekdays: [5], label: details?.dateText || "Todos los viernes." };
    }
    return { type: "daily", label: details?.dateText || "Todos los días." };
  }

  if (rawDateText.includes("todos los dias")) {
    return { type: "daily", label: details?.dateText || "Todos los días." };
  }

  if (rawDateText.includes("todos los sabados")) {
    return { type: "weekdays", allowedWeekdays: [6], label: details?.dateText || "Todos los sábados." };
  }

  if (rawDateText.includes("todos los domingos")) {
    return { type: "weekdays", allowedWeekdays: [7], label: details?.dateText || "Todos los domingos." };
  }

  if (rawDateText.includes("todos los viernes")) {
    return { type: "weekdays", allowedWeekdays: [5], label: details?.dateText || "Todos los viernes." };
  }

  if (rawDateText.includes("sabados y domingos")) {
    return { type: "weekdays", allowedWeekdays: [6, 7], label: details?.dateText || "Sábados y domingos." };
  }

  if (rawDateText.includes("jueves a domingo")) {
    return { type: "weekdays", allowedWeekdays: [4, 5, 6, 7], label: details?.dateText || "Jueves a domingo." };
  }

  if (rawDateText.includes("martes, jueves y sabados") || rawDateText.includes("martes jueves y sabados")) {
    return { type: "weekdays", allowedWeekdays: [2, 4, 6], label: details?.dateText || "Martes, jueves y sábados." };
  }

  if (rawDateText.includes("vigente") || rawDateText.includes("vigentes")) {
    return {
      type: "dates",
      allowedDates: Array.isArray(configured?.allowedDates) ? configured.allowedDates : [],
      label: details?.dateText || "Fechas vigentes",
    };
  }

  return null;
}

function buildRealTourDatePrompt(tour) {
  const availability = getRealTourAvailability(tour);
  const title = tour?.title || "este tour";

  if (!availability) {
    return `📅 Ahora dime la *fecha* que te interesa para *${title}*.
Ej: "sábado", "15 de abril" o "domingo".`;
  }

  if (availability.type === "daily") {
    return `📅 Ahora dime la *fecha* que te interesa para *${title}*.
✅ Este tour está disponible *todos los días*.
Ej: "mañana", "sábado" o "15 de abril".`;
  }

  if (availability.type === "weekdays") {
    const label = formatWeekdayList(availability.allowedWeekdays || []);
    const exampleDay = Number((availability.allowedWeekdays || [])[0] || 6);
    const exampleNames = { 1: "lunes", 2: "martes", 3: "miércoles", 4: "jueves", 5: "viernes", 6: "sábado", 7: "domingo" };
    return `📅 Ahora dime la *fecha* que te interesa para *${title}*.
✅ Este tour sale solo *${label}*.
Ejemplos válidos: "${exampleNames[exampleDay]}" o una fecha que caiga en ese día.`;
  }

  if (availability.type === "holy_week") {
    return `📅 Ahora dime la *fecha* que te interesa para *${title}*.
✅ Este tour aplica solo para *Semana Santa*.
Ejemplos válidos: "semana santa" o una fecha dentro de esa semana.`;
  }

  if (availability.type === "dates") {
    const allowed = Array.isArray(availability.allowedDates) ? availability.allowedDates : [];
    const allowedText = allowed.length ? formatAllowedDatesList(allowed) : "las fechas vigentes publicadas por la agencia";
    return `📅 Ahora dime la *fecha* que te interesa para *${title}*.
✅ Este tour está disponible solo en *${allowedText}*.`;
  }

  return `📅 Ahora dime la *fecha* que te interesa para *${title}*.`;
}

function validateRealTourRequestedDate(tour, rawText) {
  const availability = getRealTourAvailability(tour);
  const parsedInput = parseRealTourDateInput(rawText);

  if (parsedInput.kind === "empty") {
    return {
      ok: false,
      message: `Por favor, indícame la *fecha* que te interesa para el tour.

${buildRealTourDatePrompt(tour)}`,
    };
  }

  if (!availability) {
    if (parsedInput.kind === "unparsed") {
      return {
        ok: false,
        message: `Necesito una *fecha válida* o un *día* para continuar.

${buildRealTourDatePrompt(tour)}`,
      };
    }
    return { ok: true, parsedInput, availability: null };
  }

  if (availability.type === "daily") {
    if (parsedInput.kind === "unparsed") {
      return {
        ok: false,
        message: `Ese formato no me ayuda a validar la fecha 🙏

${buildRealTourDatePrompt(tour)}`,
      };
    }
    return { ok: true, parsedInput, availability };
  }

  if (availability.type === "weekdays") {
    if (!["weekday", "date"].includes(parsedInput.kind)) {
      return {
        ok: false,
        message: `Ese tour solo está disponible *${formatWeekdayList(availability.allowedWeekdays || [])}*.

${buildRealTourDatePrompt(tour)}`,
      };
    }

    const allowed = new Set((availability.allowedWeekdays || []).map((n) => Number(n)));
    if (!allowed.has(Number(parsedInput.isoWeekday))) {
      return {
        ok: false,
        message: `Ese tour solo está disponible *${formatWeekdayList(availability.allowedWeekdays || [])}*.

${buildRealTourDatePrompt(tour)}`,
      };
    }

    return { ok: true, parsedInput, availability };
  }

  if (availability.type === "holy_week") {
    if (parsedInput.kind === "holy_week") return { ok: true, parsedInput, availability };
    if (parsedInput.kind === "date" && isDateInsideHolyWeek(parsedInput.parsed.from, BUSINESS_TIMEZONE)) {
      return { ok: true, parsedInput, availability };
    }
    return {
      ok: false,
      message: `Ese tour aplica solo para *Semana Santa*.

${buildRealTourDatePrompt(tour)}`,
    };
  }

  if (availability.type === "dates") {
    const allowedDates = Array.isArray(availability.allowedDates) ? availability.allowedDates : [];
    if (!allowedDates.length) {
      return {
        ok: false,
        message: `Para este tour necesito una de las *fechas vigentes* publicadas por la agencia.`,
      };
    }
    if (parsedInput.kind !== "date") {
      return {
        ok: false,
        message: `Para este tour necesito una *fecha exacta*.
Fechas válidas: ${formatAllowedDatesList(allowedDates)}.`,
      };
    }
    if (!allowedDates.includes(parsedInput.localDate)) {
      return {
        ok: false,
        message: `Esa fecha no está dentro de las *fechas vigentes* del tour.
Fechas válidas: ${formatAllowedDatesList(allowedDates)}.`,
      };
    }
    return { ok: true, parsedInput, availability };
  }

  return { ok: true, parsedInput, availability };
}

function normalizeChildrenAgesInput(userText, expectedChildren = 0) {
  const raw = String(userText || "").trim();
  const expected = Number(expectedChildren || 0);
  const ages = (raw.match(/\d{1,2}/g) || []).map((n) => String(parseInt(n, 10)));
  if (expected <= 0) return { ok: true, formatted: "" };
  if (expected === 1) {
    const value = ages[0] || raw;
    return String(value || "").trim() ? { ok: true, formatted: String(value).trim() } : { ok: false, formatted: "" };
  }
  if (ages.length === expected) return { ok: true, formatted: ages.join(", ") };
  return { ok: false, formatted: "" };
}

function buildLeadSummary(title, fields = []) {
  const lines = [`📌 *${title}*`, ""];
  for (const f of fields) lines.push(`${f.label}: ${f.value || "—"}`);
  return lines.join("\n");
}

function serviceLineLabel(key) {
  return getServiceLineByKey(key)?.title || key || "Servicio";
}

function getDigitsOnly(text) {
  return String(text || "").replace(/[^\d]/g, "");
}

async function finalizeSimpleLead({ session, flow, from, phoneDigits }) {
  const summaryText = buildLeadSummary(flow.summaryTitle, flow.buildSummaryFields(session, phoneDigits));
  updateLead(session, { tour_key: "", quotePreview: summaryText, converted: false, followupSent: false });
  await handoffToHumanTool({ summary: summaryText });
  await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);
  await sendWhatsAppText(from, flow.buildConfirmationText(session));
  clearIntakeFlow(session);
}

async function handleSimpleServiceFlow({ session, from, userText, tNorm }) {
  for (const flow of SIMPLE_SERVICE_FLOWS) {
    const step = flow.steps.find((item) => item.state === session.state);
    if (!step) continue;

    let value = userText;
    let selectedPackage = null;

    if (step.kind === "text") {
      if (tNorm.length < (step.minLen || 1)) {
        await sendWhatsAppText(from, step.invalidPrompt);
        return true;
      }
    } else if (step.kind === "count") {
      value = parsePassengerCount(userText);
      if (value === null || value < (step.minValue || 1)) {
        await sendWhatsAppText(from, step.invalidPrompt);
        return true;
      }
    } else if (step.kind === "phone") {
      value = getDigitsOnly(userText);
      if (value.length < 8) {
        await sendWhatsAppText(from, step.invalidPrompt || `Ese número parece incompleto 🙏
Envíamelo así: 829XXXXXXX`);
        return true;
      }
      await finalizeSimpleLead({ session, flow, from, phoneDigits: value });
      return true;
    } else if (step.kind === "packageDestination") {
      selectedPackage = parsePackageChoice(session, userText);
      if (!selectedPackage) {
        await sendWhatsAppText(from, step.invalidPrompt);
        await sendPackageDestinationsList(from, session);
        return true;
      }
      value = selectedPackage.title;
      session.pendingPackageKey = selectedPackage.key;
      await sendPackagePresentation(from, selectedPackage);
    }

    if (step.field) session[step.field] = value;
    if (typeof step.assign === "function") step.assign(session, value, userText);

    if (step.nextState) session.state = step.nextState;
    if (step.prompt) await sendWhatsAppText(from, step.prompt);
    if (typeof step.afterValid === "function") await step.afterValid({ session, from, value, userText });
    return true;
  }

  return false;
}

async function startSimpleServiceFlow({ session, serviceLineKey, from }) {
  const flow = SIMPLE_SERVICE_FLOWS.find((item) => item.serviceLineKey === serviceLineKey);
  if (!flow) return false;
  session.state = flow.startState;
  await sendWhatsAppText(from, flow.startPrompt);
  if (typeof flow.afterStart === "function") await flow.afterStart({ session, from });
  return true;
}

const SIMPLE_SERVICE_FLOWS = [
  {
    serviceLineKey: "boletos_aereos",
    startState: "await_flight_origin",
    startPrompt: `Perfecto ✈️ Vamos con *boletos aéreos*.

¿Desde dónde deseas salir?
Ej: Santo Domingo, Punta Cana o Santiago.`,
    summaryTitle: "Nueva solicitud de boletos aéreos",
    buildSummaryFields: (session, phoneDigits) => [
      { label: "🧩 Servicio", value: serviceLineLabel("boletos_aereos") },
      { label: "🛫 Salida / origen", value: session.pendingDepartureCity || "—" },
      { label: "🌍 Destino", value: session.pendingDestination || "—" },
      { label: "📅 Fecha / temporada", value: session.pendingTravelDateText || "—" },
      { label: "👥 Personas", value: session.pendingPassengers || "—" },
      { label: "👤 Cliente", value: session.pendingName || "—" },
      { label: "📞 Tel", value: phoneDigits || "—" },
    ],
    buildConfirmationText: (session) =>
      `✅ *Solicitud recibida*

Recibí tu solicitud de *boletos aéreos* y nuestro equipo te contactará con opciones según:
` +
      `• salida: ${session.pendingDepartureCity || "—"}
` +
      `• destino: ${session.pendingDestination || "—"}
` +
      `• fecha: ${session.pendingTravelDateText || "—"}
` +
      `• personas: ${session.pendingPassengers || "—"}`,
    steps: [
      { state: "await_flight_origin", kind: "text", minLen: 2, field: "pendingDepartureCity", nextState: "await_flight_destination", invalidPrompt: `Indícame desde dónde deseas salir. Ej: Santo Domingo, Punta Cana o Santiago.`, prompt: `Perfecto ✈️
Ahora dime el *destino* o ciudad/país a donde quieres viajar.` },
      { state: "await_flight_destination", kind: "text", minLen: 2, field: "pendingDestination", nextState: "await_flight_date", invalidPrompt: `Por favor, indícame el *destino* del vuelo.`, prompt: `Gracias. Ahora dime la *fecha aproximada* del viaje.
Ej: "15 de abril", "en junio" o "ida y vuelta del 10 al 18 de mayo".` },
      { state: "await_flight_date", kind: "text", minLen: 2, field: "pendingTravelDateText", nextState: "await_flight_people", invalidPrompt: `Por favor, indícame la *fecha aproximada* del vuelo.`, prompt: `Perfecto. ¿Para cuántas *personas* sería el boleto aéreo?` },
      { state: "await_flight_people", kind: "count", minValue: 1, field: "pendingPassengers", nextState: "await_flight_name", invalidPrompt: `Indícame cuántas *personas* viajarían. Ej: 1, 2, 3...`, prompt: `Perfecto 👍
Ahora dime tu *nombre completo*.` },
      { state: "await_flight_name", kind: "text", minLen: 3, field: "pendingName", nextState: "await_flight_phone", invalidPrompt: `Por favor, envíame tu *nombre completo* 🙂`, prompt: `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.` },
      { state: "await_flight_phone", kind: "phone" },
    ],
  },
  {
    serviceLineKey: "solo_hoteles",
    startState: "await_hotel_destination",
    startPrompt: `Perfecto 🏨 Vamos con *Hoteles*.

¿En qué *destino o ciudad* deseas hospedarte?`,
    summaryTitle: "Nueva solicitud de hoteles",
    buildSummaryFields: (session, phoneDigits) => [
      { label: "🧩 Servicio", value: serviceLineLabel("solo_hoteles") },
      { label: "🌍 Destino", value: session.pendingDestination || "—" },
      { label: "📅 Fecha / temporada", value: session.pendingTravelDateText || "—" },
      { label: "🌙 Noches", value: session.pendingNights || "—" },
      { label: "👥 Personas", value: session.pendingPassengers || "—" },
      { label: "🏨 Categoría hotel", value: session.pendingHotelStars || "—" },
      { label: "👤 Cliente", value: session.pendingName || "—" },
      { label: "📞 Tel", value: phoneDigits || "—" },
    ],
    buildConfirmationText: (session) =>
      `✅ *Solicitud recibida*

Recibí tu solicitud de *hoteles* y nuestro equipo te contactará con opciones según:
` +
      `• destino: ${session.pendingDestination || "—"}
` +
      `• fecha: ${session.pendingTravelDateText || "—"}
` +
      `• noches: ${session.pendingNights || "—"}
` +
      `• personas: ${session.pendingPassengers || "—"}
` +
      `• categoría: ${session.pendingHotelStars || "—"}`,
    steps: [
      { state: "await_hotel_destination", kind: "text", minLen: 2, field: "pendingDestination", nextState: "await_hotel_date", invalidPrompt: `Dime el *destino* o ciudad donde deseas reservar hotel.`, prompt: `Perfecto 🏨
Ahora dime la *fecha aproximada* del check-in o temporada.` },
      { state: "await_hotel_date", kind: "text", minLen: 2, field: "pendingTravelDateText", nextState: "await_hotel_nights", invalidPrompt: `Por favor, indícame la *fecha aproximada* del hotel.`, prompt: `¿Cuántas *noches* deseas reservar?` },
      { state: "await_hotel_nights", kind: "count", minValue: 1, field: "pendingNights", nextState: "await_hotel_people", invalidPrompt: `Indícame cuántas *noches* serían. Ej: 2, 3, 5...`, prompt: `Perfecto. ¿Para cuántas *personas* sería la reserva del hotel?` },
      { state: "await_hotel_people", kind: "count", minValue: 1, field: "pendingPassengers", nextState: "await_hotel_stars", invalidPrompt: `Indícame cuántas *personas* se hospedarían. Ej: 2`, prompt: `Perfecto. ¿Qué tipo de hotel prefieres?
Ej: *3 estrellas*, *4 estrellas* o *5 estrellas*.` },
      { state: "await_hotel_stars", kind: "text", minLen: 2, field: "pendingHotelStars", nextState: "await_hotel_name", invalidPrompt: `Indícame si prefieres *3 estrellas*, *4 estrellas* o *5 estrellas*.`, prompt: `Gracias 👍
Ahora dime tu *nombre completo*.` },
      { state: "await_hotel_name", kind: "text", minLen: 3, field: "pendingName", nextState: "await_hotel_phone", invalidPrompt: `Por favor, envíame tu *nombre completo* 🙂`, prompt: `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.` },
      { state: "await_hotel_phone", kind: "phone" },
    ],
  },
  {
    serviceLineKey: "seguros_viaje",
    startState: "await_insurance_destination",
    startPrompt: `Perfecto 🛡️ Vamos con *seguros de viaje*.

¿A qué *país o destino* viajas?`,
    summaryTitle: "Nueva solicitud de seguro de viaje",
    buildSummaryFields: (session, phoneDigits) => [
      { label: "🧩 Servicio", value: serviceLineLabel("seguros_viaje") },
      { label: "🌍 Destino", value: session.pendingDestination || "—" },
      { label: "📆 Días de viaje", value: session.pendingTripDays || "—" },
      { label: "👥 Personas", value: session.pendingPassengers || "—" },
      { label: "🎂 Edades", value: session.pendingTravelerAgesText || "—" },
      { label: "👤 Cliente", value: session.pendingName || "—" },
      { label: "📞 Tel", value: phoneDigits || "—" },
    ],
    buildConfirmationText: (session) =>
      `✅ *Solicitud recibida*

Recibí tu solicitud de *seguro de viaje* y nuestro equipo te contactará con opciones según:
` +
      `• destino: ${session.pendingDestination || "—"}
` +
      `• días: ${session.pendingTripDays || "—"}
` +
      `• personas: ${session.pendingPassengers || "—"}
` +
      `• edades: ${session.pendingTravelerAgesText || "—"}`,
    steps: [
      { state: "await_insurance_destination", kind: "text", minLen: 2, field: "pendingDestination", nextState: "await_insurance_days", invalidPrompt: `Indícame el *país o destino* para tu seguro de viaje.`, prompt: `Perfecto 🛡️
¿Cuántos *días* durará el viaje?` },
      { state: "await_insurance_days", kind: "count", minValue: 1, field: "pendingTripDays", nextState: "await_insurance_people", invalidPrompt: `Indícame cuántos *días* durará el viaje. Ej: 5, 8, 12...`, prompt: `Gracias. Ahora dime cuántas *personas* viajan.` },
      { state: "await_insurance_people", kind: "count", minValue: 1, field: "pendingPassengers", nextState: "await_insurance_ages", invalidPrompt: `Indícame cuántas *personas* necesitan el seguro. Ej: 1, 2, 3...`, prompt: `Perfecto. Ahora dime las *edades* de los viajeros.
Ej: 34 y 29 / 40, 12 y 8` },
      { state: "await_insurance_ages", kind: "text", minLen: 1, field: "pendingTravelerAgesText", nextState: "await_insurance_name", invalidPrompt: `Por favor, indícame las *edades* de los viajeros.`, prompt: `Perfecto 👍
Ahora dime tu *nombre completo*.` },
      { state: "await_insurance_name", kind: "text", minLen: 3, field: "pendingName", nextState: "await_insurance_phone", invalidPrompt: `Por favor, envíame tu *nombre completo* 🙂`, prompt: `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.` },
      { state: "await_insurance_phone", kind: "phone" },
    ],
  },
  {
    serviceLineKey: "traslados",
    startState: "await_transfer_route",
    startPrompt: `Perfecto 🚕 Vamos con *traslados*.

Dime la *ruta* que necesitas.
Ej: aeropuerto → hotel / hotel → aeropuerto / ciudad → ciudad.`,
    summaryTitle: "Nueva solicitud de traslado",
    buildSummaryFields: (session, phoneDigits) => [
      { label: "🧩 Servicio", value: serviceLineLabel("traslados") },
      { label: "🚕 Ruta", value: session.pendingTransferRoute || "—" },
      { label: "📅 Fecha", value: session.pendingTravelDateText || "—" },
      { label: "👥 Personas", value: session.pendingPassengers || "—" },
      { label: "👤 Cliente", value: session.pendingName || "—" },
      { label: "📞 Tel", value: phoneDigits || "—" },
    ],
    buildConfirmationText: (session) =>
      `✅ *Solicitud recibida*

Recibí tu solicitud de *traslado* y nuestro equipo te contactará con opciones según:
` +
      `• ruta: ${session.pendingTransferRoute || "—"}
` +
      `• fecha: ${session.pendingTravelDateText || "—"}
` +
      `• personas: ${session.pendingPassengers || "—"}`,
    steps: [
      { state: "await_transfer_route", kind: "text", minLen: 2, field: "pendingTransferRoute", nextState: "await_transfer_date", invalidPrompt: `Dime la *ruta del traslado*.
Ej: aeropuerto → hotel / hotel → aeropuerto / ciudad → ciudad.`, prompt: `Perfecto 🚕
Ahora dime la *fecha aproximada* del traslado.` },
      { state: "await_transfer_date", kind: "text", minLen: 2, field: "pendingTravelDateText", nextState: "await_transfer_people", invalidPrompt: `Por favor, indícame la *fecha aproximada* del traslado.`, prompt: `Gracias. ¿Para cuántas *personas* sería el traslado?` },
      { state: "await_transfer_people", kind: "count", minValue: 1, field: "pendingPassengers", nextState: "await_transfer_name", invalidPrompt: `Indícame cuántas *personas* viajarían. Ej: 2`, prompt: `Perfecto 👍
Ahora dime tu *nombre completo*.` },
      { state: "await_transfer_name", kind: "text", minLen: 3, field: "pendingName", nextState: "await_transfer_phone", invalidPrompt: `Por favor, envíame tu *nombre completo* 🙂`, prompt: `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.` },
      { state: "await_transfer_phone", kind: "phone" },
    ],
  },
  {
    serviceLineKey: "paquetes_vacacionales",
    startState: "await_package_destination",
    startPrompt: `Perfecto 🎒 Vamos con *paquetes vacacionales*.

Te mostraré el listado completo y puedes responder con el *número* o con el *nombre* del paquete que deseas ver.`,
    afterStart: async ({ from, session }) => {
      await sendPackageDestinationsList(from, session);
    },
    summaryTitle: "Nueva solicitud de paquete vacacional",
    buildSummaryFields: (session, phoneDigits) => [
      { label: "🧩 Servicio", value: serviceLineLabel("paquetes_vacacionales") },
      { label: "🎒 Paquete", value: session.pendingDestination || "—" },
      { label: "📅 Fecha / temporada", value: session.pendingTravelDateText || "—" },
      { label: "👥 Personas", value: session.pendingPassengers || "—" },
      { label: "👤 Cliente", value: session.pendingName || "—" },
      { label: "📞 Tel", value: phoneDigits || "—" },
    ],
    buildConfirmationText: (session) =>
      `✅ *Solicitud recibida*

Recibí tu solicitud de *paquete vacacional* y nuestro equipo te contactará con opciones según:
` +
      `• paquete: ${session.pendingDestination || "—"}
` +
      `• fecha / temporada: ${session.pendingTravelDateText || "—"}
` +
      `• personas: ${session.pendingPassengers || "—"}`,
    steps: [
      { state: "await_package_destination", kind: "packageDestination", field: "pendingDestination", nextState: "await_package_date", invalidPrompt: `No pude identificar el paquete 🙏
Responde con el *número* o con el *nombre* exacto del paquete.`, prompt: `Perfecto 🎒
Ahora dime la *fecha* o *temporada* que te interesa.
Ej: "julio", "semana santa", "15 de agosto".` },
      { state: "await_package_date", kind: "text", minLen: 2, field: "pendingTravelDateText", nextState: "await_package_people", invalidPrompt: `Por favor, indícame la *fecha* o *temporada* que te interesa.`, prompt: `Gracias. ¿Para cuántas *personas* sería el paquete?` },
      { state: "await_package_people", kind: "count", minValue: 1, field: "pendingPassengers", nextState: "await_package_name", invalidPrompt: `Indícame cuántas *personas* viajarían. Ej: 2`, prompt: `Perfecto 👍
Ahora dime tu *nombre completo*.` },
      { state: "await_package_name", kind: "text", minLen: 3, field: "pendingName", nextState: "await_package_phone", invalidPrompt: `Por favor, envíame tu *nombre completo* 🙂`, prompt: `Gracias. Ahora envíame tu *número de teléfono* para que el equipo te contacte.` },
      { state: "await_package_phone", kind: "phone" },
    ],
  },
  {
    serviceLineKey: "hablar_asesor",
    startState: "await_advisor_topic",
    startPrompt: `Perfecto 👤 Vamos a conectarte con un asesor de *República Dominicana*.

Cuéntame brevemente qué necesitas.`,
    summaryTitle: "Solicitud para hablar con un asesor",
    buildSummaryFields: (session, phoneDigits) => [
      { label: "🧩 Servicio", value: serviceLineLabel("hablar_asesor") },
      { label: "🌎 Mercado", value: "República Dominicana" },
      { label: "📝 Tema", value: session.pendingAdvisorTopic || "—" },
      { label: "👤 Cliente", value: session.pendingName || "—" },
      { label: "📞 Tel", value: phoneDigits || "—" },
    ],
    buildConfirmationText: (session) =>
      `✅ *Solicitud recibida*

Ya pasé tu caso para que un asesor de *República Dominicana* te contacte.

Tema: ${session.pendingAdvisorTopic || "Consulta general"}`,
    steps: [
      { state: "await_advisor_topic", kind: "text", minLen: 2, field: "pendingAdvisorTopic", nextState: "await_advisor_name", invalidPrompt: `Cuéntame brevemente qué necesitas para poder pasarte con el asesor correcto.`, prompt: `Perfecto 👍
Ahora dime tu *nombre completo*.` },
      { state: "await_advisor_name", kind: "text", minLen: 3, field: "pendingName", nextState: "await_advisor_phone", invalidPrompt: `Por favor, envíame tu *nombre completo* 🙂`, prompt: `Gracias. Ahora envíame tu *número de teléfono* para que un asesor te contacte.` },
      { state: "await_advisor_phone", kind: "phone" },
    ],
  },
];

function categoriesEmojiText() {
  return (
    `🌴 *Tours en República Dominicana*

` +
    `Tenemos estas colecciones disponibles para ayudarte a elegir más fácil:
` +
    `${formatVisibleRealTourGroupsBulletText()}

` +
    `Selecciona la colección que deseas explorar y te mostraré las excursiones disponibles.`
  );
}

function mainMenuText() {
  const visibleLines = SERVICE_LINES.filter(s => s.key !== "ubicacion_contacto");
  const listText = visibleLines.map(s => `• ${s.title}`).join("\n");
  return (
    `👋 ¡Bienvenido a *${BUSINESS_NAME}*! Soy tu asistente virtual de viajes.

` +
    `Estoy aquí para ayudarte a cotizar, comparar opciones y dejar tu solicitud casi lista para reserva y pago.

` +
    `Puedo ayudarte con:
` +
    listText
  );
}

function buildLocationContactText() {
  const addressLine = BUSINESS_ADDRESS ? `📍 Dirección: ${BUSINESS_ADDRESS}\n` : "";
  return (`📍 *Ubicación y contacto*\n\n` + `${addressLine}` + `${MARKET_CONTACT_TEXT}`).trim();
}

function getRealTourGroupIntro(groupKey) {
  if (groupKey === "tours_punta_cana") {
    return "Excursiones y actividades disponibles para disfrutar saliendo desde Punta Cana. Salidas diarias, excepto los tours especiales que indiquen otro día específico.";
  }
  if (groupKey === "tours_santo_domingo") {
    return "Selección de excursiones saliendo desde Santo Domingo. Salidas: Sábados y Domingos (Punto de encuentro: Sambil 5:00 AM).";
  }
  if (groupKey === "tours_santiago") {
    return "Selección de excursiones saliendo desde Santiago. Salidas: Sábados y Domingos.";
  }
  if (groupKey === "tours_las_terrenas") {
    return "Selección de excursiones saliendo desde Las Terrenas. Salidas diarias.";
  }
  if (groupKey === "tours_semana_santa") {
    return "Opciones especiales de excursiones y actividades para Semana Santa.";
  }
  return "Estas son las excursiones disponibles en esta colección.";
}

function formatRealToursTextList(groupKey, session) {
  const group = getRealTourGroupByKey(groupKey);
  const tours = getRealToursByGroup(groupKey);

  if (!group || group.hidden || !tours.length) return "No encontré excursiones disponibles en esta colección ahora mismo 🙏";

  if (session) {
    session.lastRealTours = tours.map((t) => ({ key: t.key, title: t.title }));
  }

  return (
    `🌴 *${group.title}*

` +
    `${getRealTourGroupIntro(groupKey)}

` +
    `Estas son las excursiones que puedes consultar en esta colección:

` +
    tours.map((t, i) => `${i + 1}. ${t.title}`).join("\n") +
    `

Responde con el *número* o con el *nombre* del tour que deseas ver.`
  );
}

function parseRealTourChoice(session, userText) {
  const t = normalizeText(userText);
  const options = Array.isArray(session?.lastRealTours) ? session.lastRealTours : [];

  if (/^\d+$/.test(t)) {
    const idx = parseInt(t, 10) - 1;
    if (idx >= 0 && idx < options.length) return getRealTourByKey(options[idx].key);
  }

  for (const opt of options) {
    const titleNorm = normalizeText(opt.title);
    if (t === titleNorm || t.includes(titleNorm)) {
      return getRealTourByKey(opt.key);
    }
  }

  const direct = detectRealTourKeyFromUser(userText);
  return direct ? getRealTourByKey(direct) : null;
}

function formatPackagesTextList(session) {
  const packages = PACKAGE_DESTINATIONS || [];
  if (!packages.length) return "No encontré paquetes vacacionales disponibles ahora mismo 🙏";
  if (session) {
    session.lastPackages = packages.map((p) => ({ key: p.key, title: p.title }));
  }
  return (
    `🎒 *Paquetes vacacionales*

` +
    `Estas son las opciones disponibles en este momento:

` +
    packages.map((p, i) => `${i + 1}. ${p.title}`).join("\n") +
    `

Responde con el *número* o con el *nombre* del paquete que deseas ver.`
  );
}

function parsePackageChoice(session, userText) {
  const t = normalizeText(userText);
  const options = Array.isArray(session?.lastPackages) ? session.lastPackages : [];

  if (/^\d+$/.test(t)) {
    const idx = parseInt(t, 10) - 1;
    if (idx >= 0 && idx < options.length) return getPackageDestinationByKey(options[idx].key);
  }

  for (const opt of options) {
    const titleNorm = normalizeText(opt.title);
    if (t === titleNorm || t.includes(titleNorm)) {
      return getPackageDestinationByKey(opt.key);
    }
  }

  const direct = detectPackageDestinationKeyFromUser(userText);
  return direct ? getPackageDestinationByKey(direct) : null;
}

function buildPackageInfoText(pkg) {
  if (!pkg) return "";
  const lines = [`🎒 *${pkg.title || "Paquete vacacional"}*`, ""];
  if (pkg.priceText) lines.push(`💵 ${pkg.priceText}`);
  if (pkg.durationText) lines.push(`🕒 ${pkg.durationText}`);
  if (pkg.dateText) lines.push(`📅 ${pkg.dateText}`);
  if (pkg.includesText) lines.push(`\n✅ *Incluye:*\n${pkg.includesText}`);
  if (pkg.noteText) lines.push(`\nℹ️ *Nota:* ${pkg.noteText}`);
  return lines.join("\n");
}


function getRealTourTextDetails(tour) {
  if (!tour) return null;
  return REAL_TOUR_TEXT_OVERRIDES[tour.key] || null;
}

function isGoBack(textNorm) {
  const t = normalizeText(textNorm || "");
  return ["atras", "atrás", "volver", "regresar", "regresa", "volver atras", "volver atrás"].includes(t);
}

function inferRealTourExperienceText(title = "") {
  const t = normalizeText(title);

  if (t.includes("isla saona")) return "Excursión de playa con ambiente caribeño, navegación y experiencia de día completo.";
  if (t.includes("isla catalina")) return "Excursión de isla ideal para disfrutar playa, mar y actividades acuáticas.";
  if (t.includes("boat party")) return "Paseo en barco con ambiente animado, mar y actividades recreativas.";
  if (t.includes("buggies")) return "Aventura todoterreno ideal para quienes disfrutan recorridos con adrenalina.";
  if (t.includes("fourwheel")) return "Experiencia de aventura en Fourwheel con recorrido dinámico al aire libre.";
  if (t.includes("polaris")) return "Experiencia en Polaris pensada para quienes buscan una salida activa y divertida.";
  if (t.includes("jet ski")) return "Actividad acuática ideal para quienes buscan velocidad y diversión sobre el mar.";
  if (t.includes("aqua kart")) return "Experiencia acuática con enfoque de aventura y entretenimiento.";
  if (t.includes("horseback")) return "Paseo a caballo ideal para disfrutar un recorrido natural y relajado.";
  if (t.includes("dolphin")) return "Experiencia recreativa ideal para compartir en familia y vivir una actividad diferente.";
  if (t.includes("coco bongo")) return "Salida de entretenimiento perfecta para quienes desean disfrutar un show y vida nocturna.";
  if (t.includes("cayo")) return "Excursión de playa pensada para disfrutar mar, relax y ambiente tropical.";
  if (t.includes("playa")) return "Salida de playa ideal para pasar un día de descanso y disfrute.";
  if (t.includes("parapente")) return "Experiencia de aventura para quienes desean emociones fuertes y vistas impresionantes.";
  if (t.includes("ballenas")) return "Experiencia de temporada ideal para quienes desean una salida especial de observación.";
  if (t.includes("rio")) return "Excursión de naturaleza con combinación de agua, descanso y recorrido.";
  if (t.includes("city")) return "Recorrido ideal para quienes desean combinar paseo, puntos de interés y experiencia local.";
  if (t.includes("ocean world")) return "Experiencia recreativa ideal para compartir en grupo o en familia.";
  if (t.includes("scoobadoo")) return "Actividad turística ideal para quienes desean una experiencia divertida y diferente.";
  if (t.includes("santa fe")) return "Escapada full day para disfrutar relax, paseo y actividades incluidas.";
  return "Excursión disponible en esta colección para que elijas la opción que mejor conecte con tu viaje.";
}

function buildRealTourReserveHint() {
  return (
    `📲 Si esta excursión te interesa, respóndeme con la *fecha* o *salida* que prefieres y te pediré tus datos para dejar la solicitud casi lista para confirmación y pago.
` +
    `↩️ Escribe *atrás* para volver al listado de tours o *menú* para ver todos los servicios.`
  );
}

function buildRealTourInfoText(tour) {
  const details = getRealTourTextDetails(tour) || {};
  const groupKey = tour?.groupKey || "";
  const lines = [`🌴 *${tour?.title || "Tour"}*`];

  if (details.priceText) {
    lines.push(`💵 ${details.priceText}`);
  } else {
    lines.push(`💵 Precio: revisa el valor publicado en la imagen del tour o solicita confirmación con la agencia.`);
  }

  if (details.durationText) {
    lines.push(`⏳ ${details.durationText}`);
  }

  if (details.dateText) {
    lines.push(`📅 ${details.dateText}`);
  } else if (groupKey === "tours_santo_domingo" || groupKey === "tours_santiago") {
    lines.push(`📅 Salidas: Sábados y domingos.`);
  } else if (groupKey === "tours_las_terrenas" || groupKey === "tours_punta_cana") {
    lines.push(`📅 Salidas: Todos los días.`);
  } else if (groupKey === "tours_semana_santa") {
    lines.push(`📅 Salida correspondiente a la colección de Semana Santa publicada por la agencia.`);
  } else {
    lines.push(`📅 Fecha / salida: consulta la disponibilidad o la fecha mostrada en la imagen del tour.`);
  }

  if (details.pickupText) {
    lines.push(`🚐 ${details.pickupText}`);
  } else if (groupKey === "tours_punta_cana") {
    lines.push(`🚐 Pickup / salida: disponible desde Punta Cana según coordinación de la agencia.`);
  } else if (groupKey === "tours_santo_domingo") {
    lines.push(`🚐 Salida: Plaza Sambil a las 5:00 AM.`);
  }

  if (details.includesText) {
    lines.push(`✅ ${details.includesText}`);
  } else {
    lines.push(`✅ ${inferRealTourExperienceText(tour?.title || "")}`);
  }

  if (details.paymentText) {
    lines.push(`💳 ${details.paymentText}`);
  } else {
    lines.push(`💳 Pago: sujeto a validación final por parte de la agencia al momento de confirmar.`);
  }

  if (details.reserveText) {
    lines.push(`📌 ${details.reserveText}`);
  } else {
    lines.push(`📌 Reserva: la solicitud queda registrada para que un asesor te contacte y cierre la confirmación.`);
  }

  if (details.noteText) {
    lines.push(`📝 ${details.noteText}`);
  } else {
    lines.push(`🖼️ La imagen que recibiste contiene la información promocional oficial compartida por la agencia.`);
  }

  lines.push("");
  lines.push(buildRealTourReserveHint());

  return lines.join("\n");
}

function buildRealTourLeadSummary(session, phoneDigits) {
  const tour = getRealTourByKey(session.pendingRealTourKey);
  const group = getRealTourGroupByKey(session.pendingRealTourGroup || tour?.groupKey);
  const pax = Number(session.pendingAdults || 0) + Number(session.pendingChildren || 0);
  const groupContext = getRealTourGroupContext(group?.key || tour?.groupKey || "");
  const dateMeta = getRequestedDateMeta(session.pendingDesiredDate);

  const fields = [
    { label: "🧩 Servicio", value: "Tours en República Dominicana" },
    { label: "🗂️ Colección", value: group?.title || "—" },
    { label: "🌴 Tour", value: tour?.title || "—" },
    { label: "📅 Fecha solicitada", value: dateMeta.display || session.pendingDesiredDate || "—" },
    { label: "👥 Pasajeros", value: `${pax} (${session.pendingAdults || 0} adultos / ${session.pendingChildren || 0} niños)` }
  ];

  if (Number(session.pendingChildren || 0) > 0) {
    fields.push({ label: "👶 Edades de niños", value: session.pendingChildrenAges || "No especificadas" });
  }

  fields.push({ label: `📍 ${groupContext.locationLabel || "Ubicación"}`, value: session.pendingPickup || groupContext.fixedPickup || "—" });
  if (groupContext.defaultCity || session.pendingCity) {
    fields.push({ label: "🏙️ Zona", value: session.pendingCity || groupContext.defaultCity || "—" });
  }
  fields.push({ label: "👤 Cliente", value: session.pendingName || "—" });
  fields.push({ label: "📧 Correo", value: session.pendingEmail || "—" });
  fields.push({ label: "📱 WhatsApp", value: phoneDigits || "—" });
  fields.push({ label: "🖼️ Imagen", value: tour?.imageUrl || "—" });

  return buildLeadSummary("Nueva solicitud de tour", fields);
}

// =========================
// WhatsApp send helpers
// =========================
async function sendWhatsAppText(to, text, reportSource = "BOT") {
  await postWhatsAppMessage({
    messaging_product: "whatsapp",
    to,
    type: "text",
    text: { body: text },
  });

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: String(text),
    source: reportSource,
    kind: "TEXT",
  });
}

async function sendWhatsAppDocument(to, documentUrl, filename, caption = "", reportSource = "BOT", extra = {}) {
  if (!documentUrl && !extra?.mediaId) throw new Error("documentUrl or mediaId is required");
  return await sendWhatsAppMediaMessage({
    to,
    kind: "DOCUMENT",
    mediaUrl: documentUrl,
    mediaId: extra?.mediaId || "",
    mimeType: extra?.mimeType || "",
    filename,
    caption,
    reportSource,
    textFallback: buildMediaFallbackText("DOCUMENT", documentUrl, caption, filename),
  });
}

async function sendWhatsAppImage(to, imageUrl, caption = "", reportSource = "BOT", extra = {}) {
  if (!imageUrl && !extra?.mediaId) throw new Error("imageUrl or mediaId is required");
  return await sendWhatsAppMediaMessage({
    to,
    kind: "IMAGE",
    mediaUrl: imageUrl,
    mediaId: extra?.mediaId || "",
    mimeType: extra?.mimeType || "",
    filename: extra?.filename || "",
    caption,
    reportSource,
    textFallback: buildMediaFallbackText("IMAGE", imageUrl, caption),
  });
}

async function sendWhatsAppAudio(to, audioUrl, { mediaId = "", mimeType = "", filename = "", voice = false, reportSource = "BOT" } = {}) {
  if (!audioUrl && !mediaId) throw new Error("audioUrl or mediaId is required");
  return await sendWhatsAppMediaMessage({
    to,
    kind: "AUDIO",
    mediaUrl: audioUrl,
    mediaId,
    mimeType,
    filename,
    voice,
    reportSource,
    textFallback: buildMediaFallbackText("AUDIO", audioUrl, "", filename),
  });
}

async function sendWhatsAppVideo(to, videoUrl, caption = "", { mediaId = "", mimeType = "", filename = "", reportSource = "BOT" } = {}) {
  if (!videoUrl && !mediaId) throw new Error("videoUrl or mediaId is required");
  return await sendWhatsAppMediaMessage({
    to,
    kind: "VIDEO",
    mediaUrl: videoUrl,
    mediaId,
    mimeType,
    filename,
    caption,
    reportSource,
    textFallback: buildMediaFallbackText("VIDEO", videoUrl, caption, filename),
  });
}

async function sendWhatsAppLocation(to, { latitude, longitude, name = "", address = "", reportSource = "BOT" } = {}) {
  if (!Number.isFinite(Number(latitude)) || !Number.isFinite(Number(longitude))) {
    throw new Error("latitude and longitude are required");
  }

  return await sendWhatsAppMediaMessage({
    to,
    kind: "LOCATION",
    latitude,
    longitude,
    name,
    address,
    reportSource,
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

async function sendInteractiveList(to, { header, body, button, sectionTitle, rows }) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: header },
        body: { text: body },
        footer: { text: BUSINESS_NAME },
        action: { button, sections: [{ title: sectionTitle, rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );
}

async function sendServiceLinesList(to) {
  const rows = SERVICE_LINES
    .filter((s) => s.key !== "ubicacion_contacto")
    .map((s) => ({ id: s.id, title: serviceLineRowTitle(s), description: "" }));
  await sendInteractiveList(to, {
    header: "Servicios disponibles",
    body: "Selecciona el servicio que te interesa 👇",
    button: "Ver opciones",
    sectionTitle: "Servicios",
    rows,
  });
}

async function sendTourOriginsList(to) {
  const rows = TOUR_ORIGINS.map((o) => ({ id: o.id, title: waRowTitle(o.title), description: "" }));
  await sendInteractiveList(to, {
    header: "Origen del tour",
    body: "¿Desde dónde deseas salir? 👇",
    button: "Elegir origen",
    sectionTitle: "Salidas",
    rows,
  });
}

async function sendPackageDestinationsList(to, session) {
  const text = formatPackagesTextList(session);
  await sendWhatsAppText(to, text);
}

async function sendPackagePresentation(to, pkg) {
  if (!pkg) return;
  if (pkg.imageUrl) {
    await sendWhatsAppImage(to, pkg.imageUrl);
  }
  await sendWhatsAppText(to, buildPackageInfoText(pkg));
}

async function sendRealTourGroupsList(to) {
  const rows = getVisibleRealTourGroups().map((g) => ({ id: g.id, title: waRowTitle(g.title), description: "" }));
  await sendInteractiveList(to, {
    header: "Colecciones de tours",
    body: "Selecciona la temporada o colección de excursiones que deseas ver 👇",
    button: "Ver colecciones",
    sectionTitle: "Colecciones disponibles",
    rows,
  });
}

async function sendRealToursByGroup(to, groupKey, session) {
  const text = formatRealToursTextList(groupKey, session);
  await sendWhatsAppText(to, text);
}

async function sendRealTourPresentation(to, tour) {
  if (!tour) return;
  if (tour.imageUrl) {
    await sendWhatsAppImage(to, tour.imageUrl);
  }
  await sendWhatsAppText(to, buildRealTourInfoText(tour));
}

async function sendCatalogDocument(to) {
  if (!CATALOG_DOCUMENT_URL) {
    await sendWhatsAppText(
      to,
      `Todavía no tengo el documento cargado aquí 🙏\n\nMientras tanto, dime si buscas *Tours en República Dominicana*, *Boletos aéreos*, *Hoteles*, *Seguros de viaje*, *Traslados* o *Paquetes vacacionales* y te ayudo por el flujo normal.`
    );
    return;
  }

  await sendWhatsAppDocument(
    to,
    CATALOG_DOCUMENT_URL,
    CATALOG_DOCUMENT_FILENAME,
    CATALOG_DOCUMENT_CAPTION,
    "BOT"
  );
}

// =========================
// Google Calendar
// =========================
function getCalendarClient() {
  const json = safeJson(process.env.GOOGLE_SERVICE_ACCOUNT_JSON, null);
  if (!json?.client_email || !json?.private_key) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON");

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
  children_ages,
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

  const agesText = children > 0 && children_ages ? ` (Edades: ${children_ages})` : "";

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
        `Niños: ${children}${agesText}\n` +
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

async function createRealTourLeadCalendarEvent(session, phone) {
  try {
    if (!GOOGLE_CALENDAR_ID || !process.env.GOOGLE_SERVICE_ACCOUNT_JSON) return;
    const calendar = getCalendarClient();
    const tour = getRealTourByKey(session.pendingRealTourKey);
    const group = getRealTourGroupByKey(tour?.groupKey || session.pendingRealTourGroup);
    const groupContext = getRealTourGroupContext(group?.key || tour?.groupKey || "");
    const dateMeta = getRequestedDateMeta(session.pendingDesiredDate);

    const pax = Number(session.pendingAdults || 0) + Number(session.pendingChildren || 0);
    const agesText = Number(session.pendingChildren || 0) > 0 ? `
Edades de niños: ${session.pendingChildrenAges || "No especificadas"}` : "";

    const summary = `Lead Real Tour: ${tour?.title || "Tour"} - ${session.pendingName || "Cliente"}`;
    const description =
      `Colección: ${group?.title || "—"}
` +
      `Tour: ${tour?.title || "—"}
` +
      `Cliente: ${session.pendingName || "—"}
` +
      `Correo: ${session.pendingEmail || "—"}
` +
      `WhatsApp: ${phone || "—"}
` +
      `Fecha solicitada: ${dateMeta.display || session.pendingDesiredDate || "—"}
` +
      `Pasajeros: ${pax} (${session.pendingAdults || 0} adultos / ${session.pendingChildren || 0} niños)${agesText}
` +
      `${groupContext.locationLabel || "Ubicación"}: ${session.pendingPickup || groupContext.fixedPickup || "—"}
` +
      `Zona: ${session.pendingCity || groupContext.defaultCity || "—"}
` +
      `Estado: Pendiente`;

    await calendar.events.insert({
      calendarId: GOOGLE_CALENDAR_ID,
      requestBody: {
        summary,
        description,
        start: { date: dateMeta.startDate },
        end: { date: dateMeta.endDate },
      },
    });
  } catch (error) {
    console.error("Error creating real tour calendar event", error);
  }
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
    const incomingVariants = new Set(buildPhoneVariants(phone));
    if (!incomingVariants.size) return null;

    const calendar = getCalendarClient();
    const now = new Date();
    const end = addMinutes(now, windowDays * 24 * 60);
    const events = await listReservationEvents(calendar, now.toISOString(), end.toISOString());

    for (const ev of events) {
      const priv = ev.extendedProperties?.private || {};
      if (priv.status === "cancelled") continue;

      const storedVariants = new Set([
        ...buildPhoneVariants(priv.wa_phone),
        ...buildPhoneVariants(priv.wa_id),
      ]);

      const matches = Array.from(storedVariants).some((v) => incomingVariants.has(v));
      if (!matches) continue;

      const start = ev.start?.dateTime;
      const endDT = ev.end?.dateTime;
      if (!start || !endDT) continue;

      return {
        reservation_id: ev.id,
        start,
        end: endDT,
        tour_key: String(priv.tour_key || "").trim() || inferTourFromSummary(ev.summary || ""),
        passenger_name: String(priv.passenger_name || "").trim() || "",
        phone: normalizePhoneDigits(priv.wa_phone || phone),
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

// =========================
// OpenAI fallback
// =========================
async function callOpenAI({ session, userText, userPhone, extraSystem = "" }) {
  if (!OPENAI_API_KEY) {
    const fallback =
      `Puedo ayudarte con *Tours en República Dominicana*, *Boletos aéreos*, *Hoteles*, *Seguros de viaje* y *Traslados*.\n\n` +
      `Escribe *"menú"* para ver las opciones.`;
    session.messages.push({ role: "assistant", content: fallback });
    return fallback;
  }

  const today = new Date();
  const tzParts = getZonedParts(today, BUSINESS_TIMEZONE);
  const todayStr = `${tzParts.year}-${String(tzParts.month).padStart(2, "0")}-${String(tzParts.day).padStart(2, "0")}`;

  const system = {
    role: "system",
    content: `
Eres un asistente de WhatsApp de ${BUSINESS_NAME}.
Servicios:
- Tours en República Dominicana
- Boletos aéreos
- Hoteles
- Seguros de viaje
- Traslados
- Paquetes vacacionales

Reglas:
- Responde corto, claro y orientado a convertir.
- Fecha actual (zona ${BUSINESS_TIMEZONE}): ${todayStr}.
- Si el usuario habla de tours reales del cliente, enfócate en pedir datos para dejar la solicitud lista para contacto y pago.
${extraSystem}
Tel usuario: ${userPhone}.
`,
  };

  session.messages.push({ role: "user", content: userText });
  const messages = [system, ...session.messages].slice(-14);

  const resp = await axios.post(
    "https://api.openai.com/v1/chat/completions",
    {
      model: "gpt-4.1-mini",
      messages,
      temperature: 0.2,
    },
    { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` } }
  );

  const text =
    resp.data?.choices?.[0]?.message?.content?.trim() ||
    "Hola 👋 ¿Qué servicio te interesa? También puedo mostrarte el menú.";
  session.messages.push({ role: "assistant", content: text });
  return text;
}

// =========================
// Webhook verification
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

    const { waTo, text, mediaItems } = parseAgentMessagePayload(req.body || {});
    if (!waTo || !String(waTo).trim()) return res.status(400).json({ error: "waTo is required" });
    if ((!text || !String(text).trim()) && (!Array.isArray(mediaItems) || mediaItems.length === 0)) {
      return res.status(400).json({ error: "text or media is required" });
    }

    const sent = [];

    if (text && String(text).trim()) {
      await sendWhatsAppText(String(waTo), String(text), "AGENT");
      sent.push({ kind: "TEXT" });
    }

    for (const item of mediaItems || []) {
      if (!item?.kind) continue;

      if (item.kind === "IMAGE") {
        const result = await sendWhatsAppImage(String(waTo), item.mediaUrl, item.caption || "", "AGENT", { mediaId: item.mediaId || "", mimeType: item.mimeType || "", filename: item.filename || "" });
        sent.push({ kind: item.kind, ...result });
        continue;
      }

      if (item.kind === "DOCUMENT") {
        const result = await sendWhatsAppDocument(String(waTo), item.mediaUrl, item.filename || "", item.caption || "", "AGENT", { mediaId: item.mediaId || "", mimeType: item.mimeType || "" });
        sent.push({ kind: item.kind, ...result });
        continue;
      }

      if (item.kind === "AUDIO") {
        const result = await sendWhatsAppAudio(String(waTo), item.mediaUrl, {
          mediaId: item.mediaId || "",
          mimeType: item.mimeType || "",
          filename: item.filename || "",
          voice: !!item.voice,
          reportSource: "AGENT",
        });
        sent.push({ kind: item.kind, ...result });
        continue;
      }

      if (item.kind === "VIDEO") {
        const result = await sendWhatsAppVideo(String(waTo), item.mediaUrl, item.caption || "", {
          mediaId: item.mediaId || "",
          mimeType: item.mimeType || "",
          filename: item.filename || "",
          reportSource: "AGENT",
        });
        sent.push({ kind: item.kind, ...result });
        continue;
      }

      if (item.kind === "LOCATION") {
        const result = await sendWhatsAppLocation(String(waTo), {
          latitude: item.latitude,
          longitude: item.longitude,
          name: item.name || "",
          address: item.address || "",
          reportSource: "AGENT",
        });
        sent.push({ kind: item.kind, ...result });
      }
    }

    return res.json({ ok: true, sentCount: sent.length, sent });
  } catch (e) {
    console.error("agent_message error:", e?.response?.data || e?.message || e);
    return res.status(500).json({ error: "Internal error", detail: e?.response?.data || e?.message || "unknown" });
  }
});

app.post("/conversation_mode", async (req, res) => {
  try {
    if (!BOTHUB_WEBHOOK_SECRET) {
      return res.status(400).json({ error: "BOTHUB_WEBHOOK_SECRET not configured" });
    }

    const signature = getHubSignature(req);
    const okSig = verifyHubSignature(req.body, signature, BOTHUB_WEBHOOK_SECRET);

    if (!signature || !okSig) {
      return res.status(401).json({ error: "Invalid signature" });
    }

    const { waTo, mode, note, notifyUser } = req.body || {};
    const normalizedMode = String(mode || "").trim().toLowerCase();
    if (!waTo || !String(waTo).trim()) return res.status(400).json({ error: "waTo is required" });
    if (!["bot", "human"].includes(normalizedMode)) {
      return res.status(400).json({ error: "mode must be 'bot' or 'human'" });
    }

    const target = String(waTo).trim();
    const session = await getSession(target);
    session.conversationMode = normalizedMode;
    await saveSession(target, session);

    if ((typeof notifyUser === "boolean" ? notifyUser : HUMAN_MODE_NOTIFY_USER) === true) {
      if (normalizedMode === "human") {
        await sendWhatsAppText(target, note || "👤 Tu conversación quedó en modo humano. Un asesor continuará por este chat.", "BOT");
      } else {
        await sendWhatsAppText(target, note || "🤖 El asistente virtual quedó reactivado. Puedes escribirme *menú* para ver las opciones.", "BOT");
      }
    }

    return res.json({ ok: true, waTo: target, mode: normalizedMode });
  } catch (e) {
    console.error("conversation_mode error:", e?.response?.data || e?.message || e);
    return res.status(500).json({ error: "Internal error", detail: e?.response?.data || e?.message || "unknown" });
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

    markUserActivity(session);

    const userTextRaw = extractIncomingText(msg);
    const userText = (userTextRaw || "").trim();
    const tNorm = normalizeText(userText);
    if (!userText) return res.sendStatus(200);

    const inboundMeta = extractInboundMeta(msg);
    const inboundMetaWithMediaUrl = attachHubMediaUrl(req, inboundMeta);

    const bothubInboundAck = await bothubReportMessage({
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
    if (bothubInboundAck?.conversationId) session.hubConversationId = String(bothubInboundAck.conversationId);

    if (isHumanModeDisableCommand(tNorm)) {
      session.conversationMode = "bot";
      await saveSession(from, session);
      await sendWhatsAppText(from, "🤖 Listo. Reactivé el asistente virtual. Escribe *menú* para ver las opciones.");
      return res.sendStatus(200);
    }

    if (isHumanModeEnableCommand(tNorm)) {
      session.conversationMode = "human";
      await saveSession(from, session);
      await sendWhatsAppText(from, "👤 Perfecto. Dejo la conversación en modo humano para que continúe un asesor.");
      return res.sendStatus(200);
    }

    if (session.conversationMode === "human") {
      await saveSession(from, session);
      return res.sendStatus(200);
    }

    const detectedServiceLineEarly = detectServiceLineFromUser(userText);
    const detectedOriginEarly = detectOriginKeyFromUser(userText);
    const detectedCategoryEarly = detectCategoryKeyFromUser(userText);
    const detectedTourEarly = detectTourKeyFromUser(userText);
    const detectedRangeEarly = parseDateRangeFromText(userText);
    const detectedRealGroupEarly = detectRealTourGroupFromUser(userText, { allowBareOrigin: true });
    const detectedRealTourEarly = detectRealTourKeyFromUser(userText);

    const hasEarlyIntent =
      !!detectedServiceLineEarly ||
      !!detectedOriginEarly ||
      !!detectedCategoryEarly ||
      !!detectedTourEarly ||
      !!detectedRangeEarly ||
      !!detectedRealGroupEarly ||
      !!detectedRealTourEarly ||
      tNorm.includes("tour") ||
      tNorm.includes("excursion") ||
      tNorm.includes("excursión") ||
      tNorm.includes("reserva") ||
      tNorm.includes("reservar") ||
      tNorm.includes("cotizacion") ||
      tNorm.includes("cotización") ||
      tNorm.includes("boleto") ||
      tNorm.includes("vuelo") ||
      tNorm.includes("seguro") ||
      tNorm.includes("paquete") ||
      tNorm.includes("hotel") ||
      tNorm.includes("traslado");

    if (session.greeted && session.state === "idle" && isGreeting(tNorm) && !hasEarlyIntent) {
      await sendWhatsAppText(from, quickHelpText());
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle" && isGreeting(tNorm) && !hasEarlyIntent) {
      session.greeted = true;
      armMenuInactivityReminder(session);
      await sendWhatsAppText(from, mainMenuText());
      await sendServiceLinesList(from);
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle") session.greeted = true;

    const legacyTourStates = [
      "post_booking",
      "await_day",
      "await_slot_choice",
      "await_adults",
      "await_children",
      "await_children_ages",
      "await_pickup",
      "await_city",
      "await_name",
      "await_phone",
    ];

    if (legacyTourStates.includes(session.state)) {
      clearIntakeFlow(session);
      session.lastBooking = null;
      session.reschedule = defaultSession().reschedule;
    }

    if (
      ["menu", "menú", "inicio", "reiniciar", "reset", "resetear", "empezar de nuevo"].includes(tNorm)
    ) {
      clearIntakeFlow(session);
      session.lastBooking = null;
      session.reschedule = defaultSession().reschedule;
      armMenuInactivityReminder(session);
      await sendWhatsAppText(from, mainMenuText());
      await sendServiceLinesList(from);
      return res.sendStatus(200);
    }

    if (isGoBack(tNorm)) {
      const realTourIntakeStates = [
        "await_real_tour_date",
        "await_real_tour_adults",
        "await_real_tour_children",
        "await_real_tour_children_ages",
        "await_real_tour_pickup",
        "await_real_tour_city",
        "await_real_tour_name",
        "await_real_tour_email",
        "await_real_tour_phone",
      ];

      if (realTourIntakeStates.includes(session.state) && session.pendingRealTourGroup) {
        session.state = "await_real_tour_choice";
        session.pendingRealTourKey = null;
        session.pendingDesiredDate = null;
        session.pendingAdults = null;
        session.pendingChildren = null;
        session.pendingChildrenAges = null;
        session.pendingPickup = null;
        session.pendingCity = null;
        session.pendingName = null;

        await sendWhatsAppText(
          from,
          `↩️ Perfecto. Volviste al listado de tours de *${getRealTourGroupByKey(session.pendingRealTourGroup)?.title || "la colección"}*.`
        );
        await sendRealToursByGroup(from, session.pendingRealTourGroup, session);
        return res.sendStatus(200);
      }

      if (session.state === "await_real_tour_choice") {
        session.state = "await_tour_group";
        session.pendingRealTourKey = null;
        session.pendingDesiredDate = null;
        await sendWhatsAppText(from, `↩️ Perfecto. Volviste al listado de colecciones de tours.`);
        await sendRealTourGroupsList(from);
        return res.sendStatus(200);
      }

      if (session.state === "await_tour_group" || session.pendingServiceLine === "tours_rd") {
        clearIntakeFlow(session);
        armMenuInactivityReminder(session);
        await sendWhatsAppText(from, mainMenuText());
        await sendServiceLinesList(from);
        return res.sendStatus(200);
      }

      if (session.state !== "idle") {
        clearIntakeFlow(session);
        armMenuInactivityReminder(session);
        await sendWhatsAppText(from, mainMenuText());
        await sendServiceLinesList(from);
        return res.sendStatus(200);
      }
    }

    if (detectCatalogRequest(tNorm)) {
      disableMenuInactivityReminder(session);
      await sendCatalogDocument(from);
      return res.sendStatus(200);
    }

    // =========================
    // TOURS FLOW (TOURS REALES DEL CLIENTE)
    // =========================
    if (session.state === "await_tour_group") {
      const groupKey = detectRealTourGroupFromUser(userText, { allowBareOrigin: true });
      if (!groupKey) {
        await sendWhatsAppText(
          from,
          `Perfecto 🌴\nTe compartiré nuestras colecciones de excursiones disponibles.\n\nElige una de estas opciones:\n${getVisibleRealTourGroups().map((g) => `• ${g.title}`).join("\n")}`
        );
        await sendRealTourGroupsList(from);
        return res.sendStatus(200);
      }

      disableMenuInactivityReminder(session);
      session.pendingServiceLine = "tours_rd";
      session.pendingRealTourGroup = groupKey;
      session.state = "await_real_tour_choice";
      await sendRealToursByGroup(from, groupKey, session);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_choice") {
      const anotherGroup = detectRealTourGroupFromUser(userText, { allowBareOrigin: true });
      if (anotherGroup) {
        disableMenuInactivityReminder(session);
        session.pendingRealTourGroup = anotherGroup;
        await sendRealToursByGroup(from, anotherGroup, session);
        return res.sendStatus(200);
      }

      const pickedTour = parseRealTourChoice(session, userText);
      if (!pickedTour) {
        await sendWhatsAppText(
          from,
          `No pude identificar el tour 🙏\nResponde con el *número* o con el *nombre* exacto del tour.`
        );
        return res.sendStatus(200);
      }

      disableMenuInactivityReminder(session);
      session.pendingRealTourKey = pickedTour.key;
      session.pendingRealTourGroup = pickedTour.groupKey;
      updateLead(session, {
        tour_key: pickedTour.key,
        quotePreview: "",
        converted: false,
        followupSent: false,
      });

      session.state = "await_real_tour_date";
      await sendRealTourPresentation(from, pickedTour);
      await sendWhatsAppText(from, buildRealTourDatePrompt(pickedTour));
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_date") {
      const tour = getRealTourByKey(session.pendingRealTourKey);
      const validation = validateRealTourRequestedDate(tour, userText);

      if (!validation.ok) {
        await sendWhatsAppText(from, validation.message || `Por favor, indícame una *fecha válida* para el tour.`);
        return res.sendStatus(200);
      }

      session.pendingDesiredDate = userText;
      session.state = "await_real_tour_adults";
      await sendWhatsAppText(from, `Perfecto 👍
¿Cuántos *adultos* viajarían?`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_adults") {
      const count = parsePassengerCount(userText);
      if (count === null || count < 1) {
        await sendWhatsAppText(from, `Por favor, indícame cuántos *adultos* viajarían. Ej: 2`);
        return res.sendStatus(200);
      }
      session.pendingAdults = count;
      session.state = "await_real_tour_children";
      await sendWhatsAppText(from, `Gracias. Ahora dime cuántos *niños* viajarían. Si no van niños, responde *0*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_children") {
      const count = parsePassengerCount(userText);
      if (count === null || count < 0) {
        await sendWhatsAppText(from, `Indícame cuántos *niños* viajarían. Si no van niños, responde *0*.`);
        return res.sendStatus(200);
      }
      session.pendingChildren = count;

      if (count > 0) {
        session.state = "await_real_tour_children_ages";
        await sendWhatsAppText(
          from,
          count > 1
            ? `Perfecto. Ahora indícame las *edades de los niños separadas por coma*.
Ej: 5, 8`
            : `Perfecto. Ahora indícame la *edad del niño*.`
        );
        return res.sendStatus(200);
      }

      const groupContext = getRealTourGroupContext(session.pendingRealTourGroup || getRealTourByKey(session.pendingRealTourKey)?.groupKey || "");
      session.pendingCity = groupContext.defaultCity || null;
      if (groupContext.askPickup) {
        session.state = "await_real_tour_pickup";
        await sendWhatsAppText(from, groupContext.pickupPrompt);
        return res.sendStatus(200);
      }

      session.pendingPickup = groupContext.fixedPickup || null;
      session.state = "await_real_tour_name";
      await sendWhatsAppText(from, `Perfecto ✅
Ahora indícame tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_children_ages") {
      const ages = normalizeChildrenAgesInput(userText, session.pendingChildren || 0);
      if (!ages.ok) {
        await sendWhatsAppText(from, `Por favor, envíame las *edades de los niños separadas por coma*.
Ej: 5, 8`);
        return res.sendStatus(200);
      }

      session.pendingChildrenAges = ages.formatted;
      const groupContext = getRealTourGroupContext(session.pendingRealTourGroup || getRealTourByKey(session.pendingRealTourKey)?.groupKey || "");
      session.pendingCity = groupContext.defaultCity || null;

      if (groupContext.askPickup) {
        session.state = "await_real_tour_pickup";
        await sendWhatsAppText(from, groupContext.pickupPrompt);
        return res.sendStatus(200);
      }

      session.pendingPickup = groupContext.fixedPickup || null;
      session.state = "await_real_tour_name";
      await sendWhatsAppText(from, `Perfecto ✅
Ahora indícame tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_pickup") {
      const groupContext = getRealTourGroupContext(session.pendingRealTourGroup || getRealTourByKey(session.pendingRealTourKey)?.groupKey || "");
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, groupContext.pickupPrompt || `Por favor, indícame tu *ubicación* o *punto de salida*.`);
        return res.sendStatus(200);
      }
      session.pendingPickup = userText;
      session.pendingCity = groupContext.defaultCity || session.pendingCity || null;
      session.state = "await_real_tour_name";
      await sendWhatsAppText(from, `Perfecto ✅
Ahora indícame tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_city") {
      const groupContext = getRealTourGroupContext(session.pendingRealTourGroup || getRealTourByKey(session.pendingRealTourKey)?.groupKey || "");
      session.pendingCity = groupContext.defaultCity || userText;
      session.state = "await_real_tour_name";
      await sendWhatsAppText(from, `Perfecto ✅
Ahora indícame tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_name") {
      if (tNorm.length < 3 || ["si", "sí", "ok", "listo"].includes(tNorm)) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }
      session.pendingName = userText;
      session.state = "await_real_tour_email";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *correo electrónico* para dejar la solicitud lista.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_email" || session.state === "await_real_tour_phone") {
      const email = String(userText || "").trim();
      if (!isValidEmail(email)) {
        await sendWhatsAppText(from, `Ese correo parece inválido 🙏
Envíamelo así: nombre@correo.com`);
        return res.sendStatus(200);
      }

      session.pendingEmail = email;
      const contactPhone = String(from || "").replace(/[^\d]/g, "");
      const tour = getRealTourByKey(session.pendingRealTourKey);
      const groupContext = getRealTourGroupContext(session.pendingRealTourGroup || tour?.groupKey || "");
      const dateMeta = getRequestedDateMeta(session.pendingDesiredDate);
      const summaryText = buildRealTourLeadSummary(session, contactPhone);

      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, contactPhone);
      await createRealTourLeadCalendarEvent(session, contactPhone);

      session.lead = {
        ...defaultLead(),
        tour_key: session.pendingRealTourKey || "",
        followupSent: true,
        converted: true,
        quotePreview: summaryText,
        lastInteractionAt: new Date().toISOString(),
      };

      const agesMsg = Number(session.pendingChildren || 0) > 0 ? `
👶 Edades: ${session.pendingChildrenAges}` : "";
      const locationLine = `📍 ${groupContext.locationLabel || "Ubicación"}: ${session.pendingPickup || groupContext.fixedPickup || "—"}`;

      await sendWhatsAppText(
        from,
        `✅ *Solicitud de tour recibida*

` +
          `🌴 Tour: *${tour?.title || "—"}*
` +
          `📅 Fecha solicitada: *${dateMeta.display || session.pendingDesiredDate || "—"}*
` +
          `👥 Pasajeros: *${Number(session.pendingAdults || 0) + Number(session.pendingChildren || 0)}* (${session.pendingAdults || 0} adultos / ${session.pendingChildren || 0} niños)${agesMsg}
` +
          `${locationLine}
` +
          `📧 Correo: ${session.pendingEmail || "—"}

` +
          `Tu solicitud quedó casi lista. Ahora un asesor de la agencia te contactará para confirmar disponibilidad, validarte el monto final y gestionar el pago.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // SIMPLE SERVICE FLOWS
    // =========================
    if (await handleSimpleServiceFlow({ session, from, userText, tNorm })) {
      return res.sendStatus(200);
    }

    // =========================
    // FAST ROUTES / MENUS
    // =========================
    if (tNorm.includes("menu") || tNorm.includes("menú") || tNorm.includes("servicios") || tNorm.includes("ver opciones") || tNorm.includes("inicio")) {
      armMenuInactivityReminder(session);
      await sendWhatsAppText(from, mainMenuText());
      await sendServiceLinesList(from);
      return res.sendStatus(200);
    }

    if (tNorm.includes("categorias") || tNorm.includes("categorías") || tNorm.includes("ver tours")) {
      disableMenuInactivityReminder(session);
      session.pendingServiceLine = "tours_rd";
      await routeConversationToQueue({ session, phone: from, queueName: HUB_QUEUE_SERVICE_TOURS, reason: "tours_rd" });
      session.state = "await_tour_group";
      await sendWhatsAppText(from, categoriesEmojiText());
      await sendRealTourGroupsList(from);
      return res.sendStatus(200);
    }

    const directRealTourGroup = detectRealTourGroupFromUser(userText);
    if (directRealTourGroup) {
      clearIntakeFlow(session);
      disableMenuInactivityReminder(session);
      session.pendingServiceLine = "tours_rd";
      await routeConversationToQueue({ session, phone: from, queueName: HUB_QUEUE_SERVICE_TOURS, reason: "tours_rd" });
      session.pendingRealTourGroup = directRealTourGroup;
      session.state = "await_real_tour_choice";
      await sendWhatsAppText(from, `Perfecto 🌴
Aquí tienes las excursiones disponibles en *${getRealTourGroupByKey(directRealTourGroup)?.title || "Tours"}*.`);
      await sendRealToursByGroup(from, directRealTourGroup, session);
      return res.sendStatus(200);
    }

    const directRealTourKey = detectRealTourKeyFromUser(userText);
    if (directRealTourKey) {
      const tour = getRealTourByKey(directRealTourKey);
      clearIntakeFlow(session);
      disableMenuInactivityReminder(session);
      session.pendingServiceLine = "tours_rd";
      await routeConversationToQueue({ session, phone: from, queueName: HUB_QUEUE_SERVICE_TOURS, reason: "tours_rd" });
      session.pendingRealTourGroup = tour?.groupKey || null;
      session.pendingRealTourKey = directRealTourKey;
      session.state = "await_real_tour_date";
      updateLead(session, { tour_key: directRealTourKey, quotePreview: "", converted: false, followupSent: false });
      await sendRealTourPresentation(from, tour);
      await sendWhatsAppText(from, `📅 Si deseas agendar *${tour?.title || "este tour"}*, dime la *fecha* o *salida* que te interesa y seguimos con tu solicitud.`);
      return res.sendStatus(200);
    }

    const directPackageKey = detectPackageDestinationKeyFromUser(userText);
    if (directPackageKey) {
      const pkg = getPackageDestinationByKey(directPackageKey);
      clearIntakeFlow(session);
      disableMenuInactivityReminder(session);
      session.pendingServiceLine = "paquetes_vacacionales";
      await routeConversationToQueue({ session, phone: from, queueName: HUB_QUEUE_COMMERCIAL, reason: "paquetes_vacacionales" });
      session.pendingDestination = pkg?.title || userText;
      session.pendingPackageKey = directPackageKey;
      session.state = "await_package_date";
      await sendPackagePresentation(from, pkg);
      await sendWhatsAppText(from, `📅 Si deseas agendar *${pkg?.title || "este paquete"}*, dime la *fecha* o *temporada* que te interesa y seguimos con tu solicitud.`);
      return res.sendStatus(200);
    }

    const serviceLineKey = detectServiceLineFromUser(userText);
    if (serviceLineKey) {
      clearIntakeFlow(session);
      disableMenuInactivityReminder(session);
      session.pendingServiceLine = serviceLineKey;

      if (["tours_rd", "boletos_aereos", "solo_hoteles", "seguros_viaje", "traslados", "hablar_asesor"].includes(serviceLineKey)) {
        await routeConversationToQueue({ session, phone: from, queueName: HUB_QUEUE_SERVICE_TOURS, reason: serviceLineKey });
      } else if (serviceLineKey === "paquetes_vacacionales") {
        await routeConversationToQueue({ session, phone: from, queueName: HUB_QUEUE_COMMERCIAL, reason: serviceLineKey });
      }

      if (serviceLineKey === "catalogo_pdf") {
        await sendCatalogDocument(from);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "ubicacion_contacto") {
        await sendWhatsAppText(from, buildLocationContactText());
        return res.sendStatus(200);
      }

      if (serviceLineKey === "tours_rd") {
        session.state = "await_tour_group";
        await sendWhatsAppText(from, categoriesEmojiText());
        await sendRealTourGroupsList(from);
        return res.sendStatus(200);
      }

      if (await startSimpleServiceFlow({ session, serviceLineKey, from })) {
        return res.sendStatus(200);
      }
    }

    // =========================
    // FALLBACK OPENAI
    // =========================
    const reply = await callOpenAI({
      session,
      userText,
      userPhone: from,
      extraSystem:
        session.pendingRealTourKey
          ? `Nota: el tour real actual pendiente es ${session.pendingRealTourKey}.`
          : session.pendingRealTourGroup
          ? `Nota: la colección actual pendiente es ${session.pendingRealTourGroup}.`
          : session.pendingServiceLine
          ? `Nota: el servicio actual pendiente es ${session.pendingServiceLine}.`
          : "",
    });

    await sendWhatsAppText(from, reply);
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

      const tour = getAnyTourByKey(lead.tour_key);
      if (!tour) continue;

      const msg =
        `Hola 👋 Quedó pendiente tu solicitud para *${tour.title}*.\n\n` +
        `Si deseas, te ayudo a completarla. Solo responde con la fecha que te interesa 😊`;

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
    if (!GOOGLE_CALENDAR_ID || !process.env.GOOGLE_SERVICE_ACCOUNT_JSON) return;

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
      const tour = getAnyTourByKey(String(priv.tour_key || "").trim());
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

async function notifyPersonalWhatsAppLeadSummary(summaryText, customerPhone = "") {
  try {
    const leadPhone = String(customerPhone || "").replace(/[^\d]/g, "");
    const targets = Array.from(
      new Set(
        [ADMIN_WA_TO, PERSONAL_WA_TO]
          .map((v) => String(v || "").replace(/[^\d]/g, ""))
          .filter(Boolean)
      )
    ).filter((to) => !(leadPhone && to === leadPhone));

    if (!targets.length) {
      console.warn("notifyPersonalWhatsAppLeadSummary skipped: no admin destination configured");
      return;
    }

    const adminMessage = `📥 *Resumen admin*

${summaryText}`;
    for (const to of targets) {
      await sendWhatsAppText(to, adminMessage, "BOT");
    }
  } catch (e) {
    console.error("notifyPersonalWhatsAppLeadSummary error:", e?.response?.data || e?.message || e);
  }
}

async function menuInactivityReminderLoop() {
  try {
    if (!MENU_INACTIVITY_REMINDER_ENABLED) return;

    const ids = await listAllSessionIds();
    const now = Date.now();
    const maxAgeMs = MENU_REMINDER_MAX_AGE_HOURS * 60 * 60 * 1000;
    const reminder1Ms = MENU_REMINDER_1_AFTER_MIN * 60 * 1000;
    const reminder2Ms = MENU_REMINDER_2_AFTER_MIN * 60 * 1000;

    for (const id of ids) {
      const s = await getSession(id);
      const reminder = s?.menuReminder || {};
      if (!reminder.active || !reminder.menuShownAt) continue;
      if (s?.pendingServiceLine || s?.state !== "idle") {
        disableMenuInactivityReminder(s);
        await saveSession(id, s);
        continue;
      }
      if (hasUserRespondedAfterMenu(s)) {
        disableMenuInactivityReminder(s);
        await saveSession(id, s);
        continue;
      }

      const shownAtMs = Date.parse(reminder.menuShownAt || "");
      if (!Number.isFinite(shownAtMs)) {
        disableMenuInactivityReminder(s);
        await saveSession(id, s);
        continue;
      }

      const ageMs = now - shownAtMs;
      if (ageMs < 0 || ageMs > maxAgeMs) {
        disableMenuInactivityReminder(s);
        await saveSession(id, s);
        continue;
      }

      if (!reminder.reminder1Sent && ageMs >= reminder1Ms) {
        const msg = `Hola 👋 Quedamos disponibles para ayudarte con tu viaje.

Escribe *menú* y te muestro las opciones disponibles.`;
        try {
          await sendWhatsAppText(id, msg, "BOT");
          s.menuReminder.reminder1Sent = true;
          await saveSession(id, s);
        } catch (e) {
          console.error("menu reminder 1 send error:", id, e?.response?.data || e?.message || e);
        }
        continue;
      }

      if (reminder.reminder1Sent && !reminder.reminder2Sent && ageMs >= reminder2Ms) {
        const msg = `Seguimos aquí para ayudarte con tours, hoteles, boletos, seguros o traslados.

Si deseas retomar, escribe *menú*.`;
        try {
          await sendWhatsAppText(id, msg, "BOT");
          s.menuReminder.reminder2Sent = true;
          disableMenuInactivityReminder(s);
          await saveSession(id, s);
        } catch (e) {
          console.error("menu reminder 2 send error:", id, e?.response?.data || e?.message || e);
        }
      }
    }
  } catch (e) {
    console.error("menuInactivityReminderLoop error:", e?.response?.data || e?.message || e);
  }
}

app.get("/tick", async (_req, res) => {
  try {
    await reminderLoop();
    await followupLeadsLoop();
    await menuInactivityReminderLoop();
  } catch {}
  return res.status(200).send("tick ok");
});

app.listen(PORT, () => console.log(`Bot running on :${PORT}`));
