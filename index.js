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

const PERSONAL_WA_TO = (process.env.PERSONAL_WA_TO || "").trim();
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

const BOT_PUBLIC_BASE_URL = (process.env.BOT_PUBLIC_BASE_URL || "").replace(/\/$/, "");
const HUB_MEDIA_SECRET =
  (process.env.HUB_MEDIA_SECRET || BOTHUB_WEBHOOK_SECRET || VERIFY_TOKEN || "").trim();
const HUB_MEDIA_TTL_SEC = parseInt(process.env.HUB_MEDIA_TTL_SEC || "900", 10);
const META_GRAPH_VERSION =
  process.env.WHATSAPP_GRAPH_VERSION || process.env.META_GRAPH_VERSION || "v23.0";

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

// =========================
// MENÚ PRINCIPAL
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
  { key: "santiago", id: "org_santiago", title: "Santiago" },
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

// =========================
// TOURS REALES DEL CLIENTE
// =========================
const REAL_TOUR_GROUPS = [
  { key: "tours_punta_cana", id: "rtg_punta_cana", title: "Tours desde Punta Cana" },
  { key: "tours_santo_domingo", id: "rtg_santo_domingo", title: "Tours desde Santo Domingo" },
  { key: "tours_santiago", id: "rtg_santiago", title: "Tours desde Santiago" },
  { key: "tours_las_terrenas", id: "rtg_las_terrenas", title: "Tours desde Las Terrenas" },
  { key: "tours_semana_santa", id: "rtg_semana_santa", title: "Tours Semana Santa" },
];

const REAL_TOUR_GROUP_ID_TO_KEY = Object.fromEntries(
  REAL_TOUR_GROUPS.map((g) => [g.id, g.key])
);

function buildRealToursCatalog() {
  return [
    // TOURS DESDE SANTO DOMINGO
    {
      key: "sd_santa_fe_full_day",
      id: "rt_sd_santa_fe_full_day",
      title: "Santa Fe Full Day",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428140/Santa_Fe_full_day_k2twpq.jpg",
      leadOnly: true,
    },
    {
      key: "sd_rio_y_playas_san_juan",
      id: "rt_sd_rio_y_playas_san_juan",
      title: "Río y Playas San Juan",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428139/Rio_y_playas_san_juan_ivhnev.jpg",
      leadOnly: true,
    },
    {
      key: "sd_parapente_jarabacoa",
      id: "rt_sd_parapente_jarabacoa",
      title: "Parapente Jarabacoa",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428138/Parapente_Jarabacoa_itfvyv.jpg",
      leadOnly: true,
    },
    {
      key: "sd_ocean_world_confresi",
      id: "rt_sd_ocean_world_confresi",
      title: "Ocean World Cofresí",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428138/Ocean_world_confresi_punta_cana_wdbjq8.jpg",
      leadOnly: true,
    },
    {
      key: "sd_jarabacoa_fourwheel",
      id: "rt_sd_jarabacoa_fourwheel",
      title: "Jarabacoa Fourwheel",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428136/Jarabacoa_Fourwheel_doakpy.jpg",
      leadOnly: true,
    },
    {
      key: "sd_jarabacoa_city_tours",
      id: "rt_sd_jarabacoa_city_tours",
      title: "Jarabacoa City Tours",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428135/Jarabacoa_City_Tours_lzxkux.jpg",
      leadOnly: true,
    },
    {
      key: "sd_jarabacoa_city_polaris",
      id: "rt_sd_jarabacoa_city_polaris",
      title: "Jarabacoa City Polaris",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428134/Jarabacoa_city_polaris_y7aea1.jpg",
      leadOnly: true,
    },
    {
      key: "sd_isla_saona",
      id: "rt_sd_isla_saona",
      title: "Isla Saona",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428133/Isla_Saona_mcvfid.jpg",
      leadOnly: true,
    },
    {
      key: "sd_fourwheel_punta_cana",
      id: "rt_sd_fourwheel_punta_cana",
      title: "Fourwheel Punta Cana",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428133/Fourwheel_punta_cana_v8lw1l.jpg",
      leadOnly: true,
    },
    {
      key: "sd_cayo_arena",
      id: "rt_sd_cayo_arena",
      title: "Cayo Arena",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428132/Cayo_arena_twyhw9.jpg",
      leadOnly: true,
    },
    {
      key: "sd_ballenas_jorobadas",
      id: "rt_sd_ballenas_jorobadas",
      title: "Ballenas Jorobadas",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428131/Ballenas_Jorobada_rv0ioc.jpg",
      leadOnly: true,
    },
    {
      key: "sd_cayo_levantado",
      id: "rt_sd_cayo_levantado",
      title: "Cayo Levantado",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428131/Cayo_levantado_mxh6gv.jpg",
      leadOnly: true,
    },
    {
      key: "sd_buggies_punta_cana",
      id: "rt_sd_buggies_punta_cana",
      title: "Buggies Punta Cana",
      groupKey: "tours_santo_domingo",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428130/Buggies_punta_cana_wcqwdl.jpg",
      leadOnly: true,
    },

    // TOURS DESDE SANTIAGO (Ejemplo inicial para escalabilidad)
    {
      key: "santiago_city_tour",
      id: "rt_santiago_city_tour",
      title: "Santiago City Tour",
      groupKey: "tours_santiago",
      imageUrl: "", // Puedes agregar el link aquí en el futuro
      leadOnly: true,
    },
    {
      key: "santiago_cayo_arena",
      id: "rt_santiago_cayo_arena",
      title: "Cayo Arena desde Santiago",
      groupKey: "tours_santiago",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773428132/Cayo_arena_twyhw9.jpg",
      leadOnly: true,
    },

    // TOURS DESDE LAS TERRENAS (Ejemplo inicial para escalabilidad)
    {
      key: "terrenas_los_haitises",
      id: "rt_terrenas_los_haitises",
      title: "Parque Nacional Los Haitises",
      groupKey: "tours_las_terrenas",
      imageUrl: "", // Puedes agregar el link aquí en el futuro
      leadOnly: true,
    },
    {
      key: "terrenas_cascada_limon",
      id: "rt_terrenas_cascada_limon",
      title: "Salto El Limón",
      groupKey: "tours_las_terrenas",
      imageUrl: "", // Puedes agregar el link aquí en el futuro
      leadOnly: true,
    },

    // TOURS DESDE PUNTA CANA
    {
      key: "pc_scoobadoo",
      id: "rt_pc_scoobadoo",
      title: "Scoobadoo",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427504/Scoobadoo_vjqbif.jpg",
      leadOnly: true,
    },
    {
      key: "pc_polaris",
      id: "rt_pc_polaris",
      title: "Polaris",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427504/Polaris_hgbvqi.jpg",
      leadOnly: true,
    },
    {
      key: "pc_maroca",
      id: "rt_pc_maroca",
      title: "Maroca",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427503/Maroca_hzzyps.jpg",
      leadOnly: true,
    },
    {
      key: "pc_jet_ski",
      id: "rt_pc_jet_ski",
      title: "Jet Ski",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427503/Jet-ski_kfxska.jpg",
      leadOnly: true,
    },
    {
      key: "pc_jet_cars",
      id: "rt_pc_jet_cars",
      title: "Jet Cars",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427502/Jet-cars_pu2p3w.jpg",
      leadOnly: true,
    },
    {
      key: "pc_isla_catalina",
      id: "rt_pc_isla_catalina",
      title: "Isla Catalina",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427501/Isla_Catalina_kavssn.jpg",
      leadOnly: true,
    },
    {
      key: "pc_horseback_riding",
      id: "rt_pc_horseback_riding",
      title: "Horseback Riding",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427501/Horseback_Riding_fwojde.jpg",
      leadOnly: true,
    },
    {
      key: "pc_fourwheel",
      id: "rt_pc_fourwheel",
      title: "Fourwheel",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Fourwheel_cixu6i.jpg",
      leadOnly: true,
    },
    {
      key: "pc_dorado_park",
      id: "rt_pc_dorado_park",
      title: "Dorado Park",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Dorado_Park_p8unjz.jpg",
      leadOnly: true,
    },
    {
      key: "pc_dolphin_ocean_adventure",
      id: "rt_pc_dolphin_ocean_adventure",
      title: "Dolphin Ocean Adventure",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Dolphin_ocean_aventure_tzzspl.jpg",
      leadOnly: true,
    },
    {
      key: "pc_coco_bongo",
      id: "rt_pc_coco_bongo",
      title: "Coco Bongo",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Coco_Bongo_dknp2w.jpg",
      leadOnly: true,
    },
    {
      key: "pc_cayo_new",
      id: "rt_pc_cayo_new",
      title: "Cayo New",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427500/Cayo_New_m0ke20.jpg",
      leadOnly: true,
    },
    {
      key: "pc_buggies",
      id: "rt_pc_buggies",
      title: "Buggies",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/Buggies_d3s2th.jpg",
      leadOnly: true,
    },
    {
      key: "pc_jet_ski_aqua_kart_polaris",
      id: "rt_pc_jet_ski_aqua_kart_polaris",
      title: "Jet Ski + Aqua Kart + Polaris",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/3-_Jet-sky_Aqua-kart_Polaris_lm2sht.jpg",
      leadOnly: true,
    },
    {
      key: "pc_jet_ski_aqua_kart",
      id: "rt_pc_jet_ski_aqua_kart",
      title: "Jet Ski + Aqua Kart",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/2_-Jet-skit_Aqua-kart_cxpyzj.jpg",
      leadOnly: true,
    },
    {
      key: "pc_boat_party",
      id: "rt_pc_boat_party",
      title: "Boat Party",
      groupKey: "tours_punta_cana",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427499/Boat_Party_g3iycw.jpg",
      leadOnly: true,
    },

    // TOURS SEMANA SANTA
    {
      key: "ss_polaris",
      id: "rt_ss_polaris",
      title: "Polaris",
      groupKey: "tours_semana_santa",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427803/Polaris_mlhvmz.jpg",
      leadOnly: true,
    },
    {
      key: "ss_playa_dominicus",
      id: "rt_ss_playa_dominicus",
      title: "Playa Dominicus",
      groupKey: "tours_semana_santa",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427803/Playa_Dominicus_uj7pg0.jpg",
      leadOnly: true,
    },
    {
      key: "ss_jet_ski",
      id: "rt_ss_jet_ski",
      title: "Jet Ski",
      groupKey: "tours_semana_santa",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427802/Jet-ski_wr0dk5.jpg",
      leadOnly: true,
    },
    {
      key: "ss_isla_saona_2",
      id: "rt_ss_isla_saona_2",
      title: "Isla Saona 2",
      groupKey: "tours_semana_santa",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427802/Isla_Saona2_z0kre2.jpg",
      leadOnly: true,
    },
    {
      key: "ss_isla_saona",
      id: "rt_ss_isla_saona",
      title: "Isla Saona",
      groupKey: "tours_semana_santa",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427801/Isla_Saona_sndbbm.jpg",
      leadOnly: true,
    },
    {
      key: "ss_aqua_kart",
      id: "rt_ss_aqua_kart",
      title: "Aqua Kart",
      groupKey: "tours_semana_santa",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427801/Aqua-kart_krqxuj.jpg",
      leadOnly: true,
    },
    {
      key: "ss_isla_catalina",
      id: "rt_ss_isla_catalina",
      title: "Isla Catalina",
      groupKey: "tours_semana_santa",
      imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773427800/Isla_Catalina_hxfyjm.jpg",
      leadOnly: true,
    },
  ];
}

const REAL_TOURS = safeJson(process.env.REAL_TOUR_CATALOG_JSON, null) || buildRealToursCatalog();
const REAL_TOUR_ID_TO_KEY = Object.fromEntries(REAL_TOURS.map((t) => [t.id, t.key]));

const REAL_TOUR_TEXT_OVERRIDES = {
  "pc_scoobadoo": {
    "priceText": "Desde US$85 por adulto.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado disponible desde tu hotel en Punta Cana.",
    "includesText": "Experiencia Scoobadoo sumergible, snorkel para ver corales, barco panorámico y snack incluido.",
    "noteText": "La imagen promocional muestra una salida diaria para esta experiencia."
  },
  "pc_polaris": {
    "priceText": "Doble US$89 / Familiar US$249.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Experiencia en Polaris, baño en Macao, visita a cueva taína y casa típica.",
    "noteText": "La pieza promocional oficial corresponde a una salida diaria desde Punta Cana."
  },
  "pc_maroca": {
    "priceText": "US$65 por persona.",
    "dateText": "Open bar.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Admisión y experiencia nocturna con open bar según la promoción publicada.",
    "noteText": "La imagen muestra esta opción como una salida de entretenimiento en Punta Cana."
  },
  "pc_jet_ski": {
    "priceText": "US$99 por adulto.",
    "dateText": "Promoción nueva.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Experiencia Jet Ski y actividades acuáticas según la promoción visual.",
    "noteText": "La imagen oficial destaca esta excursión como promoción nueva."
  },
  "pc_jet_cars": {
    "priceText": "US$165 por adulto.",
    "dateText": "Promoción nueva.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Experiencia Jet Cars y actividades acuáticas según la promoción publicada.",
    "noteText": "La imagen oficial muestra esta opción como novedad dentro del catálogo."
  },
  "pc_isla_catalina": {
    "priceText": "Adultos US$85 / Niños US$65.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Almuerzo buffet, bebida, catamarán y snorkel para corales.",
    "noteText": "La pieza oficial publicada por la agencia corresponde a una salida diaria."
  },
  "pc_horseback_riding": {
    "priceText": "US$75 por adulto.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Paseo a caballo, playa Macao, desembocadura del río Anamuya y casa típica.",
    "noteText": "La imagen promocional presenta esta excursión con salida diaria."
  },
  "pc_fourwheel": {
    "priceText": "1 persona US$75 / 2 personas US$90.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Experiencia Fourwheel, baño en Macao, visita a cueva taína y casa típica.",
    "noteText": "La promoción oficial indica salidas diarias para esta aventura."
  },
  "pc_dorado_park": {
    "priceText": "Adultos US$129 / Niños US$69.",
    "dateText": "Jueves a domingo.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Acceso al parque, playa artificial y atracciones mostradas en la promoción oficial.",
    "noteText": "La imagen de la agencia muestra este producto con disponibilidad de jueves a domingo."
  },
  "pc_dolphin_ocean_adventure": {
    "priceText": "Encounters US$120 / Swim US$169 / Royal Swim US$239.",
    "dateText": "Martes, jueves y sábados.",
    "durationText": "Duración estimada: 5 horas.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Programas con delfines según el paquete elegido: Encounter, Swim o Royal Swim.",
    "noteText": "La pieza promocional muestra distintas modalidades y precios para esta experiencia."
  },
  "pc_coco_bongo": {
    "priceText": "Regular US$90 / Gold Member US$170 / Front Row US$190.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Admisión, show variado, snacks según plan y mesas según plan.",
    "noteText": "La promoción oficial presenta 3 paquetes para disfrutar Coco Bongo."
  },
  "pc_cayo_new": {
    "priceText": "Adultos US$160 / Niños US$149.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "La imagen promocional muestra comida, bebida y recorrido a playa/cayo con actividades incluidas.",
    "noteText": "La pieza oficial indica salida diaria para esta excursión."
  },
  "pc_buggies": {
    "priceText": "Doble US$85 / Familiar US$140.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Experiencia en buggies, baño en Macao, visita a cueva taína y casa típica.",
    "noteText": "La promoción oficial publicada por la agencia indica salida diaria."
  },
  "pc_jet_ski_aqua_kart_polaris": {
    "priceText": "US$169 por adulto.",
    "dateText": "Promoción nueva.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Combo con Jet Ski, Aqua Kart y Polaris según la pieza promocional de la agencia.",
    "noteText": "La imagen oficial presenta esta excursión como paquete combinado."
  },
  "pc_jet_ski_aqua_kart": {
    "priceText": "US$129 por adulto.",
    "dateText": "Promoción nueva.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Combo con Jet Ski y Aqua Kart según la promoción publicada por la agencia.",
    "noteText": "La pieza oficial presenta esta excursión como paquete combinado."
  },
  "pc_boat_party": {
    "priceText": "Adultos US$85 / Niños US$60.",
    "dateText": "Todos los días.",
    "pickupText": "Traslado desde tu hotel en Punta Cana.",
    "includesText": "Paseo en barco, snorkel para corales y piscina natural según la promoción oficial.",
    "noteText": "La imagen compartida por la agencia indica salida diaria para esta experiencia."
  },
  // TOURS DESDE SANTO DOMINGO
  "sd_santa_fe_full_day": {
    "priceText": "RD$3,750 adultos / RD$3,300 niños.",
    "includesText": "Transporte, desayuno, almuerzo, piscina, city tours y visita a Calles de las Sombrillas.",
  },
  "sd_rio_y_playas_san_juan": {
    "priceText": "RD$2,899 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, playa y visita a El Portón según la promoción oficial.",
  },
  "sd_parapente_jarabacoa": {
    "priceText": "RD$4,950 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, parapente, city tours y balnearios.",
  },
  "sd_ocean_world_confresi": {
    "priceText": "RD$3,750 adultos / RD$3,250 niños.",
    "includesText": "Transporte, desayuno, almuerzo, piscina, city tours y experiencia en Ocean World.",
    "noteText": "La pieza promocional indica niños de 0 a 3 años gratis."
  },
  "sd_jarabacoa_fourwheel": {
    "priceText": "RD$3,950 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, Fourwheel, city tours y balnearios.",
  },
  "sd_jarabacoa_city_tours": {
    "priceText": "RD$2,790 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, city tours y balnearios.",
  },
  "sd_jarabacoa_city_polaris": {
    "priceText": "RD$4,950 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, Polaris y balnearios.",
  },
  "sd_isla_saona": {
    "priceText": "Adultos RD$3,850 / Niños RD$3,400.",
    "includesText": "Transporte, desayuno, almuerzo, catamarán y piscina natural.",
  },
  "sd_fourwheel_punta_cana": {
    "priceText": "RD$3,450 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, playa Macao, cueva taína y casa típica.",
    "noteText": "La promoción indica 12+1 gratis para grupos."
  },
  "sd_cayo_arena": {
    "priceText": "RD$3,350 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, playa, lancha rápida y manglares.",
  },
  "sd_ballenas_jorobadas": {
    "priceText": "RD$3,950 por adulto.",
    "includesText": "Transporte, desayuno, almuerzo, lancha y visita a Cayo Levantado.",
    "noteText": "La imagen promocional corresponde a la temporada de ballenas jorobadas."
  },
  "sd_cayo_levantado": {
    "priceText": "RD$2,950 por persona.",
    "includesText": "Transporte, desayuno, almuerzo y visita a Cayo Levantado.",
  },
  "sd_buggies_punta_cana": {
    "priceText": "RD$3,299 por persona.",
    "includesText": "Transporte, desayuno, almuerzo, playa Macao, cueva taína y casa típica.",
  },
  "ss_polaris": {
    "priceText": "RD$4,750 por persona.",
    "reserveText": "Reserva con 50%.",
    "includesText": "Transporte, desayuno, almuerzo, Polaris y zipline.",
    "noteText": "La pieza promocional compartida por la agencia corresponde a la colección de Semana Santa."
  },
  "ss_playa_dominicus": {
    "priceText": "RD$2,550 adultos.",
    "includesText": "Transporte, desayuno, almuerzo, playa, letrero y experiencia en Playa Dominicus.",
    "noteText": "La promoción oficial corresponde a una salida especial de Semana Santa."
  },
  "ss_jet_ski": {
    "priceText": "RD$4,750 por persona.",
    "reserveText": "Reserva con 50%.",
    "includesText": "Transporte, desayuno, almuerzo, Jet Ski y zipline.",
    "noteText": "La pieza oficial corresponde a la colección de Semana Santa."
  },
  "ss_isla_saona_2": {
    "priceText": "Adultos RD$3,850 / Niños RD$3,350.",
    "includesText": "Transporte, desayuno, almuerzo, barco, catamarán y piscina natural.",
    "noteText": "La promoción publicada corresponde a la colección de Semana Santa."
  },
  "ss_isla_saona": {
    "priceText": "RD$3,850 por persona.",
    "includesText": "Transporte, desayuno, almuerzo, barco y catamarán.",
    "noteText": "La pieza oficial muestra esta salida como súper promo de Semana Santa."
  },
  "ss_aqua_kart": {
    "priceText": "RD$4,750 por persona.",
    "reserveText": "Reserva con 50%.",
    "includesText": "Transporte, desayuno, almuerzo, Aqua Kart y zipline.",
    "noteText": "La pieza promocional corresponde a la colección de Semana Santa."
  },
  "ss_isla_catalina": {
    "priceText": "Adultos RD$3,750 / Niños RD$3,150.",
    "includesText": "Transporte, desayuno, almuerzo, catamarán y snorkel para corales.",
    "noteText": "La imagen oficial publicada por la agencia corresponde a la colección de Semana Santa."
  }
};


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

  const maybeStringOrNull = [
    "pendingServiceLine",
    "pendingOrigin",
    "pendingCategory",
    "pendingTour",
    "pendingPickup",
    "pendingCity",
    "pendingName",
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
// TOURS RD LEGACY (SE CONSERVA CONFIGURACIÓN)
// =========================
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
      origins: ["santo_domingo"],
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
      origins: ["santo_domingo", "punta_cana"],
      description: "Excursión de día completo con playa, lancha/catamarán y ambiente caribeño.",
      durationMin: 720,
      durationLabel: "Día completo",
      basePriceAdult: 95,
      basePriceChild: 75,
      capacity: 24,
      meetingPoint: "Punto de salida coordinado según zona",
      pickupOptions: "Santo Domingo, Boca Chica, La Romana, Punta Cana",
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
      origins: ["santo_domingo", "punta_cana"],
      description: "Tour ideal para disfrutar de playa, snorkeling y día relajado.",
      durationMin: 660,
      durationLabel: "Día completo",
      basePriceAdult: 89,
      basePriceChild: 69,
      capacity: 20,
      meetingPoint: "Punto de salida coordinado según zona",
      pickupOptions: "Santo Domingo, La Romana y Punta Cana",
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
      origins: ["santo_domingo"],
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
      origins: ["punta_cana"],
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
      origins: ["santo_domingo", "las_terrenas"],
      description: "Paquete especial de temporada con transporte y experiencia guiada.",
      durationMin: 900,
      durationLabel: "Día completo",
      basePriceAdult: 120,
      basePriceChild: 95,
      capacity: 22,
      meetingPoint: "Punto coordinado según ciudad",
      pickupOptions: "Santo Domingo, San Pedro, La Romana y Las Terrenas",
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
    `Puedo ayudarte con *Tours en República Dominicana*, *Boletos aéreos*, *Solo hoteles*, *Seguros de viaje* y *Traslados*.\n\n` +
    `También puedes escribirme *"Tours desde Punta Cana"*, *"Tours desde Santo Domingo"*, *"Tours desde Santiago"*, *"Tours desde Las Terrenas"* o *"Tours Semana Santa"* para mostrarte las excursiones disponibles.`
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
    solo_hoteles: "Solo hoteles",
    seguros_viaje: "Seguros de viaje",
    traslados: "Traslados",
    paquetes_vacacionales: "Paquetes",
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

  if (t.includes("peru") || t.includes("perú")) return "peru";
  if (t.includes("bogota") || t.includes("bogotá")) return "bogota";
  if (t.includes("miami")) return "miami";
  if (t.includes("italia")) return "italia";
  if (t.includes("otro destino")) return "otro_destino";
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
    t.includes("tours desde santiago") ||
    t.includes("tour desde santiago") ||
    t.includes("tours santiago") ||
    t.includes("tour santiago") ||
    (allowBareOrigin && t === "santiago")
  ) {
    return "tours_santiago";
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

function buildLeadSummary(title, fields = []) {
  const lines = [`📌 *${title}*`, ""];
  for (const f of fields) lines.push(`${f.label}: ${f.value || "—"}`);
  return lines.join("\n");
}

function serviceLineLabel(key) {
  return getServiceLineByKey(key)?.title || key || "Servicio";
}

function categoriesEmojiText() {
  return (
    `🌴 *Tours en República Dominicana*

` +
    `Tenemos estas colecciones disponibles para ayudarte a elegir más fácil:
` +
    `🏝️ Tours desde Punta Cana
` +
    `🏙️ Tours desde Santo Domingo
` +
    `⛰️ Tours desde Santiago
` +
    `🌊 Tours desde Las Terrenas
` +
    `⛪ Tours Semana Santa

` +
    `Selecciona la colección que deseas explorar y te mostraré las excursiones disponibles.`
  );
}

function mainMenuText() {
  const visibleLines = SERVICE_LINES.filter(s => s.key !== "ubicacion_contacto" && s.key !== "paquetes_vacacionales");
  const listText = visibleLines.map(s => `• ${s.title}`).join("\n");
  return (
    `👋 ¡Bienvenido a *${BUSINESS_NAME}*! Soy tu asistente virtual de viajes.

` +
    `Estoy aquí para ayudarte a cotizar, comparar opciones y dejar tu solicitud casi lista para reserva y pago.

` +
    `Puedo ayudarte con:
` +
    listText +
  
  );
}

function buildLocationContactText() {
  const addressLine = BUSINESS_ADDRESS ? `📍 Dirección: ${BUSINESS_ADDRESS}\n` : "";
  return (`📍 *Ubicación y contacto*\n\n` + `${addressLine}` + `${MARKET_CONTACT_TEXT}`).trim();
}

function getRealTourGroupIntro(groupKey) {
  if (groupKey === "tours_punta_cana") {
    return "Excursiones y actividades disponibles para disfrutar saliendo desde Punta Cana. Salidas diarias.";
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

  if (!group || !tours.length) return "No encontré excursiones disponibles en esta colección ahora mismo 🙏";

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

  const fields = [
    { label: "🧩 Servicio", value: "Tours en República Dominicana" },
    { label: "🗂️ Colección", value: group?.title || "—" },
    { label: "🌴 Tour", value: tour?.title || "—" },
    { label: "📅 Fecha solicitada", value: session.pendingDesiredDate || "—" },
    { label: "👥 Pasajeros", value: `${pax} (${session.pendingAdults || 0} adultos / ${session.pendingChildren || 0} niños)` }
  ];

  if (session.pendingChildren > 0) {
    fields.push({ label: "👶 Edades de niños", value: session.pendingChildrenAges || "No especificadas" });
  }

  fields.push({ label: "🚐 Pickup / salida", value: session.pendingPickup || "—" });
  fields.push({ label: "📍 Ciudad", value: session.pendingCity || "—" });
  fields.push({ label: "👤 Cliente", value: session.pendingName || "—" });
  fields.push({ label: "📞 Tel", value: phoneDigits || "—" });
  fields.push({ label: "🖼️ Imagen", value: tour?.imageUrl || "—" });

  return buildLeadSummary("Nueva solicitud de tour", fields);
}

// =========================
// WhatsApp send helpers
// =========================
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

async function sendWhatsAppDocument(to, documentUrl, filename, caption = "", reportSource = "BOT") {
  if (!documentUrl) throw new Error("documentUrl is required");

  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "document",
      document: {
        link: documentUrl,
        filename: filename || undefined,
        caption: caption || undefined,
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: caption || filename || "Documento enviado",
    source: reportSource,
    kind: "DOCUMENT",
    meta: { filename: filename || undefined, link: documentUrl },
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
    meta: { link: imageUrl },
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

async function sendServiceLinesList(to) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  const visibleLines = SERVICE_LINES.filter(s => s.key !== "ubicacion_contacto" && s.key !== "paquetes_vacacionales");
  const rows = visibleLines.map((s) => ({
    id: s.id,
    title: serviceLineRowTitle(s),
    description: "",
  }));

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: "Servicios disponibles" },
        body: { text: "Selecciona el servicio que te interesa 👇" },
        footer: { text: BUSINESS_NAME },
        action: { button: "Ver opciones", sections: [{ title: "Servicios", rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );
}

async function sendTourOriginsList(to) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  const rows = TOUR_ORIGINS.map((o) => ({
    id: o.id,
    title: waRowTitle(o.title),
    description: "",
  }));

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: "Origen del tour" },
        body: { text: "¿Desde dónde deseas salir? 👇" },
        footer: { text: BUSINESS_NAME },
        action: { button: "Elegir origen", sections: [{ title: "Salidas", rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );
}

async function sendPackageDestinationsList(to) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  const rows = PACKAGE_DESTINATIONS.map((d) => ({
    id: d.id,
    title: waRowTitle(d.title),
    description: "",
  }));

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: "Paquetes vacacionales" },
        body: { text: "Elige el destino que te interesa 👇" },
        footer: { text: BUSINESS_NAME },
        action: { button: "Ver destinos", sections: [{ title: "Destinos", rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );
}

async function sendRealTourGroupsList(to) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  const rows = REAL_TOUR_GROUPS.map((g) => ({
    id: g.id,
    title: waRowTitle(g.title),
    description: "",
  }));

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: "Colecciones de tours" },
        body: { text: "Selecciona la temporada o colección de excursiones que deseas ver 👇" },
        footer: { text: BUSINESS_NAME },
        action: { button: "Ver colecciones", sections: [{ title: "Colecciones disponibles", rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );
}

async function sendRealToursByGroup(to, groupKey, session) {
  const text = formatRealToursTextList(groupKey, session);
  await sendWhatsAppText(to, text);
}

async function sendRealTourPresentation(to, tour) {
  if (!tour) return;
  if (tour.imageUrl) {
    await sendWhatsAppImage(
      to,
      tour.imageUrl,
      ``
    );
  }
  await sendWhatsAppText(to, buildRealTourInfoText(tour));
}

async function sendCatalogDocument(to) {
  if (!CATALOG_DOCUMENT_URL) {
    await sendWhatsAppText(
      to,
      `Todavía no tengo el documento cargado aquí 🙏\n\nMientras tanto, dime si buscas *Tours en República Dominicana*, *Boletos aéreos*, *Solo hoteles*, *Seguros de viaje*, *Traslados* o *Paquetes vacacionales* y te ayudo por el flujo normal.`
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

    const pax = Number(session.pendingAdults || 0) + Number(session.pendingChildren || 0);
    const agesText = session.pendingChildren > 0 ? `\nEdades de niños: ${session.pendingChildrenAges || "No especificadas"}` : "";

    const summary = `Lead Real Tour: ${tour?.title || "Tour"} - ${session.pendingName}`;
    const description =
      `Colección: ${group?.title || "—"}\n` +
      `Tour: ${tour?.title || "—"}\n` +
      `Cliente: ${session.pendingName || "—"}\n` +
      `Teléfono: ${phone || "—"}\n` +
      `Fecha solicitada: ${session.pendingDesiredDate || "—"}\n` +
      `Pasajeros: ${pax} (${session.pendingAdults || 0} adultos / ${session.pendingChildren || 0} niños)${agesText}\n` +
      `Punto de salida: ${session.pendingPickup || "—"}\n` +
      `Ciudad: ${session.pendingCity || "—"}\n` +
      `Estado: Pendiente`;

    const today = new Date().toISOString().split("T")[0];

    await calendar.events.insert({
      calendarId: GOOGLE_CALENDAR_ID,
      requestBody: {
        summary: summary,
        description: description,
        start: { date: today },
        end: { date: today }
      }
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
      `Puedo ayudarte con *Tours en República Dominicana*, *Boletos aéreos*, *Solo hoteles*, *Seguros de viaje* y *Traslados*.\n\n` +
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
- Solo hoteles
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
      await sendWhatsAppText(from, mainMenuText());
      await sendServiceLinesList(from);
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle") session.greeted = true;

    if (
      ["menu", "menú", "inicio", "reiniciar", "reset", "resetear", "empezar de nuevo"].includes(tNorm)
    ) {
      clearIntakeFlow(session);
      session.lastBooking = null;
      session.reschedule = defaultSession().reschedule;
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
        await sendWhatsAppText(from, mainMenuText());
        await sendServiceLinesList(from);
        return res.sendStatus(200);
      }

      if (session.state !== "idle") {
        clearIntakeFlow(session);
        await sendWhatsAppText(from, mainMenuText());
        await sendServiceLinesList(from);
        return res.sendStatus(200);
      }
    }

    if (detectCatalogRequest(tNorm)) {
      await sendCatalogDocument(from);
      return res.sendStatus(200);
    }

    // =========================
    // POST BOOKING
    // =========================
    if (session.state === "post_booking" && session.lastBooking) {
      const booking = session.lastBooking;
      const tour = getAnyTourByKey(booking.tour_key);

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
        await sendWhatsAppText(
          from,
          `✅ Listo. Tu reserva fue cancelada.\n\nSi deseas una nueva, escribe *"Nueva reserva"* o vuelve al *menú*.`
        );
        clearIntakeFlow(session);
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

        session.pendingServiceLine = "tours_rd";
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
        clearIntakeFlow(session);
        session.lastBooking = null;
        session.reschedule = defaultSession().reschedule;
        await sendWhatsAppText(from, `Claro ✅ Vamos con una nueva solicitud.`);
        await sendWhatsAppText(from, mainMenuText());
        await sendServiceLinesList(from);
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

    // =========================
    // TOURS FLOW (TOURS REALES DEL CLIENTE)
    // =========================
    if (session.state === "await_tour_group") {
      const groupKey = detectRealTourGroupFromUser(userText, { allowBareOrigin: true });
      if (!groupKey) {
        await sendWhatsAppText(
          from,
          `Perfecto 🌴\nTe compartiré nuestras colecciones de excursiones disponibles.\n\nElige una de estas opciones:\n• Tours desde Punta Cana\n• Tours desde Santo Domingo\n• Tours desde Santiago\n• Tours desde Las Terrenas\n• Tours Semana Santa`
        );
        await sendRealTourGroupsList(from);
        return res.sendStatus(200);
      }

      session.pendingServiceLine = "tours_rd";
      session.pendingRealTourGroup = groupKey;
      session.state = "await_real_tour_choice";
      await sendRealToursByGroup(from, groupKey, session);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_choice") {
      const anotherGroup = detectRealTourGroupFromUser(userText, { allowBareOrigin: true });
      if (anotherGroup) {
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
      await sendWhatsAppText(
        from,
        `📅 Ahora dime la *fecha* que te interesa para *${pickedTour.title}*.\nEj: "sábado", "15 de abril", "domingo" o "semana santa".`
      );
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_date") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame la *fecha* que te interesa para el tour.`);
        return res.sendStatus(200);
      }
      session.pendingDesiredDate = userText;
      session.state = "await_real_tour_adults";
      await sendWhatsAppText(from, `Perfecto 👍\n¿Cuántos *adultos* viajarían?`);
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
        await sendWhatsAppText(from, `Perfecto. Ahora indícame las edades de los niños.`);
        return res.sendStatus(200);
      } else {
        session.state = "await_real_tour_pickup";
        await sendWhatsAppText(from, `Perfecto.\nAhora dime tu *punto de salida o pickup*.`);
        return res.sendStatus(200);
      }
    }

    if (session.state === "await_real_tour_children_ages") {
      session.pendingChildrenAges = userText;
      session.state = "await_real_tour_pickup";
      await sendWhatsAppText(from, `Gracias.\nAhora dime tu *punto de salida o pickup*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_pickup") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame tu *punto de salida o pickup*.`);
        return res.sendStatus(200);
      }
      session.pendingPickup = userText;
      session.state = "await_real_tour_city";
      await sendWhatsAppText(from, `Gracias. Ahora dime tu *ciudad*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_city") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Por favor, indícame tu *ciudad*.`);
        return res.sendStatus(200);
      }
      session.pendingCity = userText;
      session.state = "await_real_tour_name";
      await sendWhatsAppText(from, `Perfecto ✅\nAhora indícame tu *nombre completo*.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_name") {
      if (tNorm.length < 3 || ["si", "sí", "ok", "listo"].includes(tNorm)) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }
      session.pendingName = userText;
      session.state = "await_real_tour_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* para dejar la solicitud lista.`);
      return res.sendStatus(200);
    }

    if (session.state === "await_real_tour_phone") {
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const tour = getRealTourByKey(session.pendingRealTourKey);
      const summaryText = buildRealTourLeadSummary(session, phoneDigits);

      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);
      await createRealTourLeadCalendarEvent(session, phoneDigits);

      session.lead = {
        ...defaultLead(),
        tour_key: session.pendingRealTourKey || "",
        followupSent: true,
        converted: true,
        quotePreview: summaryText,
        lastInteractionAt: new Date().toISOString(),
      };

      const agesMsg = session.pendingChildren > 0 ? `\n👶 Edades: ${session.pendingChildrenAges}` : "";

      await sendWhatsAppText(
        from,
        `✅ *Solicitud de tour recibida*\n\n` +
          `🌴 Tour: *${tour?.title || "—"}*\n` +
          `📅 Fecha solicitada: *${session.pendingDesiredDate || "—"}*\n` +
          `👥 Pasajeros: *${Number(session.pendingAdults || 0) + Number(session.pendingChildren || 0)}* (${session.pendingAdults || 0} adultos / ${session.pendingChildren || 0} niños)${agesMsg}\n` +
          `🚐 Pickup: ${session.pendingPickup || "—"}\n` +
          `📍 Ciudad: ${session.pendingCity || "—"}\n\n` +
          `Tu solicitud quedó casi lista. Ahora un asesor de la agencia te contactará para confirmar disponibilidad, validarte el monto final y gestionar el pago.`
      );

      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // LEGACY TOURS FLOW (SE CONSERVA)
    // =========================
    if (session.state === "await_slot_choice" && session.lastSlots?.length) {
      const picked = tryPickSlotFromUserText(session, userText);
      if (!picked) {
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

        const tour = getAnyTourByKey(session.lastBooking.tour_key);
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
      
      if (count > 0) {
        session.state = "await_children_ages";
        await sendWhatsAppText(from, `Perfecto. Ahora indícame las edades de los niños.`);
        return res.sendStatus(200);
      } else {
        const tour = getTourByKey(session.pendingTour);
        const quoteText = buildQuotePreview(tour, session.pendingAdults, session.pendingChildren);
        updateLead(session, { tour_key: session.pendingTour, quotePreview: quoteText });

        session.state = "await_pickup";
        await sendWhatsAppText(from, `${quoteText}\n\nAhora dime tu *punto de salida o pickup*. Ej: zona hotelera, aeropuerto, punto de encuentro.`);
        return res.sendStatus(200);
      }
    }

    if (session.state === "await_children_ages" && session.selectedSlot) {
      session.pendingChildrenAges = userText;
      const tour = getTourByKey(session.pendingTour);
      const quoteText = buildQuotePreview(tour, session.pendingAdults, session.pendingChildren);
      updateLead(session, { tour_key: session.pendingTour, quotePreview: quoteText });

      session.state = "await_pickup";
      await sendWhatsAppText(from, `${quoteText}\n\nAhora dime tu *punto de salida o pickup*. Ej: zona hotelera, aeropuerto, punto de encuentro.`);
      return res.sendStatus(200);
    }

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
        children_ages: session.pendingChildrenAges || null,
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

      if (PERSONAL_WA_TO) {
        const passengerPhone = String(booked?.phone || "").replace(/[^\d]/g, "");
        if (String(PERSONAL_WA_TO).replace(/[^\d]/g, "") !== passengerPhone) {
          const agesMsg = session.pendingChildren > 0 ? `👶 Edades: ${session.pendingChildrenAges || "N/A"}\n` : "";
          const summary =
            `📌 *Nueva reserva turística*\n\n` +
            `🏢 Agencia: *${BUSINESS_NAME}*\n` +
            `🌴 Tour: *${tour?.title || booked.tour_key}*\n` +
            `👤 Cliente: *${booked.passenger_name}*\n` +
            `📞 Tel: *${passengerPhone || "—"}*\n` +
            `👥 Pax: *${Number(booked.adults || 0) + Number(booked.children || 0)}* (${booked.adults || 0} adultos / ${booked.children || 0} niños)\n` +
            agesMsg +
            `📍 Ciudad: ${booked.city || "—"}\n` +
            `🚐 Salida: ${booked.pickup || "—"}\n` +
            `📅 Fecha: *${formatDateInTZ(booked.start, BUSINESS_TIMEZONE)}*\n` +
            `⏰ Hora: *${formatTimeInTZ(booked.start, BUSINESS_TIMEZONE)}*\n` +
            `💵 Estimado: *${currency(booked.quote_total || 0)}*\n` +
            `🆔 ID: ${booked.reservation_id || "—"}`;
          await notifyPersonalWhatsAppLeadSummary(summary, passengerPhone);
        }
      }

      session.lastBooking = booked;
      session.state = "post_booking";
      session.lastSlots = [];
      session.lastDisplaySlots = [];
      session.selectedSlot = null;
      session.pendingRange = null;
      session.pendingAdults = null;
      session.pendingChildren = null;
      session.pendingChildrenAges = null;
      session.pendingPickup = null;
      session.pendingCity = null;
      session.pendingName = null;
      session.reschedule = defaultSession().reschedule;
      clearLeadOnBooking(session);
      return res.sendStatus(200);
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
      await sendWhatsAppText(from, `Gracias. Ahora dime la *fecha aproximada* del viaje.\nEj: "15 de abril", "en junio" o "ida y vuelta del 10 al 18 de mayo".`);
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
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de boletos aéreos", [
        { label: "🧩 Servicio", value: serviceLineLabel("boletos_aereos") },
        { label: "🛫 Salida / origen", value: session.pendingDepartureCity || "—" },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📅 Fecha / temporada", value: session.pendingTravelDateText || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, { tour_key: "", quotePreview: summaryText, converted: false, followupSent: false });
      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *boletos aéreos* y nuestro equipo te contactará con opciones según:\n` +
          `• salida: ${session.pendingDepartureCity || "—"}\n` +
          `• destino: ${session.pendingDestination || "—"}\n` +
          `• fecha: ${session.pendingTravelDateText || "—"}\n` +
          `• personas: ${session.pendingPassengers || "—"}`
      );
      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // HOTELS
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
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de solo hoteles", [
        { label: "🧩 Servicio", value: serviceLineLabel("solo_hoteles") },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📅 Fecha / temporada", value: session.pendingTravelDateText || "—" },
        { label: "🌙 Noches", value: session.pendingNights || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "🏨 Categoría hotel", value: session.pendingHotelStars || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, { tour_key: "", quotePreview: summaryText, converted: false, followupSent: false });
      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *solo hoteles* y nuestro equipo te contactará con opciones según:\n` +
          `• destino: ${session.pendingDestination || "—"}\n` +
          `• fecha: ${session.pendingTravelDateText || "—"}\n` +
          `• noches: ${session.pendingNights || "—"}\n` +
          `• personas: ${session.pendingPassengers || "—"}\n` +
          `• categoría: ${session.pendingHotelStars || "—"}`
      );
      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // INSURANCE
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
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de seguro de viaje", [
        { label: "🧩 Servicio", value: serviceLineLabel("seguros_viaje") },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📆 Días de viaje", value: session.pendingTripDays || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "🎂 Edades", value: session.pendingTravelerAgesText || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, { tour_key: "", quotePreview: summaryText, converted: false, followupSent: false });
      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *seguro de viaje* y nuestro equipo te contactará con opciones según:\n` +
          `• destino: ${session.pendingDestination || "—"}\n` +
          `• días: ${session.pendingTripDays || "—"}\n` +
          `• personas: ${session.pendingPassengers || "—"}\n` +
          `• edades: ${session.pendingTravelerAgesText || "—"}`
      );
      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // TRANSFERS
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
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de traslado", [
        { label: "🧩 Servicio", value: serviceLineLabel("traslados") },
        { label: "🚕 Ruta", value: session.pendingTransferRoute || "—" },
        { label: "📅 Fecha", value: session.pendingTravelDateText || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, { tour_key: "", quotePreview: summaryText, converted: false, followupSent: false });
      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *traslado* y nuestro equipo te contactará con opciones según:\n` +
          `• ruta: ${session.pendingTransferRoute || "—"}\n` +
          `• fecha: ${session.pendingTravelDateText || "—"}\n` +
          `• personas: ${session.pendingPassengers || "—"}`
      );
      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // PACKAGES
    if (session.state === "await_package_destination") {
      const packageKey = detectPackageDestinationKeyFromUser(userText);
      if (packageKey && packageKey !== "otro_destino") {
        const pkg = getPackageDestinationByKey(packageKey);
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
      await sendWhatsAppText(from, `Perfecto 🎒\nAhora dime la *fecha* o *temporada* que te interesa.\nEj: "julio", "semana santa", "15 de agosto".`);
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
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Nueva solicitud de paquete vacacional", [
        { label: "🧩 Servicio", value: serviceLineLabel("paquetes_vacacionales") },
        { label: "🌍 Destino", value: session.pendingDestination || "—" },
        { label: "📅 Fecha / temporada", value: session.pendingTravelDateText || "—" },
        { label: "👥 Personas", value: session.pendingPassengers || "—" },
        { label: "🏨 Categoría hotel", value: session.pendingHotelStars || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, { tour_key: "", quotePreview: summaryText, converted: false, followupSent: false });
      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nRecibí tu solicitud de *paquete vacacional* y nuestro equipo te contactará con opciones según:\n` +
          `• destino: ${session.pendingDestination || "—"}\n` +
          `• fecha / temporada: ${session.pendingTravelDateText || "—"}\n` +
          `• personas: ${session.pendingPassengers || "—"}\n` +
          `• categoría hotel: ${session.pendingHotelStars || "—"}`
      );
      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // ADVISOR
    if (session.state === "await_advisor_topic") {
      if (tNorm.length < 2) {
        await sendWhatsAppText(from, `Cuéntame brevemente qué necesitas para poder pasarte con el asesor correcto.`);
        return res.sendStatus(200);
      }
      session.pendingAdvisorTopic = userText;
      session.state = "await_advisor_name";
      await sendWhatsAppText(from, `Perfecto 👍\nAhora dime tu *nombre completo*.`);
      return res.sendStatus(200);
    }

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
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíamelo así: 829XXXXXXX`);
        return res.sendStatus(200);
      }

      const summaryText = buildLeadSummary("Solicitud para hablar con un asesor", [
        { label: "🧩 Servicio", value: serviceLineLabel("hablar_asesor") },
        { label: "🌎 Mercado", value: "República Dominicana" },
        { label: "📝 Tema", value: session.pendingAdvisorTopic || "—" },
        { label: "👤 Cliente", value: session.pendingName || "—" },
        { label: "📞 Tel", value: phoneDigits || "—" },
      ]);

      updateLead(session, { tour_key: "", quotePreview: summaryText, converted: false, followupSent: false });
      await handoffToHumanTool({ summary: summaryText });
      await notifyPersonalWhatsAppLeadSummary(summaryText, phoneDigits);

      await sendWhatsAppText(
        from,
        `✅ *Solicitud recibida*\n\nYa pasé tu caso para que un asesor de *República Dominicana* te contacte.\n\nTema: ${session.pendingAdvisorTopic || "Consulta general"}`
      );
      clearIntakeFlow(session);
      return res.sendStatus(200);
    }

    // =========================
    // FAST ROUTES / MENUS
    // =========================
    if (tNorm.includes("menu") || tNorm.includes("menú") || tNorm.includes("servicios") || tNorm.includes("ver opciones") || tNorm.includes("inicio")) {
      await sendWhatsAppText(from, mainMenuText());
      await sendServiceLinesList(from);
      return res.sendStatus(200);
    }

    if (tNorm.includes("categorias") || tNorm.includes("categorías") || tNorm.includes("ver tours")) {
      session.pendingServiceLine = "tours_rd";
      session.state = "await_tour_group";
      await sendWhatsAppText(from, categoriesEmojiText());
      await sendRealTourGroupsList(from);
      return res.sendStatus(200);
    }

    const directRealTourGroup = detectRealTourGroupFromUser(userText);
    if (directRealTourGroup) {
      clearIntakeFlow(session);
      session.pendingServiceLine = "tours_rd";
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
      session.pendingServiceLine = "tours_rd";
      session.pendingRealTourGroup = tour?.groupKey || null;
      session.pendingRealTourKey = directRealTourKey;
      session.state = "await_real_tour_date";
      updateLead(session, { tour_key: directRealTourKey, quotePreview: "", converted: false, followupSent: false });
      await sendRealTourPresentation(from, tour);
      await sendWhatsAppText(from, `📅 Si deseas agendar *${tour?.title || "este tour"}*, dime la *fecha* o *salida* que te interesa y seguimos con tu solicitud.`);
      return res.sendStatus(200);
    }

    const serviceLineKey = detectServiceLineFromUser(userText);
    if (serviceLineKey) {
      clearIntakeFlow(session);
      session.pendingServiceLine = serviceLineKey;

      if (serviceLineKey === "catalogo_pdf") {
        await sendCatalogDocument(from);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "ubicacion_contacto") {
        await sendWhatsAppText(from, buildLocationContactText());
        return res.sendStatus(200);
      }

      if (serviceLineKey === "hablar_asesor") {
        session.state = "await_advisor_topic";
        await sendWhatsAppText(from, `Perfecto 👤 Vamos a conectarte con un asesor de *República Dominicana*.\n\nCuéntame brevemente qué necesitas.`);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "tours_rd") {
        session.state = "await_tour_group";
        await sendWhatsAppText(from, categoriesEmojiText());
        await sendRealTourGroupsList(from);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "boletos_aereos") {
        session.state = "await_flight_origin";
        await sendWhatsAppText(from, `Perfecto ✈️ Vamos con *boletos aéreos*.\n\n¿Desde dónde deseas salir?\nEj: Santo Domingo, Punta Cana o Santiago.`);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "solo_hoteles") {
        session.state = "await_hotel_destination";
        await sendWhatsAppText(from, `Perfecto 🏨 Vamos con *solo hoteles*.\n\n¿En qué *destino o ciudad* deseas hospedarte?`);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "seguros_viaje") {
        session.state = "await_insurance_destination";
        await sendWhatsAppText(from, `Perfecto 🛡️ Vamos con *seguros de viaje*.\n\n¿A qué *país o destino* viajas?`);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "traslados") {
        session.state = "await_transfer_route";
        await sendWhatsAppText(from, `Perfecto 🚕 Vamos con *traslados*.\n\nDime la *ruta* que necesitas.\nEj: aeropuerto → hotel / hotel → aeropuerto / ciudad → ciudad.`);
        return res.sendStatus(200);
      }

      if (serviceLineKey === "paquetes_vacacionales") {
        session.state = "await_package_destination";
        await sendWhatsAppText(from, `Perfecto 🎒 Vamos con *paquetes vacacionales*.\n\nDime el destino que te interesa o elige uno del menú.`);
        await sendPackageDestinationsList(from);
        return res.sendStatus(200);
      }
    }

    if (
      session.pendingTour &&
      (wantsQuote(tNorm) || wantsIncludes(tNorm) || wantsSchedule(tNorm) || wantsPayments(tNorm) || wantsPolicies(tNorm))
    ) {
      const tour = getTourByKey(session.pendingTour);
      await sendWhatsAppText(from, buildTourFaqReply(tour, tNorm));
      return res.sendStatus(200);
    }

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
          : session.pendingTour
          ? `Nota: el tour actual pendiente es ${session.pendingTour}.`
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

app.get("/tick", async (_req, res) => {
  try {
    await reminderLoop();
    await followupLeadsLoop();
  } catch {}
  return res.status(200).send("tick ok");
});

app.listen(PORT, () => console.log(`Bot running on :${PORT}`));
