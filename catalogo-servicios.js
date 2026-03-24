// =========================
// CATÁLOGO DE SERVICIOS Y MENÚS - COLOMBIA
// =========================

export const SERVICE_LINES = [
  { key: "tours_colombia", id: "svc_tours_colombia", title: "🌴 Tours en Colombia" },
  { key: "boletos_aereos", id: "svc_boletos_aereos", title: "✈️ Boletos aéreos" },
  { key: "solo_hoteles", id: "svc_solo_hoteles", title: "🏨 Hoteles" },
  { key: "seguros_viaje", id: "svc_seguros_viaje", title: "🛡 Seguros de viaje" },
  { key: "traslados", id: "svc_traslados", title: "🚌 Traslados" },
  { key: "paquetes_vacacionales", id: "svc_paquetes_vacacionales", title: "🎒 Paquete vacacional" },
  { key: "hablar_asesor", id: "svc_hablar_asesor", title: "👤 Hablar con un asesor" },
  { key: "ubicacion_contacto", id: "svc_ubicacion_contacto", title: "Ubicación y contacto" },
];

export const SERVICE_LINE_ID_TO_KEY = Object.fromEntries(SERVICE_LINES.map((s) => [s.id, s.key]));

export const TOUR_ORIGINS = [
  { key: "medellin", id: "org_medellin", title: "Medellín" },
  { key: "bogota", id: "org_bogota", title: "Bogotá" },
  { key: "cali", id: "org_cali", title: "Cali" },
  { key: "otra_ciudad", id: "org_otra_ciudad", title: "Otra ciudad" },
];

export const TOUR_ORIGIN_ID_TO_KEY = Object.fromEntries(TOUR_ORIGINS.map((o) => [o.id, o.key]));

export const PACKAGE_DESTINATIONS = [
  {
    key: "rep_dom_2026",
    id: "pkg_rep_dom_2026",
    title: "República Dominicana a tu Alcance 2026",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984219/fechas_de_salida_a_punta_cana_de_medellin.jpg_v4epml.jpg",
    priceText: "Desde COP 2.750.000 / hab. doble COP 2.550.000.",
    dateText: "Salidas 2026 entre febrero y octubre según calendario publicado.",
    includesText: "Vuelo redondo MED-PUJ-SDQ-MED con Arajet, 5 días en Punta Cana, traslados colectivos, impuestos aéreos, seguro médico, equipaje documentado y atención personalizada.",
    noteText: "Menores de 2 a 11 años: validar tarifa vigente con la agencia al momento de reservar.",
  },
  {
    key: "eje_cafetero_experiencias",
    id: "pkg_eje_cafetero_experiencias",
    title: "Eje Cafetero Experiencias",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984220/WhatsApp_Image_2026-03-19_at_8.49.42_PM_4_td5isy.jpg",
    priceText: "Múltiple desde COP 725.000 / doble desde COP 785.000 según fecha.",
    durationText: "Plan de 4 días.",
    dateText: "Fechas programadas según calendario compartido por la agencia.",
    includesText: "Ingreso al Parque del Café, Panaca, Salento y Valle del Cocora, termales de Santa Rosa de Cabal, Filandia, tour panorámico, traslados, alojamiento, alimentación y tarjeta de asistencia médica.",
    noteText: "Algunas experiencias pueden cambiar Panaca por opciones adicionales con cargo extra. Validar disponibilidad y versión del plan.",
  },
  {
    key: "punta_cana_imperdible",
    id: "pkg_punta_cana_imperdible",
    title: "Punta Cana Imperdible",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1774312569/Punta_cana_imperdible_okfsvh.jpg",
    durationText: "Plan de 5 días y 4 noches.",
    dateText: "Del 23 al 27 de abril.",
    includesText: "Hotel todo incluido (Sunscape Coco Punta Cana), transfer, seguros de viaje, bienvenida personalizada y staff.",
    noteText: "No incluye gastos personales ni propinas.",
  },
  {
    key: "semana_santa_punta_cana",
    id: "pkg_semana_santa_punta_cana",
    title: "Semana Santa en Punta Cana",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1774312538/Semana_Santa_en_punta_cana_rqlubh.jpg",
    durationText: "Plan de 6 días y 5 noches.",
    dateText: "Del 31 de marzo al 05 de abril.",
    includesText: "Hotel todo incluido (Occidental Caribe), transfer, seguros de viaje, bienvenida personalizada y staff.",
    noteText: "No incluye gastos personales ni propinas.",
  },
];

export const PACKAGE_DESTINATION_ID_TO_KEY = Object.fromEntries(PACKAGE_DESTINATIONS.map((p) => [p.id, p.key]));
