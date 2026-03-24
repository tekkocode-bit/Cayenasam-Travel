// =========================
// CATÁLOGO DE SERVICIOS Y MENÚS
// =========================

export const SERVICE_LINES = [
  { key: "tours_rd", id: "svc_tours_rd", title: "🌴 Tours en República Dominicana" },
  { key: "boletos_aereos", id: "svc_boletos_aereos", title: "✈️ Boletos aéreos" },
  { key: "solo_hoteles", id: "svc_solo_hoteles", title: "🏨 Hoteles" },
  { key: "seguros_viaje", id: "svc_seguros_viaje", title: "🛡 Seguros de viaje" },
  { key: "traslados", id: "svc_traslados", title: "🚌 Traslados" },
  { key: "paquetes_vacacionales", id: "svc_paquetes_vacacionales", title: "🎒 Paquete vacacional" },
  { key: "hablar_asesor", id: "svc_hablar_asesor", title: "👤Hablar con un asesor" },
  { key: "ubicacion_contacto", id: "svc_ubicacion_contacto", title: "Ubicación y contacto" },
];

export const SERVICE_LINE_ID_TO_KEY = Object.fromEntries(SERVICE_LINES.map((s) => [s.id, s.key]));

export const TOUR_ORIGINS = [
  { key: "santo_domingo", id: "org_santo_domingo", title: "Santo Domingo" },
  { key: "punta_cana", id: "org_punta_cana", title: "Punta Cana" },
  { key: "las_terrenas", id: "org_las_terrenas", title: "Las Terrenas" },
  { key: "santiago", id: "org_santiago", title: "Santiago" },
];

export const TOUR_ORIGIN_ID_TO_KEY = Object.fromEntries(TOUR_ORIGINS.map((o) => [o.id, o.key]));

export const PACKAGE_DESTINATIONS = [
  {
    key: "celebra_con_mama_en_medellin",
    id: "pkg_celebra_con_mama_en_medellin",
    title: "Celebra con Mamá en Medellín",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1774342501/WhatsApp_Image_2026-03-24_at_4.51.19_AM_ndcqhk.jpg",
    priceText: "Desde US$650 por persona (precio para agencias de viajes).",
    durationText: "Paquete de 5 noches y 6 días.",
    dateText: "Del 26 de mayo al 31 de mayo. Salida desde Santo Domingo.",
    includesText: "Hotel con desayuno (Living 35 Suites), transfer aeropuerto-hotel-aeropuerto, tours a Guatapé, City Tour más Comuna 13 y Santa Fe de Antioquia, almuerzo en los tours, seguro médico, staff y bienvenida personalizada.",
    noteText: "No incluye gastos personales ni propinas.",
  },
  {
    key: "octubre_en_medellin",
    id: "pkg_octubre_en_medellin",
    title: "Octubre en Medellín",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1774342501/WhatsApp_Image_2026-03-24_at_4.50.30_AM_rdgwey.jpg",
    priceText: "Desde US$680. Descuento por grupo.",
    durationText: "Paquete con salida desde Santo Domingo.",
    dateText: "Del 27 al 10 de octubre.",
    includesText: "Hotel con desayuno (Dorado de la 70), transfer, tours a Guatapé más represa, City Tour más Comuna 13 y metro cable, Hacienda Isla Verde, tour nocturno por la 70 y el bar gastronómico Con Hambre dominicano con música en vivo, comida en los tours, seguros de viaje, bienvenida personalizada y staff.",
    noteText: "No incluye gastos personales ni propinas.",
  },
  {
    key: "semana_santa_en_medellin",
    id: "pkg_semana_santa_en_medellin",
    title: "Semana Santa en Medellín",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1774342501/WhatsApp_Image_2026-03-24_at_4.50.47_AM_silj1j.jpg",
    priceText: "Desde US$950 por persona con habitación doble.",
    durationText: "Paquete especial de Semana Santa.",
    dateText: "Del 1 al 5 de abril. Salida desde Santo Domingo.",
    includesText: "Hotel con desayuno (Dorado de la 70), transfer, tours a Guatapé más represa, City Tour más Comuna 13 y metro cable, Hacienda Isla Verde, tour nocturno por la 70 y el bar gastronómico Con Hambre dominicano con música en vivo, comida en los tours, seguros de viaje, bienvenida personalizada y staff.",
    noteText: "No incluye gastos personales ni propinas.",
  },
  {
    key: "medellin_a_tu_alcance_2026",
    id: "pkg_medellin_a_tu_alcance_2026",
    title: "Medellín a tu Alcance 2026",
    imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1774342501/WhatsApp_Image_2026-03-24_at_4.50.13_AM_fwlwen.jpg",
    priceText: "Desde US$690 por persona con hab. doble.",
    durationText: "Plan de 5 a 6 días en Medellín.",
    dateText: "Salidas 2026 entre febrero y octubre según calendario publicado.",
    includesText: "Vuelo SDQ-MED-SDQ con Arajet, excursiones a Guatapé, El Peñol, Comuna 13, City Tour, Chiva Rumbera y otras actividades, traslados colectivos, impuestos aéreos, seguro médico y 1 equipaje documentado según aerolínea.",
    noteText: "No incluye servicios no especificados, gastos personales ni propinas.",
  },
];

export const PACKAGE_DESTINATION_ID_TO_KEY = Object.fromEntries(PACKAGE_DESTINATIONS.map((p) => [p.id, p.key]));
