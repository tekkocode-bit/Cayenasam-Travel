// =========================
// CATÁLOGO DE SERVICIOS Y MENÚS
// =========================

export const SERVICE_LINES = [
  { key: "tours_rd", id: "svc_tours_rd", title: "🌴 Tours en República Dominicana" },
  { key: "boletos_aereos", id: "svc_boletos_aereos", title: "✈️ Boletos aéreos" },
  { key: "solo_hoteles", id: "svc_solo_hoteles", title: "🏨 Hoteles" },
  { key: "seguros_viaje", id: "svc_seguros_viaje", title: "🛡 Seguros de viaje" },
  { key: "traslados", id: "svc_traslados", title: "🚌 Traslados" },
  { key: "paquetes_vacacionales", id: "svc_paquetes_vacacionales", title: "Paquetes vacacionales" },
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
  { key: "peru", id: "pkg_peru", title: "Perú" },
  { key: "bogota", id: "pkg_bogota", title: "Bogotá" },
  { key: "miami", id: "pkg_miami", title: "Miami" },
  { key: "italia", id: "pkg_italia", title: "Italia" },
  { key: "otro_destino", id: "pkg_otro_destino", title: "Otro destino" },
];

export const PACKAGE_DESTINATION_ID_TO_KEY = Object.fromEntries(PACKAGE_DESTINATIONS.map((p) => [p.id, p.key]));
