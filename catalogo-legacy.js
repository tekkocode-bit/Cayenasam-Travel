// =========================
// CATÁLOGO LEGACY DE TOURS - COLOMBIA
// =========================

export const TOUR_CATEGORIES = [
  { key: "tours_1_dia", id: "cat_tours_1_dia", title: "Tours de 1 día" },
  { key: "tours_2_dias", id: "cat_tours_2_dias", title: "Tours de 2 días o más" },
  { key: "experiencias_city_tours", id: "cat_experiencias_city_tours", title: "Experiencias y city tours" },
  { key: "salidas_especiales", id: "cat_salidas_especiales", title: "Salidas especiales" },
  { key: "paquetes_temporada", id: "cat_paquetes_temporada", title: "Paquetes de temporada" },
];

export const CATEGORY_ID_TO_KEY = Object.fromEntries(TOUR_CATEGORIES.map((c) => [c.id, c.key]));

export function defaultTourCatalog() {
  return [
    {
      key: "guatape_piedra_penol_legacy",
      id: "tour_guatape_piedra_penol_legacy",
      title: "Guatapé – Piedra del Peñol",
      category: "tours_1_dia",
      origins: ["medellin"],
      description: "Excursión full day desde Medellín con recorrido por Guatapé y visita a la Piedra del Peñol.",
      durationMin: 720,
      durationLabel: "Día completo",
      basePriceAdult: 129000,
      basePriceChild: 129000,
      capacity: 40,
      meetingPoint: "Metro Poblado / Metro Estadio",
      pickupOptions: "Puntos de encuentro programados por la agencia",
      paymentMethods: "Transferencia, efectivo y pago acordado con asesor",
      reservationPolicy: "Reserva sujeta a cupo y validación final.",
      paymentPolicy: "La agencia confirma el monto final antes del pago.",
      includes: ["Transporte", "Desayuno", "Almuerzo", "Tour en barco", "Guía experto"],
      schedule: { mon: ["05:00"], tue: ["05:00"], wed: ["05:00"], thu: ["05:00"], fri: ["05:00"], sat: ["05:00"], sun: ["05:00"] },
    },
    {
      key: "parque_napoles_santorini_legacy",
      id: "tour_parque_napoles_santorini_legacy",
      title: "Parque Temático Nápoles + Santorini Antioqueño",
      category: "tours_2_dias",
      origins: ["medellin"],
      description: "Plan de 2 días con Hacienda Nápoles y visita al Santorini Antioqueño.",
      durationMin: 2880,
      durationLabel: "2 días",
      basePriceAdult: 599000,
      basePriceChild: 599000,
      capacity: 40,
      meetingPoint: "Estación Estadio / Av. Oriental / Parque del Poblado",
      pickupOptions: "Puntos de encuentro programados por la agencia",
      paymentMethods: "Transferencia y pago validado por asesor",
      reservationPolicy: "Sujeto a fecha y cupo.",
      paymentPolicy: "La tarifa depende del pasaporte y acomodación.",
      includes: ["Transporte", "1 noche de hotel", "Alimentación", "Ingreso según pasaporte"],
      schedule: { fri: ["04:00"], sat: ["04:00"] },
    },
  ];
}

const TOURS = defaultTourCatalog();
const TOUR_ID_TO_KEY = Object.fromEntries(TOURS.map((t) => [t.id, t.key]));
