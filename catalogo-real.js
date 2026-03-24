// =========================
// CATÁLOGO REAL DE TOURS - COLOMBIA
// =========================

export const REAL_TOUR_GROUPS = [
  { key: "tours_colombia", id: "rtg_colombia", title: "Tours en Colombia" },
];

export const REAL_TOUR_GROUP_ID_TO_KEY = Object.fromEntries(
  REAL_TOUR_GROUPS.map((g) => [g.id, g.key])
);

export function buildRealToursCatalog() {
  return [
    { key: "tour_antioquia", id: "rt_col_tour_antioquia", title: "Tour por Antioquia", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984226/WhatsApp_Image_2026-03-19_at_8.49.46_PM_y6lvsf.jpg", leadOnly: true },
    { key: "guatape_piedra_penol", id: "rt_col_guatape", title: "Guatapé - Piedra del Peñol", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984226/WhatsApp_Image_2026-03-19_at_8.49.45_PM_3_ufaouv.jpg", leadOnly: true },
    { key: "napoles_santorini_2dias", id: "rt_col_napoles_santorini", title: "Parque Temático Nápoles + Santorini Antioqueño", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984225/WhatsApp_Image_2026-03-19_at_8.49.46_PM_1_nynngj.jpg", leadOnly: true },
    { key: "ruta_de_la_leche", id: "rt_col_ruta_leche", title: "Ruta de la Leche", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984224/WhatsApp_Image_2026-03-19_at_8.49.45_PM_sszyxq.jpg", leadOnly: true },
    { key: "concepcion_el_canelo", id: "rt_col_concepcion", title: "Concepción - El Canelo", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984224/WhatsApp_Image_2026-03-19_at_8.49.45_PM_2_z3ngj2.jpg", leadOnly: true },
    { key: "jardin_hispania_andes", id: "rt_col_jardin", title: "Jardín - Hispania y Andes", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984223/WhatsApp_Image_2026-03-19_at_8.49.45_PM_1_vbrbpk.jpg", leadOnly: true },
    { key: "buga_termales", id: "rt_col_buga_termales", title: "Buga y Los Termales", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984223/WhatsApp_Image_2026-03-19_at_8.49.44_PM_kbnmss.jpg", leadOnly: true },
    { key: "termales_espiritu_santo", id: "rt_col_termales_espiritu", title: "Termales del Espíritu Santo", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984222/WhatsApp_Image_2026-03-19_at_8.49.44_PM_1_canpzt.jpg", leadOnly: true },
    { key: "pueblos_encanto_suroeste", id: "rt_col_pueblos_encanto", title: "Pueblos de Encanto del Suroeste Antioqueño", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984221/WhatsApp_Image_2026-03-19_at_8.49.43_PM_1_owjhqc.jpg", leadOnly: true },
    { key: "santorini", id: "rt_col_santorini", title: "Santorini con Día de Sol o Rafting", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984220/WhatsApp_Image_2026-03-19_at_8.49.42_PM_2_u3m6k9.jpg", leadOnly: true },
    { key: "cisneros", id: "rt_col_cisneros", title: "Cisneros - Entre Rieles y Montañas", groupKey: "tours_colombia", imageUrl: "https://res.cloudinary.com/daqqrtg0b/image/upload/v1773984220/WhatsApp_Image_2026-03-19_at_8.49.42_PM_3_nikmoc.jpg", leadOnly: true },
    { key: "santa_fe_kanaloa", id: "rt_col_santa_fe_kanaloa", title: "Santa Fe Kanaloa", groupKey: "tours_colombia", imageUrl: "", leadOnly: true },
    { key: "jerico", id: "rt_col_jerico", title: "Tour de 1 día a Jericó", groupKey: "tours_colombia", imageUrl: "", leadOnly: true },
    { key: "amalfi", id: "rt_col_amalfi", title: "Tour de 1 día Amalfi", groupKey: "tours_colombia", imageUrl: "", leadOnly: true },
    { key: "comuna_13_grafitour", id: "rt_col_comuna13", title: "Comuna 13 Grafitour", groupKey: "tours_colombia", imageUrl: "", leadOnly: true },
  ];
}

export const REAL_TOUR_TEXT_OVERRIDES = {
  tour_antioquia: {
    durationText: "Plan de fin de semana.",
    priceText: "Validar precio vigente con la agencia.",
    dateText: "Salida programada publicada por la agencia según calendario activo.",
    pickupText: "Salida desde Éxito Robledo.",
    includesText: "Hotel, piscina, comida y bebidas, staff profesional, transporte incluido y recorrido por San Jerónimo, Santa Fe, centro histórico y Sopetrán.",
    noteText: "Si te interesa otra fecha distinta a la publicada, la agencia puede validarla manualmente.",
  },
  guatape_piedra_penol: {
    priceText: "COP 129.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Salidas todos los días.",
    pickupText: "Puntos de encuentro: Metro Poblado y Metro Estadio.",
    includesText: "Transporte ida y vuelta, desayuno y almuerzo, tour en barco, asistencia médica, Guatapé, réplica, Alto del Chocho y guía experto. El ascenso a la Piedra del Peñol no está incluido.",
  },
  napoles_santorini_2dias: {
    priceText: "Pasaporte Safari desde COP 599.000 / Premium desde COP 660.000.",
    durationText: "Plan de 2 días.",
    dateText: "Fechas de salida según calendario publicado por la agencia.",
    pickupText: "Puntos de encuentro: Estación Estadio, Av. Oriental y Parque del Poblado.",
    includesText: "Transporte ida y vuelta, 2 desayunos, 1 almuerzo, 1 cena, 1 noche de hotel con piscina, ingreso a Hacienda Nápoles según pasaporte, visita a Santorini Antioqueño, guía y tarjeta médica.",
    noteText: "Para acomodación doble se incrementa el 10% al valor del plan según pasaporte.",
  },
  ruta_de_la_leche: {
    priceText: "COP 178.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Fechas de salida según calendario mensual publicado.",
    pickupText: "Puntos de encuentro: Mayorca, Estación Poblado, Av. Oriental y Mega Norte.",
    includesText: "Transporte ida y vuelta, desayuno, almuerzo, visita a Hoyo Rico, Santa Rosa y San Pedro, Granja de Fe, Plaza del Río, asistencia médica y guía experto.",
  },
  concepcion_el_canelo: {
    priceText: "COP 160.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Fechas de salida según calendario publicado.",
    pickupText: "Puntos de encuentro: Mayorca, Estación Poblado, Panadería Al Pan Pan y Terminal del Norte.",
    includesText: "Transporte ida y vuelta, desayuno y almuerzo, cascada Matasanos, visitas guiadas, asistencia médica y guía experto.",
  },
  jardin_hispania_andes: {
    priceText: "Plan básico COP 150.000 / plan full COP 165.000.",
    durationText: "Tour de 1 día.",
    dateText: "Salidas sábados y domingos.",
    pickupText: "Puntos de encuentro: Panadería Al Pan Pan y Centro Comercial Mayorca.",
    includesText: "Transporte ida y vuelta, desayuno, almuerzo en truchera, visitas a Hispania, Andes y Jardín, tarjeta de asistencia médica y guía experto.",
    noteText: "El plan full agrega arroz con leche, walking tour con guía local y recorrido en trencito.",
  },
  buga_termales: {
    priceText: "Múltiple COP 560.000 / doble COP 610.000.",
    durationText: "Plan de 2 días.",
    dateText: "Fechas de salida 2026 según calendario publicado.",
    pickupText: "Punto de encuentro: Metro Suramericana 4:00 AM.",
    includesText: "Transporte, 1 noche de alojamiento finca hotel, 1 cena, 2 desayunos, 2 almuerzos, ingreso a termales, visita al Milagroso de Buga, Santa Rosa de Cabal, guía profesional y tarjeta médica.",
  },
  termales_espiritu_santo: {
    priceText: "Múltiple desde COP 470.000.",
    durationText: "Plan de 2 días.",
    dateText: "Fechas de salida según calendario publicado por la agencia.",
    pickupText: "Punto de encuentro: Estación de metro Exposiciones 4:00 AM.",
    includesText: "Transporte ida y vuelta, 2 desayunos, 2 almuerzos, 1 cena, 1 noche en hotel campestre con piscina, visita a Sonsón, visita a Pueblo Nuevo Caldas, experiencia termal, asistencia médica y guía experto.",
  },
  pueblos_encanto_suroeste: {
    priceText: "Desde COP 630.000.",
    durationText: "Plan de 3 días / 2 noches.",
    dateText: "Fechas de salida 2026 según calendario publicado.",
    pickupText: "Puntos de encuentro: Av. Oriental (Panadería Al Pan Pan) y Centro Comercial Mayorca.",
    includesText: "Transporte ida y vuelta, 2 noches en Jardín, 3 desayunos, 3 almuerzos, visita a Jericó, Jardín, Hispania, Andes, Amagá, Cascada del Amor, Betania Machu Picchu, seguro de viaje y guía experto.",
  },
  santorini: {
    priceText: "Con día de sol COP 180.000 / con rafting COP 230.000.",
    durationText: "Tour de 1 día.",
    dateText: "Fechas de salida según calendario publicado.",
    pickupText: "Puntos de encuentro: Estación Poblado, Estación Estadio y Av. Oriental / Panadería Al Pan Pan.",
    includesText: "Transporte ida y vuelta, desayuno y almuerzo, visita a Santorini, rafting o día de sol según plan, piscina previa reserva y guía experto.",
    noteText: "Se debe elegir el tipo de plan al momento de confirmar la reserva.",
  },
  cisneros: {
    priceText: "COP 180.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Fechas de salida según calendario publicado.",
    pickupText: "Puntos de encuentro: Mayorca, Estación Poblado, Av. Oriental y Terminal del Norte.",
    includesText: "Transporte, desayuno, almuerzo, asistencia médica, guía experto, visita a La 45, Túnel de La Quiebra, tarde de hostería y refrigerio.",
  },
  santa_fe_kanaloa: {
    priceText: "Desde COP 175.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Salidas según programación especial de la agencia.",
    includesText: "Plan recreativo en Santa Fe de Antioquia / Kanaloa según la promoción vigente.",
  },
  jerico: {
    priceText: "Desde COP 160.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Salidas según programación especial de la agencia.",
    includesText: "Tour a Jericó con recorrido y paradas según la promoción vigente.",
  },
  amalfi: {
    priceText: "Desde COP 210.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Salidas según programación especial de la agencia.",
    includesText: "Tour a Amalfi con recorrido y actividades según la promoción vigente.",
  },
  comuna_13_grafitour: {
    priceText: "Desde COP 110.000 por persona.",
    durationText: "Tour de 1 día.",
    dateText: "Salidas diarias según disponibilidad.",
    includesText: "Experiencia de city tour y grafitour por Comuna 13 según la promoción vigente.",
  },
};
