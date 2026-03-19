// =========================
// CATÁLOGO REAL DE TOURS
// =========================

export const REAL_TOUR_GROUPS = [
  { key: "tours_punta_cana", id: "rtg_punta_cana", title: "Tours desde Punta Cana" },
  { key: "tours_santo_domingo", id: "rtg_santo_domingo", title: "Tours desde Santo Domingo" },
  { key: "tours_santiago", id: "rtg_santiago", title: "Tours desde Santiago" },
  { key: "tours_las_terrenas", id: "rtg_las_terrenas", title: "Tours desde Las Terrenas" },
  { key: "tours_semana_santa", id: "rtg_semana_santa", title: "Tours Semana Santa" },
];

export const REAL_TOUR_GROUP_ID_TO_KEY = Object.fromEntries(
  REAL_TOUR_GROUPS.map((g) => [g.id, g.key])
);

export function buildRealToursCatalog() {
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


export const REAL_TOUR_TEXT_OVERRIDES = {
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
