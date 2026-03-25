function normalizeSimpleText(t) {
  return String(t || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

export function quickHelpText() {
  return (
    `¡Hola! 😊\n` +
    `Puedo ayudarte con *Tours en República Dominicana*, *Boletos aéreos*, *Solo hoteles*, *Seguros de viaje* y *Traslados*.\n\n` +
    `También puedes escribirme *"Tours desde Punta Cana"*, *"Tours desde Santo Domingo"*, *"Tours desde Santiago"*, *"Tours desde Las Terrenas"* o *"Tours Semana Santa"* para mostrarte las excursiones disponibles.`
  );
}

export function categoriesEmojiText() {
  return (
    `🌴 *Tours en República Dominicana*\n\n` +
    `Tenemos estas colecciones disponibles para ayudarte a elegir más fácil:\n` +
    `🏝️ Tours desde Punta Cana\n` +
    `🏙️ Tours desde Santo Domingo\n` +
    `⛰️ Tours desde Santiago\n` +
    `🌊 Tours desde Las Terrenas\n` +
    `⛪ Tours Semana Santa\n\n` +
    `Selecciona la colección que deseas explorar y te mostraré las excursiones disponibles.`
  );
}

export function mainMenuText({ businessName, serviceLines }) {
  const visibleLines = serviceLines.filter((s) => s.key !== "ubicacion_contacto" && s.key !== "paquetes_vacacionales");
  const listText = visibleLines.map((s) => `• ${s.title}`).join("\n");
  return (
    `👋 ¡Bienvenido a *${businessName}*! Soy tu asistente virtual de viajes.\n\n` +
    `Estoy aquí para ayudarte a cotizar, comparar opciones y dejar tu solicitud casi lista para reserva y pago.\n\n` +
    `Puedo ayudarte con:\n` +
    listText +
    `\n\nTambién puedes escribirme directamente *"Tours desde Santo Domingo"*, *"Tours desde Santiago"*, *"Tours desde Punta Cana"*, *"Tours desde Las Terrenas"* o *"Tours Semana Santa"* y te mostraré las excursiones disponibles en cada colección.`
  );
}

export function buildLocationContactText({ businessAddress, marketContactText }) {
  const addressLine = businessAddress ? `📍 Dirección: ${businessAddress}\n` : "";
  return (`📍 *Ubicación y contacto*\n\n` + `${addressLine}` + `${marketContactText}`).trim();
}

const REAL_TOUR_GROUP_INTROS = {
  tours_punta_cana: "Excursiones y actividades disponibles para disfrutar saliendo desde Punta Cana. Salidas diarias.",
  tours_santo_domingo: "Selección de excursiones saliendo desde Santo Domingo. Salidas: Sábados y Domingos (Punto de encuentro: Sambil 5:00 AM).",
  tours_santiago: "Selección de excursiones saliendo desde Santiago. Salidas: Sábados y Domingos.",
  tours_las_terrenas: "Selección de excursiones saliendo desde Las Terrenas. Salidas diarias.",
  tours_semana_santa: "Opciones especiales de excursiones y actividades para Semana Santa.",
};

export function getRealTourGroupIntro(groupKey) {
  return REAL_TOUR_GROUP_INTROS[groupKey] || "Estas son las excursiones disponibles en esta colección.";
}

const EXPERIENCE_RULES = [
  { match: "isla saona", text: "Excursión de playa con ambiente caribeño, navegación y experiencia de día completo." },
  { match: "isla catalina", text: "Excursión de isla ideal para disfrutar playa, mar y actividades acuáticas." },
  { match: "boat party", text: "Paseo en barco con ambiente animado, mar y actividades recreativas." },
  { match: "buggies", text: "Aventura todoterreno ideal para quienes disfrutan recorridos con adrenalina." },
  { match: "fourwheel", text: "Experiencia de aventura en Fourwheel con recorrido dinámico al aire libre." },
  { match: "polaris", text: "Experiencia en Polaris pensada para quienes buscan una salida activa y divertida." },
  { match: "jet ski", text: "Actividad acuática ideal para quienes buscan velocidad y diversión sobre el mar." },
  { match: "aqua kart", text: "Experiencia acuática con enfoque de aventura y entretenimiento." },
  { match: "horseback", text: "Paseo a caballo ideal para disfrutar un recorrido natural y relajado." },
  { match: "dolphin", text: "Experiencia recreativa ideal para compartir en familia y vivir una actividad diferente." },
  { match: "coco bongo", text: "Salida de entretenimiento perfecta para quienes desean disfrutar un show y vida nocturna." },
  { match: "cayo", text: "Excursión de playa pensada para disfrutar mar, relax y ambiente tropical." },
  { match: "playa", text: "Salida de playa ideal para pasar un día de descanso y disfrute." },
  { match: "parapente", text: "Experiencia de aventura para quienes desean emociones fuertes y vistas impresionantes." },
  { match: "ballenas", text: "Experiencia de temporada ideal para quienes desean una salida especial de observación." },
  { match: "rio", text: "Excursión de naturaleza con combinación de agua, descanso y recorrido." },
  { match: "city", text: "Recorrido ideal para quienes desean combinar paseo, puntos de interés y experiencia local." },
  { match: "ocean world", text: "Experiencia recreativa ideal para compartir en grupo o en familia." },
  { match: "scoobadoo", text: "Actividad turística ideal para quienes desean una experiencia divertida y diferente." },
  { match: "santa fe", text: "Escapada full day para disfrutar relax, paseo y actividades incluidas." },
];

export function inferRealTourExperienceText(title = "") {
  const t = normalizeSimpleText(title);
  const rule = EXPERIENCE_RULES.find((item) => t.includes(item.match));
  return rule?.text || "Excursión disponible en esta colección para que elijas la opción que mejor conecte con tu viaje.";
}
