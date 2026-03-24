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
    `Puedo ayudarte con *Tours en Colombia*, *Boletos aéreos*, *Hoteles*, *Seguros de viaje*, *Traslados* y *Paquete vacacional*.\n\n` +
    `También puedes escribirme *"Tours en Colombia"* o *"Paquete vacacional"* y te muestro las opciones disponibles.`
  );
}

export function categoriesEmojiText() {
  return (
    `🌴 *Tours en Colombia*\n\n` +
    `Aquí tienes el listado completo de tours disponibles.\n` +
    `Selecciona el tour que deseas ver y te mostraré su información antes de tomar tus datos.`
  );
}

export function mainMenuText({ businessName, serviceLines }) {
  const visibleLines = serviceLines.filter((s) => s.key !== "ubicacion_contacto");
  const listText = visibleLines.map((s) => `• ${s.title}`).join("\n");
  return (
    `👋 ¡Bienvenido a *${businessName}*! Soy tu asistente virtual de viajes.\n\n` +
    `Estoy aquí para ayudarte a cotizar, comparar opciones y dejar tu solicitud casi lista para reserva y pago.\n\n` +
    `Puedo ayudarte con:\n` +
    listText
  );
}

export const REAL_TOUR_GROUP_INTROS = {
  tours_colombia: "Estos son los tours disponibles en Colombia.",
};
