export const SIMPLE_SERVICE_FLOW_CONFIGS = [
  {
    serviceLineKey: "boletos_aereos",
    startState: "await_flight_origin",
    startPrompt: `Perfecto ✈️ Vamos con *boletos aéreos*.

¿Desde dónde deseas salir?
Ej: Santo Domingo, Punta Cana o Santiago.`,
    summaryTitle: "Nueva solicitud de boletos aéreos",
    summaryFields: [
      { label: "🧩 Servicio", type: "serviceLineLabel" },
      { label: "🛫 Salida / origen", source: "pendingDepartureCity" },
      { label: "🌍 Destino", source: "pendingDestination" },
      { label: "📅 Fecha / temporada", source: "pendingTravelDateText" },
      { label: "👥 Personas", source: "pendingPassengers" },
      { label: "👤 Cliente", source: "pendingName" },
      { label: "📞 Tel", type: "phoneDigits" },
    ],
    confirmationIntro: `Recibí tu solicitud de *boletos aéreos* y nuestro equipo te contactará con opciones según:`,
    confirmationLines: [
      { label: "salida", source: "pendingDepartureCity" },
      { label: "destino", source: "pendingDestination" },
      { label: "fecha", source: "pendingTravelDateText" },
      { label: "personas", source: "pendingPassengers" },
    ],
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
    startPrompt: `Perfecto 🏨 Vamos con *solo hoteles*.

¿En qué *destino o ciudad* deseas hospedarte?`,
    summaryTitle: "Nueva solicitud de solo hoteles",
    summaryFields: [
      { label: "🧩 Servicio", type: "serviceLineLabel" },
      { label: "🌍 Destino", source: "pendingDestination" },
      { label: "📅 Fecha / temporada", source: "pendingTravelDateText" },
      { label: "🌙 Noches", source: "pendingNights" },
      { label: "👥 Personas", source: "pendingPassengers" },
      { label: "🏨 Categoría hotel", source: "pendingHotelStars" },
      { label: "👤 Cliente", source: "pendingName" },
      { label: "📞 Tel", type: "phoneDigits" },
    ],
    confirmationIntro: `Recibí tu solicitud de *solo hoteles* y nuestro equipo te contactará con opciones según:`,
    confirmationLines: [
      { label: "destino", source: "pendingDestination" },
      { label: "fecha", source: "pendingTravelDateText" },
      { label: "noches", source: "pendingNights" },
      { label: "personas", source: "pendingPassengers" },
      { label: "categoría", source: "pendingHotelStars" },
    ],
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
    summaryFields: [
      { label: "🧩 Servicio", type: "serviceLineLabel" },
      { label: "🌍 Destino", source: "pendingDestination" },
      { label: "📆 Días de viaje", source: "pendingTripDays" },
      { label: "👥 Personas", source: "pendingPassengers" },
      { label: "🎂 Edades", source: "pendingTravelerAgesText" },
      { label: "👤 Cliente", source: "pendingName" },
      { label: "📞 Tel", type: "phoneDigits" },
    ],
    confirmationIntro: `Recibí tu solicitud de *seguro de viaje* y nuestro equipo te contactará con opciones según:`,
    confirmationLines: [
      { label: "destino", source: "pendingDestination" },
      { label: "días", source: "pendingTripDays" },
      { label: "personas", source: "pendingPassengers" },
      { label: "edades", source: "pendingTravelerAgesText" },
    ],
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
    summaryFields: [
      { label: "🧩 Servicio", type: "serviceLineLabel" },
      { label: "🚕 Ruta", source: "pendingTransferRoute" },
      { label: "📅 Fecha", source: "pendingTravelDateText" },
      { label: "👥 Personas", source: "pendingPassengers" },
      { label: "👤 Cliente", source: "pendingName" },
      { label: "📞 Tel", type: "phoneDigits" },
    ],
    confirmationIntro: `Recibí tu solicitud de *traslado* y nuestro equipo te contactará con opciones según:`,
    confirmationLines: [
      { label: "ruta", source: "pendingTransferRoute" },
      { label: "fecha", source: "pendingTravelDateText" },
      { label: "personas", source: "pendingPassengers" },
    ],
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

Dime el destino que te interesa o elige uno del menú.`,
    afterStartAction: "sendPackageDestinationsList",
    summaryTitle: "Nueva solicitud de paquete vacacional",
    summaryFields: [
      { label: "🧩 Servicio", type: "serviceLineLabel" },
      { label: "🌍 Destino", source: "pendingDestination" },
      { label: "📅 Fecha / temporada", source: "pendingTravelDateText" },
      { label: "👥 Personas", source: "pendingPassengers" },
      { label: "🏨 Categoría hotel", source: "pendingHotelStars" },
      { label: "👤 Cliente", source: "pendingName" },
      { label: "📞 Tel", type: "phoneDigits" },
    ],
    confirmationIntro: `Recibí tu solicitud de *paquete vacacional* y nuestro equipo te contactará con opciones según:`,
    confirmationLines: [
      { label: "destino", source: "pendingDestination" },
      { label: "fecha / temporada", source: "pendingTravelDateText" },
      { label: "personas", source: "pendingPassengers" },
      { label: "categoría hotel", source: "pendingHotelStars" },
    ],
    steps: [
      { state: "await_package_destination", kind: "packageDestination", field: "pendingDestination", nextState: "await_package_date", invalidPrompt: `Dime el *país o destino* que te interesa para el paquete vacacional.`, prompt: `Perfecto 🎒
Ahora dime la *fecha* o *temporada* que te interesa.
Ej: "julio", "semana santa", "15 de agosto".` },
      { state: "await_package_date", kind: "text", minLen: 2, field: "pendingTravelDateText", nextState: "await_package_people", invalidPrompt: `Por favor, indícame la *fecha* o *temporada* que te interesa.`, prompt: `Gracias. ¿Para cuántas *personas* sería el paquete?` },
      { state: "await_package_people", kind: "count", minValue: 1, field: "pendingPassengers", nextState: "await_package_stars", invalidPrompt: `Indícame cuántas *personas* viajarían. Ej: 2`, prompt: `Perfecto. ¿Qué tipo de hotel prefieres dentro del paquete?
Ej: *3 estrellas*, *4 estrellas* o *5 estrellas*.` },
      { state: "await_package_stars", kind: "text", minLen: 2, field: "pendingHotelStars", nextState: "await_package_name", invalidPrompt: `Indícame si prefieres *3 estrellas*, *4 estrellas* o *5 estrellas*.`, prompt: `Perfecto 👍
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
    summaryFields: [
      { label: "🧩 Servicio", type: "serviceLineLabel" },
      { label: "🌎 Mercado", value: "República Dominicana" },
      { label: "📝 Tema", source: "pendingAdvisorTopic" },
      { label: "👤 Cliente", source: "pendingName" },
      { label: "📞 Tel", type: "phoneDigits" },
    ],
    confirmationIntro: `Ya pasé tu caso para que un asesor de *República Dominicana* te contacte.`,
    confirmationLines: [
      { label: "Tema", source: "pendingAdvisorTopic", fallback: "Consulta general" },
    ],
    steps: [
      { state: "await_advisor_topic", kind: "text", minLen: 2, field: "pendingAdvisorTopic", nextState: "await_advisor_name", invalidPrompt: `Cuéntame brevemente qué necesitas para poder pasarte con el asesor correcto.`, prompt: `Perfecto 👍
Ahora dime tu *nombre completo*.` },
      { state: "await_advisor_name", kind: "text", minLen: 3, field: "pendingName", nextState: "await_advisor_phone", invalidPrompt: `Por favor, envíame tu *nombre completo* 🙂`, prompt: `Gracias. Ahora envíame tu *número de teléfono* para que un asesor te contacte.` },
      { state: "await_advisor_phone", kind: "phone" },
    ],
  },
];
