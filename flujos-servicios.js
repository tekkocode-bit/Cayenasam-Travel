export const SIMPLE_SERVICE_FLOWS = [
  {
    serviceLineKey: "boletos_aereos",
    startState: "await_flight_origin",
    startPrompt: `Perfecto ✈️ Vamos con *boletos aéreos*.

¿Desde dónde deseas salir?
Ej: Medellín, Bogotá o Cali.`,
  },
  {
    serviceLineKey: "solo_hoteles",
    startState: "await_hotel_destination",
    startPrompt: `Perfecto 🏨 Vamos con *hoteles*.

¿A qué destino deseas viajar?`,
  },
  {
    serviceLineKey: "seguros_viaje",
    startState: "await_insurance_destination",
    startPrompt: `Perfecto 🛡️ Vamos con *seguros de viaje*.

¿A qué país o destino viajas?`,
  },
  {
    serviceLineKey: "traslados",
    startState: "await_transfer_route",
    startPrompt: `Perfecto 🚕 Vamos con *traslados*.

Dime la ruta que necesitas.`,
  },
  {
    serviceLineKey: "paquetes_vacacionales",
    startState: "await_package_destination",
    startPrompt: `Perfecto 🎒 Vamos con *paquete vacacional*.

Elige el paquete que te interesa.`,
  },
  {
    serviceLineKey: "hablar_asesor",
    startState: "await_advisor_topic",
    startPrompt: `Perfecto 👤 Vamos a conectarte con un asesor de *Colombia*.

Cuéntame brevemente qué necesitas.`,
  },
];
