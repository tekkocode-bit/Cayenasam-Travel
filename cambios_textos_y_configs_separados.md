# Cambios realizados

## Objetivo
Reducir líneas del `index.js` sin eliminar funciones ni flujos importantes.

## Qué se separó
- `flujos-servicios.js`
  - configuración completa de los formularios simples:
    - boletos aéreos
    - solo hoteles
    - seguros de viaje
    - traslados
    - paquetes vacacionales
    - hablar con un asesor
- `textos-bot.js`
  - textos generales del bot
  - menú principal
  - ayuda rápida
  - texto de ubicación/contacto
  - intros de colecciones de tours
  - descripciones cortas por tipo de experiencia de tour

## Qué se mantuvo en el index
- lógica de sesión
- webhook
- WhatsApp API
- calendario
- handoff
- notificaciones
- flujo real de tours
- validaciones y transición de estados

## Resultado de líneas
- `index.js` anterior: 3532
- `index.js` nuevo: 3244
- reducción del archivo principal: 288 líneas

## Total del proyecto modularizado
- `index.js`: 3244
- `catalogo-servicios.js`: 35
- `catalogo-real.js`: 559
- `catalogo-legacy.js`: 177
- `flujos-servicios.js`: 206
- `textos-bot.js`: 87
- total: 4308

## Nota
El total del proyecto sube levemente frente a la versión anterior separada porque ahora hay más archivos con `import/export`, pero el archivo principal queda más limpio, más fácil de mantener y sin quitar flujos activos.
