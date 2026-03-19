Se corrigió el error de sintaxis que estaba tumbando el deploy en Render y se mantuvieron los flujos importantes.

Ajustes incluidos:
- Se eliminó el texto extra del mensaje de bienvenida sobre escribir directamente "Tours desde...".
- Se eliminó el caption redundante debajo de las imágenes de tours.
- La fecha del resumen y del evento de Google Calendar ahora usa la fecha solicitada por el cliente, no la fecha actual.
- Si hay varios niños, el bot pide las edades separadas por coma y valida la cantidad.
- En tours desde Santo Domingo ya no pide pickup ni ciudad; usa Sambil y Santo Domingo.
- En tours desde Punta Cana ya no pide ciudad; pide hotel o ubicación.
- En tours desde Santiago y Las Terrenas ya no pide ciudad; pide ubicación/hotel según aplique.
- El flujo real de tours ahora pide correo electrónico en lugar de número de teléfono.
- Se mantuvieron los demás flujos y helpers importantes.

Validación realizada:
- `node --check` OK en todos los archivos `.js` del paquete.
