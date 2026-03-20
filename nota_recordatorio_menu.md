Cambios aplicados:
- Se agregó recordatorio automático para clientes que reciben el menú principal y no seleccionan ninguna opción.
- Recordatorio 1: 30 minutos después.
- Recordatorio 2: 24 horas después.
- Si el cliente responde o entra a cualquier flujo, se cancela el recordatorio.
- Se mantiene la lógica existente del bot sin eliminar flujos importantes.

Variables opcionales:
- MENU_INACTIVITY_REMINDER_ENABLED=1
- MENU_REMINDER_1_AFTER_MIN=30
- MENU_REMINDER_2_AFTER_MIN=1440
- MENU_REMINDER_MAX_AGE_HOURS=48

Importante:
- Estos recordatorios corren cuando se ejecuta /tick, igual que los reminders ya existentes.
