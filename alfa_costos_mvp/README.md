# Alfa Costos MVP

Aplicacion de escritorio para importar listas de costos, vincularlas con articulos del maestro y aplicar actualizaciones controladas con auditoria.

## Objetivo del scaffold

Este modulo deja preparada la base del MVP sin asumir aun nombres reales de tablas de produccion.

Incluye:

- separacion por modulos
- modelos de dominio
- servicios stub para importacion, matching, actualizacion y auditoria
- UI base en PySide6
- SQL sugerido para historial propio del proceso

## Estado

Scaffold inicial. Antes de conectar a produccion hay que validar:

- tablas reales de articulos
- relacion articulo-proveedor
- campo real de costo
- estrategia de autenticacion / usuario operador
- umbrales de alerta por variacion

## Ejecucion esperada

```powershell
python -m alfa_costos_mvp.main
```

