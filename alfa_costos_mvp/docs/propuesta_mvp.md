# Propuesta MVP - Actualizacion de Costos

## Principios

- revision humana obligatoria antes de actualizar
- auditoria completa por fila aplicada
- matching incremental y explicable
- IA solo para PDF e imagen
- sin asumir tablas de produccion hasta validacion
- uso de V_Ta_InterODBC como perfil de proveedor / configuracion de importacion

## Orden de trabajo

1. importar archivo
2. seleccionar perfil de V_Ta_InterODBC y confirmar proveedor
3. crear cabecera de corrida con archivo origen
4. detectar / mapear columnas
5. normalizar filas
6. buscar candidatos del maestro
7. revisar manualmente
8. aplicar en transaccion
9. registrar historial

## Integraciones a validar

- tabla de articulos
- tabla de relacion articulo-proveedor
- campo real de costo
- usuario de aplicacion o usuario Windows
- reglas de alerta por variacion
- forma de pedir o persistir conexion SQL antes de operar
