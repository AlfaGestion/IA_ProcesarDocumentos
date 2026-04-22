# Lector de Facturas a JSON (`lector_facturas_to_json_v5.py`)

## Objetivo

`lector_facturas_to_json_v5.py` procesa entre 1 y 10 archivos de entrada (imágenes o PDF), arma un prompt para extracción de datos de facturas y delega el análisis a un backend remoto de IA. La salida final es un archivo `.json` normalizado, pensado para integraciones con VB6 y procesos automáticos.

Importante: este script cliente no usa `OPENAI_API_KEY` directamente. La invocación al modelo se realiza a través de `ia_backend_transport.py`, que envía la solicitud a un backend remoto autenticado con firma HMAC.

## Flujo General

1. Carga configuración desde `.env` cercano al script o desde variables de entorno.
2. Aplica overrides recibidos por línea de comandos.
3. Valida archivos de entrada y define carpeta de salida.
4. Convierte cada archivo a bloques compatibles con el backend:
   - imágenes: `input_image`
   - PDF: `input_file`
5. Arma el prompt de extracción:
   - modo normal: factura completa
   - modo `--proveedor`: solo proveedor/código/cuit/nombre
6. Llama a `call_backend(...)`.
7. Toma el texto devuelto, extrae el JSON, lo normaliza y aplica validaciones/ajustes.
8. Guarda `<archivo>.json` y escribe por `stdout` únicamente la ruta del archivo generado.

## Auditoría e `idcliente`

### Qué hace realmente

El script sí transporta un identificador de cliente para auditoría, pero no resuelve por sí mismo el cliente ALFA ni registra consumos directamente en OpenAI.

El comportamiento es este:

1. Si se ejecuta con `--idcliente`, el valor se copia a:
   - `IA_IDCLIENTE`
   - `IDCLIENTE`
2. Al invocar `call_backend(...)`, `ia_backend_transport.py` lee esas variables.
3. Si hay valor, lo envía al backend remoto:
   - dentro del payload JSON como `idcliente`
   - y también en el header `X-IA-IdCliente`
4. El backend remoto es quien luego puede:
   - decidir qué tarea ejecutar
   - llamar a OpenAI
   - registrar auditoría, consumo o trazabilidad en base de datos/logs

### Qué NO hace este script

- No consulta SQL para descubrir el cliente ALFA.
- No lleva un contador local de usos.
- No guarda métricas de tokens en archivos locales.
- No llama directo al SDK de OpenAI desde este archivo.

## De dónde sale el `idcliente`

Hay dos formas principales:

### 1. Ejecución manual

Se puede pasar por CLI:

```powershell
python .\lector_facturas_to_json_v5.py factura.pdf --idcliente 25 --outdir C:\Temp
```

### 2. Ejecución desde `agente_procesar_cliente.py`

En el flujo automático, el agente resuelve el `idcliente` usando una tabla de configuración SQL con columnas de tipo:

- `idcliente`
- `RutaIA_procesar`

Con eso arma variables de entorno para el lector:

- `IA_TASK`
- `IA_IDCLIENTE`
- `IDCLIENTE`

Luego ejecuta `lector_facturas_to_json_v5.py` con ese entorno.

Conclusión: si estabas buscando la rutina que “obtiene el código de cliente ALFA”, en este script no está la resolución del cliente; acá solo se recibe y se reenvía. La resolución del cliente ocurre aguas arriba, normalmente en `agente_procesar_cliente.py`.

## Transporte al backend remoto

El módulo `ia_backend_transport.py` construye un `POST` a:

- `IA_BACKEND_URL`
- ruta `IA_BACKEND_ROUTE` (default: `/v1/process`)

Además firma el request con HMAC usando:

- `IA_CLIENT_ID`
- `IA_CLIENT_SECRET`
- timestamp
- nonce
- body JSON

### Datos relevantes que envía

- `model`
- `max_output_tokens`
- `input`
- `text` (si aplica)
- `task` / `opcion`
- `idcliente` (si está informado)
- `source_filename` y variantes del nombre de archivo

### Headers relevantes

- `X-IA-Client-Id`
- `X-IA-Timestamp`
- `X-IA-Nonce`
- `X-IA-Signature`
- `X-IA-Task`
- `X-IA-Opcion`
- `X-IA-IdCliente`
- `X-IA-Source-Filename`

Esto refuerza que el backend tiene suficientes datos para auditoría por cliente, por tarea y por archivo.

## Parámetros principales del script

### Entrada y salida

- `files`: 1 a 10 archivos de entrada.
- `--outdir`: carpeta de salida.
- `--prompt-file`: prompt personalizado.

### IA / backend

- `--idcliente`: identificador para auditoría backend.
- `--model`: modelo principal.
- `--fallback-model`: modelo de reintento.
- `--no-fallback`: desactiva fallback.
- `--backend-url`
- `--backend-route`
- `--client-id`
- `--client-secret`
- `--ia-task`

### Procesamiento

- `--gui`: muestra ventana de progreso.
- `--per-page`: procesa por página y luego unifica.
- `--auto`: ajusta estrategia según cantidad de páginas.
- `--tile N`: divide imágenes en franjas horizontales.
- `--proveedor`: extracción reducida de proveedor.

## Variables de entorno usadas

- `IA_BACKEND_URL`
- `IA_BACKEND_ROUTE`
- `IA_CLIENT_ID`
- `IA_CLIENT_SECRET`
- `IA_TASK`
- `IA_IDCLIENTE`
- `IDCLIENTE`

## Salida

En ejecución exitosa:

- genera un `.json`
- imprime solo la ruta del archivo por `stdout`

En error:

- sale con código distinto de cero
- escribe el detalle en `stderr`

## Observaciones técnicas

- Si `--proveedor` está activo, procesa solo el primer archivo y usa un prompt más corto.
- Si el documento tiene varias páginas, puede expandir PDFs multipágina y/o procesar página por página.
- Hay lógica de normalización del esquema, deduplicación de filas y validación de consistencia de totales.
- Si el resultado parece incompleto, puede reintentar con `--fallback-model`.

## Resumen corto para auditoría

Si necesitás explicarlo en una frase:

> `lector_facturas_to_json_v5.py` no registra consumos directamente en OpenAI; reenvía `idcliente`, tarea y nombre de archivo al backend remoto, y ese backend es el candidato natural a registrar auditoría/uso por cliente.

## Confirmación en el backend (`ia_backend_proxy_server.py`)

Revisión hecha sobre:

- `C:\dev\wsAlfa-main\ia_backend\ia_backend_proxy_server.py`

### Qué registra efectivamente

Ese backend sí hace auditoría por invocación a OpenAI.

Después de llamar a:

```python
OPENAI_CLIENT.responses.create(...)
```

ejecuta `_save_audit(...)` y `_append_audit_fallback(...)`.

Los datos auditados son:

- `idcliente`
- `opcion` / `task`
- `archivo` o nombre de archivo
- `ok` (éxito o error)
- `error`
- `duracion_ms`

### Dónde lo guarda

Primero intenta insertar en SQL Server, en la tabla:

- `dbo.IA_ConsultasGPT`

Con variantes de columnas compatibles, por ejemplo:

- `idcliente`
- `opcion`
- `archivo` o `archivo_nombre`
- `ok`
- `error`
- `duracion_ms`

Si SQL falla, deja un fallback local en:

- `LOG/ia_audit_fallback.jsonl`

### Qué NO registra en este archivo

En esta implementación no se ve persistencia explícita de:

- `usage`
- `prompt_tokens`
- `completion_tokens`
- `total_tokens`
- cantidad agregada de comprobantes por cliente

O sea: sí registra una fila por llamada/procesamiento, pero no encontré en este archivo un acumulado ni métricas de tokens del response de OpenAI.

### Interpretación práctica

Como `lector_facturas_to_json_v5.py` normalmente procesa un comprobante por ejecución, esta auditoría funciona en la práctica como un registro de comprobantes procesados por cliente, tarea y archivo.

Pero técnicamente el backend está registrando llamadas al proxy, no un contador resumido.

## Mapa de llamadas completo

1. `agente_procesar_cliente.py` resuelve `idcliente` por configuración/ruta.
2. El agente ejecuta `lector_facturas_to_json_v5.py` con `IA_IDCLIENTE` e `IA_TASK`.
3. `lector_facturas_to_json_v5.py` llama a `ia_backend_transport.call_backend(...)`.
4. `ia_backend_transport.py` manda:
   - `idcliente`
   - `task/opcion`
   - nombre de archivo
   - firma HMAC
5. `ia_backend_proxy_server.py` valida firma, llama a OpenAI y audita la invocación.
