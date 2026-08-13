# Lector de Facturas a JSON (`lector_facturas_to_json_v5.py`)

## Objetivo

`lector_facturas_to_json_v5.py` procesa entre 1 y 10 archivos de entrada (imágenes o PDF) y produce un archivo `.json` normalizado con los datos de la factura, pensado para integraciones con VB6 y procesos automáticos.

Soporta dos estrategias de extracción:

- **Por layout (sin IA):** si se pasa `--layout-file`, carga el JSON del layout desde un archivo local y extrae usando coordenadas de zonas configuradas visualmente en `configurar_layout_factura.py`. No consume tokens ni llama al backend.
- **Por IA:** si no hay layout, si el layout falla o si la extracción no es suficientemente fiable, delega el análisis al backend remoto de IA a través de `ia_backend_transport.py`.

Importante: este script cliente no usa `OPENAI_API_KEY` directamente. La invocación al modelo se realiza a través de `ia_backend_transport.py`, que envía la solicitud a un backend remoto autenticado con firma HMAC.

## Flujo General

1. Carga configuración desde `.env` cercano al script o desde variables de entorno.
2. Aplica overrides recibidos por línea de comandos.
3. Valida archivos de entrada y define carpeta de salida.
4. **Si `--layout-file` está informado y `--proveedor` no está activo:**
   - Lee el JSON del archivo de layout indicado.
   - Llama a `layout_extractor.try_layout_extraction(layout_data, files)`.
   - Si la extracción es fiable, aplica normalización/validación y salta al paso 8.
   - Si falla o no es fiable, continúa con el flujo IA.
5. Convierte cada archivo a bloques compatibles con el backend:
   - imágenes: `input_image`
   - PDF: `input_file`
6. Arma el prompt de extracción:
   - modo normal: factura completa
   - modo `--proveedor`: solo proveedor/código/cuit/nombre
7. Llama a `call_backend(...)`. Si el resultado parece incompleto y hay `--fallback-model`, reintenta con ese modelo.
8. Toma los datos (ya sea por layout o por IA), los normaliza y aplica validaciones/ajustes.
9. Guarda `<archivo>.json` y escribe por `stdout` únicamente la ruta del archivo generado.

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

## Parámetros del script

### Archivos de entrada (posicional)

| Parámetro | Descripción |
|---|---|
| `files` | 1 a 10 archivos JPG / PNG / WEBP / PDF en orden de páginas. |

### Entrada / salida

| Parámetro | Descripción |
|---|---|
| `--outdir DIR` | Carpeta de salida. Default: carpeta TEMP del sistema. |
| `--prompt-file FILE` | Archivo `.txt` con prompt personalizado. Reemplaza el prompt por defecto. No aplica con `--proveedor`. |

### Estrategia de extracción

| Parámetro | Descripción |
|---|---|
| `--layout-file FILE` | Ruta al archivo JSON con el layout del proveedor. Si la extracción es confiable, guarda el JSON sin llamar a IA. Si falla o el resultado es insuficiente, continúa con IA. **Incompatible con `--proveedor`.** |
| `--proveedor` | Modo reducido: extrae solo `codigo_proveedor` / `cuit` / `nombre_proveedor`. Usa un prompt corto y procesa solo el primer archivo. **Incompatible con `--layout-file`.** |

### Modelo IA

| Parámetro | Default | Descripción |
|---|---|---|
| `--model MODEL` | `gpt-4.1-mini` | Modelo principal. |
| `--fallback-model MODEL` | `gpt-4.1` | Modelo de reintento si el resultado parece incompleto o hay error. |
| `--no-fallback` | — | Desactiva el reintento automático con `--fallback-model`. |

### Backend / transporte

| Parámetro | Descripción |
|---|---|
| `--idcliente N` | Id de cliente (entero) para auditoría en el backend. Se copia a `IA_IDCLIENTE` y `IDCLIENTE`. |
| `--backend-url URL` | Override de `IA_BACKEND_URL`. |
| `--backend-route RUTA` | Override de `IA_BACKEND_ROUTE`. |
| `--client-id ID` | Override de `IA_CLIENT_ID`. |
| `--client-secret SECRET` | Override de `IA_CLIENT_SECRET`. |
| `--ia-task TAREA` | Override de `IA_TASK` / `opcion`. |

### Procesamiento de páginas

| Parámetro | Descripción |
|---|---|
| `--per-page` | Procesa cada archivo/página por separado con IA y luego unifica filas. Mejora extracción en tablas largas o facturas multipágina. |
| `--auto` | Ajusta `--tile` automáticamente según cantidad de páginas (1 pág → tile 3; 2-3 → tile 4 + per-page; 4+ → tile 5 + per-page). También se activa automáticamente cuando se reciben varios archivos sin `--per-page` ni `--tile`. |
| `--tile N` | Divide cada imagen en N franjas horizontales solapadas (1–6) antes de enviarla a IA. Requiere Pillow. Solo afecta imágenes, no PDFs. Default: 1. |

### Entorno

| Parámetro | Descripción |
|---|---|
| `--env-file FILE` | Archivo `.env` alternativo (útil para pruebas o distintos ambientes). |
| `--no-local-env` | No carga el `.env` que está junto al exe/script. |

### Interfaz

| Parámetro | Descripción |
|---|---|
| `--gui` | Muestra ventana de progreso con estado, barra y log (Tkinter). No altera `stdout`. |

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
- Con `--layout-file`, solo se procesa el primer archivo de la lista para la extracción por layout. Si falla, el fallback a IA puede procesar todos los archivos normalmente.

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

### Flujo sin layout (solo IA)

1. `agente_procesar_cliente.py` resuelve `idcliente` por configuración/ruta.
2. El agente ejecuta `lector_facturas_to_json_v5.py` con `IA_IDCLIENTE` e `IA_TASK`.
3. `lector_facturas_to_json_v5.py` llama a `ia_backend_transport.call_backend(...)`.
4. `ia_backend_transport.py` manda:
   - `idcliente`
   - `task/opcion`
   - nombre de archivo
   - firma HMAC
5. `ia_backend_proxy_server.py` valida firma, llama a OpenAI y audita la invocación.

### Flujo con layout

1. El caller (VB6 / agente) exporta el JSON del layout a un archivo temporal y pasa `--layout-file C:\ruta\lyt_42.json` junto con los archivos.
2. `lector_facturas_to_json_v5.py` lee el archivo JSON y llama a `layout_extractor.try_layout_extraction(layout_data, files)`.
3. `layout_extractor` usa el dict recibido directamente, sin conectarse a SQL.
4. Extrae texto con coordenadas usando pdfplumber (PDF digital) o pytesseract (imagen/escaneado).
5. Si el resultado pasa el criterio de fiabilidad, el script normaliza y guarda el JSON **sin llamar a IA**.
6. Si falla en cualquier paso, continúa con el flujo IA normal (pasos 3-5 del flujo anterior).

---

## Extracción por layout (`layout_extractor.py`)

### Rol del módulo

`layout_extractor.py` es un módulo auxiliar que implementa la extracción sin IA. Recibe el dict del layout ya cargado y usa las coordenadas de zonas para localizar y capturar texto directamente del documento, sin conectarse a SQL ni enviar nada al backend.

### Estructura del archivo JSON de layout

El archivo JSON del layout es el mismo que `configurar_layout_factura.py` guarda en `TA_CONFIGURACION`. VB6 lo obtiene de SQL y lo escribe a un archivo temporal antes de llamar al v5. Estructura:

```json
{
  "zonas": {
    "proveedor":     {"x1": 0.0, "y1": 0.0, "x2": 1.0, "y2": 0.15},
    "cabecera":      {"x1": ..., "y1": ..., "x2": ..., "y2": ...},
    "cliente":       { ... },
    "detalle":       { ... },
    "totales":       { ... },
    "cae":           { ... },
    "observaciones": { ... }
  },
  "detalle_columnas": [
    {"campo": "Codigo_Articulo", "x1": 0.0,  "x2": 0.12},
    {"campo": "Descripcion",     "x1": 0.12, "x2": 0.55},
    ...
  ]
}
```

Todas las coordenadas son **relativas** (0–1) al tamaño total de la imagen/página.

### Rutas de extracción

| Tipo de documento | Método |
|---|---|
| PDF con texto nativo | pdfplumber — palabras con posición exacta |
| Imagen / PDF escaneado | pytesseract `image_to_data` — OCR con bounding boxes |

Para PDFs digitales, las coordenadas relativas se multiplican por `page.width` / `page.height` en puntos.
Para OCR, las coordenadas se convierten a píxeles y se resta el offset del recorte de zona.

### Función principal

```python
def try_layout_extraction(
    layout_data: Dict[str, Any],
    files: List[str],
    *,
    log_fn=None,
) -> Optional[Dict[str, Any]]:
```

Recibe el dict del layout (ya leído del archivo JSON por el v5) y retorna un diccionario con claves `CAB`, `ROWS`, `TOTALES`, `meta` o `None` si no pudo extraer datos fiables.

### Mapeo de zonas a campos CAB

Los campos extraídos de cada zona se mapean directamente a los nombres del esquema final:

| Zona | Campos extraídos |
|---|---|
| `proveedor` | `Proveedor`, `CUIT`, `Domicilio`, `CondicionIVA` |
| `cabecera` | `CUIT`, `Letra`, `PuntoVenta`, `Numero`, `Fecha`, `FechaSubdiario`, `Vencimiento`, `CAE`, `VtoCAE` |
| `cae` | `CAE`, `VtoCAE` |
| `totales` | campos de TOTALES |

La detección del nombre del proveedor prioriza líneas con sufijo societario (S.A., S.R.L., SAS, etc.). Si el logo no es legible como texto, busca la primera línea no numérica que no sea dirección.

### Limpieza de valores numéricos en ROWS

Después de extraer cada fila del detalle se aplica limpieza automática:

| Tipo de campo | Campos | Tratamiento |
|---|---|---|
| Importes | `Importe_Lista`, `Importe_Neto`, `Total`, `Impuestos internos`, `Tot.Imp.Int` | Extrae primer número decimal; elimina `$`, espacios y otros símbolos |
| Porcentajes | `% Dto1`, `% Dto2`, `IVA` | Elimina `%`; conserva solo el número |
| Cantidades | `Cantidad`, `Bl/Pq` | Extrae el primer número del lado correcto del `/` (ej: `"2/"` → `"2"`, `"/ 6"` → `"6"`) |

### Criterio de fiabilidad

La extracción se considera fiable si:

- Al menos 1 fila de detalle tiene algún valor (`MIN_RELIABLE_ROWS = 1`)
- El cabezal tiene CUIT o número de comprobante

Si no se cumple, `try_layout_extraction` devuelve `None` y el lector cae al flujo IA.

### Manejo de errores

Cualquier excepción dentro del módulo (import faltante, layout corrupto, error de OCR) se captura internamente y resulta en `None`. El módulo nunca propaga excepciones al lector; el fallback a IA es siempre seguro.

---

## Ejemplo de invocación con layout (desde VB6)

```vb
' VB6 obtiene el JSON del layout de SQL y lo escribe a un archivo temporal
Dim archivoLayout As String
archivoLayout = carpetaSalida & "\lyt_" & codigoProveedor & ".json"
' ... (escribir jsonLayout al archivo) ...

Dim cmd As String
cmd = "python """ & rutaScript & """ """ & archivoFactura & """"
cmd = cmd & " --layout-file """ & archivoLayout & """"
cmd = cmd & " --idcliente " & idCliente
cmd = cmd & " --outdir """ & carpetaSalida & """"
Shell "cmd /c " & cmd
```

Si VB6 no tiene layout para el proveedor, simplemente omite `--layout-file` y el lector usa IA normalmente.
