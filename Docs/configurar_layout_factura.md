# Configurar Layout Factura — Manual de uso

**Script:** `configurar_layout_factura.py`  
**Ejecutable opcional:** `configurar_layout_factura.exe`  
**Versión:** 1.0  
**Requiere:** Python 3.10+, `tkinter`, `pillow`, `pyodbc`.  
**Opcionales:** `pdfplumber`, `pymupdf`, `pytesseract`, Tesseract OCR para OCR sobre imágenes/PDF escaneados.

---

## ¿Qué hace?

Permite configurar visualmente el layout de una factura o comprobante para un proveedor específico.

Con esta herramienta se puede:

1. Abrir una imagen o PDF de ejemplo.
2. Marcar zonas del comprobante con el mouse.
3. Definir columnas del detalle.
4. Buscar o proponer el proveedor desde SQL Server.
5. Cargar un layout ya guardado para seguir editándolo.
6. Guardar la configuración en `TA_CONFIGURACION`.

El layout se graba como JSON en:

- `Clave = IA_LYT_<codigo_proveedor>`
- `Grupo = IA_COMPRAS`
- `ValorAux = <json completo>`

---

## Dependencias

Instalación recomendada:

```powershell
pip install -r requirements.txt
```

Si querés instalar manualmente:

```powershell
pip install pyodbc pillow pytesseract pdfplumber pymupdf
```

Notas:

- `pytesseract` es la librería Python.
- Para OCR real también debe estar instalado **Tesseract OCR** en Windows.
- Si no está instalado Tesseract, la herramienta igual abre y funciona, pero fallará solo al intentar OCR.
- Si Tesseract está instalado pero no está en el `PATH`, podés cargar la ruta completa en el campo `Tesseract EXE`.

---

## Ejecución

### Modo script

```powershell
python configurar_layout_factura.py
```

### Con parámetros de conexión SQL

```powershell
python configurar_layout_factura.py --server SERVER-ALFAVB6 --database ALFANET --user ALFANET --password ALFANET --driver "ODBC Driver 18 for SQL Server"
```

### Con ruta manual de Tesseract

```powershell
python configurar_layout_factura.py --tesseract-cmd "C:\Program Files\Tesseract-OCR\tesseract.exe"
```

### Modo ejecutable

```powershell
configurar_layout_factura.exe
```

---

## Parámetros de línea de comandos

| Parámetro | Descripción | Default |
|---|---|---|
| `--server` | Servidor SQL Server | `SERVER-ALFAVB6` |
| `--database` | Base de datos | `ALFANET` |
| `--user` | Usuario SQL | `ALFANET` |
| `--password` | Contraseña SQL | `ALFANET` |
| `--driver` | Driver ODBC | `ODBC Driver 18 for SQL Server` |
| `--tesseract-cmd` | Ruta completa de `tesseract.exe` | vacío |

Si no se envían parámetros:

1. La herramienta intenta usar el último valor guardado.
2. Si no hay archivo de memoria, usa los valores por defecto.

La última conexión usada se guarda en:

`layout_config_last_connection.json`

---

## Pantalla principal

La interfaz contiene estas áreas:

- **Botón Gestionar layouts...** para listar, cargar y eliminar layouts guardados
- **Botón Configuración...** para SQL Server y Tesseract
- **Pestañas:** 1. Documento / 2. Zonas / 3. Detalle / 4. Proveedor / 5. Guardar
- **Botones Anterior / Siguiente** al pie del panel izquierdo para navegar entre pestañas
- **Vista del comprobante** a la derecha con controles de zoom

En las solapas **Zonas** y **Detalle**:

- La selección se hace desde listas visibles.
- Al seleccionar una zona o campo aparece un texto de ayuda explicando qué datos se esperan.
- Los botones disponibles son: `Modificar`, `Eliminar`, `Marcar`.
- Al activar marcación aparece un banner de color indicando que hay que arrastrar sobre la imagen.
- `Esc` cancela la marcación en cualquier momento.

Sobre la imagen hay controles de zoom `+`, `-` y `Ajustar` para facilitar la marcación fina.

---

## Detección automática con IA

La herramienta incluye una ayuda inicial para proponer zonas y columnas automáticamente.

Uso:

1. abrir el comprobante
2. verificar la página si es PDF
3. elegir o dejar el `Modelo IA`
4. hacer clic en `Detectar zonas IA`

La IA analiza la imagen visible del comprobante y propone:

- zona `proveedor`
- zona `cabecera`
- zona `cliente`
- zona `detalle`
- zona `totales`
- zona `cae`
- zona `observaciones`
- columnas probables del detalle
- nombre y CUIT del proveedor si puede inferirlos

Importante:

- la detección es una propuesta inicial
- siempre debe revisarse y corregirse manualmente antes de guardar
- si una zona no puede inferirse con confianza, puede quedar vacía
- la detección usa la página actualmente cargada
- durante el análisis se muestra una ventana de progreso para indicar que la tarea sigue en ejecución
- esa ventana permite `Cancelar`; si se corta Internet o tarda demasiado, el usuario puede salir de la espera sin cerrar toda la aplicación

Requisitos:

- tener configurado el transporte IA igual que en los lectores
- backend remoto (`IA_BACKEND_URL`, `IA_CLIENT_ID`, `IA_CLIENT_SECRET`) o `OPENAI_API_KEY`
- si usás `.env`, debe estar junto al script o ejecutable

---

## Gestionar layouts existentes

Usar el botón:

- `Gestionar layouts...` (en el encabezado, visible desde cualquier pestaña)

Abre un diálogo con todos los proveedores que tienen layout guardado en `TA_CONFIGURACION`.

**Para cargar y editar un layout:**

1. opcionalmente escribir nombre o texto en el campo `Buscar` y hacer clic en `Buscar`
2. seleccionar el proveedor de la lista
3. hacer clic en `Modificar (cargar)` (o doble clic en la fila)

Esto carga automáticamente el proveedor y su layout completo (zonas, columnas, prompt_file).

**Para eliminar un layout:**

1. seleccionar el proveedor
2. hacer clic en `Eliminar`
3. confirmar el mensaje

Si el archivo de ejemplo ya no existe en disco: igual carga el layout y deja las zonas/columnas recuperadas. Podés abrir otro comprobante manualmente para seguir ajustando.

---

## Flujo recomendado de trabajo

### 1. Configurar conexión SQL

Abrir:

- `Configuración...`

Y completar o revisar:

- servidor
- base
- usuario
- contraseña
- driver ODBC
- ruta de `tesseract.exe` si no está en el `PATH`

Luego usar:

- `Probar conexión`
- `Guardar conexión`

Si la conexión funciona, ya podés usar búsqueda de proveedor y guardar layouts en SQL.

---

### 2. Abrir un comprobante de ejemplo

Usar el botón:

- `Abrir archivo`

Formatos soportados:

- PDF
- JPG / JPEG
- PNG
- WEBP
- BMP
- TIF / TIFF

Para PDF:

- se puede indicar la página en el campo `Página PDF`
- luego usar `Recargar página`

La herramienta convierte la página a imagen para poder marcarla visualmente.

---

### 3. Marcar zonas

Elegir en `Tipo zona` una de estas opciones:

- `proveedor`
- `cabecera`
- `cliente`
- `detalle`
- `totales`
- `cae`
- `observaciones`

Referencia práctica de uso:

- `proveedor`: bloque del emisor del comprobante. Normalmente incluye razón social, CUIT, domicilio y otros datos fiscales del proveedor.
- `cabecera`: bloque general del comprobante. Suele incluir tipo de comprobante, letra, punto de venta, número, fecha, condición de venta, vendedor u otros datos administrativos.
- `cliente`: bloque del receptor o comprador. Suele incluir nombre, CUIT/documento, domicilio, localidad y condición frente al IVA del cliente.

Luego hacer clic en:

- `Marcar zona`

Y arrastrar con el mouse sobre la imagen.

La zona queda guardada con coordenadas relativas entre `0` y `1`.

Importante:

- si volvés a marcar el mismo tipo de zona, reemplaza la anterior
- para guardar el layout, conviene marcar al menos `detalle`

---

### 4. Marcar columnas del detalle

Primero debe existir la zona:

- `detalle`

Luego:

1. elegir un campo en `Campo columna`
2. hacer clic en `Marcar columna detalle`
3. arrastrar en la imagen sobre el ancho de esa columna

Campos disponibles:

- `Codigo_Articulo`
- `Descripcion`
- `UD`
- `Importe_Lista`
- `Cantidad`
- `% Dto1`
- `% Dto2`
- `Importe_Neto`
- `Total`
- `AuxNroLote`
- `AuxNroSerie`
- `IVA`
- `Impuestos internos`
- `Bl/Pq`
- `Moneda`
- `Tot.Imp.Int`

Las columnas se guardan con:

- `x1`
- `x2`

Estas coordenadas son relativas al ancho total de la imagen base.

---

### 5. Buscar proveedor

Podés completar manualmente los campos:

- `Código`
- `Nombre`
- `CUIT`
- `Buscar`

Y usar:

- `Buscar proveedor` — busca en `Vt_Proveedores` por nombre, CUIT o texto libre

La herramienta autodetecta las columnas de `Vt_Proveedores`. Si no puede, mostrará un mensaje y podés ingresar el código manualmente.

Los resultados aparecen en la grilla inferior. Al hacer clic en una fila se cargan los datos del proveedor.

---

### 6. Probar OCR en zona proveedor

Si ya marcaste la zona `proveedor` podés usar:

- `Probar OCR zona proveedor`

La herramienta recorta la zona, aplica OCR, detecta posibles CUIT y propone el nombre. Útil para verificar antes de buscar el proveedor manualmente.

Si Tesseract no está instalado o no responde, se muestra error claro.

---

## Guardar layout

Antes de guardar:

- debe haber un documento cargado
- debe existir la zona `detalle`
- debe existir un código de proveedor cargado o seleccionado

Luego usar:

- `Guardar layout`

La herramienta:

1. arma el JSON del layout
2. genera la clave `IA_LYT_<codigo>`
3. fuerza `Grupo = IA_COMPRAS`
4. verifica si ya existe en `TA_CONFIGURACION`
5. si existe, actualiza `ValorAux` y `Grupo`
6. si no existe, inserta un nuevo registro

Si `TA_CONFIGURACION` requiere columnas obligatorias adicionales a `Clave`, `Grupo` y `ValorAux`, la herramienta informa cuáles faltan.

---

## Formato del JSON guardado

El JSON contiene, entre otros:

- datos del proveedor
- archivo de ejemplo
- tamaño base de imagen
- zonas marcadas
- columnas de detalle
- reglas base
- `prompt_file`

Ejemplo simplificado:

```json
{
  "version": 1,
  "proveedor": {
    "codigo": "000123",
    "nombre": "PROVEEDOR SA",
    "cuit": "30712345678"
  },
  "origen": {
    "archivo_ejemplo": "C:\\Facturas\\ejemplo.pdf",
    "tipo_archivo": "pdf",
    "pagina": 1
  },
  "imagen_base": {
    "ancho": 2480,
    "alto": 3508
  },
  "zonas": {
    "proveedor": { "x1": 0.05, "y1": 0.03, "x2": 0.42, "y2": 0.16 },
    "detalle": { "x1": 0.04, "y1": 0.30, "x2": 0.95, "y2": 0.78 }
  },
  "detalle_columnas": [
    { "campo": "Codigo_Articulo", "x1": 0.05, "x2": 0.18 },
    { "campo": "Descripcion", "x1": 0.18, "x2": 0.52 },
    { "campo": "Cantidad", "x1": 0.60, "x2": 0.68 },
    { "campo": "Importe_Neto", "x1": 0.75, "x2": 0.84 },
    { "campo": "Total", "x1": 0.86, "x2": 0.95 }
  ],
  "prompt_file": "\\\\Server\\Ruta\\Prompt_Proveedor.txt"
}
```

---

## Archivo de memoria local

La herramienta recuerda la última conexión y el último archivo usado en:

`layout_config_last_connection.json`

Ese archivo se guarda en la misma carpeta del script o ejecutable.

---

## Compilación one-file

Si existe el archivo:

`configurar_layout_factura.spec`

Podés compilar así:

```powershell
pyinstaller .\configurar_layout_factura.spec
```

El ejecutable queda en:

`dist\configurar_layout_factura.exe`

---

## Errores comunes

### No abre la ventana

Posibles causas:

- falta `tkinter`
- estás ejecutando en un entorno sin GUI

---

### Error al abrir PDF

Posible causa:

- falta `pymupdf`

Instalar:

```powershell
pip install pymupdf
```

---

### Error al hacer OCR

Posibles causas:

- falta `pytesseract`
- falta Tesseract OCR en Windows
- Tesseract no está en el `PATH`
- la ruta configurada en `Tesseract EXE` es incorrecta

Instalar librería:

```powershell
pip install pytesseract
```

Además instalar Tesseract OCR en el sistema operativo.

Ruta habitual:

```text
C:\Program Files\Tesseract-OCR\tesseract.exe
```

---

### No encuentra proveedor

Posibles causas:

- OCR defectuoso
- nombre incompleto
- CUIT no detectado
- columnas no detectables en `Vt_Proveedores`

En ese caso:

- usá `Buscar proveedor` con parte del nombre en el campo `Buscar`
- cargá manualmente el código proveedor en el campo `Código`

---

### Error al guardar en TA_CONFIGURACION

Posibles causas:

- usuario SQL sin permisos
- `TA_CONFIGURACION` tiene columnas obligatorias adicionales
- el código proveedor está vacío

La herramienta informa el detalle cuando detecta columnas obligatorias faltantes.

---

## Recomendaciones de uso

- Empezar siempre con una factura de ejemplo clara y completa.
- Marcar primero `proveedor` y `detalle`.
- Después agregar `totales`, `cae`, `cabecera` y demás zonas necesarias.
- En columnas de detalle, marcar solo el ancho real de cada dato.
- Probar OCR sobre la zona proveedor antes de buscar automáticamente.
- Verificar en SQL qué código quedó seleccionado antes de guardar.

---

## Resumen

El flujo recomendado es:

1. abrir documento
2. marcar zonas
3. marcar columnas detalle
4. probar OCR zona proveedor (opcional)
5. buscar proveedor en la pestaña 4
6. guardar layout en SQL (pestaña 5)

Para editar o eliminar un layout ya guardado usar `Gestionar layouts...`.

La clave final queda grabada como:

`IA_LYT_<codigo_proveedor>`

Y el JSON se guarda en:

`TA_CONFIGURACION.ValorAux`

---

## Referencia técnica

> Esta sección es para programadores (humanos o asistentes IA) que necesiten entender el código rápidamente.

### Archivo principal

`configurar_layout_factura.py` — script único, ~2300 líneas, sin dependencias internas propias.

### Constantes globales relevantes

| Constante | Descripción |
|---|---|
| `ZONE_TYPES` | Lista de tipos de zona: proveedor, cabecera, cliente, detalle, totales, cae, observaciones |
| `DETAIL_FIELDS` | Lista de campos de columnas del detalle (Codigo_Articulo, Descripcion, etc.) |
| `ZONE_COLORS` | Diccionario `zona → color hex` para dibujar rectángulos en el canvas |
| `ZONE_HELP` | Textos de ayuda por zona que se muestran en la UI |
| `DETAIL_FIELD_HELP` | Textos de ayuda por campo de detalle |
| `DEFAULT_LAYOUT_GROUP` | `"IA_COMPRAS"` — valor fijo del campo `Grupo` en `TA_CONFIGURACION` |
| `LAST_CONN_FILE` | Path a `layout_config_last_connection.json` (misma carpeta que el script) |

### Clases principales

#### `AppConfig` (dataclass)
Parámetros de conexión SQL y Tesseract. Se persiste en `LAST_CONN_FILE`.

#### `DocumentState` (dataclass)
Estado del documento abierto: path, tipo, página, imagen PIL original y de display, escala, offsets y caché OCR.

#### `SqlServerClient`
Todas las operaciones contra SQL Server.

| Método | Descripción |
|---|---|
| `connect()` | Abre conexión pyodbc |
| `test_connection()` | Retorna `(bool, str)` |
| `upsert_layout(clave, valor_aux)` | INSERT o UPDATE en `TA_CONFIGURACION` |
| `get_layout_payload(clave)` | Retorna el JSON guardado como string |
| `delete_layout(clave)` | DELETE de `TA_CONFIGURACION` |
| `list_required_insert_columns(table)` | Columnas NOT NULL sin default que faltan para INSERT |

#### `ProviderLookup`
Búsquedas en `Vt_Proveedores`. Autodetecta columnas de código, nombre, CUIT y domicilio.

| Método | Descripción |
|---|---|
| `detect_provider_columns()` | Mapea nombres de columnas a roles semánticos |
| `search_provider(cuit, text)` | Búsqueda libre, retorna `List[ProviderMatch]` |
| `search_saved_layouts(text)` | Solo proveedores con clave `IA_LYT_*` en `TA_CONFIGURACION` |

#### `DocumentImageLoader`
Abre PDF (via `pymupdf/fitz`) o imagen (via `Pillow`) y retorna un objeto PIL Image.

#### `OCRService`
Extrae texto de zona recortada usando `pytesseract`. Intenta primero `pdfplumber` en PDF nativos.

#### `LayoutDetectionAIService`
Llama al backend IA (OpenAI o proxy interno) con la imagen y un prompt estructurado. Retorna JSON con zonas y columnas propuestas.

#### `LayoutRepository`
Encapsula guardar y cargar layouts desde SQL usando `SqlServerClient`.

#### `LayoutManagerDialog`
Diálogo Tkinter para listar, cargar y eliminar layouts guardados. Se abre con el botón `Gestionar layouts...`.

| Método | Descripción |
|---|---|
| `_search()` | Llama a `ProviderLookup.search_saved_layouts()` y rellena el Treeview |
| `_on_modificar()` | Carga proveedor en la app principal y llama `on_load_layout()` |
| `_on_eliminar()` | Confirma y llama `SqlServerClient.delete_layout()` |

#### `LayoutEditorApp`
Clase principal de la UI (Tkinter). Contiene todo el estado de la sesión.

**Estado interno relevante:**

| Atributo | Tipo | Descripción |
|---|---|---|
| `self.zones` | `Dict[str, Dict[str,float]]` | Zonas marcadas activas (`zona → {x1,y1,x2,y2}`) |
| `self.detail_columns` | `List[Dict]` | Columnas de detalle marcadas (`campo, x1, x2`) |
| `self._mode` | `str` | `""`, `"zone"` o `"column"` — modo de dibujo activo |
| `self.selected_zone_type` | `str` | Zona actualmente seleccionada |
| `self.selected_column_field` | `str` | Campo de detalle actualmente seleccionado |
| `self._zoom_factor` | `float` | Factor de zoom del canvas |

**Métodos clave de UI:**

| Método | Descripción |
|---|---|
| `_build_tab_zonas()` | Construye la pestaña 2 con listbox de zonas, botones y help |
| `_build_tab_detalle()` | Construye la pestaña 3 con listbox de campos, botones y help |
| `_build_tab_proveedor()` | Construye la pestaña 4 con campos de proveedor |
| `on_start_mark_zone()` | Activa modo dibujo de zona, muestra banner azul |
| `on_start_mark_column()` | Activa modo dibujo de columna, muestra banner naranja |
| `_cancel_draw_mode()` | Cancela el modo dibujo (Esc) |
| `on_canvas_release()` | Finaliza el dibujo, guarda la zona o columna |
| `on_load_layout()` | Carga layout desde SQL al estado de la app |
| `on_save_layout()` | Guarda el layout actual en SQL |
| `_redraw_canvas()` | Redibuja imagen y todos los rectángulos de zonas/columnas |
| `on_manage_layouts()` | Abre `LayoutManagerDialog` |

### Flujo de datos: guardar layout

```
LayoutEditorApp.on_save_layout()
  → build_layout_json(...)           # arma el dict
  → LayoutRepository.save_layout_to_sql(code, json_dict)
      → SqlServerClient.upsert_layout(f"IA_LYT_{code}", json_str)
          → TA_CONFIGURACION (INSERT o UPDATE)
```

### Flujo de datos: cargar layout

```
LayoutManagerDialog._on_modificar()
  → set var_provider_code / name / cuit
  → LayoutEditorApp.on_load_layout()
      → LayoutRepository.load_layout_from_sql(code)
          → SqlServerClient.get_layout_payload(f"IA_LYT_{code}")
      → aplica zonas, columnas, prompt_file al estado
      → intenta abrir el archivo de ejemplo si existe en disco
      → _redraw_canvas()
```

### Formato de coordenadas

Todas las coordenadas se guardan **relativas** al tamaño de la imagen base (valores entre 0 y 1):

- Zonas: `{x1, y1, x2, y2}` relativo al ancho y alto de la imagen.
- Columnas de detalle: `{x1, x2}` relativo al **ancho total** de la imagen base (no solo del área de detalle).

### Persistencia local

`layout_config_last_connection.json` — guarda el último `AppConfig` como JSON plano. Se escribe en cada conexión exitosa.

### Transporte IA

El módulo `ia_backend_transport` (importado como `call_backend`) abstrae el acceso a modelos IA. Soporta:

- Backend remoto con `IA_BACKEND_URL` + `IA_CLIENT_ID` + `IA_CLIENT_SECRET`
- OpenAI directo con `OPENAI_API_KEY`

La variable `backend_enabled()` retorna `True` si alguno de los dos está configurado. Leer el `.env` junto al script o al ejecutable.
