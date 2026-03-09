# IA_ProcesarDocumentos

Procesamiento de documentos (PDF e imagenes) con salida estructurada para integracion con VB6/automatizaciones.

Este workspace funciona en modo cliente: los lectores llaman a un backend remoto con autenticacion HMAC. La clave `OPENAI_API_KEY` no se configura en este equipo.

## Componentes

- `lector_facturas_to_json_v5.py`: procesa facturas y genera `.json`.
- `lector_liquidaciones_to_json_v1.py`: procesa liquidaciones de tarjeta y genera `.txt`.
- `lector_gastos_bancarios_xls_v1.py`: procesa extractos `.xls/.xlsx` y genera `.txt` + archivos de control.
- `agente_procesar_cliente.py`: recorre carpetas de clientes (`TARJETAS` y `COMPRAS`) y ejecuta los lectores automaticamente.
- `ia_backend_transport.py`: transporte al backend remoto (`IA_BACKEND_URL` + firma HMAC).

## Requisitos

- Python 3.10+ (recomendado: 3.11).
- Windows (uso principal).
- Credenciales de backend remoto:
  - `IA_BACKEND_URL`
  - `IA_BACKEND_ROUTE` (default: `/v1/process`)
  - `IA_CLIENT_ID`
  - `IA_CLIENT_SECRET`

## Instalacion

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

Dependencias actuales (`requirements.txt`): `openai`, `python-dotenv`, `pillow`, `pypdf`.

## Configuracion (`.env`)

Podes usar `.env.example` como base.

```env
IA_BACKEND_URL=http://tu-servidor:8787
IA_BACKEND_ROUTE=/v1/process
IA_CLIENT_ID=cliente_oliva
IA_CLIENT_SECRET=secreto_largo_unico
IA_TASK=facturas

# Si usas agente automatico:
AGENTE_IA_TASK=PROCESO_AUTOMATICO
RUTA_CLIENTE=H:\Mi unidad\CLIENTES\PRUEBAS
# o varias rutas:
RUTAS_CLIENTE=H:\Mi unidad\CLIENTES\PRUEBAS;H:\Mi unidad\CLIENTES\OTRO
```

Notas:
- `OPENAI_API_KEY` va en el servidor backend, no en este repo cliente.
- Todos los scripts aceptan overrides por CLI (`--backend-url`, `--client-id`, etc.).

## Uso rapido

### 1) Facturas -> JSON

```powershell
python .\lector_facturas_to_json_v5.py factura.pdf --outdir E:\temp
```

Opciones importantes:
- `--model` (default: `gpt-4.1-mini`)
- `--fallback-model` (default: `gpt-4.1`)
- `--no-fallback`
- `--per-page`
- `--auto` (ajusta `tile` y `per-page` segun paginas)
- `--tile N` (1..6, solo imagenes)

Limites:
- 1 a 5 archivos por ejecucion.

Salida:
- `<nombre_original>.json` en `--outdir` (o carpeta temporal si no se indica).

### 2) Liquidaciones -> TXT

```powershell
python .\lector_liquidaciones_to_json_v1.py liquidacion.pdf --outdir E:\temp
```

Opciones importantes:
- `--model` (default: `gpt-4o-mini`)
- `--per-page`
- `--auto`
- `--tile N` (1..6)
- `--pdf-chunk-pages N` (0 = no dividir PDF)

Limites:
- hasta 100 archivos de entrada.

Salida:
- `<nombre_original>.txt`
- `<nombre_original>.log`

### 3) Gastos bancarios XLS/XLSX -> TXT

```powershell
python .\lector_gastos_bancarios_xls_v1.py extracto.xlsx --outdir E:\temp
```

Opciones importantes:
- `--rules-file` (default: `reglas_gastos_bancarios_v1.json`)
- `--model` (auditoria backend, default: `gpt-4o-mini`)
- `--no-api-audit`
- `--api-audit-strict`
- `--max-seconds` (default: 120)

Salida:
- `<nombre_original>.txt`
- `<nombre_original>.log`
- `<nombre_original>_control_conceptos.txt`

### 4) Agente automatico por carpetas

```powershell
python .\agente_procesar_cliente.py
```

Flujo:
- Toma rutas desde `RUTA_CLIENTE`/`RUTAS_CLIENTE`.
- Procesa:
  - `<RUTA>\TARJETAS` con `lector_liquidaciones_to_json_v1.py`
  - `<RUTA>\COMPRAS` con `lector_facturas_to_json_v5.py`
- Escribe resultados en `PROC_AGENTE_IA` dentro de cada carpeta.
- Usa logs para marcar archivos procesados (`<archivo>.log`).
- Reintenta archivos con estado previo `ERROR`.

Opciones:
- `--idcliente`: procesa solo el cliente de `RutaIA_procesar` (SQL).
- `--ia-task`: override de `AGENTE_IA_TASK`.

Variables relacionadas del agente:
- `ARCHIVO_ESTABLE_SEGUNDOS` (default: 120)
- `LOCK_STALE_HORAS` (default: 12)
- `REPROCESAR_TODO` (0/1)
- `PREAGRUPAR_COMPRAS` (0/1, default activo)

## Overrides comunes por CLI

Todos los scripts principales aceptan:

```text
--env-file
--no-local-env
--backend-url
--backend-route
--client-id
--client-secret
--ia-task
--idcliente
```

## Empaquetado (opcional)

```powershell
pyinstaller --onefile --noconsole lector_facturas_to_json_v5.py
pyinstaller --onefile --noconsole lector_liquidaciones_to_json_v1.py
pyinstaller --onefile --noconsole lector_gastos_bancarios_xls_v1.py
pyinstaller --onefile --noconsole agente_procesar_cliente.py
```

## Troubleshooting

- Error `No esta configurado IA_BACKEND_URL`:
  - revisar `.env` y credenciales (`IA_BACKEND_URL`, `IA_CLIENT_ID`, `IA_CLIENT_SECRET`).

- Error de escritura en salida:
  - usar `--outdir` a una carpeta local con permisos.

- En `--tile`, error de Pillow:
  - instalar dependencias (`pip install -r requirements.txt`).

- Server 2012 + PyInstaller + Python 3.11 (`python311.dll`):
  - reconstruir con Python 3.10 x64.
