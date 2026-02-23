# IA_ProcesarDocumentos

Scripts para leer documentos (im�genes o PDF) y devolver JSON normalizado usando OpenAI.

## Requisitos
- Python 3.10+ (recomendado 3.11)
- Credenciales de backend remoto (`IA_BACKEND_URL`, `IA_CLIENT_ID`, `IA_CLIENT_SECRET`)

## Instalaci�n
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Variables de entorno (cliente)
Crear un archivo `.env` en la carpeta del proyecto con:
```
IA_BACKEND_URL=http://tu-servidor:8787
IA_BACKEND_ROUTE=/v1/process
IA_CLIENT_ID=cliente_oliva
IA_CLIENT_SECRET=secreto_largo_unico
IA_TASK=facturas
```

`OPENAI_API_KEY` no se configura en este workspace del cliente.

## Modo backend remoto (recomendado para instalar en PCs de clientes)
En cliente no se expone `OPENAI_API_KEY`. Los lectores (`v5` y `v1`) usan solo backend remoto:
```
IA_BACKEND_URL=http://tu-servidor:8787
IA_BACKEND_ROUTE=/v1/process
IA_CLIENT_ID=cliente_oliva
IA_CLIENT_SECRET=secreto_largo_unico
IA_TASK=facturas
```

El script usa firma HMAC (`timestamp + nonce + body`) y no permite `OPENAI_API_KEY` local en cliente.

### Backend proxy (ubicado en wsAlfa)
El backend ahora vive en: `e:\Dev\wsAlfa\ia_backend\ia_backend_proxy_server.py`

Variables de entorno backend (en `e:\Dev\wsAlfa\.env`):
```env
OPENAI_API_KEY=tu_api_key_openai_servidor
IA_CLIENTS_JSON={"cliente_oliva":"secreto_largo_unico","cliente_demo":"otro_secreto"}
IA_BACKEND_HOST=0.0.0.0
IA_BACKEND_PORT=8787
IA_MAX_SKEW_SECONDS=300
```

Ejecutar backend:
```powershell
cd e:\Dev\wsAlfa
python .\ia_backend\ia_backend_proxy_server.py
```

## Uso b�sico (facturas)
```powershell
python .\lector_facturas_to_json_v5.py FACT_3hojasEn1.pdf --outdir E:\app\IA_ProcesarDocumentos\
```

## Varias p�ginas (im�genes)
```powershell
python .\lector_facturas_to_json_v5.py fac1.jpg fac2.jpg --outdir E:\temp
```

## Prompt personalizado
```powershell
python .\lector_facturas_to_json_v5.py fac1.jpg fac2.jpg --prompt-file E:\DocProcesar\Prompt_211010026.txt --outdir E:\DocProcesar
```

## GUI (ventana de progreso)
```powershell
python .\lector_facturas_to_json_v5.py fac1.jpg --outdir E:\temp --gui
```

## Modo por p�gina (mejora tablas largas)
```powershell
python .\lector_facturas_to_json_v5.py fac1.jpg fac2.jpg --outdir E:\temp --per-page
```

## Auto-ajuste (tile + per-page)
Auto-ajusta par�metros seg�n cantidad de p�ginas:
- 1 p�gina: `tile=3`, `per-page` OFF
- 2-3 p�ginas: `tile=4`, `per-page` ON
- 4+ p�ginas: `tile=5`, `per-page` ON
```powershell
python .\lector_facturas_to_json_v5.py fac1.jpg fac2.jpg --outdir E:\temp --auto
```

## Tileado por franjas (mejor OCR en tablas largas)
Requiere Pillow. Divide cada imagen en N franjas horizontales y unifica los resultados.
```powershell
python .\lector_facturas_to_json_v5.py fac1.jpg fac2.jpg --outdir E:\temp --per-page --tile 3
```

## Modelo
```powershell
python .\lector_facturas_to_json_v5.py fac1.jpg --model gpt-4.1 --outdir E:\temp
```
****************************************************************************************************
Para liquidaciones, el modelo por defecto es `gpt-4.1`. Pod�s cambiarlo con `--model`.
****************************************************************************************************
## Uso b�sico (liquidaciones de tarjetas)
Genera un archivo de texto con encabezado y luego dos columnas: `CONCEPTO|IMPORTE`.
```powershell
python .\lector_liquidaciones_to_json_v1.py liquidacion.pdf --outdir E:\temp
```

## Varias p�ginas (liquidaciones)
```powershell
python .\lector_liquidaciones_to_json_v1.py img1.jpg img2.jpg --outdir E:\temp
```

## Prompt personalizado (liquidaciones)
```powershell
python .\lector_liquidaciones_to_json_v1.py liquidacion.pdf --prompt-file E:\DocProcesar\Prompt_Liq.txt --outdir E:\DocProcesar
```

## GUI (liquidaciones)
```powershell
python .\lector_liquidaciones_to_json_v1.py liquidacion.pdf --outdir E:\temp --gui
```

## Modo por p�gina (liquidaciones)
```powershell
python .\lector_liquidaciones_to_json_v1.py img1.jpg img2.jpg --outdir E:\temp --per-page
```

## Auto-ajuste (liquidaciones)
```powershell
python .\lector_liquidaciones_to_json_v1.py img1.jpg img2.jpg --outdir E:\temp --auto
```

## Notas
- `--tile` solo aplica a im�genes (JPG/PNG/WEBP). Para PDF se ignora.
- M�ximo 5 archivos por ejecuci�n.
- Para liquidaciones, la salida es `.txt` con este formato:
  - L�nea 1: nombre del banco
  - L�nea 2: nombre de la tarjeta
  - L�nea 3: per�odo (mes/a�o)
  - L�nea 4: concepto (m�x. 50 caracteres)
  - L�nea 5: `CONCEPTO|IMPORTE`
  - L�neas siguientes: conceptos e importes
- Para Banco Naci�n, adem�s se genera un control diario en `*_control_diarios.xls` (tabulado).
- Se valida integridad b�sica: suma de `ROWS.Total` vs `TOTALES.Neto gravado` (o `TOTALES.Total`). Si el desv�o supera 3%, se agrega una advertencia en `meta.observaciones`.
- Si en el texto aparece "Cantidad de items: N" y se detectan menos filas, se agrega una advertencia en `meta.observaciones`.

## Troubleshooting
### Error: backend no configurado (`IA_BACKEND_URL`)
Activ� el entorno y reinstal� dependencias:
```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

### Error PyInstaller en Server 2012: `Failed to load Python DLL ... python311.dll`
Server 2012 no soporta Python 3.11. Rebuild con Python 3.10 x64 y us� `--onedir`.


## Empaquetado (opcional)
Con PyInstaller:
```powershell
pyinstaller --onefile --noconsole lector_facturas_to_json_v5.py
pyinstaller --onefile --noconsole lector_liquidaciones_to_json_v1.py
```

 




