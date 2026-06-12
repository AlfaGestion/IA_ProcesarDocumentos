# Movimientos Financieros Unificado

## Objetivo

Unificar el procesamiento de:

- extractos bancarios (`xls`, `xlsx`, `pdf`)
- liquidaciones de tarjeta (`pdf`, imágenes)
- documentos mixtos donde banco y tarjeta aparecen en la misma fuente

La unificación no debe reemplazar de golpe a los lectores actuales. Conviene montar un flujo nuevo por encima de ellos, reutilizando la lógica que ya funciona y normalizando la salida en un esquema común.

## Diagnóstico actual

- `lector_gastos_bancarios_xls_v1.py` resuelve bien extractos tabulares y clasifica por reglas.
- `lector_liquidaciones_to_json_v1.py` ya tiene heurísticas valiosas para PDFs de Nación, Patagonia y marcas de tarjeta.
- Hay documentos mixtos donde el problema real no es el formato sino separar eventos contables distintos dentro del mismo archivo.
- Los ejemplos relevados tienen texto extraíble en PDF, por lo que conviene priorizar extracción textual/heurística antes de usar IA visual plena.

## Estrategia recomendada

Pipeline híbrido:

1. Detección de fuente
2. Extracción estructurada primaria
3. Clasificación contable normalizada
4. Consolidación por documento
5. Resumen IA y propuesta de asiento
6. Emisión de salidas compatibles

Regla general:

- primero determinístico
- después IA para ambigüedad, resumen y cierre

## Tipos de documento

- `bank_statement`
- `card_settlement`
- `mixed_financial`
- `unknown`

## Categorías contables normalizadas

- `TARJETA`
- `BANCO`
- `GASTOS`
- `IVA_CREDITO`
- `RET_IVA`
- `RET_IIBB`
- `RET_GAN`
- `IDC_A_COMPUTAR`
- `OTROS`

Notas:

- En extractos bancarios la contrapartida suele terminar en `BANCO`.
- En liquidaciones la presentación bruta suele terminar en `TARJETA`.
- Algunos bancos usan labels distintos para el mismo concepto; esos aliases se deben mapear a esta tabla.

## Esquema JSON unificado

```json
{
  "schema_version": 1,
  "source_file": "BNA 04-26.pdf",
  "source_kind": "pdf",
  "document_type": "mixed_financial",
  "institution": "BANCO DE LA NACION ARGENTINA",
  "card_brand": "TARJETA VISA",
  "period": "30/04/2026",
  "currency": "ARS",
  "items": [
    {
      "date": "01/04/2026",
      "description": "CR LIQ VISA DEB-SUC 000135969773",
      "raw_amount": 378593.09,
      "signed_amount": 378593.09,
      "direction": "credit",
      "channel": "card",
      "category": "BANCO",
      "confidence": 0.98,
      "source_section": "movimientos"
    }
  ],
  "totals": {
    "TARJETA": 0.0,
    "BANCO": 0.0,
    "GASTOS": 0.0,
    "IVA_CREDITO": 0.0,
    "RET_IVA": 0.0,
    "RET_IIBB": 0.0,
    "RET_GAN": 0.0,
    "IDC_A_COMPUTAR": 0.0,
    "OTROS": 0.0
  },
  "summary": {
    "short_text": "",
    "warnings": [],
    "needs_review": false
  },
  "proposed_entry": [
    {
      "account_key": "TARJETA",
      "amount": 0.0,
      "sign": 1
    }
  ],
  "trace": {
    "extractor": "bna_pdf_v1",
    "used_ai": false,
    "notes": []
  }
}
```

## Capas del nuevo módulo

### 1. `sniffers`

Responsables de inferir:

- formato real
- banco
- marca de tarjeta
- si el documento parece bancario, de tarjeta o mixto

### 2. `extractors`

Extractores especializados por familia:

- `bank_excel_extractor`
- `bank_pdf_extractor`
- `card_pdf_extractor`
- `mixed_pdf_extractor`

### 3. `normalizers`

Mapean:

- labels del documento
- nombres de conceptos
- signos
- fechas
- cuentas contables

### 4. `summarizer`

Usa IA para:

- explicar qué se detectó
- señalar inconsistencias
- proponer asiento si hay ambigüedad

### 5. `emitters`

Salidas esperables:

- `.json` unificado
- `.txt` compatible con el proceso actual
- archivo de control
- resumen textual para revisión

## Reutilización concreta

Desde `lector_gastos_bancarios_xls_v1.py`:

- detección por reglas
- parsing de importes argentinos
- clasificación por regex
- cálculo de contrapartida

Desde `lector_liquidaciones_to_json_v1.py`:

- extracción de texto PDF
- heurísticas de Nación / Patagonia
- detección de tarjeta y período
- soporte de backend IA

## Orden recomendado de implementación

### Etapa 1

- crear módulo unificado y esquema JSON
- soportar lectura de `xlsx`, `xls` y `pdf`
- detectar banco, tarjeta y tipo de documento
- generar salida estructural mínima
- emitir `.txt`, `.log` y `.unificado.json` para pruebas de punta a punta

### Etapa 2

- incorporar extractores PDF bancarios para Nación, Patagonia y Galicia
- reutilizar el extractor de liquidaciones actual
- clasificar movimientos mixtos en un mismo documento

### Etapa 3

- consolidar asiento automático
- agregar resumen IA
- marcar warnings y posibles duplicados

### Etapa 4

- conciliación extracto vs liquidación
- deduplicación entre fuente bancaria y fuente de tarjeta

## Decisiones pendientes

- si el `.txt` seguirá siendo la salida principal o si el `.json` pasará a ser la base del proceso
- prioridad de fuente cuando existe extracto bancario y liquidación por separado
- nivel de detalle del asiento final
- criterio de deduplicación entre movimientos de tarjeta y acreditaciones bancarias

## MVP sugerido

1. Nuevo módulo unificado con salida JSON
2. Soporte PDF bancario para BNA, Patagonia y Galicia
3. Reuso de heurísticas de liquidaciones existentes
4. Resumen breve y propuesta de asiento
5. Emisión opcional de TXT compatible

## Estado actual del prototipo

El script `lector_movimientos_financieros_unificado.py` ya puede:

- detectar si la fuente es planilla, extracto bancario PDF o documento mixto
- emitir `JSON` unificado
- emitir `TXT` compatible para prueba funcional
- emitir `LOG` con trazabilidad, warnings y totales
- emitir control por conceptos para revisión contable

Uso de prueba:

```powershell
python .\lector_movimientos_financieros_unificado.py "BNA 04-26.pdf" --outdir E:\temp
```

Archivos generados:

- `<nombre>.txt`
- `<nombre>.log`
- `<nombre>.unificado.json`
- `<nombre>_control_conceptos.xls`

## Prueba recomendada para usuario final

### Opción 1: desde consola

```powershell
python .\lector_movimientos_financieros_unificado.py "C:\DOCPROCESAR\BNA 04-26.pdf" --outdir "C:\DOCPROCESAR\OUTDIR"
```

### Opción 2: con batch

Archivo:

- `probar_unificado_docprocesar.bat`

Flujo:

1. copiar el archivo a procesar dentro de `C:\DOCPROCESAR`
2. ejecutar `probar_unificado_docprocesar.bat`
3. ingresar solo el nombre del archivo

Configuración fija del batch:

- entrada: `C:\DOCPROCESAR`
- salida: `C:\DOCPROCESAR\OUTDIR`

## Cómo validar el resultado

Orden sugerido:

1. revisar el `.txt` para ver el resumen contable general
2. revisar el `_control_conceptos.xls` para validar conceptos y acumulados
3. revisar el `.log` para detectar warnings y conceptos no clasificados
4. revisar el `.unificado.json` solo cuando se necesite detalle técnico o depuración

## Limitaciones actuales

- todavía hay conceptos bancarios no clasificados en algunos PDFs, por ejemplo transferencias, `DEBIN` o `VEP`
- algunos bancos entregan texto PDF muy pegado, por lo que ciertas descripciones pueden salir poco limpias
- el flujo ya es útil para pruebas contables, pero todavía no reemplaza por completo la conciliación final ni el resumen IA definitivo
