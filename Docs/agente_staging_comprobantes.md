# Agente Staging Comprobantes — Manual de uso

**Script:** `agente_staging_comprobantes.py`  
**Versión:** 1.1  
**Requiere:** Python 3.10+, `pyodbc`, `lector_facturas_to_json_v5.py` (o `.exe`) en la misma carpeta.

---

## ¿Qué hace?

Procesa automáticamente una carpeta de comprobantes de compra (PDF / imágenes). Para cada archivo o grupo de archivos:

1. Llama al **lector IA** (`lector_facturas_to_json_v5`) para extraer los datos del comprobante.
2. Busca el **proveedor** en la vista SQL `Vt_Proveedores` (por CUIT, nombre o domicilio).
3. Graba el resultado en las tablas **`IA_Compras_CAB`** y **`IA_Compras_DET`** de SQL Server.
4. **Renombra y mueve** el archivo a `<carpeta_origen>/PROC_AGENTE_IA/` con nombre estructurado.
5. Guarda el **JSON** del lector junto al archivo renombrado.
6. Registra todo en un **log** diario dentro de `<carpeta_origen>/LOG/`.

Sin el flag `--apply` el agente **solo simula**: imprime qué haría pero no graba ni renombra nada.

---

## Modos de ejecución

### Modo gráfico (GUI)

Ejecutar sin argumentos abre una ventana con todas las opciones:

```
python agente_staging_comprobantes.py
```

La última carpeta usada se recuerda automáticamente en `agente_staging_comprobantes.gui.json`.

### Modo consola / línea de comandos

```
python agente_staging_comprobantes.py <carpeta> [opciones]
```

---

## Parámetros de línea de comandos

| Parámetro | Descripción | Default |
|---|---|---|
| `root` | Carpeta con los archivos a procesar (obligatorio) | — |
| `--apply` | Aplica cambios reales: graba en SQL y renombra archivos. Sin este flag es simulación. | desactivado |
| `--max-files N` | Máximo de archivos que puede tener un grupo multipágina | `10` |
| `--limit N` | Procesa solo los primeros N grupos (`0` = sin límite) | `0` |
| `--server` | Servidor SQL Server | `SERVER-ALFAVB6` |
| `--database` | Base de datos | `ALFANET` |
| `--user` | Usuario SQL | `ALFANET` |
| `--password` | Contraseña SQL | `ALFANET` |
| `--driver` | Driver ODBC | `ODBC Driver 18 for SQL Server` |
| `--keep-accents` | Conserva tildes y ñ en los nombres de archivo renombrados | desactivado |
| `--log-file` | Nombre del archivo de log | `agente_staging_comprobantes.log` |

---

## Ejemplos

**Simular (sin cambios) sobre una carpeta:**
```
python agente_staging_comprobantes.py "C:\Facturas\Pendientes"
```

**Procesar y aplicar cambios reales:**
```
python agente_staging_comprobantes.py "C:\Facturas\Pendientes" --apply
```

**Probar solo los primeros 3 grupos:**
```
python agente_staging_comprobantes.py "C:\Facturas\Pendientes" --apply --limit 3
```

**Con servidor y credenciales distintas:**
```
python agente_staging_comprobantes.py "C:\Facturas" --apply --server MI-SERVER --database MIDB --user sa --password 1234
```

---

## Agrupación de archivos multipágina

El agente agrupa automáticamente archivos que pertenecen al mismo comprobante antes de enviárselos al lector:

| Patrón detectado | Ejemplo | Resultado |
|---|---|---|
| Sufijo `NdeTOTAL` | `factura1de3.jpg`, `factura2de3.jpg`, `factura3de3.jpg` | 1 grupo de 3 |
| Sufijo `pagN` / `hojaN` | `remito_pag1.jpg`, `remito_pag2.jpg` | 1 grupo de 2 |
| Índice numérico final en imagen | `foto1.jpg`, `foto2.jpg`, `foto3.jpg` | 1 grupo (si secuencial) |
| PDF siempre individual | `comprobante.pdf` | siempre 1 grupo solo |
| Ya renombrado (formato agente) | `FC 00001-00000001 20260101 PROV SA.pdf` | se omite |

Grupos con más archivos que `--max-files` se saltean con estado `SKIP`.

---

## Formato del nombre de archivo renombrado

```
TIPO PVENTA-NUMEROLETRA AAAAMMDD PROVEEDOR
```

| Parte | Ejemplo | Descripción |
|---|---|---|
| `TIPO` | `FC`, `NC`, `ND`, `RM`, `TK`, `PR`, `NP`, `CPTE` | Abreviación del tipo de comprobante |
| `PVENTA-NUMEROLETRA` | `00020-00076690A` | Punto de venta (5 dígitos) + número (8 dígitos) + letra |
| `AAAAMMDD` | `20260311` | Fecha del comprobante |
| `PROVEEDOR` | `GO SHOP SA` | Razón social de SQL si matcheó, sino el nombre extraído por el lector |

**Archivo multipágina** (ej. 3 páginas):
```
FC 00020-00076690A 20260311 GO SHOP SA - PAG01DE03.jpg
FC 00020-00076690A 20260311 GO SHOP SA - PAG02DE03.jpg
FC 00020-00076690A 20260311 GO SHOP SA - PAG03DE03.jpg
```

Si el nombre destino ya existe, se agrega un sufijo `(2)`, `(3)`, etc.

---

## Estados posibles en `IA_Compras_CAB`

| Estado | Significado |
|---|---|
| `PENDIENTE` | Procesado OK, proveedor identificado, listo para revisar |
| `SIN_PROVEEDOR` | El lector extrajo datos pero no se encontró el proveedor en SQL. Grabado igual, sin cuenta contable. |
| `ERROR_LECTURA` | El lector IA falló al procesar el archivo |
| `ERROR` | Error general durante el procesamiento |
| `SKIP` | Ya existía en staging o superaba el límite de páginas |

---

## Búsqueda de proveedor

El agente intenta encontrar el proveedor en `Vt_Proveedores` en tres pasos, en orden:

1. **CUIT exacto** — compara el CUIT extraído por el lector (solo dígitos) contra `NUMERO_DOCUMENTO`.
2. **Nombre LIKE** — busca `RAZON_SOCIAL LIKE '%nombre%'` con el nombre extraído (mínimo 4 caracteres).
3. **Domicilio LIKE** — busca en `CALLE` o `LOCALIDAD` con el domicilio extraído (mínimo 5 caracteres).

El campo `Match_Metodo` en `IA_Compras_CAB` indica cuál de los tres métodos encontró el proveedor (`CUIT`, `NOMBRE`, `DOMICILIO`, o vacío si no matcheó).

---

## Prompt del lector IA

El agente construye automáticamente el prompt para el lector consultando `TA_CONFIGURACION`:

- **Prompt base:** clave `IA_PROMPT_COMPRAS` con `Valor = DEFAULT` (campo `ValorAux`).
- **Prompt específico:** clave = cuenta contable del proveedor, `Grupo = Compras` (campo `ValorAux`). Si existe, se concatena al base.

Los archivos de prompt se guardan en la ruta configurada en la clave `RutaDocumentosCompras` de `TA_CONFIGURACION`.

Si no hay prompt configurado en la BD, el lector usa su prompt interno por defecto.

---

## Archivos generados

| Archivo / Carpeta | Descripción |
|---|---|
| `<carpeta>/PROC_AGENTE_IA/<nombre_renombrado>.*` | Comprobante renombrado y movido |
| `<carpeta>/PROC_AGENTE_IA/<nombre_renombrado>.json` | JSON completo devuelto por el lector IA |
| `<carpeta>/LOG/YYYYMM_agente_staging_comprobantes.log` | Log TSV con columnas: `FechaHora`, `Estado`, `NombreOriginal`, `NombreNuevo`, `Detalle` |
| `agente_staging_comprobantes.gui.json` | Configuración de la GUI (última carpeta usada) |

---

## Dependencias SQL Server

| Objeto | Tipo | Uso |
|---|---|---|
| `IA_Compras_CAB` | Tabla | Cabecera del comprobante procesado |
| `IA_Compras_DET` | Tabla | Renglones del comprobante |
| `Vt_Proveedores` | Vista | Lookup de proveedor por CUIT / nombre / domicilio |
| `TA_CONFIGURACION` | Tabla | Prompt IA, ruta de documentos, configuración general |

---

## Resumen de ejecución

Al finalizar el agente muestra:

```
Resumen APLICADO: OK=12  SIN_PROVEEDOR=2  SKIP=1  ERROR=0
Log: C:\Facturas\Pendientes\LOG\202604_agente_staging_comprobantes.log
```

El código de salida es `0` si no hubo errores, `2` si hubo al menos un error.

---

## Notas importantes

- **Sin `--apply` nunca se modifica nada.** Siempre probá primero en modo simulación.
- Si un archivo ya fue procesado (existe en `IA_Compras_CAB` con estado distinto a `ERROR_LECTURA`) se saltea automáticamente con estado `SKIP`.
- Los archivos ya renombrados con el formato del agente también se omiten en la agrupación.
- El agente requiere que `lector_facturas_to_json_v5.py` (o `.exe` si está compilado) esté en la misma carpeta.
