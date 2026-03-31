# Proceso De Organizacion De Comprobantes De Compras

## Objetivo

Ordenar PDF e imagenes de comprobantes de compra que llegan a una carpeta comun, renombrando cada archivo segun datos extraidos del propio documento.

El objetivo final es que el archivo quede con un nombre util y consistente, por ejemplo:

```text
CAFES LA VIRGINIA S A - FACTURA - 0170-00201182-A - 20260130.jpeg
```

Si un comprobante tiene varias imagenes del mismo documento, se conserva una por archivo y se agrega un sufijo de pagina:

```text
CAFES LA VIRGINIA S A - FACTURA - 0170-00201182-A - 20260130 - PAG01DE04.jpeg
CAFES LA VIRGINIA S A - FACTURA - 0170-00201182-A - 20260130 - PAG02DE04.jpeg
CAFES LA VIRGINIA S A - FACTURA - 0170-00201182-A - 20260130 - PAG03DE04.jpeg
CAFES LA VIRGINIA S A - FACTURA - 0170-00201182-A - 20260130 - PAG04DE04.jpeg
```

## Script Involucrado

- `organizar_carpetas_compras.py`
- `lector_facturas_to_json_v5.py`

El primer script organiza y renombra.
El segundo extrae proveedor, tipo de comprobante, numero y fecha usando el backend IA.

## Flujo Actual

1. Se indica una carpeta base con archivos sueltos.
2. El script busca archivos compatibles en la raiz de esa carpeta.
3. Si detecta varias imagenes que parecen ser paginas del mismo comprobante, las agrupa.
4. Llama a `lector_facturas_to_json_v5.py` para extraer datos.
5. Arma el nombre final del archivo.
6. En simulacion solo informa que haria.
7. Con `--apply`, renombra el archivo original en la misma carpeta.

## Datos Usados Para El Nombre

El nombre final se arma con:

- proveedor
- tipo de comprobante
- punto de venta o sucursal
- numero completo
- letra
- fecha en formato `yyyymmdd`

Formato base:

```text
PROVEEDOR - TIPO - PUNTODEVENTA-NUMERO-LETRA - YYYYMMDD.ext
```

## Reglas De Extraccion

### Proveedor

Se toma con este orden de prioridad:

1. `Proveedor`
2. `Nombre`
3. nombre util tomado del archivo si no es generico
4. `CUIT_xxxxx`
5. `SIN_PROVEEDOR`

### Tipo De Comprobante

Se toma con este orden:

1. `TipoComprobante`
2. `CONCEPTO`
3. inferencia desde `meta.comprobante_raw`

Hoy reconoce al menos:

- `FACTURA`
- `REMITO`
- `NOTA DE CREDITO`
- `NOTA DE DEBITO`
- `TICKET`

### Numero Del Comprobante

Se arma usando:

- `PuntoVenta` o `SUCURSAL`
- `Numero` o `NUMERO`
- `Letra` o `LETRA`

## Agrupacion De Archivos

El script intenta agrupar paginas cuando detecta nombres tipo:

- `archivo_1de2`
- `archivo_2de2`
- `hoja1`
- `hoja2`
- `LV1`
- `LV2`

Si detecta varias paginas del mismo comprobante:

- analiza todas juntas
- conserva todos los archivos
- los renombra con sufijo `PAGxxDExx`

## Archivos Compatibles

Extensiones actuales:

- `.pdf`
- `.jpg`
- `.jpeg`
- `.png`
- `.webp`
- `.bmp`
- `.tif`
- `.tiff`

## Modo Simulacion

No toca archivos. Solo informa que renombraria.

Ejemplo:

```powershell
python .\organizar_carpetas_compras.py "C:\dev\DocProcesar"
```

Para limitar la prueba a los primeros grupos:

```powershell
python .\organizar_carpetas_compras.py "C:\dev\DocProcesar" --limit 5
```

## Modo Aplicado

Renombra los archivos en la misma carpeta.

```powershell
python .\organizar_carpetas_compras.py "C:\dev\DocProcesar" --apply
```

## Log

El log se guarda en:

```text
<carpeta_base>\LOG\YYYYMM_organizar_carpetas_compras.log
```

Ejemplo:

```text
C:\dev\DocProcesar\LOG\202603_organizar_carpetas_compras.log
```

Cada linea guarda:

- fecha y hora
- estado
- nombre anterior
- nombre nuevo
- detalle

Estados posibles:

- `OK`
- `SKIP`
- `ERROR`

## Comportamientos Importantes

### Archivos ya renombrados

Si un archivo ya parece tener formato final, el script no lo procesa automaticamente y lo deja para revision manual.

### Colisiones De Nombre

Si el nombre destino ya existe, el script agrega sufijo:

```text
(2)
(3)
```

### Subcarpetas Preexistentes

El script no procesa archivos dentro de subcarpetas.
Solo trabaja con archivos sueltos en la raiz de la carpeta indicada.

## Lecciones Aprendidas En La Primera Prueba

Durante la primera prueba hubo una confusion funcional:

- inicialmente se interpreto que habia que crear carpetas por comprobante
- el comportamiento correcto es renombrar el archivo original

Por eso:

- la version actual del script ya esta orientada a renombrar archivos
- no deberia crear carpetas nuevas

Tambien se observaron estas situaciones:

- algunos comprobantes similares producen nombres casi iguales
- algunos tipos de comprobante pueden variar entre `FACTURA` y `REMITO` si la lectura del documento es ambigua
- algunos proveedores salen mejor desde `Nombre` que desde `Proveedor`

## Recomendacion Operativa

Usar siempre este orden:

1. correr primero en simulacion
2. revisar algunos casos reales
3. ejecutar con `--apply`
4. revisar el log final

## Pendientes Posibles

Si se quiere mejorar mas adelante, estas son buenas opciones:

- unificar variaciones menores de nombres de proveedor
- normalizar mejor ceros a la izquierda en sucursal o punto de venta
- fortalecer la deteccion de `FACTURA` vs `REMITO`
- agregar modo de reproceso de archivos dentro de subcarpetas
- agregar salida CSV de auditoria
