# Esquema de Base de Datos — ALFANET

## Bases de datos involucradas

| Rol | Servidor | Base | Usuario | Contraseña |
|---|---|---|---|---|
| **Producción** (proveedores, comprobantes, staging IA) | `SERVER-ALFAVB6` | `ALFANET` | `ALFANET` | `ALFANET` |
| **Registro uso IA por cliente** (agente_procesar_cliente.py) | `10.8.0.9` | `ALFA_CENTRAL` | `ALFA_CENTRAL` | `ALFA_CENTRAL` |

> El nuevo agente de staging usa **exclusivamente** `SERVER-ALFAVB6 / ALFANET`.  
> Si no se pasan parámetros de conexión, debe pedirlos (GUI o prompt interactivo).

Driver por defecto: `ODBC Driver 18 for SQL Server`

---

## Convenciones generales

| Prefijo tabla | Significado |
|---|---|
| `MA_` | Maestros (datos fijos: cuentas, artículos, etc.) |
| `MV_` | Movimientos contables / operativos |
| `TA_` | Tablas de configuración y parámetros |
| `V_` | Ventas (comprobantes de venta) |
| `C_` | Compras (comprobantes de compra) |
| `P_` | Personal / RRHH |
| `AUX_` | Tablas auxiliares / temporales de proceso |

---

## Código de cuenta contable (`MA_CUENTAS.CODIGO`)

- `nvarchar(15)`, alineado a la izquierda, sin relleno de ceros a la derecha
- Estructura jerárquica por dígitos:
  - Dígito 1: rubro (ej. `2` = Pasivo)
  - Dígito 2: sub-rubro (ej. `1` = Pasivo Corriente)
  - Dígito 3-4: grupo (ej. `10` = Deudores corrientes / Proveedores)
  - Dígitos 5-11: cuenta título
  - Últimos 4: código individual del proveedor/cliente
- Ejemplo: `211010001` = Proveedor "Juancito" (Pasivo → Corriente → Proveedores → 0001)
- `MA_CUENTAS.TITULO = 1` → es cuenta título (no imputable), `0` → cuenta imputable
- `MA_CUENTAS.Libro_Iva_Compras = 1` → aparece en libro IVA compras

---

## MA_CUENTAS — Plan de cuentas / Proveedores / Clientes

Tabla principal del plan de cuentas. Todo proveedor, cliente, banco, etc. es una cuenta.

| Columna | Tipo | Descripción |
|---|---|---|
| `CODIGO` | nvarchar(15) PK | Código de cuenta contable |
| `DESCRIPCION` | nvarchar(50) | Nombre / razón social |
| `TITULO` | bit | 1=cuenta título (no imputable), 0=imputable |
| `BLOQUEO` | bit | 1=bloqueada |
| `Dada_De_Baja` | bit | 1=inactiva |
| `Libro_Iva_Compras` | bit | 1=participa en libro IVA compras |
| `Libro_Iva_Ventas` | bit | 1=participa en libro IVA ventas |
| `TipoVista` | nvarchar(2) | `PR`=Proveedor, `CL`=Cliente, `PE`=Personal, `CB`=Cliente Bonos |
| `FechaHora_Grabacion` | datetime | |
| `FechaHora_Modificacion` | datetime | |

**Búsqueda de proveedor por CUIT:** ver `MA_CUENTASADIC.NUMERO_DOCUMENTO`  
**Búsqueda por nombre:** `MA_CUENTAS.DESCRIPCION`

---

## MA_CUENTASADIC — Datos adicionales de cuentas

Complemento 1:1 de `MA_CUENTAS`. Contiene CUIT, domicilio, condición IVA, etc.

| Columna | Tipo | Descripción |
|---|---|---|
| `CODIGO` | nvarchar(15) PK/FK→MA_CUENTAS | Código de cuenta |
| `DOCUMENTO_TIPO` | nvarchar(4) | Tipo doc (ej. `CUIT`) |
| `NUMERO_DOCUMENTO` | nvarchar(13) | CUIT / DNI (solo dígitos o con guiones) |
| `IVA` | nvarchar(4) | FK→TA_CONDIVA (condición IVA) |
| `CALLE` | nvarchar(50) | Domicilio — calle |
| `NUMERO` | nvarchar(6) | Domicilio — número |
| `LOCALIDAD` | nvarchar(50) | |
| `PROVINCIA` | nvarchar(4) | FK→tabla provincias |
| `CPOSTAL` | nvarchar(10) | Código postal |
| `TELEFONO` | nvarchar(50) | |
| `MAIL` | nvarchar(250) | |
| `CONTACTO` | nvarchar(70) | Persona de contacto |
| `OBSERVACIONES` | nvarchar(200) | |

**Query buscar proveedor por CUIT:**
```sql
SELECT c.CODIGO, c.DESCRIPCION, a.NUMERO_DOCUMENTO, a.IVA
FROM MA_CUENTAS c
JOIN MA_CUENTASADIC a ON a.CODIGO = c.CODIGO
WHERE a.NUMERO_DOCUMENTO = @cuit
  AND c.Dada_De_Baja = 0
  AND c.TITULO = 0
```

**Query buscar por nombre (fallback):**
```sql
SELECT c.CODIGO, c.DESCRIPCION
FROM MA_CUENTAS c
WHERE c.DESCRIPCION LIKE '%' + @nombre + '%'
  AND c.Dada_De_Baja = 0
  AND c.TITULO = 0
  AND c.TipoVista = 'PR'
```

---

## TA_CONDIVA — Condiciones de IVA

| Columna | Tipo | Descripción |
|---|---|---|
| `CODIGO` | nvarchar(4) PK | Código |
| `DESCRIPCION` | nvarchar(50) | Ej: "Responsable Inscripto", "Monotributo" |

---

## TA_COMPROBANTES — Tipos de comprobante

| Columna | Tipo | Descripción |
|---|---|---|
| `CODIGO` | nvarchar(4) PK | TC (tipo comprobante), ej. `FC`, `NC`, `REM` |
| `DESCRIPCION` | nvarchar(50) | Nombre |
| `SISTEMA` | nvarchar(20) | Subsistema al que pertenece |
| `DEBE-HABER` | nvarchar(1) | `D`=Debe, `H`=Haber |
| `ES` | nvarchar(1) | Tipo operación (`C`=Compra, `V`=Venta, etc.) |
| `Externo` | bit | 1=comprobante externo (de proveedor) |
| `Talonario` | int | PK compuesta con CODIGO |

---

## TA_CONFIGURACION — Parámetros y configuración del sistema

Tabla genérica de clave/valor usada en todo el sistema (equivalente a un INI extendido).

| Columna | Tipo | Descripción |
|---|---|---|
| `Grupo` | nvarchar(50) | Agrupación lógica de parámetros (puede ser NULL) |
| `Clave` | nvarchar(100) | Clave del parámetro |
| `Valor` | nvarchar(500) | Valor principal (texto corto) |
| `ValorAux` | ntext | Valor extendido (textos largos: prompts, queries, etc.) |

### Helper `_cfg()` en Python (equivalente VB6 `cfg()`)

```python
_cfg(conn_str, clave, *, grupo=None, valor_filter=None, field="Valor") -> Optional[str]
```

- Sin parámetros extra → devuelve `Valor` para esa `Clave`
- `field="ValorAux"` → devuelve el campo ntext
- `grupo=` y `valor_filter=` → filtros adicionales

### Claves conocidas relevantes para IA

| Grupo | Clave | Valor | ValorAux | Uso |
|---|---|---|---|---|
| *(NULL)* | `IA_PROMPT_COMPRAS` | `DEFAULT` | Texto del prompt base | Prompt por defecto para leer facturas |
| `Compras` | *(código cuenta proveedor)* | *(cualquiera)* | Texto de excepción | Se **agrega** al prompt base para ese proveedor |
| *(NULL)* | `RutaDocumentosCompras` | Ruta local (ej. `C:\DocCompras`) | — | Carpeta donde se guardan archivos de trabajo (prompts, etc.) |

### Lógica de prompt por proveedor

1. Obtener prompt base: `Clave='IA_PROMPT_COMPRAS'`, `Valor='DEFAULT'` → `ValorAux`
2. Leer documento con prompt base → extraer CUIT → lookup proveedor → obtener `cuenta_contable`
3. Buscar excepción: `Grupo='Compras'`, `Clave=cuenta_contable` → `ValorAux`
4. Si existe excepción → concatenar (base + excepción) → re-leer documento
5. Guardar prompt combinado en `{RutaDocumentosCompras}/Prompt_{cuenta_contable}.txt`

---

## C_MV_Cpte — Cabecera de comprobantes de COMPRAS (producción)

PK: `(TC, IDCOMPROBANTE, CUENTA)`

| Columna | Tipo | Descripción |
|---|---|---|
| `ID` | int IDENTITY | |
| `TC` | nvarchar(4) | Tipo comprobante |
| `IDCOMPROBANTE` | nvarchar(13) | Número interno del comprobante |
| `CUENTA` | nvarchar(15) | Código cuenta proveedor (FK→MA_CUENTAS) |
| `FECHA` | datetime | Fecha del comprobante |
| `VENCIMIENTO` | datetime | Fecha de vencimiento |
| `NOMBRE` | nvarchar(50) | Nombre proveedor (desnormalizado) |
| `DOMICILIO` | nvarchar(50) | |
| `DOCUMENTOTIPO` | nvarchar(4) | |
| `DOCUMENTONUMERO` | nvarchar(13) | CUIT |
| `CONDICIONIVA` | nvarchar(4) | FK→TA_CONDIVA |
| `SUCURSAL` | nvarchar(4) | Punto de venta |
| `NUMERO` | nvarchar(8) | Número comprobante |
| `LETRA` | nvarchar(1) | A/B/C/M/X |
| `IMPORTE` | money | Total con IVA |
| `IMPORTE_S_IVA` | money | Total sin IVA |
| `NetoGravado` | money | Neto gravado |
| `NetoNoGravado` | money | Neto no gravado |
| `ImporteIva` | money | IVA 21% |
| `ImporteIvaRec` | money | IVA 10.5% |
| `ImporteIva2` | money | IVA 27% |
| `AlicIva` | float | Alícuota IVA principal |
| `ImporteImpuestosInternos` | money | |
| `Moneda` | nvarchar(4) | FK→TA_MONEDAS |
| `Cotizacion` | money | |
| `FechaSubdiario` | datetime | Fecha contable |
| `ANULADA` | bit | |
| `Finalizado` | bit | |
| `FechaHora_Grabacion` | datetime | |
| `FechaHora_Modificacion` | datetime | |

---

## C_MV_CPTE_PERCEPCIONES — Percepciones por comprobante de compra

PK: `(CUENTA, TC, IDCOMPROBANTE, PROVINCIA)`

| Columna | Tipo | Descripción |
|---|---|---|
| `ID` | int IDENTITY | |
| `CUENTA` | nvarchar(15) | FK→MA_CUENTAS |
| `TC` | nvarchar(4) | Tipo comprobante |
| `IDCOMPROBANTE` | nvarchar(13) | |
| `PROVINCIA` | nvarchar(4) | Código provincia |
| `PERCEPCION` | money | Importe percepción |

---

## MV_ASIENTOS — Asientos contables (incluye datos libro IVA)

Tabla central de contabilidad. Los campos `LIVA_*` se usan para el libro IVA.

Campos relevantes libro IVA compras:

| Columna | Tipo | Descripción |
|---|---|---|
| `CUENTA` | nvarchar(15) | Cuenta contable |
| `TC` | nvarchar(4) | Tipo comprobante |
| `SUCURSAL` | nvarchar(4) | Punto de venta |
| `NUMERO` | nvarchar(8) | Número |
| `LETRA` | nvarchar(1) | |
| `FECHA` | datetime | |
| `FechaSubdiario` | datetime | Fecha contable |
| `CABCUENTA` | nvarchar(15) | Cuenta del proveedor |
| `CABNOMBRE` | nvarchar(50) | Nombre proveedor |
| `CABCUIT` | nvarchar(13) | CUIT |
| `CABCONDIVA` | nvarchar(4) | Condición IVA |
| `LIVA_TIPO` | nvarchar(8) | Tipo comprobante libro IVA |
| `LIVA_ImpNetoGrav` | money | Neto gravado |
| `LIVA_ImpNetoNGrav` | money | Neto no gravado |
| `LIVA_EXENTO` | money | Exento |
| `LIVA_AlicIVA` | float | Alícuota IVA 1 |
| `LIVA_ImpIVA` | money | IVA 1 (21%) |
| `LIVA_AlicIVAREC` | float | Alícuota IVA reducido |
| `LIVA_ImpIVARec` | money | IVA reducido (10.5%) |
| `LIVA_AlicIva2` | float | Alícuota IVA 2 |
| `LIVA_ImpIva2` | money | IVA 2 (27%) |
| `LIVA_Ret_Perc` | money | Percepción IVA |
| `LIVA_Ret_IBtos` | money | Percepción IIBB |
| `LIVA_Ret_Ganancias` | money | Percepción Ganancias |
| `LIVA_TOTAL` | money | Total comprobante |
| `PERIODO` | nvarchar(6) | Período contable AAAAMM |

---

## Tablas staging IA (a crear — nuevas)

### IA_Compras_CAB

Cabecera staging para comprobantes procesados por IA, pendientes de revisión.

```sql
CREATE TABLE [dbo].[IA_Compras_CAB] (
    [ID]                    int IDENTITY(1,1) PRIMARY KEY,
    -- Control de estado
    [Estado]                nvarchar(20)  NOT NULL DEFAULT 'PENDIENTE',
      -- PENDIENTE | APROBADO | RECHAZADO | ERROR_LECTURA | SIN_PROVEEDOR
    [FechaHora_Proceso]     datetime      NOT NULL DEFAULT GETDATE(),
    [FechaHora_Modificacion] datetime     NULL,
    [Usuario_Proceso]       nvarchar(50)  NULL,
    [Observaciones_Rev]     nvarchar(500) NULL,  -- notas del revisor
    -- Archivo origen
    [Archivo_RutaOriginal]  nvarchar(500) NOT NULL,
    [Archivo_NombreOriginal] nvarchar(260) NOT NULL,
    [Archivo_NombreRenombrado] nvarchar(260) NULL,
    -- Proveedor (del JSON + lookup SQL)
    [Proveedor_Nombre]      nvarchar(50)  NULL,
    [Proveedor_CUIT]        nvarchar(13)  NULL,
    [Proveedor_Domicilio]   nvarchar(100) NULL,
    [Proveedor_CondIVA]     nvarchar(4)   NULL,
    [Cuenta_Contable]       nvarchar(15)  NULL,  -- FK→MA_CUENTAS (NULL si no matcheó)
    [Match_Metodo]          nvarchar(20)  NULL,  -- CUIT | NOMBRE | MANUAL | NULL
    -- Comprobante
    [TipoComprobante]       nvarchar(50)  NULL,
    [Letra]                 nvarchar(1)   NULL,
    [PuntoVenta]            nvarchar(4)   NULL,
    [Numero]                nvarchar(8)   NULL,
    [Fecha]                 datetime      NULL,
    [Vencimiento]           datetime      NULL,
    [CAE]                   nvarchar(14)  NULL,
    [VtoCAE]                datetime      NULL,
    [Moneda]                nvarchar(4)   NULL,
    -- Importes (del JSON TOTALES)
    [NetoGravado]           money         NULL,
    [NetoNoGravado]         money         NULL,
    [Exento]                money         NULL,
    [IVA_21]                money         NULL,
    [IVA_105]               money         NULL,
    [IVA_27]                money         NULL,
    [Percepcion_IVA]        money         NULL,
    [Percepcion_IIBB]       money         NULL,
    [Percepcion_Ganancias]  money         NULL,
    [ImpuestosInternos]     money         NULL,
    [OtrosImpuestos]        money         NULL,
    [Total]                 money         NULL,
    -- Observaciones del lector
    [Lector_Observaciones]  nvarchar(500) NULL,
    [Lector_Error]          nvarchar(500) NULL,
)
```

### IA_Compras_DET

Renglones del comprobante (ROWS del JSON).

```sql
CREATE TABLE [dbo].[IA_Compras_DET] (
    [ID]                    int IDENTITY(1,1) PRIMARY KEY,
    [ID_CAB]                int           NOT NULL,  -- FK→IA_Compras_CAB
    [NroRenglon]            int           NOT NULL,
    -- Campos del JSON ROWS
    [Cantidad]              nvarchar(20)  NULL,
    [Codigo_Articulo]       nvarchar(50)  NULL,
    [Descripcion]           nvarchar(200) NULL,
    [UD]                    nvarchar(10)  NULL,
    [Importe_Lista]         money         NULL,
    [Dto1]                  float         NULL,
    [Dto2]                  float         NULL,
    [Importe_Neto]          money         NULL,
    [IVA]                   float         NULL,
    [ImpuestosInternos]     money         NULL,
    [Total]                 money         NULL,
    [AuxNroLote]            nvarchar(50)  NULL,
    [AuxNroSerie]           nvarchar(50)  NULL,
)
```

**Estados de `IA_Compras_CAB.Estado`:**

| Valor | Significado |
|---|---|
| `PENDIENTE` | Procesado, esperando revisión |
| `APROBADO` | Revisado y aprobado, listo para pasar a producción |
| `RECHAZADO` | Revisado y rechazado |
| `ERROR_LECTURA` | El lector no pudo extraer datos (archivo original intacto) |
| `SIN_PROVEEDOR` | Lectura OK pero no matcheó proveedor en MA_CUENTAS |

---

## Vistas listas para uso

### Vt_Proveedores
JOIN de `MA_CUENTAS` + `MA_CUENTASADIC` filtrado por `TipoVista = 'PR'`.  
Campos clave: `CODIGO`, `RAZON_SOCIAL`, `NUMERO_DOCUMENTO` (CUIT), `IVA`, `CALLE`, `LOCALIDAD`, `NUMERO_DOCUMENTO`, `Dada_De_Baja`, `TITULO`, `BLOQUEO`.

### Vt_Clientes
Igual que `Vt_Proveedores` pero sin filtro de `TipoVista` (incluye todos los tipos de cuenta con datos adicionales).

---

## Consulta lookup proveedor (lógica del agente)

Usar siempre `Vt_Proveedores` — ya filtra por tipo PR y hace el JOIN con CUENTASADIC.

```sql
-- 1. Buscar por CUIT exacto
SELECT TOP 1 CODIGO, RAZON_SOCIAL, IVA, CALLE, LOCALIDAD
FROM Vt_Proveedores
WHERE REPLACE(REPLACE(NUMERO_DOCUMENTO, '-', ''), ' ', '') = @cuit_solo_digitos
  AND Dada_De_Baja = 0 AND TITULO = 0

-- 2. Fallback por nombre (LIKE)
SELECT TOP 5 CODIGO, RAZON_SOCIAL, NUMERO_DOCUMENTO
FROM Vt_Proveedores
WHERE RAZON_SOCIAL LIKE '%' + @nombre_limpio + '%'
  AND Dada_De_Baja = 0 AND TITULO = 0

-- 3. Fallback por domicilio / localidad (último recurso)
SELECT TOP 5 CODIGO, RAZON_SOCIAL, NUMERO_DOCUMENTO, LOCALIDAD
FROM Vt_Proveedores
WHERE LOCALIDAD LIKE '%' + @localidad + '%'
  AND Dada_De_Baja = 0 AND TITULO = 0
```
