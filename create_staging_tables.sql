USE [ALFANET]
GO

-- ============================================================
-- Tablas staging para comprobantes procesados por IA
-- ============================================================

-- ------------------------------------------------------------
-- IA_Compras_CAB — Cabecera del comprobante
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IA_Compras_CAB' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[IA_Compras_CAB] (
        [ID]                        int             IDENTITY(1,1)   NOT NULL,

        -- Control de estado / workflow
        [Estado]                    nvarchar(20)    NOT NULL    CONSTRAINT DF_IA_Compras_CAB_Estado DEFAULT 'PENDIENTE',
            -- PENDIENTE | APROBADO | RECHAZADO | ERROR_LECTURA | SIN_PROVEEDOR
        [FechaHora_Proceso]         datetime        NOT NULL    CONSTRAINT DF_IA_Compras_CAB_FhProceso DEFAULT GETDATE(),
        [FechaHora_Modificacion]    datetime        NULL,
        [Usuario_Proceso]           nvarchar(50)    NULL,
        [Observaciones_Rev]         nvarchar(500)   NULL,

        -- Archivo origen
        [Archivo_RutaOriginal]      nvarchar(500)   NOT NULL,
        [Archivo_NombreOriginal]    nvarchar(260)   NOT NULL,
        [Archivo_NombreRenombrado]  nvarchar(260)   NULL,

        -- Proveedor (extraído del JSON + lookup en Vt_Proveedores)
        [Proveedor_Nombre]          nvarchar(50)    NULL,
        [Proveedor_CUIT]            nvarchar(13)    NULL,
        [Proveedor_Domicilio]       nvarchar(100)   NULL,
        [Proveedor_CondIVA]         nvarchar(4)     NULL,
        [Cuenta_Contable]           nvarchar(15)    NULL,   -- NULL si no matcheó
        [Match_Metodo]              nvarchar(20)    NULL,   -- CUIT | NOMBRE | MANUAL | NULL

        -- Comprobante
        [TipoComprobante]           nvarchar(50)    NULL,
        [Letra]                     nvarchar(1)     NULL,
        [PuntoVenta]                nvarchar(4)     NULL,
        [Numero]                    nvarchar(8)     NULL,
        [Fecha]                     datetime        NULL,
        [Vencimiento]               datetime        NULL,
        [CAE]                       nvarchar(14)    NULL,
        [VtoCAE]                    datetime        NULL,
        [Moneda]                    nvarchar(4)     NULL,

        -- Importes (del JSON TOTALES)
        [NetoGravado]               money           NULL,
        [NetoNoGravado]             money           NULL,
        [Exento]                    money           NULL,
        [IVA_21]                    money           NULL,
        [IVA_105]                   money           NULL,
        [IVA_27]                    money           NULL,
        [Percepcion_IVA]            money           NULL,
        [Percepcion_IIBB]           money           NULL,
        [Percepcion_Ganancias]      money           NULL,
        [ImpuestosInternos]         money           NULL,
        [OtrosImpuestos]            money           NULL,
        [Total]                     money           NULL,

        -- Info del lector
        [Lector_Observaciones]      nvarchar(500)   NULL,
        [Lector_Error]              nvarchar(500)   NULL,

        CONSTRAINT [PK_IA_Compras_CAB] PRIMARY KEY CLUSTERED ([ID] ASC),
        CONSTRAINT [CK_IA_Compras_CAB_Estado] CHECK (
            [Estado] IN ('PENDIENTE','APROBADO','RECHAZADO','ERROR_LECTURA','SIN_PROVEEDOR')
        )
    )
    PRINT 'Tabla IA_Compras_CAB creada.'
END
ELSE
    PRINT 'Tabla IA_Compras_CAB ya existe, no se modifica.'
GO

-- Índices útiles para la app de revisión
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_IA_Compras_CAB_Estado' AND object_id = OBJECT_ID('dbo.IA_Compras_CAB'))
    CREATE INDEX [IX_IA_Compras_CAB_Estado]
        ON [dbo].[IA_Compras_CAB] ([Estado], [FechaHora_Proceso] DESC)
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_IA_Compras_CAB_CUIT' AND object_id = OBJECT_ID('dbo.IA_Compras_CAB'))
    CREATE INDEX [IX_IA_Compras_CAB_CUIT]
        ON [dbo].[IA_Compras_CAB] ([Proveedor_CUIT])
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_IA_Compras_CAB_Fecha' AND object_id = OBJECT_ID('dbo.IA_Compras_CAB'))
    CREATE INDEX [IX_IA_Compras_CAB_Fecha]
        ON [dbo].[IA_Compras_CAB] ([Fecha] DESC)
GO


-- ------------------------------------------------------------
-- IA_Compras_DET — Renglones del comprobante
-- ------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'IA_Compras_DET' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[IA_Compras_DET] (
        [ID]                int             IDENTITY(1,1)   NOT NULL,
        [ID_CAB]            int             NOT NULL,
        [NroRenglon]        int             NOT NULL,

        -- Campos del JSON ROWS (mismos nombres que el lector)
        [Cantidad]          nvarchar(20)    NULL,
        [Codigo_Articulo]   nvarchar(50)    NULL,
        [Descripcion]       nvarchar(200)   NULL,
        [UD]                nvarchar(10)    NULL,
        [Importe_Lista]     money           NULL,
        [Dto1]              float           NULL,
        [Dto2]              float           NULL,
        [Importe_Neto]      money           NULL,
        [IVA]               float           NULL,
        [ImpuestosInternos] money           NULL,
        [Total]             money           NULL,
        [AuxNroLote]        nvarchar(50)    NULL,
        [AuxNroSerie]       nvarchar(50)    NULL,

        CONSTRAINT [PK_IA_Compras_DET] PRIMARY KEY CLUSTERED ([ID] ASC),
        CONSTRAINT [FK_IA_Compras_DET_CAB] FOREIGN KEY ([ID_CAB])
            REFERENCES [dbo].[IA_Compras_CAB] ([ID])
            ON DELETE CASCADE
    )
    PRINT 'Tabla IA_Compras_DET creada.'
END
ELSE
    PRINT 'Tabla IA_Compras_DET ya existe, no se modifica.'
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_IA_Compras_DET_ID_CAB' AND object_id = OBJECT_ID('dbo.IA_Compras_DET'))
    CREATE INDEX [IX_IA_Compras_DET_ID_CAB]
        ON [dbo].[IA_Compras_DET] ([ID_CAB], [NroRenglon])
GO

PRINT 'Script finalizado.'
GO
