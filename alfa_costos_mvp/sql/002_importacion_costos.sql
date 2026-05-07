 
/*
Proceso de importacion y actualizacion controlada de costos.

Diseno:
- V_Ta_InterODBC se usa como configuracion/perfil de proveedor
- estas tablas guardan la corrida, las filas importadas, la revision y el resultado
- no reemplazan el historial nativo existente; agregan trazabilidad propia del proceso
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'IA_Costos_Importacion_CAB'
      AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE [dbo].[IA_Costos_Importacion_CAB] (
        [ID] int IDENTITY(1,1) NOT NULL,
        [FechaHora_Alta] datetime NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_FechaHoraAlta] DEFAULT GETDATE(),
        [FechaHora_InicioProceso] datetime NULL,
        [FechaHora_FinProceso] datetime NULL,
        [Estado] nvarchar(20) NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_Estado] DEFAULT 'PENDIENTE',
        [Usuario] nvarchar(50) NOT NULL,

        [IdInterODBC] int NULL,
        [Proveedor] nvarchar(50) NOT NULL,
        [CuentaProveedor] nvarchar(15) NOT NULL,
        [PoliticaPrecios] nvarchar(4) NULL,
        [Lista] nvarchar(4) NULL,
        [SoloAlta] bit NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_SoloAlta] DEFAULT 0,
        [SoloModificacion] bit NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_SoloModificacion] DEFAULT 0,

        [ArchivoOrigen] nvarchar(500) NOT NULL,
        [ArchivoNombre] nvarchar(260) NOT NULL,
        [ArchivoHash] nvarchar(64) NULL,
        [TipoArchivo] nvarchar(10) NULL,
        [HojaDetectada] nvarchar(100) NULL,
        [HojaConfigurada] nvarchar(100) NULL,
        [RangoDesde] nvarchar(10) NULL,
        [RangoHasta] nvarchar(10) NULL,

        [ColumnasDetectadas] nvarchar(max) NULL,
        [ColumnasMapeadas] nvarchar(max) NULL,
        [PrecioColumnaSeleccionada] nvarchar(100) NULL,
        [NotasConfiguracion] nvarchar(max) NULL,
        [NotasProceso] nvarchar(max) NULL,
        [PromptIAAdicional] nvarchar(max) NULL,

        [TotalFilasLeidas] int NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_TotalFilasLeidas] DEFAULT 0,
        [TotalFilasConCosto] int NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_TotalFilasConCosto] DEFAULT 0,
        [TotalFilasConfirmadas] int NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_TotalFilasConfirmadas] DEFAULT 0,
        [TotalActualizadas] int NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_TotalActualizadas] DEFAULT 0,
        [TotalAltas] int NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_TotalAltas] DEFAULT 0,
        [TotalSinCambios] int NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_TotalSinCambios] DEFAULT 0,
        [TotalErrores] int NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_CAB_TotalErrores] DEFAULT 0,

        [ErrorDetalle] nvarchar(max) NULL,

        CONSTRAINT [PK_IA_Costos_Importacion_CAB] PRIMARY KEY CLUSTERED ([ID] ASC),
        CONSTRAINT [CK_IA_Costos_Importacion_CAB_Estado] CHECK (
            [Estado] IN ('PENDIENTE','CONFIGURADA','IMPORTADA','EN_REVISION','LISTA_PARA_APLICAR','APLICADA','APLICADA_PARCIAL','ERROR','CANCELADA')
        )
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_IA_Costos_Importacion_CAB_FechaHora'
      AND object_id = OBJECT_ID('dbo.IA_Costos_Importacion_CAB')
)
BEGIN
    CREATE INDEX [IX_IA_Costos_Importacion_CAB_FechaHora]
        ON [dbo].[IA_Costos_Importacion_CAB] ([FechaHora_Alta] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_IA_Costos_Importacion_CAB_Proveedor'
      AND object_id = OBJECT_ID('dbo.IA_Costos_Importacion_CAB')
)
BEGIN
    CREATE INDEX [IX_IA_Costos_Importacion_CAB_Proveedor]
        ON [dbo].[IA_Costos_Importacion_CAB] ([CuentaProveedor], [FechaHora_Alta] DESC);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'IA_Costos_Importacion_DET'
      AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE [dbo].[IA_Costos_Importacion_DET] (
        [ID] int IDENTITY(1,1) NOT NULL,
        [ID_CAB] int NOT NULL,
        [FilaOrigen] int NOT NULL,
        [Estado] nvarchar(20) NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_DET_Estado] DEFAULT 'PENDIENTE',

        [CodigoProveedorLeido] nvarchar(100) NULL,
        [DescripcionLeida] nvarchar(250) NULL,
        [PrecioCostoLeido] money NULL,
        [MonedaLeida] nvarchar(4) NULL,
        [UnidadLeida] nvarchar(20) NULL,

        [JsonFilaOriginal] nvarchar(max) NULL,
        [ObservacionesLectura] nvarchar(500) NULL,

        [IdArticulo] nvarchar(25) NULL,
        [DescripcionArticulo] nvarchar(100) NULL,
        [CostoActual] money NULL,
        [CostoNuevo] money NULL,
        [FhUltimoCosto] datetime NULL,

        [TipoMatch] nvarchar(30) NULL,
        [ScoreMatch] float NULL,
        [CoincidenciaCodigoProveedor] bit NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_DET_CodProv] DEFAULT 0,
        [ScoreDescripcion] float NULL,
        [ScorePrecioApoyo] float NULL,
        [FueSeleccionManual] bit NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_DET_Manual] DEFAULT 0,

        [DecisionUsuario] nvarchar(20) NULL,
        [UsuarioRevision] nvarchar(50) NULL,
        [FechaHoraRevision] datetime NULL,
        [ObservacionesRevision] nvarchar(500) NULL,

        [AlertaVariacion] bit NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_DET_Alerta] DEFAULT 0,
        [AlertaDetalle] nvarchar(250) NULL,
        [VariacionPct] float NULL,

        [Aplicado] bit NOT NULL CONSTRAINT [DF_IA_Costos_Importacion_DET_Aplicado] DEFAULT 0,
        [FechaHoraAplicacion] datetime NULL,
        [UsuarioAplicacion] nvarchar(50) NULL,
        [ResultadoAplicacion] nvarchar(20) NULL,
        [ErrorAplicacion] nvarchar(500) NULL,

        CONSTRAINT [PK_IA_Costos_Importacion_DET] PRIMARY KEY CLUSTERED ([ID] ASC),
        CONSTRAINT [FK_IA_Costos_Importacion_DET_CAB] FOREIGN KEY ([ID_CAB])
            REFERENCES [dbo].[IA_Costos_Importacion_CAB] ([ID])
            ON DELETE CASCADE,
        CONSTRAINT [CK_IA_Costos_Importacion_DET_Estado] CHECK (
            [Estado] IN ('PENDIENTE','MATCHEADO','CONFIRMADO','DESCARTADO','LISTO_APLICAR','APLICADO','SIN_MATCH','ERROR')
        ),
        CONSTRAINT [CK_IA_Costos_Importacion_DET_Decision] CHECK (
            [DecisionUsuario] IS NULL OR [DecisionUsuario] IN ('CONFIRMAR','DESCARTAR','REASIGNAR')
        ),
        CONSTRAINT [CK_IA_Costos_Importacion_DET_Resultado] CHECK (
            [ResultadoAplicacion] IS NULL OR [ResultadoAplicacion] IN ('OK','SIN_CAMBIO','ALTA','ERROR','BLOQUEADO')
        )
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_IA_Costos_Importacion_DET_ID_CAB'
      AND object_id = OBJECT_ID('dbo.IA_Costos_Importacion_DET')
)
BEGIN
    CREATE INDEX [IX_IA_Costos_Importacion_DET_ID_CAB]
        ON [dbo].[IA_Costos_Importacion_DET] ([ID_CAB], [FilaOrigen]);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_IA_Costos_Importacion_DET_Articulo'
      AND object_id = OBJECT_ID('dbo.IA_Costos_Importacion_DET')
)
BEGIN
    CREATE INDEX [IX_IA_Costos_Importacion_DET_Articulo]
        ON [dbo].[IA_Costos_Importacion_DET] ([IdArticulo], [Aplicado]);
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_IA_Costos_Importacion_DET_CodigoProv'
      AND object_id = OBJECT_ID('dbo.IA_Costos_Importacion_DET')
)
BEGIN
    CREATE INDEX [IX_IA_Costos_Importacion_DET_CodigoProv]
        ON [dbo].[IA_Costos_Importacion_DET] ([CodigoProveedorLeido]);
END
GO
