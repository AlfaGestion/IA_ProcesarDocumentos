 

/*
Tabla propia del proceso de actualizacion de costos.
Los nombres reales de tablas de articulos y campos de costo quedan a validar.
*/

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE name = 'IA_Costos_Actualizacion_Hist'
      AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE [dbo].[IA_Costos_Actualizacion_Hist] (
        [ID] int IDENTITY(1,1) NOT NULL,
        [FechaHora] datetime NOT NULL CONSTRAINT [DF_IA_Costos_Actualizacion_Hist_FechaHora] DEFAULT GETDATE(),
        [ImportacionID] int NULL,
        [ImportacionDetID] int NULL,
        [Usuario] nvarchar(50) NOT NULL,
        [Proveedor] nvarchar(50) NULL,
        [CuentaProveedor] nvarchar(15) NULL,
        [ArchivoOrigen] nvarchar(500) NOT NULL,
        [FilaOrigen] int NOT NULL,
        [ArticuloID] nvarchar(50) NOT NULL,
        [ArticuloCodigo] nvarchar(50) NULL,
        [ProveedorCodigo] nvarchar(50) NULL,
        [DescripcionImportada] nvarchar(250) NULL,
        [DescripcionArticulo] nvarchar(250) NULL,
        [CostoAnterior] money NULL,
        [CostoNuevo] money NOT NULL,
        [VariacionPct] float NULL,
        [MatchTipo] nvarchar(30) NOT NULL,
        [MatchScore] float NULL,
        [AlertaVariacion] bit NOT NULL CONSTRAINT [DF_IA_Costos_Actualizacion_Hist_Alerta] DEFAULT 0,
        [AlertaDetalle] nvarchar(250) NULL,
        [Observaciones] nvarchar(500) NULL,
        CONSTRAINT [PK_IA_Costos_Actualizacion_Hist] PRIMARY KEY CLUSTERED ([ID] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = 'IX_IA_Costos_Actualizacion_Hist_FechaHora'
      AND object_id = OBJECT_ID('dbo.IA_Costos_Actualizacion_Hist')
)
BEGIN
    CREATE INDEX [IX_IA_Costos_Actualizacion_Hist_FechaHora]
        ON [dbo].[IA_Costos_Actualizacion_Hist] ([FechaHora] DESC);
END
GO

IF COL_LENGTH('dbo.IA_Costos_Actualizacion_Hist', 'ImportacionID') IS NULL
BEGIN
    ALTER TABLE [dbo].[IA_Costos_Actualizacion_Hist] ADD [ImportacionID] int NULL;
END
GO

IF COL_LENGTH('dbo.IA_Costos_Actualizacion_Hist', 'ImportacionDetID') IS NULL
BEGIN
    ALTER TABLE [dbo].[IA_Costos_Actualizacion_Hist] ADD [ImportacionDetID] int NULL;
END
GO

IF COL_LENGTH('dbo.IA_Costos_Actualizacion_Hist', 'Proveedor') IS NULL
BEGIN
    ALTER TABLE [dbo].[IA_Costos_Actualizacion_Hist] ADD [Proveedor] nvarchar(50) NULL;
END
GO

IF COL_LENGTH('dbo.IA_Costos_Actualizacion_Hist', 'CuentaProveedor') IS NULL
BEGIN
    ALTER TABLE [dbo].[IA_Costos_Actualizacion_Hist] ADD [CuentaProveedor] nvarchar(15) NULL;
END
GO
