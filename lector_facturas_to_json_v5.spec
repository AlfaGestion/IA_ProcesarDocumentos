# -*- mode: python ; coding: utf-8 -*-
import os

# pdfplumber (usado por layout_extractor) necesita los cmap de pdfminer en runtime
try:
    import pdfminer
    _cmap_src = os.path.join(os.path.dirname(pdfminer.__file__), 'cmap')
    extra_datas = [(_cmap_src, 'pdfminer/cmap')]
except ImportError:
    extra_datas = []

a = Analysis(
    ['lector_facturas_to_json_v5.py'],
    pathex=[],
    binaries=[],
    datas=extra_datas,
    hiddenimports=[
        # módulos locales importados dinámicamente (no detectados por PyInstaller)
        'layout_extractor',
        'ia_backend_transport',
        # pypdf (separación de PDFs multipágina)
        'pypdf',
        'pypdf._reader',
        'pypdf._writer',
        'pypdf.filters',
        'pypdf.generic',
        # pdfminer (requerido por pdfplumber)
        'pdfminer',
        'pdfminer.high_level',
        'pdfminer.layout',
        'pdfminer.pdfpage',
        'pdfminer.pdfinterp',
        'pdfminer.converter',
        'pdfminer.pdfdocument',
        'pdfminer.pdfparser',
        'pdfminer.cmapdb',
        'pdfminer.utils',
        # Pillow
        'PIL._tkinter_finder',
        # charset_normalizer (dependencia transitiva)
        'charset_normalizer',
        'charset_normalizer.md__mypyc',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='lector_facturas_to_json_v5',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # Sin ventana negra; VB6 predice la ruta por outdir+stem+".json"
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
