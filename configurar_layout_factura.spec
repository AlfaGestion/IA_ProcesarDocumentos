# -*- mode: python ; coding: utf-8 -*-
import os

# pdfplumber usa los cmap de pdfminer para decodificar PDFs — sin esto falla en runtime
try:
    import pdfminer
    _cmap_src = os.path.join(os.path.dirname(pdfminer.__file__), 'cmap')
    extra_datas = [(_cmap_src, 'pdfminer/cmap')]
except ImportError:
    extra_datas = []

a = Analysis(
    ['configurar_layout_factura.py'],
    pathex=[],
    binaries=[],
    datas=extra_datas,
    hiddenimports=[
        # pyodbc: conector SQL Server
        'pyodbc',
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
        # Pillow + Tkinter
        'PIL._tkinter_finder',
        'PIL.ImageTk',
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
    name='configurar_layout_factura',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
