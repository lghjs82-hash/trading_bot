# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['dashboard_app.py'],
    pathex=[],
    binaries=[],
    datas=[('dashboard/templates', 'dashboard/templates'), ('strategies', 'strategies')],
    hiddenimports=['uvicorn.protocols.http.httptools_impl', 'uvicorn.protocols.http.h11_impl', 'uvicorn.protocols.websockets.websockets_impl', 'uvicorn.lifespan.on', 'jinja2.ext', 'dotenv'],
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
    name='BinanceTradingBot',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
