# -*- mode: python ; coding: utf-8 -*-


block_cipher = None


a = Analysis(
    ['pymain.py'],
    pathex=[],
    binaries=[],
    datas=[('inputs/industrial.xlsx', 'inputs'), ('inputs/recall_points.xlsx', 'inputs'), ('inputs/atenuacions_generacions_backup.xlsx', 'inputs'), ('inputs/edar_data.xlsx', 'inputs'), ('inputs/catalonia_graph.pkl', 'inputs'), ('inputs/percentatges_eliminacio_tots_calibrats.csv', 'inputs'), ('inputs/excel_scenario.xlsx', 'inputs'), ('inputs/coords_to_pixel_llob.csv', 'inputs'), ('inputs/coord_codi_llob.csv', 'inputs'), ('inputs/llindars_massa_aigua.xlsx', 'inputs'), ('inputs/resultat.json', 'inputs')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='pymain',
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
