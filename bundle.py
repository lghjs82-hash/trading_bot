import PyInstaller.__main__
import os
import sys
import shutil

def bundle():
    print("현 시스템 운영체제:", sys.platform)
    
    # Define assets to bundle
    # Format: (source_path, target_path_inside_bundle)
    assets = [
        ('dashboard/templates', 'dashboard/templates'),
        ('strategies', 'strategies'),
    ]
    
    # OS specfic data separator
    sep = ';' if sys.platform.startswith('win') else ':'
    
    add_data_args = []
    for src, dst in assets:
        if os.path.exists(src):
            add_data_args.extend(['--add-data', f'{src}{sep}{dst}'])
    
    # Binary name
    name = 'BinanceTradingBot'
    if sys.platform.startswith('win'):
        name += '.exe'
    
    print(f"빌드 시작: {name} (Onefile mode)")
    
    PyInstaller.__main__.run([
        'dashboard_app.py',             # Entry point
        '--onefile',                    # Single executable
        '--name', 'BinanceTradingBot',   # Name
        '--clean',                      # Clean cache
        *add_data_args,                 # Include templates and strategies
        '--hidden-import', 'uvicorn.protocols.http.httptools_impl',
        '--hidden-import', 'uvicorn.protocols.http.h11_impl',
        '--hidden-import', 'uvicorn.protocols.websockets.websockets_impl',
        '--hidden-import', 'uvicorn.lifespan.on',
        '--hidden-import', 'jinja2.ext',
        '--hidden-import', 'dotenv',
    ])
    
    print("\n" + "="*50)
    print(f"빌드 완료! 실행 파일 위치: dist/BinanceTradingBot")
    print("="*50)

if __name__ == "__main__":
    bundle()
