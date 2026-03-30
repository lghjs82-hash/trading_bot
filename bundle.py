import os
import sys
import shutil
import subprocess

def bundle():
    print("현 시스템 운영체제:", sys.platform)
    print("Python 실행 경로:", sys.executable)
    
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
    
    # Construct PyInstaller command
    # Use 'python -m PyInstaller' to ensure it uses the current environment
    cmd = [
        sys.executable, '-m', 'PyInstaller',
        'dashboard_app.py',             # Entry point
        '--onefile',                    # Single executable
        '--name', 'BinanceTradingBot',   # Name
        '--clean',                      # Clean cache
        '--noconfirm',                  # Don't ask for overwrite
    ]
    
    # Add data arguments
    cmd.extend(add_data_args)
    
    # Add hidden imports
    hidden_imports = [
        'uvicorn.protocols.http.httptools_impl',
        'uvicorn.protocols.http.h11_impl',
        'uvicorn.protocols.http.auto_impl',
        'uvicorn.protocols.websockets.websockets_impl',
        'uvicorn.protocols.websockets.auto_impl',
        'uvicorn.lifespan.on',
        'uvicorn.lifespan.off',
        'uvicorn.lifespan.auto',
        'jinja2.ext',
        'dotenv',
        'email.mime.text',
        'email.mime.multipart',
        'engineio.async_drivers.threading',
        'ccxt.base.exchange',
    ]
    
    for hi in hidden_imports:
        cmd.extend(['--hidden-import', hi])

    print(f"실행 명령어: {' '.join(cmd)}")
    
    try:
        # Run PyInstaller via subprocess to capture output clearly
        result = subprocess.run(cmd, check=True, text=True)
        print("\n" + "="*50)
        print(f"빌드 완료! 실행 파일 위치: dist/{name}")
        print("="*50)
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] PyInstaller build failed with exit code {e.returncode}")
        sys.exit(e.returncode)
    except Exception as e:
        print(f"\n[ERROR] Unexpected error during build: {e}")
        sys.exit(1)

if __name__ == "__main__":
    bundle()
