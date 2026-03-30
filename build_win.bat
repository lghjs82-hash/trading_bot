@echo off
echo.
echo ============================================
echo Binance Trading Bot Windows Build Script
echo ============================================
echo.

:: Check for python
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python이 설치되어 있지 않거나 PATH에 추가되어 있지 않습니다.
    echo Please install Python 3.9+ and add to PATH.
    pause
    exit /b 1
)

:: Install / Upgrade pip and requirements
echo 1. Installing dependencies...
python -m pip install --upgrade pip
if exist requirements.txt (
    pip install -r requirements.txt
)
pip install pyinstaller

:: Run the build script
echo 2. Starting build...
python bundle.py

if exist dist\BinanceTradingBot.exe (
    echo.
    echo ============================================
    echo 빌드 완료! (Build Successful!)
    echo 위치: dist\BinanceTradingBot.exe
    echo ============================================
) else (
    echo.
    echo [ERROR] 빌드에 실패했습니다. (Build Failed.)
)

pause
