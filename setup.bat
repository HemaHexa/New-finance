@echo off
setlocal
cd /d "%~dp0"
echo [1/3] Checking Python 3.12...
py -3.12 --version
if errorlevel 1 (
  echo Python 3.12 was not found. Install Python 3.12 or edit this script for your installed Python version.
  pause
  exit /b 1
)
echo [2/3] Creating isolated environment .goldvenv...
if not exist ".goldvenv\Scripts\python.exe" py -3.12 -m venv .goldvenv
echo [3/3] Installing pinned demo dependencies...
.goldvenv\Scripts\python.exe -m pip install --upgrade pip
.goldvenv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 (
  echo Dependency installation failed.
  pause
  exit /b 1
)
echo.
echo Setup complete. Run run_demo.bat next.
pause
