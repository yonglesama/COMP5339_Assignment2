@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
  if errorlevel 1 goto failed
)
.venv\Scripts\python.exe -c "import flask,amqtt,duckdb,paho.mqtt.client" >nul 2>&1
if errorlevel 1 (
  .venv\Scripts\python.exe -m pip install -r A2\requirements-dev.txt
  if errorlevel 1 goto failed
)
echo Starting C history and A MQTT replay in one dashboard.
echo Only a private working copy is changed. Close an older dashboard first.
.venv\Scripts\python.exe A2\src\dashboard\run.py --project-root . --mode unified --replay-delay 0.5 --port 8050 --open-browser
if errorlevel 1 goto failed
exit /b 0
:failed
echo.
echo Dashboard could not start. Use Python 3.11, 3.12 or 3.13 and review the error above.
pause
exit /b 1
