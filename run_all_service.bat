@echo off
setlocal
set BASE_DIR=C:\Users\PC RACING\Documents\TkGate\TSHHHT\sd\SD_task1-MAIN\SD_task1

echo Ejecutando servicios y filtros...

REM Ejecutar Pyro
echo.
echo ===== Pyro =====
start "" cmd /k "cd /d %BASE_DIR%\Pyro && python InsultService.py"
start "" cmd /k "cd /d %BASE_DIR%\Pyro && python InsultFilter.py"

REM Ejecutar RabbitMQ
REM echo.
REM echo ===== RabbitMQ =====
REM start "" cmd /k "cd /d %BASE_DIR%\RabbitMQ && python InsultService.py"
REM start "" cmd /k "cd /d %BASE_DIR%\RabbitMQ && python InsultFilter.py"

REM Ejecutar Redis
REM echo.
REM echo ===== Redis =====
REM start "" cmd /k "cd /d %BASE_DIR%\Redis && python InsultService.py"
REM start "" cmd /k "cd /d %BASE_DIR%\Redis && python InsultFilter.py"

REM Ejecutar XMLRPC
echo.
echo ===== XMLRPC =====
start "" cmd /k "cd /d %BASE_DIR%\XMLRPC && python InsultService.py"
start "" cmd /k "cd /d %BASE_DIR%\XMLRPC && python InsultFilter.py"

echo.
echo Todos los servicios y filtros fueron iniciados.
pause
endlocal
