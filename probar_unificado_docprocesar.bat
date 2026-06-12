@echo off
setlocal

set "BASE_DIR=C:\DOCPROCESAR"
set "OUT_DIR=%BASE_DIR%\OUTDIR"
set "SCRIPT_DIR=%~dp0"
set "PYTHON_EXE=%SCRIPT_DIR%\.venv\Scripts\python.exe"
set "SCRIPT_PATH=%SCRIPT_DIR%lector_movimientos_financieros_unificado.py"

if not exist "%BASE_DIR%" (
  echo ERROR: No existe la carpeta base "%BASE_DIR%".
  pause
  exit /b 1
)

if not exist "%PYTHON_EXE%" (
  echo ERROR: No se encontro Python del entorno virtual en:
  echo %PYTHON_EXE%
  pause
  exit /b 1
)

if not exist "%SCRIPT_PATH%" (
  echo ERROR: No se encontro el script:
  echo %SCRIPT_PATH%
  pause
  exit /b 1
)

if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

echo.
echo Carpeta de entrada: %BASE_DIR%
echo Carpeta de salida : %OUT_DIR%
echo.
set /p "FILE_NAME=Ingrese solo el nombre del archivo a procesar: "

if "%FILE_NAME%"=="" (
  echo ERROR: Debe ingresar un nombre de archivo.
  pause
  exit /b 1
)

set "INPUT_FILE=%BASE_DIR%\%FILE_NAME%"

if not exist "%INPUT_FILE%" (
  echo ERROR: No existe el archivo:
  echo %INPUT_FILE%
  pause
  exit /b 1
)

echo.
echo Procesando...
"%PYTHON_EXE%" "%SCRIPT_PATH%" "%INPUT_FILE%" --outdir "%OUT_DIR%"
set "EXIT_CODE=%ERRORLEVEL%"

echo.
if not "%EXIT_CODE%"=="0" (
  echo El proceso termino con error. Codigo: %EXIT_CODE%
  pause
  exit /b %EXIT_CODE%
)

echo Proceso finalizado.
echo Revise los archivos generados en:
echo %OUT_DIR%
pause
exit /b 0
