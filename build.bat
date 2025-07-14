Title %~n0
@echo off
REM Скрипт для сборки watchdog_backup.exe

setlocal

set SCRIPT_NAME=watchdog_backup.py
set PROJECT_DIR=%~dp0
set VENV_DIR=venv

if not exist "%VENV_DIR%" (
    echo Virtual environment not found. Creating it...
    python -m venv "%VENV_DIR%"
    if %errorlevel% neq 0 (
        echo Failed to create virtual environment. Ensure Python is installed and in PATH.
        goto :eof
    )
    echo Virtual environment created successfully.
) else (
    echo Virtual environment already exists.
)

echo Activating virtual environment...
call "%PROJECT_DIR%\%VENV_DIR%\Scripts\activate.bat"

if %ERRORLEVEL% NEQ 0 (
    echo Error: Failed to activate virtual environment.
    goto :eof
)

echo Virtual environment '%VENV_DIR%' activated.
echo Install packages.
pip install -r requirements.txt

pyinstaller --clean --onefile --icon=app.ico --noconfirm %SCRIPT_NAME%
deactivate
copy /y dist\watchdog_backup.exe .

endlocal
echo Build finished.
pause