@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
set "MAIN_PY=%ROOT_DIR%\main.py"

if not exist "%MAIN_PY%" (
    echo [view_pkl_tool] main.py not found: %MAIN_PY%
    exit /b 1
)

python "%MAIN_PY%" %*

