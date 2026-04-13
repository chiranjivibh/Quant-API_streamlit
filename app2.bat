# -*- coding: utf-8 -*-
"""
Created on Sun Apr 12 17:21:55 2026

@author: Sinchina
"""
@echo off
REM --- Set your QuantAQ API key here or in system env vars ---
set QUANTAQAPIKEY="JTAS7R9CS5YST0IYVU9RQOWJ"

REM --- (Optional) activate conda/venv ---
REM call C:\Users\YourUser\anaconda3\Scripts\activate.bat yourenv

setlocal EnableDelayedExpansion
title QUANTAQ Air Quality Analyzer — Launcher

:: ════════════════════════════════════════════════════════════════════════════
::  QUANTAQ Air Quality Analyzer  ·  Windows Auto-Launcher
::  ──────────────────────────────────────────────────────────────────────────
::  Automatically:
::    1. Finds Python 3 (PATH → py launcher → common install folders)
::    2. Checks Python version is 3.8+
::    3. Upgrades pip silently
::    4. Installs / updates all required packages
::    5. Launches Streamlit and opens the browser
::
::  Place this file in the SAME folder as quantaq_dashboard.py
:: ════════════════════════════════════════════════════════════════════════════

:: ── Styling helpers ──────────────────────────────────────────────────────────
set "LINE=────────────────────────────────────────────────────────────"
set "APP_NAME=QUANTAQ Air Quality Analyzer"

cls
echo.
echo   %LINE%
echo    🌫  %APP_NAME%
echo   %LINE%
echo.

:: ── Locate the app script ────────────────────────────────────────────────────
set "SCRIPT_DIR=%~dp0"
set "APP_SCRIPT=%SCRIPT_DIR%app2.py"
set "REQ_FILE=%SCRIPT_DIR%requirements.txt"

if not exist "%APP_SCRIPT%" (
    echo   [ERROR] quantaq_dashboard.py not found in:
    echo           %SCRIPT_DIR%
    echo.
    echo   Make sure launch.bat is in the same folder as quantaq_dashboard.py
    goto :fatal
)

:: ════════════════════════════════════════════════════════════════════════════
::  STEP 1 — Find Python
:: ════════════════════════════════════════════════════════════════════════════
echo   [1/4] Searching for Python 3...
echo.

set "PYTHON_EXE="

:: ── Try 1: python3 on PATH ───────────────────────────────────────────────────
where python3 >nul 2>&1
if !errorlevel! == 0 (
    set "PYTHON_EXE=python3"
    goto :found_python
)

:: ── Try 2: python on PATH ────────────────────────────────────────────────────
where python >nul 2>&1
if !errorlevel! == 0 (
    :: Confirm it is actually Python 3, not Python 2
    for /f "tokens=2 delims= " %%V in ('python --version 2^>^&1') do (
        set "PY_VER=%%V"
    )
    if "!PY_VER:~0,1!" == "3" (
        set "PYTHON_EXE=python"
        goto :found_python
    )
)

:: ── Try 3: Windows py launcher (py.exe) ─────────────────────────────────────
where py >nul 2>&1
if !errorlevel! == 0 (
    py -3 --version >nul 2>&1
    if !errorlevel! == 0 (
        set "PYTHON_EXE=py -3"
        goto :found_python
    )
)

:: ── Try 4: Common install paths (user + system) ──────────────────────────────
set "SEARCH_ROOTS=%LOCALAPPDATA%\Programs\Python %APPDATA%\Python %ProgramFiles%\Python %ProgramFiles(x86)%\Python C:\Python"
set "PY_VERSIONS=313 312 311 310 39 38"

for %%R in (%SEARCH_ROOTS%) do (
    for %%V in (%PY_VERSIONS%) do (
        if exist "%%R%%V\python.exe" (
            set "PYTHON_EXE=%%R%%V\python.exe"
            goto :found_python
        )
    )
)

:: ── Try 5: Microsoft Store Python stubs ─────────────────────────────────────
for %%V in (3.13 3.12 3.11 3.10 3.9 3.8) do (
    set "STORE_PATH=%LOCALAPPDATA%\Microsoft\WindowsApps\python%%V.exe"
    if exist "!STORE_PATH!" (
        set "PYTHON_EXE=!STORE_PATH!"
        goto :found_python
    )
)

:: ── Not found ────────────────────────────────────────────────────────────────
echo   [ERROR] Python 3 was not found on this system.
echo.
echo   Please install Python 3.8 or newer from:
echo     https://www.python.org/downloads/
echo.
echo   During installation, check  "Add Python to PATH"
goto :fatal

:found_python
:: ════════════════════════════════════════════════════════════════════════════
::  STEP 2 — Verify version is 3.8+
:: ════════════════════════════════════════════════════════════════════════════
for /f "tokens=*" %%V in ('!PYTHON_EXE! --version 2^>^&1') do set "FULL_VER=%%V"
echo   Found : !FULL_VER!
echo   Path  : !PYTHON_EXE!
echo.

:: Extract major.minor for comparison
for /f "tokens=2 delims= " %%V in ('!PYTHON_EXE! --version 2^>^&1') do (
    for /f "tokens=1,2 delims=." %%A in ("%%V") do (
        set "PY_MAJOR=%%A"
        set "PY_MINOR=%%B"
    )
)

if !PY_MAJOR! LSS 3 (
    echo   [ERROR] Python 3.8+ required, found version !PY_MAJOR!.!PY_MINOR!
    goto :fatal
)
if !PY_MAJOR! EQU 3 (
    if !PY_MINOR! LSS 8 (
        echo   [ERROR] Python 3.8+ required, found version !PY_MAJOR!.!PY_MINOR!
        goto :fatal
    )
)

echo   [OK] Python version check passed.
echo.

:: ════════════════════════════════════════════════════════════════════════════
::  STEP 3 — Upgrade pip + install packages
:: ════════════════════════════════════════════════════════════════════════════
echo   [2/4] Upgrading pip...
echo.
!PYTHON_EXE! -m pip install --upgrade pip --quiet
if !errorlevel! NEQ 0 (
    echo   [WARNING] pip upgrade failed — continuing anyway.
)

echo   [3/4] Installing / verifying required packages...
echo   (This may take a minute on first run)
echo.

if exist "%REQ_FILE%" (
    !PYTHON_EXE! -m pip install -r "%REQ_FILE%" --quiet
) else (
    :: Inline fallback if requirements.txt is missing
    !PYTHON_EXE! -m pip install ^
        streamlit ^
        requests ^
        pandas ^
        numpy ^
        plotly ^
        --quiet
)

if !errorlevel! NEQ 0 (
    echo.
    echo   [ERROR] Package installation failed.
    echo   Try running manually:
    echo     !PYTHON_EXE! -m pip install -r requirements.txt
    goto :fatal
)

echo.
echo   [OK] All packages ready.
echo.

:: ════════════════════════════════════════════════════════════════════════════
::  STEP 4 — Launch Streamlit
:: ════════════════════════════════════════════════════════════════════════════
echo   [4/4] Launching %APP_NAME%...
echo.
echo   %LINE%
echo    Open your browser at:  http://localhost:8501
echo   %LINE%
echo.
echo   Press Ctrl+C in this window to stop the app.
echo.

:: Small delay so the user can read the URL before the terminal scrolls
timeout /t 2 /nobreak >nul

:: Open browser after a 3-second delay (gives Streamlit time to start)
start "" cmd /c "timeout /t 3 /nobreak >nul && start http://localhost:8501"

:: Run Streamlit — keep window open on crash so user can read error
!PYTHON_EXE! -m streamlit run "%APP_SCRIPT%" ^
    --server.port 8501 ^
    --server.headless false ^
    --browser.gatherUsageStats false

if !errorlevel! NEQ 0 (
    echo.
    echo   [ERROR] Streamlit exited with an error (code !errorlevel!).
    goto :fatal
)

goto :end

:: ════════════════════════════════════════════════════════════════════════════
:fatal
echo.
echo   %LINE%
echo    App failed to start. See error above.
echo   %LINE%
echo.
pause
exit /b 1

:end
echo.
echo   App closed. Goodbye!
pause
exit /b 0
