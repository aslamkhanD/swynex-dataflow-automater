@echo off
REM ============================================================================
REM DataFlow Automator - 1-Click Runner for Windows
REM ============================================================================
echo [START] Checking runtime environment...

where node >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [RUN] Executing via Node.js runtime...
    node automate.js %*
    goto end
)

where python >nul 2>&1
if %ERRORLEVEL% EQU 0 (
    echo [RUN] Executing via Python 3 runtime...
    python automate.py %*
    goto end
)

echo [ERROR] Neither Node.js nor Python 3 was detected in your PATH.
echo Please install Node.js (https://nodejs.org) or Python 3 (https://python.org).
pause

:end

