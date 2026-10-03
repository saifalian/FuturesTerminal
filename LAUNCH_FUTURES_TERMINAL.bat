@echo off
setlocal

set "ROOT=%~dp0"
set "RUNNER=%ROOT%scripts\run_terminal.ps1"

if not exist "%RUNNER%" (
  echo Could not find "%RUNNER%".
  pause
  exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%RUNNER%"
