@echo off
setlocal EnableExtensions EnableDelayedExpansion
title Disable Edge Updates + Disable SysInfoCap (HP)
color 0A

:: --- Admin check ---
net session >nul 2>&1
if not %errorlevel%==0 (
  echo [!] Run this as Administrator.
  pause
  exit /b 1
)

echo =========================================================
echo  1) DISABLE MICROSOFT EDGE AUTO-UPDATE (SERVICES + TASKS)
echo =========================================================

:: Kill Edge + updater (safe)
taskkill /f /im msedge.exe >nul 2>&1
taskkill /f /im MicrosoftEdgeUpdate.exe >nul 2>&1

:: Stop + disable Edge Update services
for %%S in (edgeupdate edgeupdatem) do (
  sc stop "%%S" >nul 2>&1
  sc config "%%S" start= disabled >nul 2>&1
)

:: Disable scheduled tasks commonly used by Edge updater
schtasks /Change /TN "MicrosoftEdgeUpdateTaskMachineCore" /Disable >nul 2>&1
schtasks /Change /TN "MicrosoftEdgeUpdateTaskMachineUA"   /Disable >nul 2>&1

:: Also disable any EdgeUpdate tasks with GUID/user suffixes
for /f "tokens=*" %%T in ('schtasks /Query /FO LIST ^| findstr /I "MicrosoftEdgeUpdateTask"') do (
  for /f "tokens=2 delims=:" %%N in ("%%T") do (
    set "TN=%%N"
    set "TN=!TN:~1!"
    schtasks /Change /TN "!TN!" /Disable >nul 2>&1
  )
)

:: Set EdgeUpdate policies: block automatic updates
:: (Policies are the cleanest way vs deleting files)
reg add "HKLM\SOFTWARE\Policies\Microsoft\EdgeUpdate" /f >nul 2>&1
reg add "HKLM\SOFTWARE\Policies\Microsoft\EdgeUpdate" /v UpdateDefault /t REG_DWORD /d 0 /f >nul 2>&1
reg add "HKLM\SOFTWARE\Policies\Microsoft\EdgeUpdate" /v Update /t REG_DWORD /d 0 /f >nul 2>&1
reg add "HKLM\SOFTWARE\Policies\Microsoft\EdgeUpdate" /v AutoUpdateCheckPeriodMinutes /t REG_DWORD /d 0 /f >nul 2>&1

echo.
echo [i] Edge auto-updates should now be blocked (services/tasks/policies).

echo.
echo =========================================================
echo  2) OPTIONAL: ATTEMPT TO UNINSTALL EDGE (MAY BE BLOCKED)
echo =========================================================
echo [!] On Windows 10/11, uninstall may fail because Edge is a system component.
choice /c YN /m "Try to uninstall Edge anyway?"
if errorlevel 2 goto SKIP_UNINSTALL

set "EDGESETUP="

for /f "delims=" %%P in ('dir /b /s "%ProgramFiles(x86)%\Microsoft\Edge\Application\*\Installer\setup.exe" 2^>nul') do (
  set "EDGESETUP=%%P"
  goto :FOUNDSETUP
)
:FOUNDSETUP

if not defined EDGESETUP (
  echo [!] Edge setup.exe not found. Skipping uninstall attempt.
  goto SKIP_UNINSTALL
)

echo [+] Using: %EDGESETUP%
"%EDGESETUP%" --uninstall --system-level --force-uninstall >nul 2>&1

:SKIP_UNINSTALL
echo.
echo =========================================================
echo  3) DISABLE SysInfoCap.exe (HP) IF PRESENT
echo =========================================================
echo [i] SysInfoCap.exe is commonly an HP system info capture component. :contentReference[oaicite:2]{index=2}

:: Kill the process if running
taskkill /f /im SysInfoCap.exe >nul 2>&1

:: Try disabling likely HP service names (varies by model/software)
for %%S in ("HP System Info HSA Service" "SysInfoCap" "HP Insights Analytics Service") do (
  sc stop %%S >nul 2>&1
  sc config %%S start= disabled >nul 2>&1
)

echo.
echo =========================================================
echo  DONE
echo =========================================================
echo Notes:
echo - Edge may still exist (Windows may protect it), but updates/services are disabled.
echo - If SysInfoCap comes back, uninstall the HP component from Apps: "HP System Info HSA".
echo.
pause
endlocal