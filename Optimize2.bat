@echo off
setlocal EnableExtensions EnableDelayedExpansion
title Windows Service Tuner + Update Control + Pagefile + RAM Trim
color 0A

:: -----------------------------
:: ADMIN CHECK
:: -----------------------------
net session >nul 2>&1
if not %errorlevel%==0 (
  echo [!] Please run this script as Administrator.
  pause
  exit /b 1
)

echo ==========================================================
echo  Windows Tuner (Reversible) - Services / Updates / Pagefile
echo ==========================================================
echo.

:: -----------------------------
:: BACKUP FOLDER
:: -----------------------------
set "BK=%~dp0backup_%DATE:~-4%-%DATE:~4,2%-%DATE:~7,2%_%TIME:~0,2%%TIME:~3,2%%TIME:~6,2%"
set "BK=%BK: =0%"
mkdir "%BK%" >nul 2>&1

echo [+] Backup folder: "%BK%"
echo.

:: -----------------------------
:: OPTIONAL: CREATE RESTORE POINT
:: -----------------------------
echo [+] Creating a System Restore Point (may fail if disabled)...
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"try { Enable-ComputerRestore -Drive 'C:\' | Out-Null } catch {} ; ^
 try { Checkpoint-Computer -Description 'Before_Service_Tune' -RestorePointType 'MODIFY_SETTINGS' } catch {}" >nul 2>&1

:: -----------------------------
:: DEFINE 10 SERVICES TO DISABLE
:: Edit this list to your preference
:: -----------------------------
set SERVICES=DiagTrack dmwappushservice  XblGameSave XboxGipSvc XboxNetApiSvc WerSvc RetailDemo Fax

echo ==========================================================
echo  Step 1: Stop + Disable 10 Services (editable list)
echo ==========================================================
echo Services:
for %%S in (%SERVICES%) do echo   - %%S
echo.
echo [!] Warning: Disabling services can break features. This script backs up configs.
choice /c YN /m "Proceed to disable these services?"
if errorlevel 2 goto SKIP_SERVICES

echo [+] Backing up current service configs...
for %%S in (%SERVICES%) do (
  sc qc "%%S" > "%BK%\service_qc_%%S.txt" 2>&1
  reg query "HKLM\SYSTEM\CurrentControlSet\Services\%%S" /v Start > "%BK%\service_reg_%%S.txt" 2>&1
)

echo [+] Stopping and disabling...
for %%S in (%SERVICES%) do (
  echo    -> %%S
  sc stop "%%S" >nul 2>&1
  sc config "%%S" start= disabled >nul 2>&1
)

:SKIP_SERVICES
echo.

:: -----------------------------
:: WINDOWS UPDATE: STOP + ONLY UPDATE ON CLICKS
:: "Notify for download and auto install" = AUOptions=2
:: Also set wuauserv to manual so it doesn't auto-run
:: -----------------------------
echo ==========================================================
echo  Step 2: Control Windows Update (Only update when you click)
echo ==========================================================
choice /c YN /m "Stop Windows Update and set to 'Notify (click to download/install)'?"
if errorlevel 2 goto SKIP_WU

echo [+] Backing up Windows Update policy keys...
reg export "HKLM\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate" "%BK%\WU_Policies_Backup.reg" /y >nul 2>&1

echo [+] Setting Windows Update policy to Notify (AUOptions=2)...
reg add "HKLM\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU" /f >nul 2>&1
reg add "HKLM\SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU" /v AUOptions /t REG_DWORD /d 2 /f >nul 2>&1

echo [+] Stopping Update services...
net stop wuauserv >nul 2>&1
net stop bits >nul 2>&1

echo [+] Setting wuauserv startup to Manual (trigger start)...
sc config wuauserv start= demand >nul 2>&1

echo [i] You can still update via: Settings ^> Windows Update ^> Check for updates
:SKIP_WU
echo.

:: -----------------------------
:: PAGEFILE: SET 100GB
:: NOTE: This can consume large disk space. User asked 100GB.
:: We'll set initial=max=102400 MB (100GB).
:: -----------------------------
echo ==========================================================
echo  Step 3: Set Pagefile to 100GB (Initial=Max=102400 MB)
echo ==========================================================
choice /c YN /m "Apply a 100GB pagefile on C:\pagefile.sys? (requires reboot)"
if errorlevel 2 goto SKIP_PAGEFILE

echo [+] Backing up current pagefile settings...
reg export "HKLM\SYSTEM\CurrentControlSet\Control\Session Manager\Memory Management" "%BK%\MemoryManagement_Backup.reg" /y >nul 2>&1

echo [+] Disabling 'Automatically manage paging file size'...
wmic computersystem where name="%computername%" set AutomaticManagedPagefile=False >nul 2>&1

echo [+] Setting pagefile to 102400 MB (100GB)...
wmic pagefileset where name="C:\\pagefile.sys" delete >nul 2>&1
wmic pagefileset create name="C:\\pagefile.sys" >nul 2>&1
wmic pagefileset where name="C:\\pagefile.sys" set InitialSize=102400,MaximumSize=102400 >nul 2>&1

echo [i] Pagefile change will fully apply after reboot.
:SKIP_PAGEFILE
echo.

:: -----------------------------
:: "RAM CLEAN" EVERY 5 MINUTES
:: Reality: Windows manages RAM. We implement a gentle working-set trim.
:: Creates a PS script + Scheduled Task.
:: -----------------------------
echo ==========================================================
echo  Step 4: Schedule light RAM working-set trim every 5 minutes
echo ==========================================================
echo [i] This trims process working sets (can reduce memory pressure in some cases,
echo     but may cause occasional stutter). It's NOT magic RAM cleaning.
choice /c YN /m "Create scheduled task to trim working sets every 5 minutes?"
if errorlevel 2 goto SKIP_RAMTRIM

set "PSS=%BK%\trim_workingset.ps1"

echo [+] Writing PowerShell script: "%PSS%"
> "%PSS%" (
  echo $ErrorActionPreference = 'SilentlyContinue'
  echo Add-Type -Namespace Win32 -Name Mem -MemberDefinition @"
  echo [System.Runtime.InteropServices.DllImport("psapi.dll")] public static extern int EmptyWorkingSet(System.IntPtr hProcess);
  echo "@
  echo $procs = Get-Process ^| Where-Object { $_.Id -ne $PID -and $_.Handle ^> 0 }
  echo foreach ($p in $procs) { [Win32.Mem]::EmptyWorkingSet($p.Handle) ^| Out-Null }
)

echo [+] Creating scheduled task: RAM_WorkingSet_Trim (every 5 minutes)
schtasks /Create /F /TN "RAM_WorkingSet_Trim" ^
  /SC MINUTE /MO 5 ^
  /RL HIGHEST ^
  /TR "powershell -NoProfile -ExecutionPolicy Bypass -File \"%PSS%\"" >nul 2>&1

echo [i] To remove later:  schtasks /Delete /TN "RAM_WorkingSet_Trim" /F
:SKIP_RAMTRIM
echo.

:: -----------------------------
:: RESTORE INSTRUCTIONS
:: -----------------------------
echo ==========================================================
echo  DONE. Restore / rollback options:
echo ==========================================================
echo 1) Use System Restore: "Before_Service_Tune" (if created)
echo 2) Service backups are in: "%BK%"
echo 3) Windows Update policy backup: WU_Policies_Backup.reg
echo 4) Pagefile backup: MemoryManagement_Backup.reg
echo 5) Remove RAM task: schtasks /Delete /TN "RAM_WorkingSet_Trim" /F
echo.
echo [!] Recommended: Reboot to fully apply changes.
pause
endlocal