@echo off
title Startup Cleaner (Safe Disable)
color 0B

echo ============================================
echo     STARTUP DISABLER (Safe + Reversible)
echo ============================================
echo.
echo This will:
echo  - Backup startup registry keys to .reg files
echo  - Move startup entries out of RUN keys into RUN_Disabled keys
echo  - Move Startup-folder shortcuts into Disabled_Startup folders
echo.
pause

:: ---- Create folders for backups/logs ----
set "BK=%~dp0startup_backup"
if not exist "%BK%" mkdir "%BK%"

echo Backing up startup registry keys...
reg export "HKCU\Software\Microsoft\Windows\CurrentVersion\Run" "%BK%\HKCU_Run_Backup.reg" /y >nul 2>&1
reg export "HKLM\Software\Microsoft\Windows\CurrentVersion\Run" "%BK%\HKLM_Run_Backup.reg" /y >nul 2>&1

echo.
echo Moving registry startup items to disabled keys (reversible)...

:: ---- Disable HKCU Run entries (current user) ----
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$src='HKCU:\Software\Microsoft\Windows\CurrentVersion\Run';" ^
"$dst='HKCU:\Software\Microsoft\Windows\CurrentVersion\Run_Disabled';" ^
"if(!(Test-Path $dst)){New-Item -Path $dst | Out-Null};" ^
"if(Test-Path $src){" ^
"  $props=(Get-ItemProperty $src | Select-Object -Property * -ExcludeProperty PSPath,PSParentPath,PSChildName,PSDrive,PSProvider);" ^
"  foreach($p in $props.PSObject.Properties){" ^
"    if($p.Name -and $p.Value){" ^
"      New-ItemProperty -Path $dst -Name $p.Name -Value $p.Value -PropertyType String -Force | Out-Null;" ^
"      Remove-ItemProperty -Path $src -Name $p.Name -ErrorAction SilentlyContinue;" ^
"    }" ^
"  }" ^
"}" >nul 2>&1

:: ---- Disable HKLM Run entries (all users) ----
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$src='HKLM:\Software\Microsoft\Windows\CurrentVersion\Run';" ^
"$dst='HKLM:\Software\Microsoft\Windows\CurrentVersion\Run_Disabled';" ^
"if(!(Test-Path $dst)){New-Item -Path $dst -Force | Out-Null};" ^
"if(Test-Path $src){" ^
"  $props=(Get-ItemProperty $src | Select-Object -Property * -ExcludeProperty PSPath,PSParentPath,PSChildName,PSDrive,PSProvider);" ^
"  foreach($p in $props.PSObject.Properties){" ^
"    if($p.Name -and $p.Value){" ^
"      New-ItemProperty -Path $dst -Name $p.Name -Value $p.Value -PropertyType String -Force | Out-Null;" ^
"      Remove-ItemProperty -Path $src -Name $p.Name -ErrorAction SilentlyContinue;" ^
"    }" ^
"  }" ^
"}" >nul 2>&1

echo.
echo Disabling Startup-folder shortcuts (moving them)...

:: ---- Startup folder (current user) ----
set "SU=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"
set "SUD=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Disabled_Startup"
if not exist "%SUD%" mkdir "%SUD%"
if exist "%SU%\*" move /y "%SU%\*" "%SUD%\" >nul 2>&1

:: ---- Startup folder (all users) ----
set "SA=%ProgramData%\Microsoft\Windows\Start Menu\Programs\Startup"
set "SAD=%ProgramData%\Microsoft\Windows\Start Menu\Programs\Disabled_Startup"
if not exist "%SAD%" mkdir "%SAD%"
if exist "%SA%\*" move /y "%SA%\*" "%SAD%\" >nul 2>&1

echo.
echo ============================================
echo Done.
echo Backups saved in: %BK%
echo Disabled entries were moved to:
echo  - HKCU Run_Disabled
echo  - HKLM Run_Disabled  (Admin required)
echo  - Disabled_Startup folders
echo ============================================
echo.
pause