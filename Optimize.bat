@echo off
title System Cleanup Utility
color 0A

echo ============================================
echo        SYSTEM CLEANUP IN PROGRESS
echo ============================================
echo.

:: Delete user temp files
echo Cleaning User Temp Files...
del /s /f /q "%temp%\*.*" >nul 2>&1
for /d %%x in ("%temp%\*") do rd /s /q "%%x" >nul 2>&1

:: Delete Windows temp files
echo Cleaning Windows Temp Files...
del /s /f /q C:\Windows\Temp\*.* >nul 2>&1
for /d %%x in (C:\Windows\Temp\*) do rd /s /q "%%x" >nul 2>&1

:: Clear Prefetch (optional performance reset)
echo Cleaning Prefetch Files...
del /s /f /q C:\Windows\Prefetch\*.* >nul 2>&1

:: Clear Recycle Bin
echo Emptying Recycle Bin...
PowerShell.exe -NoProfile -Command "Clear-RecycleBin -Force" >nul 2>&1

:: Flush DNS
echo Flushing DNS Cache...
ipconfig /flushdns >nul

:: Reset Network (Optional Advanced)
echo Releasing and Renewing IP...
ipconfig /release >nul
ipconfig /renew >nul

:: Clear ARP Cache
echo Clearing ARP Cache...
arp -d * >nul 2>&1

:: Clear Windows Update Cache
echo Cleaning Windows Update Cache...
net stop wuauserv >nul 2>&1
del /s /f /q C:\Windows\SoftwareDistribution\Download\*.* >nul 2>&1
net start wuauserv >nul 2>&1

echo.
echo ============================================
echo            CLEANUP COMPLETE
echo ============================================
echo.
pause