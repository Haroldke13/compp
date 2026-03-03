@echo off
echo Clearing RAM Cache...
powershell -command "Clear-Host; [System.GC]::Collect(); [System.GC]::WaitForPendingFinalizers()"
echo Done!
pause


