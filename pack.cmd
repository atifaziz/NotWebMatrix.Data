@echo off
setlocal
chcp 1252 > nul
cd "%~dp0"
if not exist dist md dist
if not %errorlevel%==0 exit /b %errorlevel%
call build /v:m && for %%i in (pkg\*.nuspec) do .nuget\NuGet pack %%i -OutputDirectory dist -Symbols
