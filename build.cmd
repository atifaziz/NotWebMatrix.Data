@echo off
pushd "%~dp0"
call :main %*
popd
goto :EOF

:main
setlocal
set MSBUILDEXE=%SystemRoot%\Microsoft.NET\Framework\v4.0.30319\MSBuild.exe
if not exist "%MSBUILDEXE%" (
    echo The .NET Framework 4.0 does not appear to be installed on this 
    echo machine, which is required to build the solution.
    exit /b 1
)
set EnableNuGetPackageRestore=true
set BUILD="%MSBUILDEXE%" NotWebMatrix.sln /v:m
.nuget\NuGet restore ^
  && %BUILD% /p:Configuration=Debug %* ^
  && %BUILD% /p:Configuration=Release /v:m %*
