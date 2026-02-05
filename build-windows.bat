@echo off
setlocal enabledelayedexpansion

echo ========================================
echo WZ Extension Windows Build Script
echo ========================================
echo.

:: Check if submodules are initialized
if not exist "duckdb\CMakeLists.txt" (
    echo Initializing git submodules...
    git submodule update --init --recursive
    if errorlevel 1 (
        echo ERROR: Failed to initialize submodules
        exit /b 1
    )
)

:: Create build directory
if not exist "build" mkdir build

:: Run the build
echo Building WZ extension...
call make release

if errorlevel 1 (
    echo.
    echo ========================================
    echo BUILD FAILED
    echo ========================================
    exit /b 1
)

:: Find the extension file
echo.
echo Looking for extension file...
for /r build %%f in (*.duckdb_extension) do (
    set "EXTENSION_FILE=%%f"
    goto :found
)

:found
if not defined EXTENSION_FILE (
    echo ERROR: No extension file found
    exit /b 1
)

echo Found: %EXTENSION_FILE%

:: Copy to dist
if not exist "dist" mkdir dist
copy "%EXTENSION_FILE%" dist\wz.duckdb_extension

echo.
echo ========================================
echo BUILD SUCCESSFUL
echo ========================================
echo.
echo Extension file: dist\wz.duckdb_extension
echo.
echo To use:
echo   INSTALL 'dist/wz.duckdb_extension';
echo   LOAD wz;
echo   SELECT * FROM into_wz(gui_verfahren_id := 'your-id', lng_kanzlei_konten_rahmen_id := 56);
echo.

endlocal
