@echo off
REM Build script for acp-bridge on Windows
REM Requires Rust (rustup) to be installed: https://rustup.rs/

echo ============================================
echo  acp-bridge Windows Build Script
echo ============================================
echo.

REM Check if Rust is installed
where cargo >nul 2>nul
if %ERRORLEVEL% neq 0 (
    echo [ERROR] Rust/Cargo not found in PATH
    echo Please install Rust from https://rustup.rs/
    echo.
    pause
    exit /b 1
)

echo [INFO] Rust version:
cargo --version
echo.

REM Clean previous builds (optional)
echo [INFO] Cleaning previous builds...
cargo clean
echo.

REM Build in release mode with optimizations
echo [INFO] Building acp-bridge in release mode...
echo       This may take several minutes on first build...
echo.

cargo build --release

if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Build failed!
    echo Check the error messages above for details.
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================
echo  Build successful!
echo ============================================
echo.
echo Executable location:
echo   target\release\acp-bridge.exe
echo.
echo To run acp-bridge in ACP mode (stdin/stdout):
echo   target\release\acp-bridge.exe
echo.
echo To run acp-bridge in WebSocket mode:
echo   target\release\acp-bridge.exe --ws
echo.
echo WebSocket environment variables:
echo   WS_HOST - Bind host (default: 127.0.0.1)
echo   WS_PORT - Bind port (default: 8765)
echo.
echo Example WebSocket server:
echo   set WS_HOST=0.0.0.0
echo   set WS_PORT=8765
echo   target\release\acp-bridge.exe --ws
echo.
echo Or copy the executable to your desired location:
echo   copy target\release\acp-bridge.exe C:\path\to\your\bin\
echo.

REM Show binary info
echo [INFO] Binary information:
dir target\release\acp-bridge.exe
echo.

pause
