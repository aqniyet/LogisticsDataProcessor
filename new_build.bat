@echo off
echo Cleaning previous builds...
rmdir /s /q "dist" 2>nul
rmdir /s /q "build" 2>nul
del /f /q "*.spec" 2>nul

echo Installing required packages...
pip install -r requirements.txt

echo Building executable...
pyinstaller --clean new_build.spec

echo Build complete!
if exist "dist\LogisticsProcessor.exe" (
    echo Executable created successfully at dist\LogisticsProcessor.exe
) else (
    echo Failed to create executable
    exit /b 1
)

pause 