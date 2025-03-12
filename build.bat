@echo off
echo Installing required packages...
python -m pip install -r requirements.txt

echo Building executable...
python -m PyInstaller --name="LogisticsProcessor" ^
    --windowed ^
    --icon=usm.ico ^
    --add-data="app;app" ^
    --add-data="usm.ico;." ^
    --add-data="config.json;." ^
    --hidden-import=pandas ^
    --hidden-import=openpyxl ^
    --hidden-import=PyQt5 ^
    --hidden-import=sqlalchemy ^
    main.py

echo Build complete!
if exist "dist\LogisticsProcessor" (
    echo Executable created successfully at dist\LogisticsProcessor\LogisticsProcessor.exe
) else (
    echo Failed to create executable
) 