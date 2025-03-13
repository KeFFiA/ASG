%1@mshta vbscript:Execute("CreateObject(""Wscript.Shell"").Run """"""%~f0"""" :"",0:Close()")& exit/b

@echo off

timeout /t 10 /nobreak > nul

echo "Starting script..." >> C:\ASGBuilds\ICAO\log.txt

C:\ASGBuilds\ICAO\.venv\Scripts\python.exe C:\ASGBuilds\ICAO\main.py >> C:\ASGBuilds\ICAO\log.txt 2>&1
