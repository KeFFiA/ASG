%1@mshta vbscript:Execute("CreateObject(""Wscript.Shell"").Run """"""%~f0"""" :"",0:Close()")& exit/b

@echo off

C:\ASGBuilds\ICAO\.venv\Scripts\python.exe C:\ASGBuilds\ICAO\main.py
