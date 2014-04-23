@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto err

set SCRIPT_DIR=%~dp0
for %%I in ("%SCRIPT_DIR%..") do set HDATA_HOME=%%~dpfI

set MAIN_CLASSPATH=.;%HDATA_HOME%\lib\*
set HDATA_CONF_DIR=%HDATA_HOME%\conf

set JAVA_OPTS=%JAVA_OPTS% -Xss256k
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseParNewGC
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseConcMarkSweepGC

set JAVA_OPTS=%JAVA_OPTS% -XX:CMSInitiatingOccupancyFraction=75
set JAVA_OPTS=%JAVA_OPTS% -XX:+UseCMSInitiatingOccupancyOnly
set JAVA_OPTS=%JAVA_OPTS% -XX:+HeapDumpOnOutOfMemoryError
set JAVA_OPTS=%JAVA_OPTS% -Dhdata.conf.dir="%HDATA_CONF_DIR%"
set JAVA_OPTS=%JAVA_OPTS% -Dlog4j.configurationFile="file:///%HDATA_CONF_DIR%/log4j2.xml"

set FIRST_ARG=%1
set MAIN_CLASS="com.suning.hdata.CliDriver"
if "%FIRST_ARG%"=="execute-sql" (set MAIN_CLASS="com.suning.hdata.tool.SQLExecuteTool")

"%JAVA_HOME%\bin\java" %JAVA_OPTS% -cp "%MAIN_CLASSPATH%" %MAIN_CLASS% %*

goto finally

:err
echo JAVA_HOME environment variable must be set!
pause


:finally

ENDLOCAL