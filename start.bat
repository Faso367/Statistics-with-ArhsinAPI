@echo off
setlocal

REM Путь к Git Bash. Измените его, если у вас другой путь установки.
set "GIT_BASH=C:\Program Files\Git\bin\bash.exe"

REM Путь к скрипту относительно текущей папки
set "SCRIPT_PATH=%~dp0StartScript.sh"

REM chmod +x "%SCRIPT_PATH%"

REM Запуск Git Bash и выполнение скрипта
"%GIT_BASH%" --login -i "%SCRIPT_PATH%"

endlocal
