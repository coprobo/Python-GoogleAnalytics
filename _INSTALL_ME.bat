pip install virtualenv
IF NOT EXIST "%cd%\venv" (virtualenv venv/)

SET CURRENT=%cd%
SET VENV=%cd%\venv\Scripts\
SET REQUIREMENTS=%cd%\files\requirements.txt

cd "%VENV%"
pip install -r "%REQUIREMENTS%"

cd "%CURRENT%\Dictionary"
IF EXIST en_core_web_lg (move en_core_web_lg "%CURRENT%\venv\Lib\site-packages")
IF EXIST en_core_web_lg-2.2.5.dist-info (move en_core_web_lg-2.2.5.dist-info "%CURRENT%\venv\Lib\site-packages")

cd "%CURRENT%"
IF EXIST Dictionary (rd /s /q Dictionary)

pause