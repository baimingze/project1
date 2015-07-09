ECHO OFF
REM simple batch file to place our python script on the windows path
REM so that one can simply substitute "mr-tandem <tandem_params>" 
REM for "tandem <tandem_params>" on commandline
mr-tandem.py general_config.json %1 %2 %3 %4 %5 %6 %7 %8 %9
