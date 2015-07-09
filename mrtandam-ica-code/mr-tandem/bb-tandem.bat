ECHO OFF
REM simple batch file to invoke MPI version on EC2
mr-tandem.py general_config.json %1 %2 %3 %4 %5 %6 %7 %8 %9 {\"runBangBang\":\"true\"} 
