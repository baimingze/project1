!ifndef _checkPython_nsh
!define _checkPython_nsh

# 64 bit limitations around pycrypto prebuilt binaries
!define PYTHON_VER_32BIT "2.7.2"
!define PYTHON_VER_64BIT "2.6.6"

; from http://nsis.sourceforge.net/Include/x64.nsh
!macro _RunningX64 _a _b _t _f
  !insertmacro _LOGICLIB_TEMP
  System::Call kernel32::GetCurrentProcess()i.s
  System::Call kernel32::IsWow64Process(is,*i.s)
  Pop $_LOGICLIB_TEMP
  !insertmacro _!= $_LOGICLIB_TEMP 0 `${_t}` `${_f}`
!macroend
!define RunningX64 `"" RunningX64 ""`
   
 ;--------------------------------
;Check for Python
Function getPythonPath
	StrCpy $R0 ""
	; is .py a registered filetype?
	ReadRegStr $0 HKCR ".py" ""
	StrCmp "$0" "" getPythonPathDone 0
	; get the associated exe
	ReadRegStr $0 HKCR "$0\shell\open\command" ""
	# response will look something like '"c:\python25\python.exe" "%1" %*'
	# or maybe just 'c:\python25\python.exe "%1" %*'
	# find that "% and trim there
	StrCpy $R0 $0 1  # copy first char
    findpctloop:
	StrLen $R1 $R0 # length of R0 to R1
	IntOp $R1 $R1 + 1
	StrCpy $R0 $0 $R1 # new slightly longer substring of path
    StrCpy $R1 $R0 "" -2 # copy the last two characters
	StrCmp "$R1" "%1" 0 findpctloop # are they the %1 arg?
	StrLen $R1 $R0 # length of R0 to R1
	IntOp $R1 $R1 - 3
	StrCpy $R0 $0 $R1 # copy the python command to R0
getPythonPathDone:
FunctionEnd
	
Function checkPython
PythonTop:
	SetOutPath $TEMP
	; copy python exe path to $R0
	Call getPythonPath
    StrCmp "$R0" "0" BadPythonVersion 0
    StrCmp "$R0" "" BadPythonVersion 0
    ; run a python script to parse "python --version" and
    ; verify that we're running python 2.6 or higher
    !insertmacro LogExecWait '$R0 -c "import sys; exit(1) if sys.version_info < (2 , 6, 0) else exit(0)"' $0
    StrCmp $0 "0" 0 BadPythonVersion
	${If} ${RunningX64}
		!insertmacro LogExecWait '$R0 -c "import sys; exit(1) if sys.version_info >= (2 , 7, 0) else exit(0)"' $0
		StrCmp $0 "0" 0 BadPythonVersion
	${Endif}
	PythonVerOK:
	!insertmacro LogDetail "Python 2.6 or higher appears to be installed, good"
    goto PythonDone

	
  BadPythonVersion:
   ${If} ${RunningX64}
     StrCpy $R0 'python-${PYTHON_VER_64BIT}.amd64.msi'
     StrCpy $R1 '${PYTHON_VER_64BIT}'
     MessageBox MB_OKCANCEL|MB_ICONQUESTION "${PRODUCT_SHORTNAME} requires Python version 2.6 for 64 bit use.  Would you like me to download and install Python from 'http://www.python.org/ftp/python/$R1/$R0'?" IDCANCEL abort
   ${Else}
     StrCpy $R0 'python-${PYTHON_VER_32BIT}.msi'
     StrCpy $R1 '${PYTHON_VER_32BIT}'
     MessageBox MB_OKCANCEL|MB_ICONQUESTION "${PRODUCT_SHORTNAME} requires Python version 2.6 or greater.  Would you like me to download and install Python from 'http://www.python.org/ftp/python/$R1/$R0'?" IDCANCEL abort
   ${EndIf}  
	!insertmacro LogExecWait '$INSTDIR\win32utils\wget http://www.python.org/ftp/python/$R1/$R0 -O $TEMP\$R0' $0
	!insertmacro LogExecWait 'msiexec /i "$TEMP\$R0" /qb' $0
    Goto PythonTop
  abort:
    !insertmacro LogAbort "Python installation did not complete"

  PythonDone:
    !insertmacro LogDetail "Python installation is OK"
	SetOutPath $INSTDIR


FunctionEnd


!endif ; _checkPython_nsh 