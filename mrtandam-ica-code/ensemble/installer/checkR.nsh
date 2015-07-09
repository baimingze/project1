!ifndef _checkR_nsh
!define _checkR_nsh

   
 ;--------------------------------
;Check for R
Function checkR
	SetOutPath $TEMP
  RTop:

	ReadRegStr $0 HKCR ".RData" ""
	# response will look something like 'RWorkspace'
	StrCmp "$0" "" NeedsR RDone
	
  NeedsR:
	MessageBox MB_YESNOCANCEL|MB_ICONQUESTION "If you want to do local ${PRODUCT_SHORTNAME} runs you'll need to have R installed on this computer.  Would you like me to download and install R from 'http://cran.r-project.org/bin/windows/base/release.htm'?" IDCANCEL RAbort IDNO RSkip
	; put python executable path in $R0
	Call getPythonPath 
	!insertmacro LogExecWait '$R0 $INSTDIR\installer\installLatestR.py' $0
    Goto RTop
  RAbort:
    !insertmacro LogAbort "R installation did not complete"

  RDone:
    !insertmacro LogDetail "R installation is OK"
  RSkip:
	SetOutPath $INSTDIR




FunctionEnd


!endif ; _checkR_nsh 