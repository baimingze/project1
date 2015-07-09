; ECA installer script using NSIS installer
;
; Copyright (c) 2011 Insilicos, LLC
;
; This work uses the Apache license.  See the LICENSE-2_0.txt file elsewhere in this package.
;
; $Author: bpratt $
;
; NOTES:
;
; TODO:
;
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;

; note we may wrap this entire file with the MR-Tandem installer
!define ECA_BUILD_DIR "..\..\ensemble"
!ifndef PRODUCT_NAME
!define PRODUCT_NAME "Ensemble Cloud Army"
!define PRODUCT_FULLNAME "Insilicos Ensemble Cloud Army"
!define PRODUCT_SHORTNAME "ECA"
!define PRODUCT_SRCDIR "$INSTDIR\src"
!define WANT_R 1 ; install R
!define PRODUCT_BUILD_DIR ${ECA_BUILD_DIR}
!define PRODUCT_MAJOR_VERSION_NUMBER 2
!define PRODUCT_MINOR_VERSION_NUMBER 0
!define PRODUCT_REV_VERSION_NUMBER 0
; HM NIS Edit Wizard helper defines
!endif

!define PRODUCT_INSTALL_DIR_PARENT "C:\InsilicosCloudArmy"
!define HELPDOC "$INSTDIR\doc\${PRODUCT_SHORTNAME}_QuickStartGuide.pdf"
!define PRODUCT_VERSION "${PRODUCT_MAJOR_VERSION_NUMBER}.${PRODUCT_MINOR_VERSION_NUMBER}.${PRODUCT_REV_VERSION_NUMBER}"
!define PRODUCT_INSTALL_DIR "${PRODUCT_INSTALL_DIR_PARENT}\${PRODUCT_SHORTNAME}"

SetCompressor /SOLID /FINAL lzma  ; gives best compression, but it's slow

!define SIMPLEJSON_VER "2.1.6"
!define BOTO_VER "2.1"



!define SUPPORT_URL "http://sourceforge.net/projects/ica/support"

!define PRODUCT_PUBLISHER "Insilicos LLC"
!define PRODUCT_WEB_SITE "http://www.insilicos.com"
!define PRODUCT_DIR_REGKEY "Software\Microsoft\Windows\CurrentVersion\App Paths\${PRODUCT_NAME}"
!define PRODUCT_UNINST_KEY "Software\Microsoft\Windows\CurrentVersion\Uninstall\${PRODUCT_NAME}"
!define PRODUCT_UNINST_ROOT_KEY "HKLM"

!define PRODUCT_REGKEY "Software\${PRODUCT_NAME}"
!define PRODUCT_REGKEY_ROOT "HKLM"



!include "FileFunc.nsh"
!insertmacro DirState

; MUI 1.67 compatible ------
!include "MUI.nsh"

; MUI Settings
!define MUI_ABORTWARNING
!define MUI_ICON "${ECA_BUILD_DIR}\installer\ica.ico"
!define MUI_UNICON "${ECA_BUILD_DIR}\installer\ica.ico"

!define ALL_USERS 1 ; set path for all users

; Welcome page
!insertmacro MUI_PAGE_WELCOME
; License page
!insertmacro MUI_PAGE_LICENSE "${ECA_BUILD_DIR}\LICENSE-2_0.txt"
; Instfiles page
!insertmacro MUI_PAGE_INSTFILES
; Finish page
!define LAUNCH_LINK "$SMPROGRAMS\${PRODUCT_NAME}\${PRODUCT_NAME}.lnk"

!define MUI_FINISHPAGE_RUN
!define MUI_FINISHPAGE_RUN_FUNCTION "LaunchLink"
!define MUI_FINISHPAGE_RUN_TEXT "Run ${PRODUCT_NAME}"
!define MUI_FINISHPAGE_LINK_LOCATION "${LAUNCH_LINK}"

!define MUI_FINISHPAGE_SHOWREADME ${HELPDOC}


!insertmacro MUI_PAGE_FINISH

Function LaunchLink
  ExecShell "" "${LAUNCH_LINK}"
FunctionEnd


; Uninstaller pages
!insertmacro MUI_UNPAGE_INSTFILES

; Language files
!insertmacro MUI_LANGUAGE "English"

; add an XP manifest to the installer
XPStyle on

; MUI end ------

!include "${ECA_BUILD_DIR}\installer\AddToPath.nsh"           


Function .onInit
FunctionEnd

Name "${PRODUCT_FULLNAME}"

; for Vista and Win7, warn that we want admin priv for install
RequestExecutionLevel admin

BrandingText "Insilicos Cloud Army: sourceforge.net/projects/ica"
Caption "${PRODUCT_NAME} ${PRODUCT_VERSION}"

OutFile "${PRODUCT_BUILD_DIR}\${PRODUCT_SHORTNAME}_Setup_${PRODUCT_VERSION}.exe"
InstallDir "${PRODUCT_INSTALL_DIR}"
InstallDirRegKey HKLM "${PRODUCT_DIR_REGKEY}" ""
ShowInstDetails show
ShowUnInstDetails show

Var /GLOBAL bFirstInstall

; we maintain a single list of files for both install and uninstall,
; these macros let us use the list both ways
!macro addFile sourcedir file
  !define afID ${__LINE__}
; is there a path in the filename?
  Push ${file}
  Call GetDirDepth
  Pop $R0  
  StrCmp $R0 "0" 0 haspath_${afID}
  SetOutPath "$INSTDIR"
  goto setfile_${afID}
haspath_${afID}:  
  Push ${file}
  Call GetParent
  Pop $R0  
  SetOutPath "$INSTDIR\$R0"
setfile_${afID}:  
  File "${sourcedir}\${file}"
  !undef afID
!macroend

!macro deleteFile sourcedir file
  Delete "$INSTDIR\${file}"
!macroend

!include "${PRODUCT_SHORTNAME}_files.nsh" ; the list of distro files

!include "${ECA_BUILD_DIR}\installer\checkPython.nsh"

!ifdef WANT_R
!include "${ECA_BUILD_DIR}\installer\checkR.nsh"
!endif

Section "${PRODUCT_NAME}" SEC01
    MessageBox MB_OK "Welcome to the ${PRODUCT_SHORTNAME} installation process.  If you need assistance with the installation or ${PRODUCT_SHORTNAME} in general, please visit ${SUPPORT_URL}. $1"
    StrCpy $bFirstInstall ""
    # call userInfo plugin to get user info.  The plugin puts the result in the stack
    userInfo::getAccountType  
    # pop the result from the stack into $0
    pop $0   
    # compare the result with the string "Admin" to see if the user is admin. 
    strCmp $0 "Admin" admin_ok   
    # if there is not a match, print message and return
    messageBox MB_OKCANCEL "You do not appear to have administrative privilege - this is install may fail.  Proceed anyway?" IDOK pathfix IDCANCEL badpriv
badpriv:
    abort
admin_ok:
  ${DirState} "$INSTDIR" $1
  StrCmp $1 "-1" 0 create_installdir_done
  !insertmacro LogDetail "creating $INSTDIR"
  CreateDirectory "$INSTDIR"
  StrCpy $bFirstInstall "1"
create_installdir_done:

pathfix:
  ; add us to the path
  Push $INSTDIR
  Call AddToPath
  Push "$INSTDIR\win32utils"
  Call AddToPath

  SetOutPath "$INSTDIR"
  SetOverwrite ifnewer
  CreateDirectory "$SMPROGRAMS\${PRODUCT_NAME}"
  # use "$INSTDIR\uninst.exe" as icon source
  CreateShortCut "${LAUNCH_LINK}" "cmd.exe" "/v /K $\"set PATH=!PATH!;$INSTDIR;$INSTDIR\win32utils&&cd ${PRODUCT_SRCDIR}&&type README$\"" "$INSTDIR\uninst.exe"
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\Support.lnk" "${SUPPORT_URL}" "" "$INSTDIR\uninst.exe"
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\Insilicos Website.lnk" "${PRODUCT_WEB_SITE}" "" "$INSTDIR\uninst.exe"

;distrofiles: 
  !insertmacro handleFiles addFile ; install the distro files

  StrCmp $bFirstInstall "1" 0 checkpython
  !insertmacro LogDetail "setting permissions on directory tree $INSTDIR"
  ExecWait '"$INSTDIR\win32utils\setWin32Permissions" "$INSTDIR"'


recordRegistryData:
  WriteRegStr ${PRODUCT_REGKEY_ROOT} "${PRODUCT_REGKEY}" "DisplayName" "${PRODUCT_FULLNAME}"
  WriteRegStr ${PRODUCT_REGKEY_ROOT} "${PRODUCT_REGKEY}" "ProductVersion" "${PRODUCT_VERSION}"
  WriteRegStr ${PRODUCT_REGKEY_ROOT} "${PRODUCT_REGKEY}" "ProductVersionMajor" "${PRODUCT_MAJOR_VERSION_NUMBER}"
  WriteRegStr ${PRODUCT_REGKEY_ROOT} "${PRODUCT_REGKEY}" "ProductVersionMinor" "${PRODUCT_MINOR_VERSION_NUMBER}"
  WriteRegStr ${PRODUCT_REGKEY_ROOT} "${PRODUCT_REGKEY}" "ProductVersionRev" "${PRODUCT_REV_VERSION_NUMBER}"

checkpython:
 ; now see if we need to install Python
  Call checkPython
 
  ; and run the python package installer
Call getPythonPath
# do we have pycrypto yet?
!insertmacro LogExecWait '$R0 -c "import Crypto; exit(0)"' $0
StrCmp $0 "0" PythonVerOK 0
${If} ${RunningX64}
	MessageBox MB_OKCANCEL|MB_ICONQUESTION "We'll need to download and install the pyCrypto package for use in SSH communication with AWS.  When the pyCrypto installer begins, just accept all the default values.  Press OK to proceed." IDOK PyCryptoBinary
	!insertmacro LogAbort "User cancelled"
${Endif}
PyCryptoBinary:
	Call getPythonPath
	!insertmacro LogExecWait '$R0 $INSTDIR\installer\installPyCryptoBinary.py' $0
PythonVerOK:

Call getPythonPath
!insertmacro LogExecWait '$R0 $INSTDIR\src\download_eca.py $INSTDIR http://ica.svn.sourceforge.net/svnroot/ica/branches/${PRODUCT_MAJOR_VERSION_NUMBER}.${PRODUCT_MINOR_VERSION_NUMBER}/ensemble/' $0 
Call getPythonPath
!insertmacro LogExecWait '$R0 $INSTDIR\src\setup.py install' $0 
Call getPythonPath
!insertmacro LogExecWait '$R0 -c "import boto; exit(0)"' $0
StrCmp $0 "0" BotoOK 0
!insertmacro LogAbort "installation did not complete - consult installer.log"
  
BotoOK:
!ifdef WANT_R
  ; offer to install R if none already
  Call checkR
!endif

; if this is our first install, set permissions
  StrCmp $bFirstInstall "1" 0 recheck_path
  ; windows equivalent of chmod g+w
    !insertmacro LogDetail "setting permissions on newly installed files and data directories"
    Exec '$INSTDIR\win32utils\setWin32Permissions "$INSTDIR"'

  ; sometimes python install blows away our path change, try setting again
recheck_path:  
  Push $INSTDIR
  Call AddToPath
  Push "$INSTDIR\win32utils"
  Call AddToPath  


main_done:
  Call DumpLog
  SetRebootFlag false
SectionEnd

Section -AdditionalIcons
  CreateShortCut "$SMPROGRAMS\${PRODUCT_NAME}\Uninstall.lnk" "$INSTDIR\uninst.exe"
SectionEnd


Section -Post
  WriteUninstaller "$INSTDIR\uninst.exe"
;  WriteRegStr HKLM "${PRODUCT_DIR_REGKEY}" "" "$INSTDIR\xinteract.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayName" "${PRODUCT_FULLNAME}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "UninstallString" "$INSTDIR\uninst.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayIcon" "$INSTDIR\uninst.exe"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "DisplayVersion" "${PRODUCT_VERSION}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "URLInfoAbout" "${PRODUCT_WEB_SITE}"
  WriteRegStr ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}" "Publisher" "${PRODUCT_PUBLISHER}"
  
  ; copy details window to logfile
  !insertmacro LogDetail "done"


SectionEnd


Function un.onUninstSuccess
  HideWindow
  MessageBox MB_ICONINFORMATION|MB_OK "${PRODUCT_FULLNAME} was successfully removed from your computer."
FunctionEnd

Function un.onInit
  MessageBox MB_ICONQUESTION|MB_YESNO|MB_DEFBUTTON2 "Are you sure you want to completely remove ${PRODUCT_FULLNAME} and all of its components?" IDYES +2
  Abort
FunctionEnd

Section Uninstall
  RMDir /r /REBOOTOK "$SMPROGRAMS\${PRODUCT_NAME}"
  Delete "$INSTDIR\uninst.exe"

  !insertmacro handleFiles deleteFile ; remove the distro files
  
  Delete "$INSTDIR\installer.log"

  RMDir "$INSTDIR"
 

  DeleteRegKey ${PRODUCT_UNINST_ROOT_KEY} "${PRODUCT_UNINST_KEY}"
  DeleteRegKey HKLM "${PRODUCT_DIR_REGKEY}"

  ; delete our registry data
  DeleteRegKey ${PRODUCT_REGKEY_ROOT} "${PRODUCT_REGKEY}"

  SetAutoClose true
SectionEnd
