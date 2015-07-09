; for insertion in ECA_files.nsh or mr-tandem_files.nsh

!insertmacro ${addOrDelete} "${ECA_BUILD_DIR}" "src\download_eca.py"
!insertmacro ${addOrDelete} "${ECA_BUILD_DIR}" "installer\installPyCryptoBinary.py"

; win32 versions of unix utilities from http://unxutils.sourceforge.net/ 
!insertmacro ${addOrDelete} "${ECA_BUILD_DIR}" "win32utils\*"

; helps install R
!insertmacro ${addOrDelete} "${ECA_BUILD_DIR}" "installer\installLatestR.py"


