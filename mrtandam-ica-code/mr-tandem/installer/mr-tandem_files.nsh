;
; MR-Tandem installer script component:
; a list of install files, broken out from rest of script to 
; distinguish content changes from installer logic changes
;
; Copyright (c) 2011 Insilicos, LLC
;
; This work uses the Apache license.  See the LICENSE-2_0.txt file elsewhere in this package.
;
; $Author: bpratt $
;;
; NOTES:
;
; TODO:
;
;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


!macro handleFiles addOrDelete ; "addOrDelete" is a parameter that defines the action

SetOverwrite on

!include "${ECA_BUILD_DIR}\installer\ECA_helperfiles.nsh" ; stuff that's useful to both ensemble and mr-tandem 

!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "README"
!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "doc\MR-Tandem_QuickStartGuide.pdf"
!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "doc\MR-Tandem_UserManual.pdf"
!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "mapreduce_helper.py"
!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "setup.py"
!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "mr-tandem.py"
!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "mr-tandem.bat"
!insertmacro ${addOrDelete} "${PRODUCT_BUILD_DIR}" "bb-tandem.bat"


!macroend
