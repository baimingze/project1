;
; ECA installer script component:
; a list of ECA install files, broken out from rest of script to 
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

; note example data files are now downloaded from sourceforge at install time



!macroend
