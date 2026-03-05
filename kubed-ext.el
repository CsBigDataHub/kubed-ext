;;; Kubed-Ext.El --- Extensions For Kubed With Async Metrics -*- Lexical-Binding: T; -*-

;; Author: Chetan Koneru
;; Version: 0.4.0
;; Url: Https://Github.Com/Csbigdatahub/Kubed-Ext
;; Keywords: Tools, Kubernetes
;; Package-Requires: ((Emacs "29.1") (Kubed "0.5.1") (Transient "0.4.0"))

;;; Commentary:
;; A Comprehensive Extension Suite For Kubed, Transforming Emacs Into A
;; Production-Grade Kubernetes Dashboard.

;;; Code:

(Require 'Kubed)
(Require 'Kubed-Transient)
(Require 'Transient)
(Require 'Cl-Lib)
(Require 'Json)

(Defgroup Kubed-Ext Nil
  "Extensions For Kubed."
  :Group 'Kubed)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0.  Autoload Fixes + Full-Screen List Buffers
;;; ═══════════════════════════════════════════════════════════════

(Defalias 'Kubed-Ext-List-Ingresss      #'Kubed-List-Ingresses)
(Defalias 'Kubed-Ext-List-Ingressclasss #'Kubed-List-Ingressclasses)

(Add-To-List 'Display-Buffer-Alist
             '("\\`\\*Kubed [A-Z][A-Z0-9]*[@[]"
               (Display-Buffer-Same-Window)))
(Add-To-List 'Display-Buffer-Alist
             '("\\`\\*Kubed-Top-"
               (Display-Buffer-Same-Window)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 0a.  Utility Functions
;;; ═══════════════════════════════════════════════════════════════

(Defsubst Kubed-Ext-None-P (S)
  "Return Non-Nil If S Is Nil, Empty, Or A Kubectl Placeholder Value."
  (Or (Null S)
      (Not (Stringp S))
      (String-Empty-P S)
      (String= S "<None>")
      (String= S "")
      (String= S "Nil")
      (String-Match-P "\\`[ \T]*\'" S)))

(Defun Kubed-Ext--Resource-At-Event (Event)
  "Return The Tabulated-List Resource Id At Event.
Works For Mouse Clicks, Keyboard Invocations, And Context-Menu Events.
For Mouse Events The Window And Buffer-Position Are Read From The Event
Itself So Point Is Never Moved As A Side-Effect.  For Keyboard Events
The Resource At The Current Point Position Is Returned.
Returns Nil When No Resource Is Found."
  (If (Mouse-Event-P Event)
      (Let* ((Start  (Event-Start Event))
             (Window (Posn-Window Start))
             (Pt     (Posn-Point  Start)))
            (When (And (Windowp Window) (Numberp Pt))
                  (With-Current-Buffer (Window-Buffer Window)
                                       (Tabulated-List-Get-Id Pt))))
      ;; Keyboard / Non-Mouse Path — Use Current Point.
      (Tabulated-List-Get-Id)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 0b.  Defcustoms: Status Faces, Output Format, Shell
;;; ═══════════════════════════════════════════════════════════════

(Defcustom Kubed-Ext-Status-Faces
  '(("Running"                      . Success)
    ("Succeeded"                    . Shadow)
    ("Completed"                    . (:Foreground "Yellow"))
    ("Pending"                      . Warning)
    ("Containercreating"            . Warning)
    ("Podinitializing"              . Warning)
    ("Unknown"                      . Warning)
    ("Terminating"                  . (:Foreground "Blue"))
    ("Failed"                       . Error)
    ("Error"                        . Error)
    ("Oomkilled"                    . Error)
    ("Crashloopbackoff"             . Error)
    ("Imagepullbackoff"             . Error)
    ("Errimagepull"                 . Error)
    ("Createcontainerconfigerror"   . Error)
    ("Invalidimagename"             . Error)
    ("Evicted"                      . Error)
    ("Runcontainererror"            . Error)
    ("Starterror"                   . Error)
    ("Ready"                        . Success)
    ("Notready"                     . Error)
    ("Active"                       . Success)
    ("Bound"                        . Success)
    ("Released"                     . Shadow)
    ("Healthy"                      . Success))
  "Association List Mapping Pod/Resource Status Strings To Faces.
Customize This To Change Colors For Different Kubernetes Statuses."
  :Type '(Alist :Key-Type String :Value-Type (Choice Face Sexp))
  :Group 'Kubed-Ext)

(Defcustom Kubed-Ext-Output-Format "Yaml"
  "Default Output Format For Resource Display Buffers (Yaml Or Json)."
  :Type '(Choice (Const "Yaml") (Const "Json"))
  :Group 'Kubed-Ext)

(Defcustom Kubed-Ext-Pod-Shell "/Bin/Sh"
  "Shell To Run When Opening A Terminal In A Kubernetes Pod."
  :Type 'String
  :Group 'Kubed-Ext)

(Defcustom Kubed-Ext-Production-Context-Regexp "\\Bprod\\B"
  "Regexp Matched Against A Kubectl Context Name To Flag It As Production.
When Matched The Mode Line Shows The Context In Red With A ☢ Prefix And
Suffix.  The Default Pattern Matches The Word `Prod' But Not `Preprod'.
Set To Nil To Disable Production Highlighting Entirely."
  :Type '(Choice (Regexp :Tag "Regexp")
                 (Const  :Tag "Disabled" Nil))
  :Group 'Kubed-Ext)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0c.  Buffer-Local Variables
;;; ═══════════════════════════════════════════════════════════════

(Defvar-Local Kubed-Ext-Resource-Filter ""
  "Substring Filter For Highlighting Matching Resources In List Buffers.")

(Defvar-Local Kubed-Ext--Header-Error Nil
  "Current Error Message To Display, Or Nil If No Error.")

(Defvar-Local Kubed-Ext--Error-Overlay Nil
  "Overlay Used To Display Error Message At Top Of Buffer.")

(Defvar-Local Kubed-Ext-List-Label-Selector Nil
  "Active Label Selector For Server-Side Kubectl Filtering.")

;; External Package Variable Declarations (Suppress Byte-Compiler Warnings).
(Defvar Vterm-Shell)
(Defvar Vterm-Buffer-Name)
(Defvar Eat-Buffer-Name)
(Defvar Eshell-Buffer-Name)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0d.  Namespace Prefetch Cache
;;; ═══════════════════════════════════════════════════════════════

(Defvar Kubed-Ext--Namespace-Prefetch-Contexts (Make-Hash-Table :Test 'Equal)
  "Contexts For Which Namespace Prefetch Has Been Triggered.")

(Defun Kubed-Ext-Prefetch-Namespaces (&Optional Context)
  "Asynchronously Prefetch Namespace List For Context.
Does Nothing If Namespaces Are Already Cached Or A Fetch Is In Progress.
This Ensures `Kubed-Read-Namespace' Completions Appear Without Delay."
  (Let ((Ctx (Or Context (Ignore-Errors (Kubed-Local-Context)))))
       (When Ctx
             (Condition-Case Nil
                             (When (And (Not (Alist-Get 'Resources
                                                        (Kubed--Alist "Namespaces" Ctx Nil)))
                                        (Not (Process-Live-P
                                              (Alist-Get 'Process
                                                         (Kubed--Alist "Namespaces" Ctx Nil)))))
                                   (Puthash Ctx T Kubed-Ext--Namespace-Prefetch-Contexts)
                                   (Kubed-Update "Namespaces" Ctx Nil))
                             (Error Nil)))))

(Defun Kubed-Ext--Ensure-Namespaces-Ready (Context)
  "Ensure Namespace List For Context Is Available, Waiting If Necessary.
If A Fetch Is In Progress, Wait For It.  If No Fetch Has Started And No
Cached Data Exists, Start One And Wait.  If Data Is Already Cached,
Return Immediately."
  (Let ((Ctx (Or Context (Kubed-Local-Context))))
       (When Ctx
             (Unless (Alist-Get 'Resources (Kubed--Alist "Namespaces" Ctx Nil))
                     (Let ((Proc (Alist-Get 'Process
                                            (Kubed--Alist "Namespaces" Ctx Nil))))
                          ;; No Live Fetch In Progress — Start One.
                          (Unless (And Proc (Process-Live-P Proc))
                                  (Condition-Case Nil
                                                  (Kubed-Update "Namespaces" Ctx Nil)
                                                  (Error Nil))
                                  (Setq Proc (Alist-Get 'Process
                                                        (Kubed--Alist "Namespaces" Ctx Nil))))
                          ;; Wait For The Running Fetch To Finish.
                          (When (And Proc (Process-Live-P Proc))
                                (Message "Loading Namespaces For `%S'..." Ctx)
                                (While (Process-Live-P Proc)
                                       (Accept-Process-Output Proc 1))))))))

(Defun Kubed-Ext--Prefetch-Namespaces-Hook ()
  "Prefetch Namespaces When A Kubed List Buffer Is Created."
  (When (Bound-And-True-P Kubed-List-Context)
        (Kubed-Ext-Prefetch-Namespaces Kubed-List-Context)))

(Add-Hook 'Kubed-List-Mode-Hook #'Kubed-Ext--Prefetch-Namespaces-Hook)

(Defun Kubed-Ext--Prefetch-After-Use-Context (&Rest _)
  "Prefetch Namespaces After `Kubed-Use-Context' Change The Default Context."
  (When (And (Consp Kubed-Default-Context-And-Namespace)
             (Car Kubed-Default-Context-And-Namespace))
        ;; Clear Prefetch Flag So The New Context Gets A Fresh Fetch.
        (Remhash (Car Kubed-Default-Context-And-Namespace)
                 Kubed-Ext--Namespace-Prefetch-Contexts)
        (Kubed-Ext-Prefetch-Namespaces
         (Car Kubed-Default-Context-And-Namespace))))

(Advice-Add 'Kubed-Use-Context :After #'Kubed-Ext--Prefetch-After-Use-Context)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0e.  Auto-Refresh Visible List Buffers
;;; ═══════════════════════════════════════════════════════════════

(Defcustom Kubed-Ext-Auto-Refresh-Interval '(15 . 30)
  "Interval Range In Seconds For Auto-Refreshing Visible Kubed List Buffers.

A Cons Cell (Min . Max).  Each Cycle Picks A Random Delay Between Min
And Max Seconds Before The Next Refresh.  Set To Nil To Disable Even
When The Mode Is On."
  :Type '(Choice (Const :Tag "Disabled" Nil)
                 (Cons :Tag "Interval Range (Seconds)"
                   (Natnum :Tag "Minimum")
                   (Natnum :Tag "Maximum")))
  :Group 'Kubed-Ext)

(Defvar Kubed-Ext--Auto-Refresh-Timer Nil
  "Timer For Auto-Refreshing Visible Kubed List Buffers.")

(Defun Kubed-Ext--Auto-Refresh-Random-Delay ()
  "Return A Random Delay In Seconds, Or Nil If Auto-Refresh Is Disabled."
  (When (Consp Kubed-Ext-Auto-Refresh-Interval)
        (Let ((Lo (Car Kubed-Ext-Auto-Refresh-Interval))
              (Hi (Cdr Kubed-Ext-Auto-Refresh-Interval)))
             (+ Lo (Random (Max 1 (1+ (- Hi Lo))))))))


(Defun Kubed-Ext--Auto-Refresh-Schedule ()
  "Schedule The Next Auto-Refresh Tick With A Random Delay."
  (Kubed-Ext--Auto-Refresh-Cancel-Timer)
  (When-Let ((Delay (Kubed-Ext--Auto-Refresh-Random-Delay)))
            (Setq Kubed-Ext--Auto-Refresh-Timer
                  (Run-With-Timer Delay Nil #'Kubed-Ext--Auto-Refresh-Tick))))

(Defun Kubed-Ext--Auto-Refresh-Cancel-Timer ()
  "Cancel The Pending Auto-Refresh Timer If Any."
  (When (Timerp Kubed-Ext--Auto-Refresh-Timer)
        (Cancel-Timer Kubed-Ext--Auto-Refresh-Timer)
        (Setq Kubed-Ext--Auto-Refresh-Timer Nil)))

;;;###Autoload
(Define-Minor-Mode Kubed-Ext-Auto-Refresh-Mode
  "Periodically Refresh Visible Kubed List Buffers.

Refresh Interval Is Randomized Between The Bounds In
`Kubed-Ext-Auto-Refresh-Interval' (Default 15-30 S).  Only Buffers
Currently Shown In A Window Are Refreshed, And A Refresh Is Skipped
When An Update Is Already In Progress Or The Minibuffer Is Active."
  :Global T
  :Lighter " Kref"
  :Group 'Kubed-Ext
  (If Kubed-Ext-Auto-Refresh-Mode
      (Progn
       (Kubed-Ext--Auto-Refresh-Schedule)
       (Message "Kubed Auto-Refresh Enabled (%D-%Ds)."
                (Car Kubed-Ext-Auto-Refresh-Interval)
                (Cdr Kubed-Ext-Auto-Refresh-Interval)))
      (Kubed-Ext--Auto-Refresh-Cancel-Timer)
      (Message "Kubed Auto-Refresh Disabled.")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 0f.  Upstream Bug Fix — Kubed-Update Column Offset Overrun
;;; ═══════════════════════════════════════════════════════════════
;;
;; `Kubed-Update' Calculates Column Offsets From The Header Line And
;; Applies Them To Every Data Row.  When A Trailing Column Is Absent
;; (E.G. A Pod With No Ip, No Node, Or No Deletion Timestamp) The Data
;; Row Is Shorter Than The Header, So The Fixed Offset Exceeds The Line
;; Length And Triggers "Args Out Of Range".
;;
;; The Fix Wraps The Process Sentinel That Kubed-Update Registers So
;; That Every Buffer-Substring Call Is Clamped To Pos-Eol.

(Defun Kubed-Ext--Safe-Update (Orig-Fn Type Context &Optional Namespace)
  "Around Advice For `Kubed-Update': Clamp `Buffer-Substring' In The Sentinel.
Orig-Fn Type Context/Namespace."
  (Cl-Letf* ((Orig-Mp (Symbol-Function 'Make-Process))
             ((Symbol-Function 'Make-Process)
              (Lambda (&Rest Args)
                      (Let* ((Orig-Sentinel (Plist-Get Args :Sentinel))
                             (Safe-Sentinel
                              (Lambda (Proc Status)
                                      (Cl-Letf* ((Orig-Bs (Symbol-Function 'Buffer-Substring))
                                                 ((Symbol-Function 'Buffer-Substring)
                                                  (Lambda (Start End)
                                                          (With-Current-Buffer (Process-Buffer Proc)
                                                                               (Let ((Limit (Point-Max)))
                                                                                    (Funcall Orig-Bs
                                                                                             (Min Start Limit)
                                                                                             (Min End   Limit)))))))
                                                (Funcall Orig-Sentinel Proc Status)))))
                            (Apply Orig-Mp (Plist-Put Args :Sentinel Safe-Sentinel))))))
            (Funcall Orig-Fn Type Context Namespace)))

(Advice-Add 'Kubed-Update :Around #'Kubed-Ext--Safe-Update)

;;; ═══════════════════════════════════════════════════════════════
;;; § 1.  Crd Auto-Column Discovery
;;; ═══════════════════════════════════════════════════════════════

(Defvar Kubed-Ext-Enriched-Resources (Make-Hash-Table :Test 'Equal)
  "Track Resource Type+Context Pairs Already Enriched With Crd Columns.")

(Defun Kubed-Ext--Crd-Printer-Columns (Resource-Plural Context)
  "Fetch Additionalprintercolumns From Crd For Resource-Plural In Context."
  (Condition-Case Err
                  (Let* ((Ctx-Args (When Context (List "--Context" Context)))
                         (Crd-Name
                          (With-Temp-Buffer
                           (When (Zerop (Apply #'Call-Process
                                               Kubed-Kubectl-Program Nil '(T Nil) Nil
                                               "Api-Resources" "--No-Headers" "-O" "Name"
                                               Ctx-Args))
                                 (Goto-Char (Point-Min))
                                 (When (Re-Search-Forward
                                        (Concat "^" (Regexp-Quote Resource-Plural) "\\.")
                                        Nil T)
                                       (String-Trim (Thing-At-Point 'Line T)))))))
                        (When Crd-Name
                              (Let ((Json-Str
                                     (With-Temp-Buffer
                                      (When (Zerop
                                             (Apply #'Call-Process
                                                    Kubed-Kubectl-Program Nil '(T Nil) Nil
                                                    "Get" "Crd" Crd-Name "-O"
                                                    "Jsonpath={.Spec.Versions[0].Additionalprintercolumns}"
                                                    Ctx-Args))
                                            (String-Trim (Buffer-String))))))
                                   (When (And Json-Str
                                              (Not (String-Empty-P Json-Str))
                                              (Not (String= Json-Str "Null")))
                                         (Let ((All-Cols (Json-Parse-String
                                                          Json-Str
                                                          :Array-Type 'List :Object-Type 'Alist)))
                                              (Seq-Remove
                                               (Lambda (C) (String= (Alist-Get 'Name C) "Age"))
                                               All-Cols))))))
                  (Error (Message "Kubed-Ext Crd Error: %S" Err) Nil)))

(Defun Kubed-Ext-Enrich-Columns (Resource-Plural Context)
  "Inject Crd Printer Columns Into Kubed For Resource-Plural In Context."
  (When-Let ((Cols (Kubed-Ext--Crd-Printer-Columns Resource-Plural Context)))
            (Setf (Alist-Get Resource-Plural Kubed--Columns Nil Nil #'String=)
                  (Cons '("Name:.Metadata.Name")
                        (Mapcar
                         (Lambda (C)
                                 (Cons (Format "%S:%S"
                                               (Upcase (Replace-Regexp-In-String
                                                        "[ /]" "_" (Alist-Get 'Name C)))
                                               (Alist-Get 'Jsonpath C))
                                       Nil))
                         Cols)))
            (Let ((Fmt-Var (Intern (Format "Kubed-%S-Columns" Resource-Plural))))
                 (When (Boundp Fmt-Var)
                       (Set Fmt-Var
                            (Mapcar (Lambda (C)
                                            (List (Alist-Get 'Name C)
                                                  (Max (+ 2 (Length (Alist-Get 'Name C))) 14)
                                                  T))
                                    Cols))))
            T))

(Defun Kubed-Ext--Maybe-Enrich-Columns (&Rest _)
  "Before-Advice: Lazily Discover Crd Columns On First List."
  (When-Let ((Type Kubed-List-Type))
            (Let ((Key (Cons Type (Or Kubed-List-Context ""))))
                 (Unless (Gethash Key Kubed-Ext-Enriched-Resources)
                         (Puthash Key T Kubed-Ext-Enriched-Resources)
                         (Let ((Existing (Alist-Get Type Kubed--Columns Nil Nil #'String=)))
                              (When (Or (Null Existing) (<= (Length Existing) 1))
                                    (When (Kubed-Ext-Enrich-Columns Type Kubed-List-Context)
                                          (Let ((Fmt-Var (Intern (Format "Kubed-%S-Columns" Type))))
                                               (When (And (Boundp Fmt-Var) (Symbol-Value Fmt-Var))
                                                     (Setq Tabulated-List-Format
                                                           (Apply #'Vector
                                                                  (Cons Kubed-Name-Column
                                                                        (Symbol-Value Fmt-Var))))
                                                     (Tabulated-List-Init-Header))))))))))

(Advice-Add 'Kubed-List-Update :Before #'Kubed-Ext--Maybe-Enrich-Columns)

(Defun Kubed-Ext-Refresh-Crd-Columns ()
  "Force Re-Discovery Of Crd Columns After Switching Context."
  (Interactive)
  (Clrhash Kubed-Ext-Enriched-Resources)
  (Message "Crd Column Cache Cleared."))

;;; ═══════════════════════════════════════════════════════════════
;;; § 2.  Port-Forward With Auto-Complete
;;; ═══════════════════════════════════════════════════════════════

(Defconst Kubed-Ext--Dq (String 34)
  "Double-Quote Character As A One-Char String.")

(Defconst Kubed-Ext--Jq-Tab
  (Concat "{" Kubed-Ext--Dq "\\T" Kubed-Ext--Dq "}")
  "Kubectl Jsonpath Tab Separator Token.")

(Defconst Kubed-Ext--Jq-Nl
  (Concat "{" Kubed-Ext--Dq "\\N" Kubed-Ext--Dq "}")
  "Kubectl Jsonpath Newline Separator Token.")

(Defun Kubed-Ext--Json-Patch (Key Value)
  "Return Json String {Spec:{Key:Value}} For Kubectl Patch."
  (Let ((Q Kubed-Ext--Dq))
       (Concat "{" Q "Spec" Q ":{" Q Key Q ":" Value "}}")))

(Defun Kubed-Ext-Resource-Container-Ports (Type Name &Optional Context Namespace)
  "Discover Container Ports For Resource Name Of Type In Context And Namespace."
  (Let ((Jsonpath
         (Pcase Type
                ((Or "Pod" "Pods")
                 ".Spec.Containers[*].Ports[*]")
                ((Or "Deployment" "Deployments"
                     "Statefulset" "Statefulsets"
                     "Daemonset" "Daemonsets"
                     "Replicaset" "Replicasets")
                 ".Spec.Template.Spec.Containers[*].Ports[*]")
                (_ Nil))))
       (When Jsonpath
             (Let ((Output
                    (With-Temp-Buffer
                     (Apply #'Call-Process
                            Kubed-Kubectl-Program Nil '(T Nil) Nil
                            "Get" Type Name "-O"
                            (Concat "Jsonpath={Range " Jsonpath "}"
                                    "{.Containerport}" Kubed-Ext--Jq-Tab
                                    "{.Name}" Kubed-Ext--Jq-Tab
                                    "{.Protocol}" Kubed-Ext--Jq-Nl
                                    "{End}")
                            (Append
                             (When Namespace (List "-N" Namespace))
                             (When Context (List "--Context" Context))))
                     (Buffer-String))))
                  (Delq Nil
                        (Mapcar (Lambda (Line)
                                        (Let ((Parts (Split-String Line "\T")))
                                             (When (And (Car Parts)
                                                        (Not (String-Empty-P (Car Parts))))
                                                   (List (String-To-Number (Nth 0 Parts))
                                                         (Or (Nth 1 Parts) "")
                                                         (Or (Nth 2 Parts) "Tcp")))))
                                (Split-String Output "\N" T)))))))

(Defun Kubed-Ext-Service-Ports (Service &Optional Context Namespace)
  "Discover Ports For Service In Context And Namespace."
  (Let ((Output
         (With-Temp-Buffer
          (Apply #'Call-Process
                 Kubed-Kubectl-Program Nil '(T Nil) Nil
                 "Get" "Service" Service "-O"
                 (Concat "Jsonpath={Range .Spec.Ports[*]}"
                         "{.Port}" Kubed-Ext--Jq-Tab
                         "{.Targetport}" Kubed-Ext--Jq-Tab
                         "{.Name}" Kubed-Ext--Jq-Tab
                         "{.Protocol}" Kubed-Ext--Jq-Nl
                         "{End}")
                 (Append
                  (When Namespace (List "-N" Namespace))
                  (When Context (List "--Context" Context))))
          (Buffer-String))))
       (Delq Nil
             (Mapcar (Lambda (Line)
                             (Let ((Parts (Split-String Line "\T")))
                                  (When (And (>= (Length Parts) 4)
                                             (Not (String-Empty-P (Car Parts))))
                                        (List (String-To-Number (Nth 0 Parts))
                                              (Nth 1 Parts)
                                              (Nth 2 Parts)
                                              (Nth 3 Parts)))))
                     (Split-String Output "\N" T)))))

(Defun Kubed-Ext--Format-Port-Candidate (Port-Num Name Protocol)
  "Format A Port Completion Candidate With Port-Num, Name, And Protocol."
  (Format "%D%S%S" Port-Num
          (If (Or (Null Name) (String-Empty-P Name)) ""
              (Format " (%S)" Name))
          (If (Or (Null Protocol) (String= Protocol "Tcp")) ""
              (Format " [%S]" Protocol))))

(Defun Kubed-Ext-Read-Resource-Port (Type Name Prompt &Optional Context Namespace)
  "Read Port From Container Ports Of Type/Name With Prompt In Context/Namespace."
  (Let* ((Ports (Kubed-Ext-Resource-Container-Ports
                 Type Name Context Namespace))
         (Candidates
          (Mapcar (Lambda (P)
                          (Kubed-Ext--Format-Port-Candidate
                           (Nth 0 P) (Nth 1 P) (Nth 2 P)))
                  Ports)))
        (String-To-Number
         (If Candidates
             (Completing-Read (Format-Prompt Prompt Nil) Candidates Nil Nil)
             (Read-String (Format-Prompt Prompt Nil))))))

(Defun Kubed-Ext-Read-Service-Port (Service Prompt &Optional Context Namespace)
  "Read Port From Service Ports With Prompt In Context And Namespace."
  (Let* ((Ports (Kubed-Ext-Service-Ports Service Context Namespace))
         (Candidates
          (Mapcar (Lambda (P)
                          (Kubed-Ext--Format-Port-Candidate
                           (Nth 0 P) (Nth 2 P) (Nth 3 P)))
                  Ports)))
        (String-To-Number
         (If Candidates
             (Completing-Read (Format-Prompt Prompt Nil) Candidates Nil Nil)
             (Read-String (Format-Prompt Prompt Nil))))))

(Defun Kubed-Ext-Forward-Port (Type Name Local-Port Remote-Port Context Namespace)
  "Forward Local-Port To Remote-Port Of Type/Name In Context/Namespace."
  (Let ((Res-Spec (Pcase Type
                         ((Or "Pod" "Pods") Name)
                         ((Or "Service" "Services") (Concat "Svc/" Name))
                         (_ (Concat (Replace-Regexp-In-String "S\'" "" Type)
                                    "/" Name))))
        (Desc (Format "%S/%S %D:%D In %S[%S]"
                      Type Name Local-Port Remote-Port
                      (Or Namespace "Default") (Or Context "Current"))))
       (Message "Forwarding Localhost:%D -> %S:%D..."
                Local-Port Res-Spec Remote-Port)
       (Push (Cons Desc
                   (Apply #'Start-Process
                          "*Kubed-Port-Forward*" Nil
                          Kubed-Kubectl-Program "Port-Forward"
                          Res-Spec (Format "%D:%D" Local-Port Remote-Port)
                          (Append
                           (When Namespace (List "-N" Namespace))
                           (When Context (List "--Context" Context)))))
             Kubed-Port-Forward-Process-Alist)
       (Kubed-Ext-Refresh-Pf-Markers-In-Visible-Buffers)))

(Defun Kubed-Ext-Forward-Port-To-Pod
  (Pod Local-Port Remote-Port Context Namespace)
  "Forward Local-Port To Remote-Port Of Pod Pod In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg))
          (P (Kubed-Read-Pod "Forward Port To Pod" Nil Nil C N))
          (R (Kubed-Ext-Read-Resource-Port "Pods" P "Remote Port" C N))
          (L (Read-Number (Format "Local Port (Remote=%D): " R) R)))
         (List P L R C N)))
  (Kubed-Ext-Forward-Port "Pods" Pod Local-Port Remote-Port Context Namespace))

(Defun Kubed-Ext-Pods-Forward-Port (Click)
  "Forward Local Network Port To Remote Port Of Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((R (Kubed-Ext-Read-Resource-Port
                     "Pods" Pod "Remote Port"
                     Kubed-List-Context Kubed-List-Namespace))
                 (L (Read-Number (Format "Local Port (Remote=%D): " R) R)))
                (Kubed-Ext-Forward-Port "Pods" Pod L R
                                        Kubed-List-Context Kubed-List-Namespace))
          (User-Error "No Kubernetes Pod At Point")))

(Defun Kubed-Ext-Forward-Port-To-Service
  (Service Local-Port Remote-Port Context Namespace)
  "Forward Local-Port To Remote-Port Of Service Service In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg))
          (S (Kubed-Read-Service "Forward Port To Service" Nil Nil C N))
          (R (Kubed-Ext-Read-Service-Port S "Remote Port" C N))
          (L (Read-Number (Format "Local Port (Remote=%D): " R) R)))
         (List S L R C N)))
  (Kubed-Ext-Forward-Port "Services" Service Local-Port Remote-Port
                          Context Namespace))

(Defun Kubed-Ext-Services-Forward-Port (Click)
  "Forward Local Port To Kubernetes Service At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Services-Mode)
  (If-Let ((Service (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((R (Kubed-Ext-Read-Service-Port
                     Service "Remote Port"
                     Kubed-List-Context Kubed-List-Namespace))
                 (L (Read-Number (Format "Local Port (Remote=%D): " R) R)))
                (Kubed-Ext-Forward-Port "Services" Service L R
                                        Kubed-List-Context Kubed-List-Namespace))
          (User-Error "No Kubernetes Service At Point")))

(Defun Kubed-Ext-Forward-Port-To-Deployment
  (Deployment Local-Port Remote-Port Context Namespace)
  "Forward Local-Port To Remote-Port Of Deployment In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg))
          (D (Kubed-Read-Deployment "Forward Port To Deployment" Nil Nil C N))
          (R (Kubed-Ext-Read-Resource-Port "Deployments" D "Remote Port" C N))
          (L (Read-Number (Format "Local Port (Remote=%D): " R) R)))
         (List D L R C N)))
  (Kubed-Ext-Forward-Port "Deployments" Deployment Local-Port Remote-Port
                          Context Namespace))

(Defun Kubed-Ext-Deployments-Forward-Port (Click)
  "Forward Local Port To Kubernetes Deployment At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Deployments-Mode)
  (If-Let ((Deployment (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((R (Kubed-Ext-Read-Resource-Port
                     "Deployments" Deployment "Remote Port"
                     Kubed-List-Context Kubed-List-Namespace))
                 (L (Read-Number (Format "Local Port (Remote=%D): " R) R)))
                (Kubed-Ext-Forward-Port "Deployments" Deployment L R
                                        Kubed-List-Context Kubed-List-Namespace))
          (User-Error "No Kubernetes Deployment At Point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 3.  Port-Forward Markers + Port-Forward List Buffer
;;; ═══════════════════════════════════════════════════════════════

(Defface Kubed-Ext-Port-Forward-Face
  '((((Background Dark))  :Background "#1a2a1a")
    (((Background Light)) :Background "#E8f5e8"))
  "Face For Resources With Active Port-Forwards.")

(Defun Kubed-Ext-Has-Port-Forward-P (Name Context Namespace)
  "Return Non-Nil If Name Has Active Port-Forward In Namespace And Context."
  (Seq-Some (Lambda (Pair)
                    (And (Process-Live-P (Cdr Pair))
                         (String-Match-P (Regexp-Quote Name) (Car Pair))
                         (Or (Null Namespace)
                             (String-Match-P (Regexp-Quote Namespace) (Car Pair)))
                         (Or (Null Context)
                             (String-Match-P (Regexp-Quote Context) (Car Pair)))))
            Kubed-Port-Forward-Process-Alist))

(Defun Kubed-Ext-Mark-Port-Forwards (&Rest _)
  "Highlight List Rows With Active Port-Forwards."
  (When (Derived-Mode-P 'Kubed-List-Mode)
        (Remove-Overlays (Point-Min) (Point-Max) 'Kubed-Pf T)
        (When (Kubed-Port-Forward-Process-Alist)
              (Save-Excursion
               (Goto-Char (Point-Min))
               (While (Not (Eobp))
                      (When-Let ((Id (Tabulated-List-Get-Id)))
                                (When (Kubed-Ext-Has-Port-Forward-P
                                       Id Kubed-List-Context Kubed-List-Namespace)
                                      (Let ((Ov (Make-Overlay (Line-Beginning-Position)
                                                              (Line-End-Position))))
                                           (Overlay-Put Ov 'Kubed-Pf T)
                                           (Overlay-Put Ov 'Face 'Kubed-Ext-Port-Forward-Face)
                                           (Overlay-Put Ov 'Help-Echo "Port-Forwarding Active"))))
                      (Forward-Line))))))

(Advice-Add 'Kubed-List-Revert :After #'Kubed-Ext-Mark-Port-Forwards)

(Defun Kubed-Ext-Refresh-Pf-Markers-In-Visible-Buffers (&Rest _)
  "Refresh Port-Forward Markers In All Visible Kubed List Buffers."
  (Dolist (Win (Window-List))
          (With-Current-Buffer (Window-Buffer Win)
                               (When (Derived-Mode-P 'Kubed-List-Mode)
                                     (Kubed-Ext-Mark-Port-Forwards)))))

(Advice-Add 'Kubed-Stop-Port-Forward :After
            #'Kubed-Ext-Refresh-Pf-Markers-In-Visible-Buffers)

(Defun Kubed-Ext-Parse-Pf-Descriptor (Desc)
  "Parse Port-Forward Descriptor Desc Into List.
Desc Format: Type/Name Local:Remote In Namespace[Context]."
  (If (String-Match
       "$.+?$ $[0-9]+:[0-9]+$ In $.+?$$$$.+?$$$" Desc)
      (List (Match-String 1 Desc) (Match-String 2 Desc)
            (Match-String 3 Desc) (Match-String 4 Desc))
      (List Desc "" "" "")))

(Defun Kubed-Ext-Port-Forward-Entries ()
  "Return Entries For Port-Forward List Buffer."
  (Mapcar (Lambda (Pair)
                  (Let* ((Desc (Car Pair))
                         (Parsed (Kubed-Ext-Parse-Pf-Descriptor Desc))
                         (Status (If (Process-Live-P (Cdr Pair))
                                     (Propertize "Active" 'Face 'Success)
                                     (Propertize "Dead" 'Face 'Error))))
                        (List Desc (Vector (Nth 0 Parsed) (Nth 1 Parsed)
                                           (Nth 2 Parsed) (Nth 3 Parsed) Status))))
          (Kubed-Port-Forward-Process-Alist)))

(Defun Kubed-Ext-Port-Forwards-Stop (Click)
  "Stop Port-Forward At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Ext-Port-Forwards-Mode)
  (If-Let ((Desc (Kubed-Ext--Resource-At-Event Click)))
          (When (Y-Or-N-P (Format "Stop Port-Forward: %S?" Desc))
                (Kubed-Stop-Port-Forward Desc)
                (Tabulated-List-Print T))
          (User-Error "No Port-Forward At Point")))

(Defun Kubed-Ext-Port-Forwards-Stop-All ()
  "Stop All Active Port-Forwards."
  (Interactive Nil Kubed-Ext-Port-Forwards-Mode)
  (When (Y-Or-N-P (Format "Stop All %D Port-Forwards?"
                          (Length Kubed-Port-Forward-Process-Alist)))
        (Dolist (Pair (Kubed-Port-Forward-Process-Alist))
                (When (Process-Live-P (Cdr Pair))
                      (Delete-Process (Cdr Pair))))
        (Setq Kubed-Port-Forward-Process-Alist Nil)
        (Tabulated-List-Print T)
        (Kubed-Ext-Refresh-Pf-Markers-In-Visible-Buffers)
        (Message "All Port-Forwards Stopped.")))

(Defun Kubed-Ext-Port-Forwards-Refresh ()
  "Refresh Port-Forward List."
  (Interactive Nil Kubed-Ext-Port-Forwards-Mode)
  (Kubed-Port-Forward-Process-Alist)
  (Tabulated-List-Print T))

(Define-Derived-Mode Kubed-Ext-Port-Forwards-Mode Tabulated-List-Mode
  "Kubed Pf"
  "Major Mode For Listing Active Kubernetes Port-Forwards."
  (Setq Tabulated-List-Format
        [("Resource" 32 T) ("Ports" 14 T)
         ("Namespace" 20 T) ("Context" 24 T) ("Status" 8 T)]
        Tabulated-List-Padding 2
        Tabulated-List-Entries #'Kubed-Ext-Port-Forward-Entries)
  (Tabulated-List-Init-Header))

(Keymap-Set Kubed-Ext-Port-Forwards-Mode-Map "D"
            #'Kubed-Ext-Port-Forwards-Stop)
(Keymap-Set Kubed-Ext-Port-Forwards-Mode-Map "K"
            #'Kubed-Ext-Port-Forwards-Stop-All)
(Keymap-Set Kubed-Ext-Port-Forwards-Mode-Map "G"
            #'Kubed-Ext-Port-Forwards-Refresh)
(Keymap-Set Kubed-Ext-Port-Forwards-Mode-Map "Q" #'Quit-Window)

(Defun Kubed-Ext-List-Port-Forwards ()
  "Display Buffer Listing All Active Port-Forwards."
  (Interactive)
  (Let ((Buf (Get-Buffer-Create "*Kubed Port Forwards*")))
       (With-Current-Buffer Buf
                            (Kubed-Ext-Port-Forwards-Mode)
                            (Tabulated-List-Print T))
       (Switch-To-Buffer Buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 3a.  Error Overlay Header
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext--Set-Header-Error (Msg)
  "Set Error Msg As An Overlay At The Top Of The Current Buffer."
  (Kubed-Ext--Clear-Header-Error)
  (When (And Msg (Not (String-Empty-P (String-Trim Msg))))
        (Setq Kubed-Ext--Header-Error (String-Trim Msg))
        (When (> (Buffer-Size) 0)
              (Let ((Ov (Make-Overlay (Point-Min) (Point-Min))))
                   (Overlay-Put Ov 'Before-String
                                (Propertize
                                 (Format "⚠ %S  [Type `$' For Process Buffer]\N"
                                         Kubed-Ext--Header-Error)
                                 'Face 'Error))
                   (Overlay-Put Ov 'Kubed-Ext-Error T)
                   (Overlay-Put Ov 'Priority 100)
                   (Setq Kubed-Ext--Error-Overlay Ov)))))

(Defun Kubed-Ext--Clear-Header-Error ()
  "Clear Any Error Overlay From The Current Buffer."
  (When Kubed-Ext--Error-Overlay
        (Delete-Overlay Kubed-Ext--Error-Overlay)
        (Setq Kubed-Ext--Error-Overlay Nil))
  (Setq Kubed-Ext--Header-Error Nil))

(Defun Kubed-Ext--Reapply-Overlays (&Rest _)
  "Re-Apply Error Overlay And Filter Highlight After Buffer Revert."
  (When (Derived-Mode-P 'Kubed-List-Mode)
        (When Kubed-Ext--Header-Error
              (Let ((Msg Kubed-Ext--Header-Error))
                   (Setq Kubed-Ext--Error-Overlay Nil)
                   (Kubed-Ext--Set-Header-Error Msg)))
        (Kubed-Ext--Apply-Filter-Highlights)))

(Advice-Add 'Kubed-List-Revert :After #'Kubed-Ext--Reapply-Overlays)

(Defun Kubed-Ext--Update-Error-Advice (Orig-Fn Type Context &Optional Namespace)
  "Around Advice For `Kubed-Update' To Capture Errors As Header Overlays.
Orig-Fn Is The Original Function, Type Context Namespace Are Its Args."
  (Funcall Orig-Fn Type Context Namespace)
  ;; Wrap The Sentinel Of The Process That Was Just Created.
  (When-Let ((Proc (Alist-Get 'Process
                              (Kubed--Alist Type Context Namespace))))
            (When (Process-Live-P Proc)
                  (Let ((Orig-Sentinel (Process-Sentinel Proc))
                        (Err-Buf-Name (Format " *Kubed-Get-%S-Stderr*" Type)))
                       (Set-Process-Sentinel
                        Proc
                        (Lambda (P Status)
                                (Funcall Orig-Sentinel P Status)
                                (Dolist (Buf (Buffer-List))
                                        (When (Buffer-Live-P Buf)
                                              (With-Current-Buffer Buf
                                                                   (When (And (Derived-Mode-P 'Kubed-List-Mode)
                                                                              (Equal Kubed-List-Type Type)
                                                                              (Equal Kubed-List-Context Context)
                                                                              (Equal Kubed-List-Namespace Namespace))
                                                                         (If (String= Status "Exited Abnormally With Code 1\N")
                                                                             (Let ((Err-Buf (Get-Buffer Err-Buf-Name)))
                                                                                  (Kubed-Ext--Set-Header-Error
                                                                                   (If (And Err-Buf (Buffer-Live-P Err-Buf))
                                                                                       (With-Current-Buffer Err-Buf
                                                                                                            (String-Trim (Buffer-String)))
                                                                                       "Kubectl Command Failed")))
                                                                             (Kubed-Ext--Clear-Header-Error))))))))))))

(Advice-Add 'Kubed-Update :Around #'Kubed-Ext--Update-Error-Advice)

;;; ═══════════════════════════════════════════════════════════════
;;; § 3b.  Mark / Unmark System (Kubel-Style)
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Marked-Items ()
  "Return List Of Resource Ids Marked With `*' In Current Buffer."
  (Let (Items)
       (Save-Excursion
        (Goto-Char (Point-Min))
        (While (Not (Eobp))
               (When (Eq (Char-After) ?*)
                     (When-Let ((Id (Tabulated-List-Get-Id)))
                               (Push Id Items)))
               (Forward-Line)))
       (Nreverse Items)))

(Defun Kubed-Ext-Mark-Item ()
  "Mark The Resource At Point With `*'."
  (Interactive Nil Kubed-List-Mode)
  (Tabulated-List-Put-Tag
   (Propertize "*" 'Face 'Dired-Marked 'Help-Echo "Selected") T))

(Defun Kubed-Ext-Unmark-Item ()
  "Unmark The Resource At Point."
  (Interactive Nil Kubed-List-Mode)
  (Tabulated-List-Put-Tag " " T))

(Defun Kubed-Ext-Mark-All ()
  "Mark All Visible Resources In The Current Buffer."
  (Interactive Nil Kubed-List-Mode)
  (Save-Excursion
   (Goto-Char (Point-Min))
   (While (Not (Eobp))
          (When (Tabulated-List-Get-Id)
                (Tabulated-List-Put-Tag
                 (Propertize "*" 'Face 'Dired-Marked)))
          (Forward-Line)))
  (Message "Marked All Items."))

(Defun Kubed-Ext-Unmark-All ()
  "Unmark All Resources In The Current Buffer."
  (Interactive Nil Kubed-List-Mode)
  (Save-Excursion
   (Goto-Char (Point-Min))
   (While (Not (Eobp))
          (When (Memq (Char-After) '(?* ?D))
                (Tabulated-List-Put-Tag " "))
          (Forward-Line)))
  (Message "Unmarked All Items."))

;;; ── Mark Persistence Across Refreshes ──────────────────────────────

(Defvar-Local Kubed-Ext--Persisted-Mark-Ids Nil
  "List Of Resource Ids That Were Marked Before The Last Buffer Refresh.
Populated By `Kubed-Ext--Persist-Marks-Before-Revert' Immediately
Before `Kubed-List-Revert' Wipes The Buffer, And Consumed By
`Kubed-Ext--Restore-Persisted-Marks' After The Reprint Completes.
Declared Permanent-Local So It Survives `Kill-All-Local-Variables'.")

(Put 'Kubed-Ext--Persisted-Mark-Ids 'Permanent-Local T)

(Defun Kubed-Ext--Persist-Marks-Before-Revert (&Rest _)
  "Save The Ids Of All Marked Rows Before `Kubed-List-Revert' Wipes Them.
Installed As `:Before' Advice On `Kubed-List-Revert'."
  (When (Derived-Mode-P 'Kubed-List-Mode)
        (Setq Kubed-Ext--Persisted-Mark-Ids (Kubed-Ext-Marked-Items))))

(Defun Kubed-Ext--Restore-Persisted-Marks (&Rest _)
  "Re-Mark Any Rows Whose Ids Were Saved Before The Last Refresh.
Installed As `:After' Advice On `Kubed-List-Revert'.
Walks Every Visible Row; Any Whose Tabulated-List Id Appears In
`Kubed-Ext--Persisted-Mark-Ids' Gets A Fresh `*' Tag With The
Correct Face.  Rows That No Longer Exist After The Refresh Are
Silently Skipped."
  (When (And (Derived-Mode-P 'Kubed-List-Mode)
             Kubed-Ext--Persisted-Mark-Ids)
        (Let ((Ids Kubed-Ext--Persisted-Mark-Ids))
             (Save-Excursion
              (Goto-Char (Point-Min))
              (While (Not (Eobp))
                     (When-Let ((Id (Tabulated-List-Get-Id)))
                               (When (Member Id Ids)
                                     (Tabulated-List-Put-Tag
                                      (Propertize "*"
                                                  'Face       'Dired-Marked
                                                  'Help-Echo  "Selected"))))
                     (Forward-Line))))))

(Advice-Add 'Kubed-List-Revert :Before
            #'Kubed-Ext--Persist-Marks-Before-Revert)
(Advice-Add 'Kubed-List-Revert :After
            #'Kubed-Ext--Restore-Persisted-Marks)

;;; ═══════════════════════════════════════════════════════════════
;;; § 3c.  Substring Filter With Highlight (Kubel-Style)
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext--Apply-Filter-Highlights ()
  "Apply Filter Highlighting To Current Buffer.
Matching Rows Stay Normal; Non-Matching Rows Get `Shadow' Face."
  (When (Derived-Mode-P 'Kubed-List-Mode)
        (Remove-Overlays (Point-Min) (Point-Max) 'Kubed-Ext-Filter T)
        (When (And (Bound-And-True-P Kubed-Ext-Resource-Filter)
                   (Not (String-Empty-P Kubed-Ext-Resource-Filter)))
              (Save-Excursion
               (Goto-Char (Point-Min))
               (While (Not (Eobp))
                      (When-Let ((Entry (Tabulated-List-Get-Entry)))
                                (Let ((Line-Text (Mapconcat #'Identity
                                                            (Append Entry Nil) " ")))
                                     (Unless (String-Match-P Kubed-Ext-Resource-Filter Line-Text)
                                             (Let ((Ov (Make-Overlay (Line-Beginning-Position)
                                                                     (Line-End-Position))))
                                                  (Overlay-Put Ov 'Kubed-Ext-Filter T)
                                                  (Overlay-Put Ov 'Face 'Shadow)))))
                      (Forward-Line))))))

(Defun Kubed-Ext-Set-Filter (Filter)
  "Set Substring Filter For Highlighting Resources.
Empty String Clears The Filter."
  (Interactive
   (List (Read-String (Format-Prompt "Filter" "Clear")
                      Kubed-Ext-Resource-Filter))
   Kubed-List-Mode)
  (Setq-Local Kubed-Ext-Resource-Filter (Or Filter ""))
  (Kubed-Ext--Apply-Filter-Highlights)
  (Force-Mode-Line-Update)
  (If (String-Empty-P Kubed-Ext-Resource-Filter)
      (Message "Filter Cleared.")
      (Message "Filter: %S" Kubed-Ext-Resource-Filter)))

(Defun Kubed-Ext-Jump-To-Next-Highlight ()
  "Jump To The Next Resource Matching `Kubed-Ext-Resource-Filter'."
  (Interactive Nil Kubed-List-Mode)
  (Unless (And Kubed-Ext-Resource-Filter
               (Not (String-Empty-P Kubed-Ext-Resource-Filter)))
          (User-Error "No Filter Set (Use `F' To Set One)"))
  (Let ((Found Nil) (Start (Point)))
       (End-Of-Line)
       (While (And (Not Found) (Not (Eobp)))
              (Forward-Line)
              (When-Let ((Entry (Tabulated-List-Get-Entry)))
                        (Let ((Text (Mapconcat #'Identity (Append Entry Nil) " ")))
                             (When (String-Match-P Kubed-Ext-Resource-Filter Text)
                                   (Setq Found T)))))
       (Unless Found
               (Goto-Char (Point-Min))
               (While (And (Not Found) (< (Point) Start))
                      (When-Let ((Entry (Tabulated-List-Get-Entry)))
                                (Let ((Text (Mapconcat #'Identity (Append Entry Nil) " ")))
                                     (When (String-Match-P Kubed-Ext-Resource-Filter Text)
                                           (Setq Found T))))
                      (Unless Found (Forward-Line))))
       (If Found
           (Beginning-Of-Line)
           (Goto-Char Start)
           (Message "No Match For `%S'" Kubed-Ext-Resource-Filter))))

(Defun Kubed-Ext-Jump-To-Previous-Highlight ()
  "Jump To The Previous Resource Matching `Kubed-Ext-Resource-Filter'."
  (Interactive Nil Kubed-List-Mode)
  (Unless (And Kubed-Ext-Resource-Filter
               (Not (String-Empty-P Kubed-Ext-Resource-Filter)))
          (User-Error "No Filter Set (Use `F' To Set One)"))
  (Let ((Found Nil) (Start (Point)))
       (Beginning-Of-Line)
       (While (And (Not Found) (Not (Bobp)))
              (Forward-Line -1)
              (When-Let ((Entry (Tabulated-List-Get-Entry)))
                        (Let ((Text (Mapconcat #'Identity (Append Entry Nil) " ")))
                             (When (String-Match-P Kubed-Ext-Resource-Filter Text)
                                   (Setq Found T)))))
       (Unless Found
               (Goto-Char (Point-Max))
               (While (And (Not Found) (> (Point) Start))
                      (Forward-Line -1)
                      (When-Let ((Entry (Tabulated-List-Get-Entry)))
                                (Let ((Text (Mapconcat #'Identity (Append Entry Nil) " ")))
                                     (When (String-Match-P Kubed-Ext-Resource-Filter Text)
                                           (Setq Found T))))))
       (If Found
           (Beginning-Of-Line)
           (Goto-Char Start)
           (Message "No Match For `%S'" Kubed-Ext-Resource-Filter))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 3d.  Wide View + Output Format Toggle
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext--Parse-Kubectl-Table (Text)
  "Parse Text From Kubectl Output Into (Headers . Rows).
Headers Is A List Of Strings, Rows Is A List Of Lists Of Strings."
  (Let* ((Lines (Split-String Text "\N" T))
         (Header-Line (Car Lines))
         (Data-Lines (Cdr Lines))
         (Starts Nil)
         (Pos 0))
        (Push 0 Starts)
        (While (String-Match "\\S-+$\\S-+$\\S-" Header-Line Pos)
               (Let ((Gap-End (Match-End 1)))
                    (Push Gap-End Starts)
                    (Setq Pos Gap-End)))
        (Setq Starts (Nreverse Starts))
        (Let* ((Parse-Line
                (Lambda (Line)
                        (Let ((Cols Nil)
                              (Offsets Starts))
                             (While Offsets
                                    (Let* ((Beg (Car Offsets))
                                           (End (If (Cdr Offsets) (Cadr Offsets) (Length Line)))
                                           (End (Min End (Length Line)))
                                           (Str (If (>= Beg (Length Line)) ""
                                                    (String-Trim
                                                     (Substring Line Beg End)))))
                                          (Push Str Cols))
                                    (Setq Offsets (Cdr Offsets)))
                             (Nreverse Cols))))
               (Headers (Funcall Parse-Line Header-Line))
               (Rows (Delq Nil
                           (Mapcar (Lambda (Line)
                                           (When (Not (String-Empty-P
                                                       (String-Trim Line)))
                                                 (Funcall Parse-Line Line)))
                                   Data-Lines))))
              (Cons Headers Rows))))

(Defvar-Local Kubed-Ext-Wide--Type      Nil "Resource Type Shown In This Wide View Buffer.")
(Defvar-Local Kubed-Ext-Wide--Context   Nil "`Kubectl' Context Used In This Wide View Buffer.")
(Defvar-Local Kubed-Ext-Wide--Namespace Nil "Namespace Used In This Wide View Buffer.")
(Defvar-Local Kubed-Ext-Wide--Name Nil "Resource Name Shown In This Wide View Buffer, Or Nil For All.")

(Dolist (Sym '(Kubed-Ext-Wide--Type Kubed-Ext-Wide--Context Kubed-Ext-Wide--Namespace Kubed-Ext-Wide--Name))
        (Put Sym 'Permanent-Local T))

(Defun Kubed-Ext--Colorize-Wide-Cell (Header Value)
  "Return Value Propertized With A Face Based On Column Header Name."
  (Let ((H (Upcase (String-Trim Header))))
       (Cond
        ((Or (String= H "Status") (String= H "Phase"))
         (Let ((Face (Cdr (Assoc Value Kubed-Ext-Status-Faces))))
              (If Face (Propertize Value 'Face Face) Value)))
        ((String= H "Ready")
         (If (String-Match "$[0-9]+$/$[0-9]+$" Value)
             (Let* ((R    (String-To-Number (Match-String 1 Value)))
                    (Tot  (String-To-Number (Match-String 2 Value)))
                    (Face (Cond ((And (= R 0) (= Tot 0)) 'Shadow)
                                ((= R Tot)               'Success)
                                ((= R 0)                 'Error)
                                (T                       'Warning))))
                   (Propertize Value 'Face Face))
             Value))
        ((String= H "Restarts")
         (Let* ((N    (String-To-Number Value))
                (Face (Cond ((= N 0) Nil)
                            ((< N 5) 'Warning)
                            (T       'Error))))
               (If Face (Propertize Value 'Face Face) Value)))
        (T Value))))

;; Note: Callees Defined Before The Caller To Satisfy The Byte-Compiler.

(Defun Kubed-Ext--Wide-Row-Primary-P ()
  "Return Non-Nil If Point Is On A Primary Wide-View Row.

In The Multi-Row Deployments Wide View, Continuation Rows Use An Empty
Name Column.  We Treat Rows With A Non-Empty Name Column As Primary."
  (When-Let ((Ent (Tabulated-List-Get-Entry)))
            (Let ((Name (Aref Ent 0)))
                 (And (Stringp Name)
                      (Not (String-Empty-P (String-Trim Name)))))))

(Defun Kubed-Ext--Wide-Populate-Kubectl-Wide (Type Ctx Ns &Optional Name)
  "Populate Current Buffer Using Raw `Kubectl Get -O Wide' For Type, Ctx, Ns.
Optional Name Limits Output To A Single Named Resource."
  (Let* ((Raw     (With-Temp-Buffer
                   (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                          `("Get" ,Type ,@(When Name (List Name))
                            "-O" "Wide"
                            ,@(When Ns  `("-N"        ,Ns))
                            ,@(When Ctx `("--Context" ,Ctx))))
                   (Buffer-String)))
         (Parsed (Kubed-Ext--Parse-Kubectl-Table Raw))
         (Headers (Car Parsed))
         (Rows (Cdr Parsed)))
        (Unless Headers
                (User-Error "`Kubectl' Returned No Output For %S" Type))
        (Let* ((Content-Widths
                (Cl-Loop For H In Headers
                         For I From 0
                         Collect (Apply #'Max
                                        (Length H)
                                        4
                                        (Mapcar (Lambda (Row)
                                                        (Length (Or (Nth I Row) "")))
                                                Rows))))
               (Min-Widths
                (Cl-Loop For H In Headers
                         For I From 0
                         Collect (Max 6 (Min (Nth I Content-Widths)
                                             (Max (Length H) 6)))))
               (Available
                (Max 20 (- (Window-Body-Width (Selected-Window)) 2)))
               (Padding 2)
               (Total (Lambda (Widths)
                              (+ (Apply #'+ Widths)
                                 (* Padding (Max 0 (1- (Length Widths)))))))
               (Widths (Copy-Sequence Content-Widths)))
              (While (> (Funcall Total Widths) Available)
                     (Let* ((Idx (Cl-Position (Apply #'Max Widths) Widths))
                            (W (Nth Idx Widths))
                            (Minw (Nth Idx Min-Widths)))
                           (If (<= W Minw)
                               (Setq Widths Nil)
                               (Setf (Nth Idx Widths) (1- W)))))
              (Unless Widths
                      (Setq Widths Min-Widths))
              (Setq Tabulated-List-Format
                    (Apply #'Vector
                           (Cl-Loop For H In Headers
                                    For W In Widths
                                    Collect (List H W T)))))
        (Setq Tabulated-List-Entries
              (Mapcar (Lambda (Row)
                              (List (Or (Car Row) "")
                                    (Apply #'Vector
                                           (Cl-Loop For H In Headers
                                                    For I From 0
                                                    Collect (Kubed-Ext--Colorize-Wide-Cell
                                                             H (Or (Nth I Row) ""))))))
                      Rows))
        (Setq Tabulated-List-Padding 2)
        (Tabulated-List-Init-Header)
        (Tabulated-List-Print T)))

(Defun Kubed-Ext--Wide-Populate-Deployments (Ctx Ns &Optional Name)
  "Populate A Multi-Row, Intuitive Wide View For Deployments.

Ctx Is The Kubectl Context, Ns Is The Namespace.
Optional Name Limits Output To A Single Named Deployment."
  (Let* ((Json-Str
          (With-Temp-Buffer
           (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                  (Append (List "Get" "Deployments")
                          (When Name (List Name))
                          (List "-O" "Json")
                          (When Ns (List "-N" Ns))
                          (When Ctx (List "--Context" Ctx))))
           (Buffer-String)))
         (Obj (Json-Parse-String Json-Str :Object-Type 'Alist :Array-Type 'List))
         (Items (If Name
                    (If (Alist-Get 'Items Obj)
                        (Alist-Get 'Items Obj)
                        (List Obj))
                    (Or (Alist-Get 'Items Obj) '())))
         (Entries Nil)
         (Seen-Detail (Make-Hash-Table :Test 'Equal)))
        (Setq Tabulated-List-Padding 2)
        (Setq Tabulated-List-Format
              (Vector
               (List "Name" 34 T)
               (List "Ready" 10 T)
               (List "Up-To-Date" 12 T)
               (List "Available" 12 T)
               (List "Age" 10 T)
               (List "Detail" 70 T)))
        (Dolist (It Items)
                (Let* ((Meta (Alist-Get 'Metadata It))
                       (Spec (Alist-Get 'Spec It))
                       (Status (Alist-Get 'Status It))
                       (Dep-Name (Or (Alist-Get 'Name Meta) ""))
                       (Labels (Alist-Get 'Labels Meta))
                       (Selector (Alist-Get 'Selector Spec))
                       (Match-Labels (Alist-Get 'Matchlabels Selector))
                       (Replicas (Number-To-String (Or (Alist-Get 'Replicas Spec) 0)))
                       (Ready (Number-To-String (Or (Alist-Get 'Readyreplicas Status) 0)))
                       (Updated (Number-To-String (Or (Alist-Get 'Updatedreplicas Status) 0)))
                       (Available (Number-To-String (Or (Alist-Get 'Availablereplicas Status) 0)))
                       (Creation (Or (Alist-Get 'Creationtimestamp Meta) ""))
                       (Age (Kubed-Ext--Format-Age Creation))
                       (Ready-Str (Kubed-Ext--Format-Ready-Total Ready Replicas))
                       (Updated-Str (Kubed-Ext--Format-Deployment-Count Updated Replicas))
                       (Avail-Str (Kubed-Ext--Format-Deployment-Count Available Replicas))
                       (Tmpl (Alist-Get 'Template Spec))
                       (Podspec (And Tmpl (Alist-Get 'Spec Tmpl)))
                       (Containers (And Podspec (Alist-Get 'Containers Podspec))))
                      (Let* ((Pairs
                              (Mapcar (Lambda (C)
                                              (Let ((Cname (Or (Alist-Get 'Name C) ""))
                                                    (Img (Or (Alist-Get 'Image C) "")))
                                                   (Cons Cname Img)))
                                      (Or Containers '())))
                             (Summary
                              (If (Null Pairs)
                                  ""
                                  (Mapconcat
                                   (Lambda (P)
                                           (Let ((Cname (Car P))
                                                 (Img (Cdr P))
                                                 (Img-Face 'Font-Lock-Constant-Face))
                                                (Concat
                                                 (Propertize Cname 'Face 'Shadow)
                                                 (Propertize " → " 'Face 'Shadow)
                                                 (Propertize Img 'Face Img-Face))))
                                   Pairs
                                   (Propertize " | " 'Face 'Shadow)))))
                            (Push (List Dep-Name
                                        (Vector Dep-Name Ready-Str Updated-Str Avail-Str Age Summary))
                                  Entries))
                      (Cl-Flet
                       ((Push-Detail
                         (Detail)
                         (When (And (Stringp Detail) (Not (String-Empty-P (String-Trim Detail))))
                               (Let ((K (Cons Dep-Name Detail)))
                                    (Unless (Gethash K Seen-Detail)
                                            (Puthash K T Seen-Detail)
                                            (Push (List Dep-Name
                                                        (Vector "" "" "" "" ""
                                                                (Propertize Detail 'Face 'Shadow)))
                                                  Entries))))))
                       (Dolist (C (Or Containers '()))
                               (Let* ((Cname (Or (Alist-Get 'Name C) ""))
                                      (Img (Or (Alist-Get 'Image C) ""))
                                      (Img-Face 'Font-Lock-Constant-Face)
                                      (Detail (Concat "Container: " Cname "  Image: " Img)))
                                     (Let ((K (Cons Dep-Name Detail)))
                                          (Unless (Gethash K Seen-Detail)
                                                  (Puthash K T Seen-Detail)
                                                  (Push (List Dep-Name
                                                              (Vector "" "" "" "" ""
                                                                      (Concat
                                                                       (Propertize "Container: " 'Face 'Shadow)
                                                                       (Propertize Cname 'Face 'Shadow)
                                                                       (Propertize "  Image: " 'Face 'Shadow)
                                                                       (Propertize Img 'Face Img-Face))))
                                                        Entries)))))
                       (When (And Match-Labels (Listp Match-Labels))
                             (Dolist (Pair Match-Labels)
                                     (Push-Detail (Format "Selector: %S=%S" (Car Pair) (Cdr Pair)))))
                       (When (And Labels (Listp Labels))
                             (Dolist (Pair Labels)
                                     (Push-Detail (Format "Label: %S=%S" (Car Pair) (Cdr Pair))))))))
        (Setq Tabulated-List-Entries (Nreverse Entries))
        (Tabulated-List-Init-Header)
        (Tabulated-List-Print T)))

(Defun Kubed-Ext--Wide-Populate (Type Ctx Ns &Optional Name)
  "Fill The Current Buffer With A Wide-Format Table For Type In Ctx/Ns.

Optional Argument Name Limits Output To A Single Named Resource.

For Most Types, This Shells Out To:
  Kubectl Get Type -O Wide

For Deployments, We Render A More Intuitive Multi-Row View Based On Json.

This Runs `Kubectl' Synchronously."
  (Setq Kubed-Ext-Wide--Type      Type
        Kubed-Ext-Wide--Context   Ctx
        Kubed-Ext-Wide--Namespace Ns
        Kubed-Ext-Wide--Name      Name)
  (If (String= Type "Deployments")
      (Kubed-Ext--Wide-Populate-Deployments Ctx Ns Name)
      (Kubed-Ext--Wide-Populate-Kubectl-Wide Type Ctx Ns Name)))

(Defun Kubed-Ext-List-Wide ()
  "Display The Current Resource List In Kubectl Wide Format With Color Coding.
When Point Is On A Resource, Show Only That Resource."
  (Interactive Nil Kubed-List-Mode)
  (Let* ((Type Kubed-List-Type)
         (Ctx  Kubed-List-Context)
         (Ns   Kubed-List-Namespace)
         (Name (Tabulated-List-Get-Id))
         (Buf  (Get-Buffer-Create
                (Format "*Kubed Wide %S/%S/%S%S*"
                        Type (Or Ns "Cluster") (Or Ctx "Current")
                        (If Name (Concat "/" Name) "")))))
        (Pop-To-Buffer Buf)
        (Kubed-Ext-Wide-Mode)
        (Let ((Inhibit-Read-Only T))
             (Kubed-Ext--Wide-Populate Type Ctx Ns Name))))

(Defun Kubed-Ext-Wide-Describe-Resource ()
  "Describe The Kubernetes Resource At Point In The Wide View Buffer."
  (Interactive Nil Kubed-Ext-Wide-Mode)
  (Unless (Kubed-Ext--Wide-Row-Primary-P)
          (User-Error "No Primary Resource Row At Point"))
  (If-Let ((Name (Tabulated-List-Get-Id)))
          (Let* ((Type Kubed-Ext-Wide--Type)
                 (Ns   Kubed-Ext-Wide--Namespace)
                 (Ctx  Kubed-Ext-Wide--Context))
                (Unless Type
                        (User-Error "No Resource Type Context In This Buffer"))
                (Let ((Buf (Get-Buffer-Create
                            (Format "*Kubed Describe %S/%S/%S/%S*"
                                    Type Name
                                    (Or Ns "Default")
                                    (Or Ctx "Current")))))
                     (With-Current-Buffer Buf
                                          (Let ((Inhibit-Read-Only T))
                                               (Erase-Buffer)
                                               (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                                                      (Append (List "Describe" Type Name)
                                                              (When Ns (List "-N" Ns))
                                                              (When Ctx (List "--Context" Ctx))))
                                               (Goto-Char (Point-Min))
                                               (Special-Mode)))
                     (Display-Buffer Buf)))
          (User-Error "No Kubernetes Resource At Point")))

(Defun Kubed-Ext-Wide-Fit-Column (N)
  "Fit Width Of Nth Table Column To Its Content In Wide View.
If N Is Negative, Fit All Columns.  Interactively, N Is The Column
Number At Point, Or The Numeric Prefix Argument If You Provide One."
  (Interactive
   (List (If Current-Prefix-Arg
             (Prefix-Numeric-Value Current-Prefix-Arg)
             (Kubed-List-Column-Number-At-Point)))
   Kubed-Ext-Wide-Mode)
  (Kubed-List-Fit-Column-Width-To-Content N))

(Define-Derived-Mode Kubed-Ext-Wide-Mode Tabulated-List-Mode "Kubed Wide"
  "Major Mode For Displaying Kubectl Wide Output With Color Coding.
\\{Kubed-Ext-Wide-Mode-Map}"
  (Setq Truncate-Lines T)
  (Setq-Local Word-Wrap Nil)
  (Setq-Local Truncate-String-Ellipsis (Propertize "…" 'Face 'Shadow))
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "G"   #'Kubed-Ext-List-Wide-Refresh)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "Q"   #'Quit-Window)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "B"   #'Kubed-Ext-Switch-Buffer)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "<"   #'Kubed-Ext-Wide-Fit-Column)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "|"   #'Kubed-Ext-Wide-Fit-Column)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "}"   #'Tabulated-List-Widen-Current-Column)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "{"   #'Tabulated-List-Narrow-Current-Column)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "?"   #'Kubed-Ext-Transient-Menu)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "D"   #'Kubed-Ext-Wide-Describe-Resource)
  (Keymap-Set Kubed-Ext-Wide-Mode-Map "Ret" #'Kubed-Ext-Wide-Describe-Resource))

(Defun Kubed-Ext-List-Wide-Refresh ()
  "Refresh The Wide View Buffer By Re-Running Kubectl."
  (Interactive Nil Kubed-Ext-Wide-Mode)
  (Unless Kubed-Ext-Wide--Type
          (User-Error "No Resource Context; Open Wide View From A Resource List Buffer"))
  (Message "Refreshing %S Wide View..." Kubed-Ext-Wide--Type)
  (Let ((Inhibit-Read-Only T))
       (Kubed-Ext--Wide-Populate
        Kubed-Ext-Wide--Type
        Kubed-Ext-Wide--Context
        Kubed-Ext-Wide--Namespace
        Kubed-Ext-Wide--Name)))

(Defun Kubed-Ext-Toggle-Output-Format ()
  "Toggle Resource Display Format Between Yaml And Json And Revert."
  (Interactive)
  (Setq Kubed-Ext-Output-Format
        (If (String= Kubed-Ext-Output-Format "Yaml") "Json" "Yaml"))
  (When (Bound-And-True-P Kubed-Display-Resource-Mode)
        (Revert-Buffer))
  (Message "Output Format: %S" Kubed-Ext-Output-Format))

(Defun Kubed-Ext-Set-Output-Format ()
  "Prompt For And Set The Output Format For Resource Display."
  (Interactive)
  (Setq Kubed-Ext-Output-Format
        (Completing-Read "Output Format: " '("Yaml" "Json") Nil T
                         Nil Nil Kubed-Ext-Output-Format))
  (When (Bound-And-True-P Kubed-Display-Resource-Mode)
        (Revert-Buffer))
  (Message "Output Format Set To %S." Kubed-Ext-Output-Format))

(Defun Kubed-Ext--Display-Revert-Format (Orig-Fn &Rest Args)
  "Around Advice For `Kubed-Display-Resource-Revert' For Configurable Format.
Orig-Fn Is The Original Function, Args Its Arguments."
  (If (String= Kubed-Ext-Output-Format "Yaml")
      (Apply Orig-Fn Args)
      (Seq-Let (Type Name Context Namespace)
               Kubed-Display-Resource-Info
               (Let ((Inhibit-Read-Only T)
                     (Target (Current-Buffer)))
                    (Buffer-Disable-Undo)
                    (With-Temp-Buffer
                     (Unless (Zerop
                              (Apply #'Call-Process
                                     Kubed-Kubectl-Program Nil T Nil "Get"
                                     Type (Concat "--Output=" Kubed-Ext-Output-Format)
                                     Name
                                     (Append (When Namespace (List "-N" Namespace))
                                             (When Context (List "--Context" Context)))))
                             (Error "Failed To Display Kubernetes Resource `%S'" Name))
                     (Let ((Source (Current-Buffer)))
                          (With-Current-Buffer Target
                                               (Replace-Buffer-Contents Source)
                                               (Set-Buffer-Modified-P Nil)
                                               (Buffer-Enable-Undo))))))))

(Advice-Add 'Kubed-Display-Resource-Revert :Around
            #'Kubed-Ext--Display-Revert-Format)

;;; ═══════════════════════════════════════════════════════════════
;;; § 3e.  Batch Operations On Marked Items
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Delete-Marked ()
  "Delete All Resources Marked With `*'."
  (Interactive Nil Kubed-List-Mode)
  (Let ((Items (Kubed-Ext-Marked-Items)))
       (Unless Items
               (User-Error "No Resources Marked With `*'"))
       (When (Y-Or-N-P (Format "Delete %D Marked %S?"
                               (Length Items) Kubed-List-Type))
             (Kubed-Delete-Resources Kubed-List-Type Items
                                     Kubed-List-Context Kubed-List-Namespace)
             (Kubed-List-Update T))))

(Defun Kubed-Ext-Logs-Marked ()
  "Show Logs For All Pods Marked With `*'."
  (Interactive Nil Kubed-List-Mode)
  (Let ((Items (Kubed-Ext-Marked-Items)))
       (Unless Items
               (User-Error "No Resources Marked With `*'"))
       (Dolist (Pod Items)
               (Kubed-Logs Kubed-List-Type Pod Kubed-List-Context Kubed-List-Namespace
                           T T Nil Nil Nil Nil Nil))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 4.  Strimzi Operation Helpers
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Strimzi-Pause-Reconciliation
  (Type Name &Optional Context Namespace)
  "Pause Strimzi Reconciliation For Resource Name Of Type In Context."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg))
          (Type (Completing-Read "Resource Type: "
                                 '("Kafka" "Kafkaconnect" "Kafkabridge"
                                   "Kafkamirrormaker2" "Kafkamirrormaker")
                                 Nil T))
          (Name (Kubed-Read-Resource-Name
                 (Concat Type "S") "Pause Reconciliation For" Nil Nil C N)))
         (List Type Name C N)))
  (Let ((Context (Or Context (Kubed-Local-Context)))
        (Namespace (Or Namespace
                       (Kubed--Namespace (Or Context
                                             (Kubed-Local-Context))))))
       (Unless (Zerop
                (Apply #'Call-Process
                       Kubed-Kubectl-Program Nil Nil Nil
                       "Annotate" Type Name
                       "Strimzi.Io/Pause-Reconciliation=True" "--Overwrite"
                       (Append
                        (When Namespace (List "-N" Namespace))
                        (When Context (List "--Context" Context)))))
               (User-Error "Failed To Pause Reconciliation For %S/%S" Type Name))
       (Message "Paused Reconciliation For %S/%S." Type Name)))

(Defun Kubed-Ext-Strimzi-Resume-Reconciliation
  (Type Name &Optional Context Namespace)
  "Resume Strimzi Reconciliation For Resource Name Of Type In Context."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg))
          (Type (Completing-Read "Resource Type: "
                                 '("Kafka" "Kafkaconnect" "Kafkabridge"
                                   "Kafkamirrormaker2" "Kafkamirrormaker")
                                 Nil T))
          (Name (Kubed-Read-Resource-Name
                 (Concat Type "S") "Resume Reconciliation For" Nil Nil C N)))
         (List Type Name C N)))
  (Let ((Context (Or Context (Kubed-Local-Context)))
        (Namespace (Or Namespace
                       (Kubed--Namespace (Or Context
                                             (Kubed-Local-Context))))))
       (Unless (Zerop
                (Apply #'Call-Process
                       Kubed-Kubectl-Program Nil Nil Nil
                       "Annotate" Type Name
                       "Strimzi.Io/Pause-Reconciliation-"
                       (Append
                        (When Namespace (List "-N" Namespace))
                        (When Context (List "--Context" Context)))))
               (User-Error "Failed To Resume Reconciliation For %S/%S" Type Name))
       (Message "Resumed Reconciliation For %S/%S." Type Name)))

(Defun Kubed-Ext-Strimzi-Restart-Connector
  (Name &Optional Context Namespace)
  "Trigger Restart Of Strimzi Kafkaconnector Name In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List (Kubed-Read-Resource-Name
                "Kafkaconnectors" "Restart Connector" Nil Nil C N)
               C N)))
  (Let ((Context (Or Context (Kubed-Local-Context)))
        (Namespace (Or Namespace
                       (Kubed--Namespace (Or Context
                                             (Kubed-Local-Context))))))
       (Unless (Zerop
                (Apply #'Call-Process
                       Kubed-Kubectl-Program Nil Nil Nil
                       "Annotate" "Kafkaconnector" Name
                       "Strimzi.Io/Restart=True" "--Overwrite"
                       (Append
                        (When Namespace (List "-N" Namespace))
                        (When Context (List "--Context" Context)))))
               (User-Error "Failed To Restart Connector %S" Name))
       (Message "Triggered Restart Of Connector %S." Name)))

(Defun Kubed-Ext-Scale-Kafkaconnect
  (Name Replicas &Optional Context Namespace)
  "Scale Strimzi Kafkaconnect Name To Replicas In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List (Kubed-Read-Resource-Name
                "Kafkaconnects" "Scale Kafkaconnect" Nil Nil C N)
               (Read-Number "Number Of Replicas: ")
               C N)))
  (Let* ((Context (Or Context (Kubed-Local-Context)))
         (Namespace (Or Namespace (Kubed--Namespace Context))))
        (Kubed-Patch "Kafkaconnects" Name
                     (Kubed-Ext--Json-Patch "Replicas" (Number-To-String Replicas))
                     Context Namespace "Merge")
        (Message "Scaled Kafkaconnect %S To %D Replicas." Name Replicas)
        (Kubed-Update "Kafkaconnects" Context Namespace)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 5.  Column Helpers, Metrics, Pod/Deployment Formatting
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Append-Columns (Resource-Plural Fetch-Columns Display-Columns)
  "Append Extra Fetch-Columns And Display-Columns To Resource-Plural."
  (Let ((Existing (Alist-Get Resource-Plural Kubed--Columns Nil Nil #'String=))
        (Fmt-Var (Intern (Format "Kubed-%S-Columns" Resource-Plural))))
       (Setf (Alist-Get Resource-Plural Kubed--Columns Nil Nil #'String=)
             (Append Existing Fetch-Columns))
       (When (Boundp Fmt-Var)
             (Set Fmt-Var (Append (Symbol-Value Fmt-Var) Display-Columns)))))

(Defun Kubed-Ext-Set-Columns (Resource-Plural Fetch-Columns Display-Columns)
  "Replace Columns For Resource-Plural With Fetch-Columns And Display-Columns."
  (Setf (Alist-Get Resource-Plural Kubed--Columns Nil Nil #'String=)
        (Cons '("Name:.Metadata.Name") Fetch-Columns))
  (Let ((Fmt-Var (Intern (Format "Kubed-%S-Columns" Resource-Plural))))
       (When (Boundp Fmt-Var)
             (Set Fmt-Var Display-Columns))))

(Defun Kubed-Ext-Parse-Cpu (S)
  "Parse Kubernetes Cpu Quantity S To Millicores (Number)."
  (Cond
   ((Kubed-Ext-None-P S) 0)
   ((String-Suffix-P "M" S)  (String-To-Number S))
   ((String-Suffix-P "N" S)  (/ (String-To-Number S) 1000000.0))
   ((String-Suffix-P "U" S)  (/ (String-To-Number S) 1000.0))
   (T (* 1000.0 (String-To-Number S)))))

(Defun Kubed-Ext-Parse-Mem (S)
  "Parse Kubernetes Memory Quantity S To Mib (Number)."
  (Cond
   ((Kubed-Ext-None-P S) 0)
   ((String-Suffix-P "Ti" S) (* 1048576.0 (String-To-Number S)))
   ((String-Suffix-P "Gi" S) (* 1024.0    (String-To-Number S)))
   ((String-Suffix-P "Mi" S) (String-To-Number S))
   ((String-Suffix-P "Ki" S) (/ (String-To-Number S) 1024.0))
   (T (/ (String-To-Number S) 1048576.0))))

(Defun Kubed-Ext-Sum-Cpu (S)
  "Sum Comma-Separated Cpu Values In S."
  (If (Kubed-Ext-None-P S) 0
      (Apply #'+ (Mapcar #'Kubed-Ext-Parse-Cpu
                         (Split-String S "," T "[ \T]")))))

(Defun Kubed-Ext-Sum-Mem (S)
  "Sum Comma-Separated Memory Values In S."
  (If (Kubed-Ext-None-P S) 0
      (Apply #'+ (Mapcar #'Kubed-Ext-Parse-Mem
                         (Split-String S "," T "[ \T]")))))

(Defun Kubed-Ext-Format-Cpu (Millicores)
  "Format Millicores As Human-Readable Cpu String."
  (Let ((Str (Cond
              ((<= Millicores 0) "0")
              ((>= Millicores 1000) (Format "%.1f" (/ Millicores 1000.0)))
              (T (Format "%Dm" (Round Millicores))))))
       (Propertize Str 'Kubed-Sort-Value Millicores)))

(Defun Kubed-Ext-Format-Mem (Mib)
  "Format Mib As Human-Readable Memory String."
  (Let ((Str (Cond
              ((<= Mib 0) "0")
              ((>= Mib 1024) (Format "%.1fgi" (/ Mib 1024.0)))
              (T (Format "%.0fmi" Mib)))))
       (Propertize Str 'Kubed-Sort-Value Mib)))

(Defun Kubed-Ext-Format-Pct (Used Total)
  "Format Percentage Used/Total With Color Coding."
  (If (<= Total 0)
      (Propertize "N/A" 'Face 'Shadow 'Kubed-Sort-Value -1)
      (Let* ((Pct (/ (* 100.0 Used) Total))
             (Str (Format "%.0f%%" Pct)))
            (Propertize Str
                        'Face (Cond ((>= Pct 90) 'Error)
                                    ((>= Pct 70) 'Warning)
                                    (T 'Success))
                        'Kubed-Sort-Value Pct))))

(Defun Kubed-Ext-Top-Numeric-Sorter (Col-Idx)
  "Return A Sort Predicate Comparing Column Col-Idx By `Kubed-Sort-Value'."
  (Lambda (A B)
          (Let ((Va (Get-Text-Property 0 'Kubed-Sort-Value (Aref (Cadr A) Col-Idx)))
                (Vb (Get-Text-Property 0 'Kubed-Sort-Value (Aref (Cadr B) Col-Idx))))
               (< (Or Va 0) (Or Vb 0)))))

;; ── Kubernetes Timestamp ──

(Defun Kubed-Ext--Parse-K8s-Timestamp (S)
  "Parse Kubernetes Rfc3339/Iso8601 Timestamp S To An Emacs Time Value.

Supports The Common Kubernetes Forms:
- Yyyy-Mm-Ddthh:Mm:Ssz
- Yyyy-Mm-Ddthh:Mm:Ss.Sssz
- Yyyy-Mm-Ddthh:Mm:Ss±Hh:Mm

Return Nil If S Cannot Be Parsed."
  (When (And (Stringp S)
             (Not (String-Empty-P S))
             ;; Quick Guard: Must Start With Yyyy-Mm-Ddthh:Mm:Ss
             (String-Match-P
              "\\`[0-9]\\{4\\}-[0-9]\\{2\\}-[0-9]\\{2\\}T[0-9]\\{2\\}:[0-9]\\{2\\}:[0-9]\\{2\\}"
              S))
        (Condition-Case Nil
                        (Parse-Iso8601-Time-String S)
                        (Error Nil))))

(Defun Kubed-Ext--Format-Age (Timestamp)
  "Convert Timestamp To Human-Readable Relative Age Like Kubectl.

If Timestamp Already Carries A `Kubed-Sort-Value' Text Property (I.E. It
Was Already Formatted By A Previous Call), Return It Unchanged.
Returns `N/A' For Nil, Empty, Or Placeholder Values Like `<None>'."
  (Cond
   ;; Nil, Empty, Or Kubectl Placeholder.
   ((Kubed-Ext-None-P Timestamp)
    (Propertize "N/A" 'Face 'Shadow 'Kubed-Sort-Value 0))
   ;; Already Formatted — Return As-Is (Idempotency Guard).
   ((And (Stringp Timestamp)
         (Get-Text-Property 0 'Kubed-Sort-Value Timestamp))
    Timestamp)
   ;; Try To Parse As Iso 8601 Timestamp.
   (T
    (Let ((Parsed (Kubed-Ext--Parse-K8s-Timestamp Timestamp)))
         (If (Null Parsed)
             ;; Not A Recognizable Timestamp — Pass Through.
             (Propertize (If (Stringp Timestamp) Timestamp
                             (Format "%S" Timestamp))
                         'Kubed-Sort-Value 0)
             (Let* ((Secs (Max 0 (Floor (Float-Time (Time-Subtract Nil Parsed)))))
                    (Total-Minutes (/ Secs 60))
                    (Hours (/ Secs 3600))
                    (Days (/ Secs 86400))
                    (Years (/ Days 365))
                    (Str (Cond
                          ((< Secs 120)
                           (Format "%Ds" Secs))
                          ((< Total-Minutes 10)
                           (Let ((S (Mod Secs 60)))
                                (If (= S 0) (Format "%Dm" Total-Minutes)
                                    (Format "%Dm%Ds" Total-Minutes S))))
                          ((< Hours 3)
                           (Format "%Dm" Total-Minutes))
                          ((< Hours 8)
                           (Let ((M (Mod Total-Minutes 60)))
                                (If (= M 0) (Format "%Dh" Hours)
                                    (Format "%Dh%Dm" Hours M))))
                          ((< Hours 48)
                           (Format "%Dh" Hours))
                          ((< Days 8)
                           (Let ((H (Mod Hours 24)))
                                (If (= H 0) (Format "%Dd" Days)
                                    (Format "%Dd%Dh" Days H))))
                          ((< Years 2)
                           (Format "%Dd" Days))
                          ((< Years 8)
                           (Format "%Dy%Dd" Years (Mod Days 365)))
                          (T (Format "%Dy" Years)))))
                   (Propertize Str 'Kubed-Sort-Value (Float Secs))))))))

;; ── Pod Status Computation (Kubel-Style) ──

(Defun Kubed-Ext--Compute-Pod-Status (Phase Waiting Terminated Deletion)
  "Compute Kubel-Style Pod Status From Phase, Waiting, Terminated, Deletion.
Returns A Propertized String With Face From `Kubed-Ext-Status-Faces'.
Deletion Is Detected By Positive Timestamp Match."
  (Let* ((Waiting-Reason (Unless (Kubed-Ext-None-P Waiting)
                                 (Car (Split-String Waiting ","))))
         (Terminated-Reason (Unless (Kubed-Ext-None-P Terminated)
                                    (Car (Split-String Terminated ","))))
         (Deleting (And (Stringp Deletion)
                        (String-Match-P
                         "\\`[0-9]\\{4\\}-[0-9]\\{2\\}-[0-9]\\{2\\}T"
                         Deletion)))
         (Status (Cond
                  (Deleting "Terminating")
                  (Waiting-Reason Waiting-Reason)
                  (Terminated-Reason Terminated-Reason)
                  (T (Or Phase "Unknown"))))
         (Face (Cdr (Assoc Status Kubed-Ext-Status-Faces))))
        (If Face (Propertize Status 'Face Face) Status)))

(Defun Kubed-Ext--Format-Pod-Ready (Ready-Count Total-Count)
  "Format Ready-Count/Total-Count As Kubel-Style `X/Y' With Color."
  (Let* ((R (String-To-Number (Or Ready-Count "0")))
         (Tot (String-To-Number (Or Total-Count "0")))
         (Str (Format "%D/%D" R Tot))
         (Face (Cond
                ((And (= R 0) (= Tot 0)) 'Shadow)
                ((= R Tot)                'Success)
                ((> R 0)                  'Warning)
                (T                        'Error))))
        (Propertize Str 'Face Face 'Kubed-Sort-Value R)))

(Defun Kubed-Ext--Format-Pod-Restarts (Count)
  "Format Pod Restart Count With Color Coding."
  (Let* ((Str (Number-To-String Count))
         (Face (Cond
                ((= Count 0) Nil)
                ((< Count 5) 'Warning)
                (T 'Error))))
        (If Face
            (Propertize Str 'Face Face 'Kubed-Sort-Value Count)
            (Propertize Str 'Kubed-Sort-Value Count))))

;; ── Deployment Formatting ──

(Defun Kubed-Ext--Format-Ready-Total (Ready Replicas)
  "Format Ready/Replicas As Ready/Total With Color Coding."
  (Let* ((R (String-To-Number (Or Ready "0")))
         (Tot (String-To-Number (Or Replicas "0")))
         (Str (Format "%D/%D" R Tot))
         (Face (Cond
                ((And (= R 0) (= Tot 0)) 'Shadow)
                ((= R Tot)                'Success)
                ((> R 0)                  'Warning)
                (T                        'Error))))
        (Propertize Str 'Face Face 'Kubed-Sort-Value R)))

(Defun Kubed-Ext--Format-Deployment-Count (Count Replicas)
  "Format Deployment Count Relative To Replicas With Color Coding."
  (Let* ((C (String-To-Number (Or Count "0")))
         (Tot (String-To-Number (Or Replicas "0")))
         (Str (Number-To-String C))
         (Face (Cond
                ((And (= C 0) (= Tot 0)) 'Shadow)
                ((>= C Tot)               'Success)
                ((> C 0)                  'Warning)
                (T                        'Error))))
        (Propertize Str 'Face Face 'Kubed-Sort-Value C)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 6.  Enhance Upstream Resource Columns + Mode Hooks
;;; ═══════════════════════════════════════════════════════════════

(Kubed-Ext-Set-Columns
 "Pods"
 (List
  '("Phase:.Status.Phase")
  '("Ready_Names:.Status.Containerstatuses[?(@.Ready==True)].Name")
  '("All_Names:.Status.Containerstatuses[*].Name")
  '("Starttime:.Status.Starttime")
  '("Restarts:.Status.Containerstatuses[*].Restartcount")
  '("Ip:.Status.Podip")
  '("Node:.Spec.Nodename")
  '("Waiting:.Status.Containerstatuses[*].State.Waiting.Reason")
  '("Terminated:.Status.Containerstatuses[*].State.Terminated.Reason")
  '("Deletion:.Metadata.Deletiontimestamp"))
 Nil)

(Defun Kubed-Ext--Count-Csv-Items (S)
  "Count Comma-Separated Items In S.  Empty/None=0, Single Item=1."
  (Cond
   ((Kubed-Ext-None-P S)             0)
   ((String-Empty-P (String-Trim S)) 0)
   (T (1+ (Seq-Count (Lambda (C) (= C ?,)) S)))))

(Defun Kubed-Ext--Pod-Entries ()
  "Custom Entries For Pods With Kubel-Style Status And Color Coding.
Reads 11-Element Fetch Vectors And Produces 7-Column Display Vectors:
Name, Ready(X/Y), Status, Restarts, Age, Ip, Node."
  (Let* ((Raw  (Alist-Get 'Resources (Kubed--Alist Kubed-List-Type
                                                   Kubed-List-Context
                                                   Kubed-List-Namespace)))
         (Pred (Kubed-List-Interpret-Filter))
         (Result Nil))
        (Dolist (Entry Raw)
                (Let* ((Vec (Cadr Entry))
                       (New-Entry
                        (If (And (Vectorp Vec) (>= (Length Vec) 11))
                            (Let* ((Ready-Names  (Aref Vec 2))
                                   (All-Names    (Aref Vec 3))
                                   (Restart-Raw  (Aref Vec 5))
                                   (Ready-Count  (Kubed-Ext--Count-Csv-Items Ready-Names))
                                   (Total-Count  (Kubed-Ext--Count-Csv-Items All-Names))
                                   (Restart-N    (If (Kubed-Ext-None-P Restart-Raw) 0
                                                     (Apply #'+
                                                            (Mapcar #'String-To-Number
                                                                    (Split-String
                                                                     Restart-Raw "," T " "))))))
                                  (List (Car Entry)
                                        (Vector
                                         (Aref Vec 0)
                                         (Kubed-Ext--Format-Pod-Ready
                                          (Number-To-String Ready-Count)
                                          (Number-To-String Total-Count))
                                         (Kubed-Ext--Compute-Pod-Status
                                          (Aref Vec 1) (Aref Vec 8) (Aref Vec 9) (Aref Vec 10))
                                         (Kubed-Ext--Format-Pod-Restarts Restart-N)
                                         (Kubed-Ext--Format-Age (Aref Vec 4))
                                         (If (Kubed-Ext-None-P (Aref Vec 6)) "" (Aref Vec 6))
                                         (If (Kubed-Ext-None-P (Aref Vec 7)) "" (Aref Vec 7)))))
                            Entry)))
                      (When (Funcall Pred New-Entry)
                            (Push New-Entry Result))))
        (Nreverse Result)))

(Defun Kubed-Ext--Pod-Mode-Setup ()
  "Set Up Pod List With Kubel-Style Ready, Status, Restarts, Age Columns."
  (Setq-Local Tabulated-List-Format
              (Vector
               Kubed-Name-Column
               (List "Ready" 7 (Kubed-Ext-Top-Numeric-Sorter 1))
               (List "Status" 22 T)
               (List "Restarts" 9
                     (Kubed-Ext-Top-Numeric-Sorter 3) :Right-Align T)
               (List "Age" 10 (Kubed-Ext-Top-Numeric-Sorter 4))
               (List "Ip" 16 T)
               (List "Node" 32 T)))
  (Setq-Local Tabulated-List-Entries #'Kubed-Ext--Pod-Entries)
  (Tabulated-List-Init-Header))

(Add-Hook 'Kubed-Pods-Mode-Hook #'Kubed-Ext--Pod-Mode-Setup)

(Kubed-Ext-Append-Columns
 "Services"
 (List
  (Cons "Selector:.Spec.Selector"
        (Lambda (S)
                (Cond
                 ((Kubed-Ext-None-P S) "")
                 ((String-Prefix-P "Map[" S) (Substring S 4 -1))
                 (T S)))))
 (List (List "Selector" 40 T)))

(Kubed-Ext-Set-Columns
 "Ingresses"
 (List
  '("Class:.Spec.Ingressclassname")
  '("Hosts:.Spec.Rules[*].Host")
  (Cons "Address:.Status.Loadbalancer.Ingress[0]"
        (Lambda (S)
                (Cond
                 ((Kubed-Ext-None-P S) "")
                 ((String-Match "Ip:$[^] :]+$" S) (Match-String 1 S))
                 ((String-Match "Hostname:$[^] :]+$" S) (Match-String 1 S))
                 (T S))))
  (Cons "Ports:.Spec.Tls[0].Hosts"
        (Lambda (S)
                (If (Kubed-Ext-None-P S) "80" "80, 443")))
  (Cons "Age:.Metadata.Creationtimestamp" #'Kubed-Ext--Format-Age))
 (List
  (List "Class" 20 T)
  (List "Hosts" 40 T)
  (List "Address" 40 T)
  (List "Ports" 10 T)
  (List "Age" 20 T)))

(Kubed-Ext-Set-Columns
 "Persistentvolumes"
 (List
  '("Capacity:.Spec.Capacity.Storage")
  '("Access-Modes:.Spec.Accessmodes[*]")
  '("Reclaim:.Spec.Persistentvolumereclaimpolicy")
  (Cons "Status:.Status.Phase"
        (Lambda (Ph)
                (Let ((Face (Cdr (Assoc Ph Kubed-Ext-Status-Faces))))
                     (If Face (Propertize Ph 'Face Face) Ph))))
  '("Claim-Ns:.Spec.Claimref.Namespace")
  '("Claim-Name:.Spec.Claimref.Name")
  '("Storageclass:.Spec.Storageclassname"))
 (List
  (List "Capacity" 10 T)
  (List "Access-Modes" 18 T)
  (List "Reclaim" 14 T)
  (List "Status" 10 T)
  (List "Claim-Ns" 16 T)
  (List "Claim-Name" 28 T)
  (List "Storageclass" 20 T)))

(Defun Kubed-Ext--Deployment-Entries ()
  "Custom Entries For Deployments With Kubel-Style Ready And Age."
  (Let* ((Raw (Alist-Get 'Resources (Kubed--Alist Kubed-List-Type
                                                  Kubed-List-Context
                                                  Kubed-List-Namespace)))
         (Pred (Kubed-List-Interpret-Filter))
         (Result Nil))
        (Dolist (Entry Raw)
                (Let* ((Vec (Cadr Entry))
                       (New-Entry
                        (If (And (Vectorp Vec) (>= (Length Vec) 6))
                            (List (Car Entry)
                                  (Vector
                                   (Aref Vec 0)
                                   (Kubed-Ext--Format-Ready-Total
                                    (Let ((V (Aref Vec 1)))
                                         (If (Kubed-Ext-None-P V) "0" V))
                                    (Let ((V (Aref Vec 4)))
                                         (If (Kubed-Ext-None-P V) "0" V)))
                                   (Kubed-Ext--Format-Deployment-Count
                                    (Let ((V (Aref Vec 2)))
                                         (If (Kubed-Ext-None-P V) "0" V))
                                    (Let ((V (Aref Vec 4)))
                                         (If (Kubed-Ext-None-P V) "0" V)))
                                   (Kubed-Ext--Format-Deployment-Count
                                    (Let ((V (Aref Vec 3)))
                                         (If (Kubed-Ext-None-P V) "0" V))
                                    (Let ((V (Aref Vec 4)))
                                         (If (Kubed-Ext-None-P V) "0" V)))
                                   (Kubed-Ext--Format-Age (Aref Vec 5))))
                            Entry)))
                      (When (Funcall Pred New-Entry)
                            (Push New-Entry Result))))
        (Nreverse Result)))

(Defun Kubed-Ext--Deployment-Mode-Setup ()
  "Set Up Deployment List With Kubel-Style Ready And Age Columns."
  (Setq-Local Tabulated-List-Format
              (Vector
               Kubed-Name-Column
               (List "Ready" 10 (Kubed-Ext-Top-Numeric-Sorter 1))
               (List "Up-To-Date" 12
                     (Kubed-Ext-Top-Numeric-Sorter 2) :Right-Align T)
               (List "Available" 12
                     (Kubed-Ext-Top-Numeric-Sorter 3) :Right-Align T)
               (List "Age" 12 (Kubed-Ext-Top-Numeric-Sorter 4))))
  (Setq-Local Tabulated-List-Entries #'Kubed-Ext--Deployment-Entries)
  (Tabulated-List-Init-Header))

(Add-Hook 'Kubed-Deployments-Mode-Hook #'Kubed-Ext--Deployment-Mode-Setup)

;;; ═══════════════════════════════════════════════════════════════
;;; § 7.  New Built-In Resources
;;; ═══════════════════════════════════════════════════════════════

(Eval '(Kubed-Define-Resource Node
                              ((Status ".Status.Conditions[-1:].Status" 10
                                       Nil
                                       (Lambda (S)
                                               (Cond
                                                ((String= S "True")    (Propertize "Ready"    'Face 'Success))
                                                ((String= S "False")   (Propertize "Notready" 'Face 'Error))
                                                ((String= S "Unknown") (Propertize "Unknown"  'Face 'Warning))
                                                (T S))))
                               (Roles ".Metadata.Labels" 16
                                      Nil
                                      (Lambda (S)
                                              (Let ((Roles Nil) (Pos 0))
                                                   (While (String-Match
                                                           "Node-Role\\.Kubernetes\\.Io/$[^:=, ]+$"
                                                           S Pos)
                                                          (Push (Match-String 1 S) Roles)
                                                          (Setq Pos (Match-End 0)))
                                                   (If Roles (String-Join (Nreverse Roles) ",") ""))))
                               (Taints ".Spec.Taints[*].Key" 7
                                       (Lambda (L R)
                                               (< (String-To-Number L) (String-To-Number R)))
                                       (Lambda (S)
                                               (If (Or (String-Empty-P S) (String= S "")) "0"
                                                   (Number-To-String
                                                    (1+ (Seq-Count (Lambda (C) (= C ?,)) S)))))
                                       :Right-Align T)
                               (Version ".Status.Nodeinfo.Kubeletversion" 14)
                               (Cpu-Alloc ".Status.Allocatable.Cpu" 6
                                          (Lambda (L R)
                                                  (< (String-To-Number L) (String-To-Number R)))
                                          Nil :Right-Align T)
                               (Mem-Alloc ".Status.Allocatable.Memory" 8
                                          Nil
                                          (Lambda (S)
                                                  (Let ((Mib (Kubed-Ext-Parse-Mem S)))
                                                       (If (>= Mib 1024)
                                                           (Format "%.1fgi" (/ Mib 1024.0))
                                                           (Format "%.0fmi" Mib))))
                                          :Right-Align T)
                               (Internal-Ip ".Status.Addresses[0].Address" 16)
                               (Creationtimestamp ".Metadata.Creationtimestamp" 20))
                              :Namespaced Nil
                              (Top "T" "Show Resource Metrics For"
                                   (Let* ((Ctx Kubed-List-Context)
                                          (Top-Out (With-Temp-Buffer
                                                    (Call-Process Kubed-Kubectl-Program Nil T Nil
                                                                  "Top" "Node" Node
                                                                  "--No-Headers" "--Context" Ctx)
                                                    (Buffer-String)))
                                          (Top-F (Split-String (String-Trim Top-Out) Nil T))
                                          (Cpu-Used (Kubed-Ext-Parse-Cpu (Nth 1 Top-F)))
                                          (Mem-Used (Kubed-Ext-Parse-Mem (Nth 3 Top-F)))
                                          (Alloc-Out (With-Temp-Buffer
                                                      (Call-Process
                                                       Kubed-Kubectl-Program Nil T Nil
                                                       "Get" "Node" Node "--No-Headers"
                                                       "--Context" Ctx "-O"
                                                       "Custom-Columns=Cpu:.Status.Allocatable.Cpu,Mem:.Status.Allocatable.Memory")
                                                      (Buffer-String)))
                                          (Alloc-F (Split-String (String-Trim Alloc-Out) Nil T))
                                          (Cpu-A (Kubed-Ext-Parse-Cpu (Nth 0 Alloc-F)))
                                          (Mem-A (Kubed-Ext-Parse-Mem (Nth 1 Alloc-F)))
                                          (Buf (Get-Buffer-Create
                                                (Format "*Kubed-Top Node/%S[%S]*" Node Ctx))))
                                         (With-Current-Buffer Buf
                                                              (Let ((Inhibit-Read-Only T))
                                                                   (Erase-Buffer)
                                                                   (Insert (Propertize (Format "Node Metrics: %S\N" Node)
                                                                                       'Face 'Bold))
                                                                   (Insert (Make-String 60 ?-) "\N\N")
                                                                   (Insert (Format "  Cpu:  %S Used  /  %S Allocatable  (%S)\N"
                                                                                   (Kubed-Ext-Format-Cpu Cpu-Used)
                                                                                   (Kubed-Ext-Format-Cpu Cpu-A)
                                                                                   (Kubed-Ext-Format-Pct Cpu-Used Cpu-A)))
                                                                   (Insert (Format "  Mem:  %S Used  /  %S Allocatable  (%S)\N"
                                                                                   (Kubed-Ext-Format-Mem Mem-Used)
                                                                                   (Kubed-Ext-Format-Mem Mem-A)
                                                                                   (Kubed-Ext-Format-Pct Mem-Used Mem-A)))
                                                                   (Goto-Char (Point-Min))
                                                                   (Special-Mode)))
                                         (Display-Buffer Buf))))
      T)

(Eval '(Kubed-Define-Resource Persistentvolumeclaim
                              ((Status ".Status.Phase" 10 Nil
                                       (Lambda (Ph)
                                               (Propertize Ph 'Face
                                                           (Pcase Ph
                                                                  ("Bound"   'Success)
                                                                  ("Pending" 'Warning)
                                                                  ("Lost"    'Error)
                                                                  (_         'Default)))))
                               (Volume ".Spec.Volumename" 36)
                               (Capacity ".Status.Capacity.Storage" 10)
                               (Access-Modes ".Spec.Accessmodes[*]" 18)
                               (Storageclass ".Spec.Storageclassname" 20)))
      T)

(Eval '(Kubed-Define-Resource Configmap
                              ((Creationtimestamp ".Metadata.Creationtimestamp" 20)))
      T)

(Eval '(Kubed-Define-Resource Event
                              ((Type ".Type" 8 Nil
                                     (Lambda (S)
                                             (Propertize S 'Face
                                                         (Pcase S
                                                                ("Normal"  'Success)
                                                                ("Warning" 'Warning)
                                                                (_         'Default)))))
                               (Reason ".Reason" 20)
                               (Object-Kind ".Involvedobject.Kind" 12)
                               (Object-Name ".Involvedobject.Name" 28)
                               (Count ".Count" 6
                                      (Lambda (L R)
                                              (< (String-To-Number L) (String-To-Number R)))
                                      Nil :Right-Align T)
                               (Message ".Message" 50)
                               (Last-Seen ".Lasttimestamp" 20)))
      T)

(Eval '(Kubed-Define-Resource Networkpolicy
                              ((Pod-Selector ".Spec.Podselector.Matchlabels" 50 Nil
                                             (Lambda (S)
                                                     (Cond
                                                      ((Or (String-Empty-P S) (String= S ""))
                                                       "(All Pods)")
                                                      ((String-Prefix-P "Map[" S) (Substring S 4 -1))
                                                      (T S))))
                               (Policy-Types ".Spec.Policytypes[*]" 18)
                               (Creationtimestamp ".Metadata.Creationtimestamp" 20))
                              :Plural Networkpolicies)
      T)

(Eval '(Kubed-Define-Resource Horizontalpodautoscaler
                              ((Reference ".Spec.Scaletargetref.Name" 28)
                               (Ref-Kind ".Spec.Scaletargetref.Kind" 14)
                               (Min-Pods ".Spec.Minreplicas" 8
                                         (Lambda (L R)
                                                 (< (String-To-Number L) (String-To-Number R)))
                                         Nil :Right-Align T)
                               (Max-Pods ".Spec.Maxreplicas" 8
                                         (Lambda (L R)
                                                 (< (String-To-Number L) (String-To-Number R)))
                                         Nil :Right-Align T)
                               (Replicas ".Status.Currentreplicas" 8
                                         (Lambda (L R)
                                                 (< (String-To-Number L) (String-To-Number R)))
                                         Nil :Right-Align T)))
      T)

(Eval '(Kubed-Define-Resource Poddisruptionbudget
                              ((Min-Available ".Spec.Minavailable" 14)
                               (Max-Unavailable ".Spec.Maxunavailable" 16)
                               (Allowed-Disruptions ".Status.Disruptionsallowed" 20
                                                    (Lambda (L R)
                                                            (< (String-To-Number L)
                                                               (String-To-Number R)))
                                                    Nil :Right-Align T)
                               (Current-Healthy ".Status.Currenthealthy" 16
                                                (Lambda (L R)
                                                        (< (String-To-Number L)
                                                           (String-To-Number R)))
                                                Nil :Right-Align T)
                               (Desired-Healthy ".Status.Desiredhealthy" 16
                                                (Lambda (L R)
                                                        (< (String-To-Number L)
                                                           (String-To-Number R)))
                                                Nil :Right-Align T)))
      T)

(Eval '(Kubed-Define-Resource Customresourcedefinition
                              ((Group ".Spec.Group" 34)
                               (Kind ".Spec.Names.Kind" 24)
                               (Scope ".Spec.Scope" 12 Nil
                                      (Lambda (S)
                                              (Propertize S 'Face
                                                          (Pcase S
                                                                 ("Namespaced" 'Success)
                                                                 ("Cluster"    'Warning)
                                                                 (_            'Default)))))
                               (Versions ".Spec.Versions[*].Name" 16)
                               (Creationtimestamp ".Metadata.Creationtimestamp" 20))
                              :Namespaced Nil
                              (List-Instances "I" "List Instances Of"
                                              (Let* ((Plural (Car (Split-String
                                                                   Customresourcedefinition "\\.")))
                                                     (Entry  (Tabulated-List-Get-Entry))
                                                     (Scope  (And Entry
                                                                  (Substring-No-Properties
                                                                   (Aref Entry 3))))
                                                     (Ns-P   (String= Scope "Namespaced"))
                                                     (Ctx    Kubed-List-Context)
                                                     (Buf    (Get-Buffer-Create
                                                              (Format "*Kubed %S[%S]*" Plural Ctx))))
                                                    (With-Current-Buffer Buf
                                                                         (Let ((Inhibit-Read-Only T))
                                                                              (Erase-Buffer)
                                                                              (Apply #'Call-Process
                                                                                     Kubed-Kubectl-Program Nil T Nil
                                                                                     "Get" Plural "-O" "Wide"
                                                                                     (Append
                                                                                      (When Ctx (List "--Context" Ctx))
                                                                                      (When Ns-P (List "--All-Namespaces"))))
                                                                              (Goto-Char (Point-Min))
                                                                              (Special-Mode)))
                                                    (Display-Buffer Buf))))
      T)

;;; ═══════════════════════════════════════════════════════════════
;;; § 8–9.  Strimzi + Plain Resources
;;; ═══════════════════════════════════════════════════════════════

(Eval '(Kubed-Define-Resource Kafka ()
                              :Suffixes ([("P" "Pause Reconciliation"  Kubed-Kafkas-Pause)
                                          ("R" "Resume Reconciliation" Kubed-Kafkas-Resume)])
                              (Pause "P" "Pause Reconciliation For"
                                     (Kubed-Ext-Strimzi-Pause-Reconciliation
                                      "Kafka" Kafka Kubed-List-Context Kubed-List-Namespace)
                                     (Kubed-List-Update T))
                              (Resume "R" "Resume Reconciliation For"
                                      (Kubed-Ext-Strimzi-Resume-Reconciliation
                                       "Kafka" Kafka Kubed-List-Context Kubed-List-Namespace)
                                      (Kubed-List-Update T)))
      T)

(Eval '(Kubed-Define-Resource Kafkaconnect ()
                              :Plural Kafkaconnects
                              :Prefix (("$" "Scale" Kubed-Scale-Kafkaconnect))
                              :Suffixes ([("$" "Scale"                 Kubed-Kafkaconnects-Scale)
                                          ("P" "Pause Reconciliation"  Kubed-Kafkaconnects-Pause)
                                          ("R" "Resume Reconciliation" Kubed-Kafkaconnects-Resume)])
                              (Scale "$" "Scale"
                                     (Kubed-Ext-Scale-Kafkaconnect
                                      Kafkaconnect
                                      (If Current-Prefix-Arg
                                          (Prefix-Numeric-Value Current-Prefix-Arg)
                                          (Read-Number "Number Of Replicas: "))
                                      Kubed-List-Context Kubed-List-Namespace))
                              (Pause "P" "Pause Reconciliation For"
                                     (Kubed-Ext-Strimzi-Pause-Reconciliation
                                      "Kafkaconnect" Kafkaconnect
                                      Kubed-List-Context Kubed-List-Namespace)
                                     (Kubed-List-Update T))
                              (Resume "R" "Resume Reconciliation For"
                                      (Kubed-Ext-Strimzi-Resume-Reconciliation
                                       "Kafkaconnect" Kafkaconnect
                                       Kubed-List-Context Kubed-List-Namespace)
                                      (Kubed-List-Update T)))
      T)

(Eval '(Kubed-Define-Resource Kafkaconnector ()
                              :Suffixes ([("P" "Pause"   Kubed-Kafkaconnectors-Pause)
                                          ("R" "Resume"  Kubed-Kafkaconnectors-Resume)
                                          ("X" "Restart" Kubed-Kafkaconnectors-Restart)])
                              (Pause "P" "Pause"
                                     (Kubed-Patch "Kafkaconnectors" Kafkaconnector
                                                  (Kubed-Ext--Json-Patch "Pause" "True")
                                                  Kubed-List-Context Kubed-List-Namespace "Merge")
                                     (Kubed-List-Update T))
                              (Resume "R" "Resume"
                                      (Kubed-Patch "Kafkaconnectors" Kafkaconnector
                                                   (Kubed-Ext--Json-Patch "Pause" "False")
                                                   Kubed-List-Context Kubed-List-Namespace "Merge")
                                      (Kubed-List-Update T))
                              (Restart "X" "Restart"
                                       (Kubed-Ext-Strimzi-Restart-Connector
                                        Kafkaconnector Kubed-List-Context Kubed-List-Namespace)
                                       (Kubed-List-Update T)))
      T)

(Eval '(Kubed-Define-Resource Kafkatopic ()
                              :Suffixes ([("P" "Set Partitions" Kubed-Kafkatopics-Set-Partitions)])
                              (Set-Partitions "P" "Set Partitions For"
                                              (Let ((N (Read-Number "Number Of Partitions: ")))
                                                   (Kubed-Patch "Kafkatopics" Kafkatopic
                                                                (Kubed-Ext--Json-Patch "Partitions"
                                                                                       (Number-To-String N))
                                                                Kubed-List-Context Kubed-List-Namespace
                                                                "Merge"))
                                              (Kubed-List-Update T)))
      T)

(Dolist (Spec '((Kafkabridge) (Kafkamirrormaker2) (Kafkamirrormaker)
                (Kafkanodepool) (Kafkarebalance) (Kafkauser) (Strimzipodset)))
        (Eval (Cons 'Kubed-Define-Resource Spec) T))

(Dolist (Spec '((Endpoint) (Limitrange) (Podtemplate) (Replicationcontroller)
                (Resourcequota) (Serviceaccount) (Controllerrevision) (Lease)
                (Endpointslice) (Rolebinding) (Role)
                (Azureapplicationgatewayrewrite) (Podmonitor) (Servicemonitor)))
        (Eval (Cons 'Kubed-Define-Resource Spec) T))

;;; ═══════════════════════════════════════════════════════════════
;;; § 9a.  Human-Readable Age For All Timestamp Columns
;;; ═══════════════════════════════════════════════════════════════

(Defvar Kubed-Ext--Timestamp-Patched-Types
  (Make-Hash-Table :Test 'Equal)
  "Resource Types Whose Timestamp Columns Carry A Formatter.")

(Defun Kubed-Ext--Timestamp-Column-Spec-P (Spec)
  "Non-Nil If Spec Names A Kubernetes Timestamp Column.
Spec Is A Fetch-Column String Such As
`Creationtimestamp:.Metadata.Creationtimestamp'."
  (And
   (Stringp Spec)
   (Or (String-Match-P "\\.Creationtimestamp" Spec)
       (String-Match-P "\\.Starttime" Spec)
       (String-Match-P "\\.Lasttimestamp" Spec)
       (String-Match-P "\\.Lastscheduletime" Spec)
       (String-Match-P "\\.Lastsuccessfultime" Spec)
       (String-Match-P "\\.Deletiontimestamp" Spec)
       (String-Match-P "\\`Age:" Spec)
       (String-Match-P "\\`Last.Seen:" Spec)
       (String-Match-P "\\`Lastschedule:" Spec)
       (String-Match-P "\\`Lastsuccess:" Spec)
       (String-Match-P "\\`Starttime:" Spec)
       (String-Match-P "Timestamp:" Spec))))

(Defun Kubed-Ext--Patch-Type-Timestamp-Columns (Type)
  "Attach Age Formatter To Timestamp Columns For Type.
Pods And Deployments Are Skipped Because They Format
Ages Inside Their Custom Entries Functions."
  (Unless (Or (Gethash Type Kubed-Ext--Timestamp-Patched-Types)
              (Member Type '("Pods" "Deployments")))
          (Puthash Type T Kubed-Ext--Timestamp-Patched-Types)
          (Let ((Columns (Alist-Get Type Kubed--Columns
                                    Nil Nil #'String=)))
               (When Columns
                     (Setf (Alist-Get Type Kubed--Columns
                                      Nil Nil #'String=)
                           (Mapcar
                            (Lambda (Col)
                                    (Let ((Spec (If (Consp Col)
                                                    (Car Col)
                                                    Col)))
                                         (If (And (Kubed-Ext--Timestamp-Column-Spec-P
                                                   Spec)
                                                  (Not (And (Consp Col) (Cdr Col))))
                                             (Cons Spec #'Kubed-Ext--Format-Age)
                                             Col)))
                            Columns))))))

(Defun Kubed-Ext--Patch-All-Timestamp-Columns ()
  "Patch Timestamp Columns For All Known Resource Types."
  (Dolist (Entry Kubed--Columns)
          (Kubed-Ext--Patch-Type-Timestamp-Columns (Car Entry))))

(Defun Kubed-Ext--Fix-Timestamp-Display-Columns ()
  "Rename Timestamp Display Columns To Short Labels.
Install Numeric Sort Via The `Kubed-Sort-Value' Text
Property That `Kubed-Ext--Format-Age' Attaches."
  (Dolist (Type-Name '("Services" "Secrets" "Jobs"
                       "Replicasets" "Daemonsets"
                       "Statefulsets" "Ingressclasses"
                       "Namespaces" "Nodes" "Events"
                       "Cronjobs"))
          (Let ((Fmt-Var (Intern
                          (Format "Kubed-%S-Columns"
                                  Type-Name))))
               (When (And (Boundp Fmt-Var)
                          (Symbol-Value Fmt-Var))
                     (Let ((Idx 0))
                          (Set Fmt-Var
                               (Mapcar
                                (Lambda (Col)
                                        (Cl-Incf Idx)
                                        (Let ((Nm (Downcase (Car Col))))
                                             (Cond
                                              ((Or (String= Nm "Creationtimestamp")
                                                   (String= Nm "Starttime"))
                                               (Append
                                                (List "Age" 10
                                                      (Kubed-Ext-Top-Numeric-Sorter
                                                       Idx))
                                                (Nthcdr 3 Col)))
                                              ((String= Nm "Last-Seen")
                                               (Append
                                                (List "Last Seen" 10
                                                      (Kubed-Ext-Top-Numeric-Sorter
                                                       Idx))
                                                (Nthcdr 3 Col)))
                                              ((String= Nm "Lastschedule")
                                               (Append
                                                (List "Last Sched" 10
                                                      (Kubed-Ext-Top-Numeric-Sorter
                                                       Idx))
                                                (Nthcdr 3 Col)))
                                              ((String= Nm "Lastsuccess")
                                               (Append
                                                (List "Last Ok" 10
                                                      (Kubed-Ext-Top-Numeric-Sorter
                                                       Idx))
                                                (Nthcdr 3 Col)))
                                              (T Col))))
                                (Symbol-Value Fmt-Var))))))))

(Defun Kubed-Ext--Ensure-Age-Formatting
  (Orig-Fn Type Context &Optional Namespace)
  "Around-Advice For `Kubed-Update'.
Patch Timestamp Columns For Type Before Calling
Orig-Fn With Type, Context, And Namespace."
  (Kubed-Ext--Patch-Type-Timestamp-Columns Type)
  (Funcall Orig-Fn Type Context Namespace))

(Advice-Add 'Kubed-Update :Around
            #'Kubed-Ext--Ensure-Age-Formatting)

;; Apply Patches Now -- All Kubed-Define-Resource Forms
;; Have Already Been Evaluated.
(Kubed-Ext--Patch-All-Timestamp-Columns)
(Kubed-Ext--Fix-Timestamp-Display-Columns)

;;; ═══════════════════════════════════════════════════════════════
;;; § 10.  Async Helpers
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext--Async-Kubectl (Args Callback &Optional Errback)
  "Run Kubectl With Args Asynchronously.
Call Callback With Output String On Success.
Call Errback With Error String On Failure."
  (Let* ((Output-Buf (Generate-New-Buffer " *Kubed-Ext-Async*"))
         (Default-Directory Temporary-File-Directory))
        (Make-Process
         :Name "Kubed-Ext-Kubectl"
         :Buffer Output-Buf
         :Command (Cons Kubed-Kubectl-Program Args)
         :Noquery T
         :Sentinel
         (Lambda (Proc _Event)
                 (When (Memq (Process-Status Proc) '(Exit Signal))
                       (Let ((Exit-Code (Process-Exit-Status Proc))
                             (Output ""))
                            (When (Buffer-Live-P Output-Buf)
                                  (Setq Output (With-Current-Buffer Output-Buf (Buffer-String)))
                                  (Kill-Buffer Output-Buf))
                            (If (Zerop Exit-Code)
                                (Funcall Callback Output)
                                (When Errback
                                      (Funcall Errback
                                               (Format "Kubectl Exited %D" Exit-Code))))))))))

(Defun Kubed-Ext--Async-Kubectl-2 (Args1 Args2 Callback &Optional Errback)
  "Run Two Kubectl Commands Concurrently.
Args1 And Args2 Are Arg Lists.
Callback Is Called With (List Out1 Out2) On Success.
Errback Is Called With Error Message On Failure."
  (Let ((Results (Cons Nil Nil))
        (Count 0)
        (Failed Nil))
       (Kubed-Ext--Async-Kubectl
        Args1
        (Lambda (Out)
                (Unless Failed
                        (Setcar Results Out)
                        (Cl-Incf Count)
                        (When (= Count 2)
                              (Funcall Callback (List (Car Results) (Cdr Results))))))
        (Lambda (Err)
                (Unless Failed (Setq Failed T) (When Errback (Funcall Errback Err)))))
       (Kubed-Ext--Async-Kubectl
        Args2
        (Lambda (Out)
                (Unless Failed
                        (Setcdr Results Out)
                        (Cl-Incf Count)
                        (When (= Count 2)
                              (Funcall Callback (List (Car Results) (Cdr Results))))))
        (Lambda (Err)
                (Unless Failed (Setq Failed T) (When Errback (Funcall Errback Err)))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 10b.  Top Commands
;;; ═══════════════════════════════════════════════════════════════

(Defvar-Local Kubed-Ext-Top-Context Nil "Context For Top Buffer.")
(Defvar-Local Kubed-Ext-Top-Namespace Nil "Namespace For Top Buffer.")
(Defvar-Local Kubed-Ext-Top--Fetching Nil "Non-Nil When Async Fetch In Progress.")

(Defun Kubed-Ext-Top-Refresh ()
  "Refresh The Current Top Buffer Asynchronously."
  (Interactive)
  (If Kubed-Ext-Top--Fetching
      (Message "Metrics Fetch Already In Progress...")
      (Setq Kubed-Ext-Top--Fetching T)
      (Setq Tabulated-List-Entries Nil)
      (Let ((Inhibit-Read-Only T))
           (Erase-Buffer)
           (Insert (Propertize "\N  Fetching Metrics From Api...\N" 'Face 'Shadow)))
      (Cond
       ((Derived-Mode-P 'Kubed-Ext-Top-Nodes-Mode) (Kubed-Ext--Top-Nodes-Fetch))
       ((Derived-Mode-P 'Kubed-Ext-Top-Pods-Mode)  (Kubed-Ext--Top-Pods-Fetch)))))

(Defun Kubed-Ext--Top-Nodes-Fetch ()
  "Fetch Node Metrics Asynchronously And Populate The Table."
  (Let ((Ctx Kubed-Ext-Top-Context) (Buf (Current-Buffer)))
       (Kubed-Ext--Async-Kubectl-2
        (List "Top" "Nodes" "--No-Headers" "--Context" Ctx)
        (List "Get" "Nodes" "--No-Headers" "--Context" Ctx "-O"
              (Concat "Custom-Columns=Name:.Metadata.Name,"
                      "Cpu:.Status.Allocatable.Cpu,Mem:.Status.Allocatable.Memory"))
        (Lambda (Results)
                (When (Buffer-Live-P Buf)
                      (With-Current-Buffer Buf
                                           (Setq Kubed-Ext-Top--Fetching Nil)
                                           (Let ((Alloc-Map (Make-Hash-Table :Test 'Equal)) (Entries Nil))
                                                (Dolist (Line (Split-String (Nth 1 Results) "\N" T))
                                                        (Let ((F (Split-String Line Nil T)))
                                                             (Puthash (Nth 0 F) (List (Kubed-Ext-Parse-Cpu (Nth 1 F))
                                                                                      (Kubed-Ext-Parse-Mem (Nth 2 F)))
                                                                      Alloc-Map)))
                                                (Dolist (Line (Split-String (Nth 0 Results) "\N" T))
                                                        (Let* ((F (Split-String Line Nil T))
                                                               (Name (Nth 0 F))
                                                               (Cpu-U (Kubed-Ext-Parse-Cpu (Nth 1 F)))
                                                               (Mem-U (Kubed-Ext-Parse-Mem (Nth 3 F)))
                                                               (Alloc (Gethash Name Alloc-Map '(0 0)))
                                                               (Cpu-A (Nth 0 Alloc)) (Mem-A (Nth 1 Alloc)))
                                                              (Push (List Name (Vector Name
                                                                                       (Kubed-Ext-Format-Cpu Cpu-U)
                                                                                       (Kubed-Ext-Format-Cpu Cpu-A)
                                                                                       (Kubed-Ext-Format-Pct Cpu-U Cpu-A)
                                                                                       (Kubed-Ext-Format-Mem Mem-U)
                                                                                       (Kubed-Ext-Format-Mem Mem-A)
                                                                                       (Kubed-Ext-Format-Pct Mem-U Mem-A)))
                                                                    Entries)))
                                                (Setq Tabulated-List-Entries (Nreverse Entries))
                                                (Tabulated-List-Print T)))))
        (Lambda (Err)
                (When (Buffer-Live-P Buf)
                      (With-Current-Buffer Buf
                                           (Setq Kubed-Ext-Top--Fetching Nil)
                                           (Message "Node Metrics Failed: %S" Err)))))))

(Defun Kubed-Ext--Top-Pods-Fetch ()
  "Fetch Pod Metrics Asynchronously And Populate The Table."
  (Let ((Ctx Kubed-Ext-Top-Context) (Ns Kubed-Ext-Top-Namespace)
        (Buf (Current-Buffer)))
       (Kubed-Ext--Async-Kubectl-2
        (Append (List "Top" "Pods" "--No-Headers" "--Context" Ctx)
                (When Ns (List "-N" Ns)))
        (Append (List "Get" "Pods" "--No-Headers" "--Context" Ctx "-O"
                      (Concat "Custom-Columns=Name:.Metadata.Name,"
                              "Cr:.Spec.Containers[*].Resources.Requests.Cpu,"
                              "Cl:.Spec.Containers[*].Resources.Limits.Cpu,"
                              "Mr:.Spec.Containers[*].Resources.Requests.Memory,"
                              "Ml:.Spec.Containers[*].Resources.Limits.Memory"))
                (When Ns (List "-N" Ns)))
        (Lambda (Results)
                (When (Buffer-Live-P Buf)
                      (With-Current-Buffer Buf
                                           (Setq Kubed-Ext-Top--Fetching Nil)
                                           (Let ((Spec-Map (Make-Hash-Table :Test 'Equal)) (Entries Nil))
                                                (Dolist (Line (Split-String (Nth 1 Results) "\N" T))
                                                        (Let ((F (Split-String Line Nil T)))
                                                             (Puthash (Nth 0 F) (List (Kubed-Ext-Sum-Cpu (Nth 1 F))
                                                                                      (Kubed-Ext-Sum-Cpu (Nth 2 F))
                                                                                      (Kubed-Ext-Sum-Mem (Nth 3 F))
                                                                                      (Kubed-Ext-Sum-Mem (Nth 4 F)))
                                                                      Spec-Map)))
                                                (Dolist (Line (Split-String (Nth 0 Results) "\N" T))
                                                        (Let* ((F (Split-String Line Nil T))
                                                               (Name (Nth 0 F))
                                                               (Cpu-U (Kubed-Ext-Parse-Cpu (Nth 1 F)))
                                                               (Mem-U (Kubed-Ext-Parse-Mem (Nth 2 F)))
                                                               (Spec (Gethash Name Spec-Map '(0 0 0 0))))
                                                              (Push (List Name
                                                                          (Vector Name
                                                                                  (Kubed-Ext-Format-Cpu Cpu-U)
                                                                                  (Kubed-Ext-Format-Mem Mem-U)
                                                                                  (Kubed-Ext-Format-Cpu (Nth 0 Spec))
                                                                                  (Kubed-Ext-Format-Cpu (Nth 1 Spec))
                                                                                  (Kubed-Ext-Format-Pct Cpu-U (Nth 0 Spec))
                                                                                  (Kubed-Ext-Format-Pct Cpu-U (Nth 1 Spec))
                                                                                  (Kubed-Ext-Format-Mem (Nth 2 Spec))
                                                                                  (Kubed-Ext-Format-Mem (Nth 3 Spec))
                                                                                  (Kubed-Ext-Format-Pct Mem-U (Nth 2 Spec))
                                                                                  (Kubed-Ext-Format-Pct Mem-U (Nth 3 Spec))))
                                                                    Entries)))
                                                (Setq Tabulated-List-Entries (Nreverse Entries))
                                                (Tabulated-List-Print T)))))
        (Lambda (Err)
                (When (Buffer-Live-P Buf)
                      (With-Current-Buffer Buf
                                           (Setq Kubed-Ext-Top--Fetching Nil)
                                           (Message "Pod Metrics Failed: %S" Err)))))))

(Define-Derived-Mode Kubed-Ext-Top-Nodes-Mode Tabulated-List-Mode
  "Kubed Top Nodes" "Major Mode For Displaying Node Resource Usage."
  (Setq Tabulated-List-Format
        (Vector (List "Name" 40 T)
                (List "Cpu"   8 (Kubed-Ext-Top-Numeric-Sorter 1) :Right-Align T)
                (List "Cpu/A" 8 (Kubed-Ext-Top-Numeric-Sorter 2) :Right-Align T)
                (List "%Cpu"  6 (Kubed-Ext-Top-Numeric-Sorter 3) :Right-Align T)
                (List "Mem"   8 (Kubed-Ext-Top-Numeric-Sorter 4) :Right-Align T)
                (List "Mem/A" 8 (Kubed-Ext-Top-Numeric-Sorter 5) :Right-Align T)
                (List "%Mem"  6 (Kubed-Ext-Top-Numeric-Sorter 6) :Right-Align T))
        Tabulated-List-Padding 2
        Tabulated-List-Entries Nil)
  (Tabulated-List-Init-Header))

(Define-Derived-Mode Kubed-Ext-Top-Pods-Mode Tabulated-List-Mode
  "Kubed Top Pods" "Major Mode For Displaying Pod Resource Usage."
  (Setq Tabulated-List-Format
        (Vector (List "Name"   48 T)
                (List "Cpu"     7 (Kubed-Ext-Top-Numeric-Sorter 1)  :Right-Align T)
                (List "Mem"     8 (Kubed-Ext-Top-Numeric-Sorter 2)  :Right-Align T)
                (List "Cpu/R"   7 (Kubed-Ext-Top-Numeric-Sorter 3)  :Right-Align T)
                (List "Cpu/L"   7 (Kubed-Ext-Top-Numeric-Sorter 4)  :Right-Align T)
                (List "%Cpu/R"  7 (Kubed-Ext-Top-Numeric-Sorter 5)  :Right-Align T)
                (List "%Cpu/L"  7 (Kubed-Ext-Top-Numeric-Sorter 6)  :Right-Align T)
                (List "Mem/R"   8 (Kubed-Ext-Top-Numeric-Sorter 7)  :Right-Align T)
                (List "Mem/L"   8 (Kubed-Ext-Top-Numeric-Sorter 8)  :Right-Align T)
                (List "%Mem/R"  7 (Kubed-Ext-Top-Numeric-Sorter 9)  :Right-Align T)
                (List "%Mem/L"  7 (Kubed-Ext-Top-Numeric-Sorter 10) :Right-Align T))
        Tabulated-List-Padding 2
        Tabulated-List-Entries Nil)
  (Tabulated-List-Init-Header))

(Keymap-Set Kubed-Ext-Top-Nodes-Mode-Map "G" #'Kubed-Ext-Top-Refresh)
(Keymap-Set Kubed-Ext-Top-Nodes-Mode-Map "Q" #'Quit-Window)
(Keymap-Set Kubed-Ext-Top-Nodes-Mode-Map "B" #'Kubed-Ext-Switch-Buffer)
(Keymap-Set Kubed-Ext-Top-Pods-Mode-Map  "G" #'Kubed-Ext-Top-Refresh)
(Keymap-Set Kubed-Ext-Top-Pods-Mode-Map  "Q" #'Quit-Window)
(Keymap-Set Kubed-Ext-Top-Pods-Mode-Map  "B" #'Kubed-Ext-Switch-Buffer)

(Defun Kubed-Ext-Top-Nodes (&Optional Context)
  "Display Node Resource Usage In Context."
  (Interactive
   (List (If Current-Prefix-Arg
             (Kubed-Read-Context "Context" (Kubed-Local-Context))
             (Kubed-Local-Context))))
  (Let* ((Context (Or Context (Kubed-Local-Context)))
         (Buf (Get-Buffer-Create (Format "*Kubed-Top-Nodes[%S]*" Context))))
        (With-Current-Buffer Buf
                             (Kubed-Ext-Top-Nodes-Mode)
                             (Setq-Local Kubed-Ext-Top-Context Context)
                             (Kubed-Ext-Top-Refresh))
        (Display-Buffer Buf)))

(Defun Kubed-Ext-Top-Pods (&Optional Context Namespace)
  "Display Pod Resource Usage In Context And Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List C N)))
  (Let* ((Context (Or Context (Kubed-Local-Context)))
         (Namespace (Or Namespace (Kubed--Namespace Context)))
         (Buf (Get-Buffer-Create
               (Format "*Kubed-Top-Pods@%S[%S]*" Namespace Context))))
        (With-Current-Buffer Buf
                             (Kubed-Ext-Top-Pods-Mode)
                             (Setq-Local Kubed-Ext-Top-Context Context)
                             (Setq-Local Kubed-Ext-Top-Namespace Namespace)
                             (Kubed-Ext-Top-Refresh))
        (Display-Buffer Buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 11.  Terminal Support (Vterm, Eat, Eshell, Ansi-Term)
;;; ═══════════════════════════════════════════════════════════════

(Defvar Kubed-Ext--Tramp-Optimized Nil
  "Non-Nil When Tramp Has Already Been Optimized For Kubed.")

(Defun Kubed-Ext-Optimize-Tramp ()
  "Configure Tramp To Cache Connections, Improving Eshell/Dired Speed."
  (Unless Kubed-Ext--Tramp-Optimized
          (Setq Kubed-Ext--Tramp-Optimized T)
          (Require 'Tramp)
          (When (Require 'Kubed-Tramp Nil T)
                (When (Boundp 'Kubed-Tramp-Method)
                      (Add-To-List 'Tramp-Connection-Properties
                                   (List (Regexp-Quote (Format "/%S:" Kubed-Tramp-Method))
                                         "Direct-Async-Process" T))))
          (When (Boundp 'Tramp-Completion-Reread-Directory-Timeout)
                (Setq Tramp-Completion-Reread-Directory-Timeout Nil))))

(Add-Hook 'Kubed-List-Mode-Hook #'Kubed-Ext-Optimize-Tramp)

(Defun Kubed-Ext--Kubectl-Exec-Command (Pod Container Context Namespace Shell)
  "Build Kubectl Exec Command For Pod/Container/Context/Namespace/Shell."
  (Mapconcat #'Shell-Quote-Argument
             (Append (List Kubed-Kubectl-Program "Exec" "-It" Pod
                           "-C" Container)
                     (When Namespace (List "-N" Namespace))
                     (When Context   (List "--Context" Context))
                     (List "--" Shell))
             " "))

(Defun Kubed-Ext-Pods-Vterm (Click)
  "Open Vterm In Kubernetes Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (Unless (Require 'Vterm Nil T)
          (User-Error "This Command Requires The `Vterm' Package"))
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Container (Kubed-Read-Container Pod "Container" T
                                                  Kubed-List-Context Kubed-List-Namespace))
                 (Vterm-Shell (Kubed-Ext--Kubectl-Exec-Command
                               Pod Container Kubed-List-Context Kubed-List-Namespace
                               Kubed-Ext-Pod-Shell))
                 (Vterm-Buffer-Name
                  (Format "*Kubed Vterm %S*"
                          (Kubed-Display-Resource-Short-Description
                           "Pods" Pod Kubed-List-Context Kubed-List-Namespace))))
                (Vterm Vterm-Buffer-Name))
          (User-Error "No Kubernetes Pod At Point")))

(Defun Kubed-Ext-Vterm-Pod (Pod &Optional Context Namespace)
  "Open Vterm In Kubernetes Pod In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List (Kubed-Read-Pod "Open Vterm In Pod" Nil Nil C N) C N)))
  (Unless (Require 'Vterm Nil T)
          (User-Error "This Command Requires The `Vterm' Package"))
  (Let* ((Context (Or Context (Kubed-Local-Context)))
         (Namespace (Or Namespace (Kubed--Namespace Context)))
         (Container (Kubed-Read-Container Pod "Container" T Context Namespace))
         (Vterm-Shell (Kubed-Ext--Kubectl-Exec-Command
                       Pod Container Context Namespace Kubed-Ext-Pod-Shell))
         (Vterm-Buffer-Name
          (Format "*Kubed Vterm %S*"
                  (Kubed-Display-Resource-Short-Description
                   "Pods" Pod Context Namespace))))
        (Vterm Vterm-Buffer-Name)))

(Defun Kubed-Ext-Pods-Eat (Click)
  "Open Eat Terminal In Kubernetes Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (Unless (Require 'Eat Nil T)
          (User-Error "This Command Requires The `Eat' Package"))
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Container (Kubed-Read-Container Pod "Container" T
                                                  Kubed-List-Context Kubed-List-Namespace))
                 (Cmd (Kubed-Ext--Kubectl-Exec-Command
                       Pod Container Kubed-List-Context Kubed-List-Namespace
                       Kubed-Ext-Pod-Shell))
                 (Eat-Buffer-Name
                  (Format "*Kubed Eat %S*"
                          (Kubed-Display-Resource-Short-Description
                           "Pods" Pod Kubed-List-Context Kubed-List-Namespace))))
                (Eat Cmd T))
          (User-Error "No Kubernetes Pod At Point")))

(Defun Kubed-Ext-Eat-Pod (Pod &Optional Context Namespace)
  "Open Eat Terminal In Kubernetes Pod In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List (Kubed-Read-Pod "Open Eat In Pod" Nil Nil C N) C N)))
  (Unless (Require 'Eat Nil T)
          (User-Error "This Command Requires The `Eat' Package"))
  (Let* ((Context (Or Context (Kubed-Local-Context)))
         (Namespace (Or Namespace (Kubed--Namespace Context)))
         (Container (Kubed-Read-Container Pod "Container" T Context Namespace))
         (Cmd (Kubed-Ext--Kubectl-Exec-Command
               Pod Container Context Namespace Kubed-Ext-Pod-Shell))
         (Eat-Buffer-Name
          (Format "*Kubed Eat %S*"
                  (Kubed-Display-Resource-Short-Description
                   "Pods" Pod Context Namespace))))
        (Eat Cmd T)))

(Defun Kubed-Ext-Pods-Eshell (Click)
  "Open Eshell In Kubernetes Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (Require 'Kubed-Tramp) (Kubed-Tramp-Assert-Support)
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Default-Directory
                   (Kubed-Remote-File-Name
                    Kubed-List-Context Kubed-List-Namespace Pod))
                 (Eshell-Buffer-Name
                  (Format "*Kubed Eshell %S*"
                          (Kubed-Display-Resource-Short-Description
                           "Pods" Pod Kubed-List-Context Kubed-List-Namespace))))
                (Eshell T))
          (User-Error "No Kubernetes Pod At Point")))

(Defun Kubed-Ext-Eshell-Pod (Pod &Optional Context Namespace)
  "Open Eshell In Kubernetes Pod In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List (Kubed-Read-Pod "Open Eshell In Pod" Nil Nil C N) C N)))
  (Require 'Kubed-Tramp) (Kubed-Tramp-Assert-Support)
  (Let* ((Context (Or Context (Kubed-Local-Context)))
         (Namespace (Or Namespace (Kubed--Namespace Context)))
         (Default-Directory (Kubed-Remote-File-Name Context Namespace Pod))
         (Eshell-Buffer-Name
          (Format "*Kubed Eshell %S*"
                  (Kubed-Display-Resource-Short-Description
                   "Pods" Pod Context Namespace))))
        (Eshell T)))

(Defun Kubed-Ext-Pods-Ansi-Term (Click)
  "Open `Ansi-Term' In Kubernetes Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Container (Kubed-Read-Container Pod "Container" T
                                                  Kubed-List-Context Kubed-List-Namespace))
                 (Cmd (Kubed-Ext--Kubectl-Exec-Command
                       Pod Container Kubed-List-Context Kubed-List-Namespace
                       Kubed-Ext-Pod-Shell))
                 (Buf-Name (Format "Kubed Term %S"
                                   (Kubed-Display-Resource-Short-Description
                                    "Pods" Pod Kubed-List-Context Kubed-List-Namespace))))
                (With-Current-Buffer (Ansi-Term "/Bin/Bash" Buf-Name)
                                     (Process-Send-String (Get-Buffer-Process (Current-Buffer))
                                                          (Concat Cmd "\N"))))
          (User-Error "No Kubernetes Pod At Point")))

(Defun Kubed-Ext-Pods-Shell-Command (Click)
  "Run A Shell Command Via Kubectl Exec In Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Container (Kubed-Read-Container Pod "Container" T
                                                  Kubed-List-Context Kubed-List-Namespace))
                 (Prefix (Concat
                          (Mapconcat #'Shell-Quote-Argument
                                     (Append (List Kubed-Kubectl-Program
                                                   "Exec" "-It" Pod "-C" Container)
                                             (When Kubed-List-Namespace
                                                   (List "-N" Kubed-List-Namespace))
                                             (When Kubed-List-Context
                                                   (List "--Context" Kubed-List-Context))
                                             (List "--"))
                                     " ")
                          " "))
                 (Command (Read-String "Shell Command: " Prefix)))
                (Shell-Command Command))
          (User-Error "No Kubernetes Pod At Point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 12.  Describe Resource
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-List-Describe-Resource (Click)
  "Describe Kubernetes Resource At Click Position Using Kubectl Describe."
  (Interactive (List Last-Nonmenu-Event) Kubed-List-Mode)
  (If-Let ((Name (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Type Kubed-List-Type)
                 (Namespace Kubed-List-Namespace)
                 (Context Kubed-List-Context)
                 (Buf (Get-Buffer-Create
                       (Format "*Kubed Describe %S/%S@%S[%S]*"
                               Type Name
                               (Or Namespace "Default")
                               (Or Context "Current")))))
                (With-Current-Buffer Buf
                                     (Let ((Inhibit-Read-Only T))
                                          (Erase-Buffer)
                                          (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                                                 (Append (List "Describe" Type Name)
                                                         (When Namespace (List "-N" Namespace))
                                                         (When Context (List "--Context" Context))))
                                          (Goto-Char (Point-Min))
                                          (Special-Mode)))
                (Display-Buffer Buf))
          (User-Error "No Kubernetes Resource At Point")))

(Defun Kubed-Ext-Describe-Resource (Type Name &Optional Context Namespace)
  "Describe Resource Name Of Type In Context And Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (Type (Kubed-Read-Resource-Type "Type To Describe" Nil C))
          (Ns (When (Kubed-Namespaced-P Type C)
                    (Kubed--Namespace C Current-Prefix-Arg)))
          (Name (Kubed-Read-Resource-Name Type "Describe" Nil Nil C Ns)))
         (List Type Name C Ns)))
  (Let* ((Ctx (Or Context (Kubed-Local-Context)))
         (Ns (Or Namespace
                 (When (Kubed-Namespaced-P Type Ctx)
                       (Kubed--Namespace Ctx))))
         (Buf (Get-Buffer-Create
               (Format "*Kubed Describe %S/%S@%S[%S]*"
                       Type Name (Or Ns "Cluster") Ctx))))
        (With-Current-Buffer Buf
                             (Let ((Inhibit-Read-Only T))
                                  (Erase-Buffer)
                                  (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                                         (Append (List "Describe" Type Name)
                                                 (When Ns (List "-N" Ns))
                                                 (When Ctx (List "--Context" Ctx))))
                                  (Goto-Char (Point-Min))
                                  (Special-Mode)))
        (Display-Buffer Buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 12a.  Initcontainer Logs
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Pod-Init-Containers (Pod &Optional Context Namespace)
  "Return List Of Init Container Names In Pod In Context/Namespace, Or Nil."
  (Condition-Case Nil
                  (Let ((Output (Car (Apply #'Process-Lines
                                            Kubed-Kubectl-Program "Get" "Pod" Pod
                                            "-O" "Jsonpath={.Spec.Initcontainers[*].Name}"
                                            (Append
                                             (When Namespace (List "--Namespace" Namespace))
                                             (When Context (List "--Context" Context)))))))
                       (When (And Output
                                  (Not (Kubed-Ext-None-P Output)))
                             (Split-String Output " " T)))
                  (Error Nil)))

(Defun Kubed-Ext-Logs-Init-Container (&Optional Context Namespace)
  "Show Logs For An Init Container Of A Pod In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List C N)))
  (Let* ((Ctx (Or Context (Kubed-Local-Context)))
         (Ns (Or Namespace (Kubed--Namespace Ctx)))
         (Pod (Kubed-Read-Pod "Pod" Nil Nil Ctx Ns))
         (Init-Containers (Kubed-Ext-Pod-Init-Containers Pod Ctx Ns)))
        (Unless Init-Containers
                (User-Error "Pod `%S' Has No Init Containers" Pod))
        (Let ((Container (If (= (Length Init-Containers) 1)
                             (Car Init-Containers)
                             (Completing-Read "Init Container: "
                                              Init-Containers Nil T))))
             (Kubed-Logs "Pods" Pod Ctx Ns Container Nil Nil Nil Nil Nil Nil))))

(Defun Kubed-Ext-Pods-Logs-Init-Container (Click)
  "Show Init Container Logs For Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let ((Init-Containers (Kubed-Ext-Pod-Init-Containers
                                  Pod Kubed-List-Context Kubed-List-Namespace)))
               (Unless Init-Containers
                       (User-Error "Pod `%S' Has No Init Containers" Pod))
               (Let ((Container (If (= (Length Init-Containers) 1)
                                    (Car Init-Containers)
                                    (Completing-Read "Init Container: "
                                                     Init-Containers Nil T))))
                    (Kubed-Logs "Pods" Pod Kubed-List-Context Kubed-List-Namespace
                                Container Nil Nil Nil Nil Nil Nil)))
          (User-Error "No Kubernetes Pod At Point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 12b.  Logs By Label Selector
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Logs-By-Label (&Optional Context Namespace)
  "Show Logs For Pods Matching A Label Selector In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List C N)))
  (Let* ((Ctx (Or Context (Kubed-Local-Context)))
         (Ns (Or Namespace (Kubed--Namespace Ctx)))
         (Labels (Kubed-Ext-Discover-Labels "Pods" Ctx Ns))
         (Label (Completing-Read "Label Selector: " Labels Nil Nil
                                 Nil 'Kubed-Ext-Label-Selector-History))
         (Tail (Read-Number "Tail Lines: " 100))
         (Buf (Generate-New-Buffer
               (Format "*Kubed-Logs -L %S@%S[%S]*" Label Ns Ctx))))
        (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
        (Start-Process "*Kubed-Logs-By-Label*" Buf
                       Kubed-Kubectl-Program "Logs" "-L" Label
                       "--Tail" (Number-To-String Tail)
                       "-N" Ns "--Context" Ctx)
        (Display-Buffer Buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 13.  Rollout Management
;;; ═══════════════════════════════════════════════════════════════

(Defvar-Local Kubed-Ext-Rollout-Type Nil)
(Defvar-Local Kubed-Ext-Rollout-Name Nil)
(Defvar-Local Kubed-Ext-Rollout-Context Nil)
(Defvar-Local Kubed-Ext-Rollout-Namespace Nil)

(Defun Kubed-Ext-Rollout-History (Type Name &Optional Context Namespace)
  "Show Rollout History For Type/Name In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg))
          (Type (Completing-Read "Resource Type: "
                                 '("Deployment" "Statefulset" "Daemonset") Nil T))
          (Name (Kubed-Read-Resource-Name
                 (Concat Type "S") "Rollout History For" Nil Nil C N)))
         (List Type Name C N)))
  (Let* ((Ctx (Or Context (Kubed-Local-Context)))
         (Ns (Or Namespace (Kubed--Namespace Ctx)))
         (Typename (Format "%S/%S" Type Name))
         (Buf (Get-Buffer-Create
               (Format "*Kubed Rollout %S@%S[%S]*"
                       Typename (Or Ns "Default") Ctx))))
        (With-Current-Buffer Buf
                             (Let ((Inhibit-Read-Only T))
                                  (Erase-Buffer)
                                  (Insert (Propertize (Format "Rollout History: %S\N" Typename)
                                                      'Face 'Bold))
                                  (Insert (Format "Namespace: %S  Context: %S\N" (Or Ns "Default") Ctx))
                                  (Insert (Make-String 60 ?─) "\N\N")
                                  (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                                         "Rollout" "History" Typename
                                         (Append (When Ns (List "-N" Ns))
                                                 (When Ctx (List "--Context" Ctx))))
                                  (Insert "\N" (Make-String 60 ?─) "\N")
                                  (Insert (Propertize
                                           "\Nkeys: R = View Revision  U = Undo  G = Refresh  Q = Quit\N"
                                           'Face 'Shadow))
                                  (Goto-Char (Point-Min))
                                  (Setq-Local Kubed-Ext-Rollout-Type Type
                                              Kubed-Ext-Rollout-Name Name
                                              Kubed-Ext-Rollout-Context Ctx
                                              Kubed-Ext-Rollout-Namespace Ns)
                                  (Kubed-Ext-Rollout-History-Mode)))
        (Display-Buffer Buf)))

(Defun Kubed-Ext-Rollout-Show-Revision ()
  "Show A Specific Revision From The Rollout History Buffer."
  (Interactive Nil Kubed-Ext-Rollout-History-Mode)
  (Let* ((Typename (Format "%S/%S" Kubed-Ext-Rollout-Type
                           Kubed-Ext-Rollout-Name))
         (Ctx Kubed-Ext-Rollout-Context)
         (Ns Kubed-Ext-Rollout-Namespace)
         (Rev (Read-Number "Revision: "))
         (Buf (Get-Buffer-Create
               (Format "*Kubed Rollout %S Rev %D*" Typename Rev))))
        (With-Current-Buffer Buf
                             (Let ((Inhibit-Read-Only T))
                                  (Erase-Buffer)
                                  (Insert (Propertize (Format "Revision %D: %S\N" Rev Typename)
                                                      'Face 'Bold))
                                  (Insert (Make-String 60 ?─) "\N\N")
                                  (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                                         "Rollout" "History" Typename (Format "--Revision=%D" Rev)
                                         (Append (When Ns (List "-N" Ns))
                                                 (When Ctx (List "--Context" Ctx))))
                                  (Goto-Char (Point-Min))
                                  (Special-Mode)))
        (Display-Buffer Buf)))

(Defun Kubed-Ext-Rollout-Undo (Type Name &Optional Revision Context Namespace)
  "Undo Rollout Of Type/Name To Revision In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg))
          (Type (Completing-Read "Resource Type: "
                                 '("Deployment" "Statefulset" "Daemonset") Nil T))
          (Name (Kubed-Read-Resource-Name
                 (Concat Type "S") "Undo Rollout For" Nil Nil C N))
          (Rev (When (Y-Or-N-P "Specify Target Revision? ")
                     (Read-Number "To Revision: "))))
         (List Type Name Rev C N)))
  (Let* ((Ctx (Or Context (Kubed-Local-Context)))
         (Ns (Or Namespace (Kubed--Namespace Ctx)))
         (Typename (Format "%S/%S" Type Name)))
        (Unless (Zerop (Apply #'Call-Process Kubed-Kubectl-Program Nil Nil Nil
                              "Rollout" "Undo" Typename
                              (Append
                               (When Revision
                                     (List (Format "--To-Revision=%D" Revision)))
                               (When Ns (List "-N" Ns))
                               (When Ctx (List "--Context" Ctx)))))
                (User-Error "Failed To Undo Rollout For %S" Typename))
        (Message "Rolled Back %S%S." Typename
                 (If Revision (Format " To Revision %D" Revision) ""))))

(Defun Kubed-Ext-Rollout-Undo-From-History ()
  "Undo Rollout From History Buffer."
  (Interactive Nil Kubed-Ext-Rollout-History-Mode)
  (Let* ((Typename (Format "%S/%S" Kubed-Ext-Rollout-Type
                           Kubed-Ext-Rollout-Name))
         (Rev (When (Y-Or-N-P "Specify Revision? ")
                    (Read-Number "To Revision: "))))
        (When (Y-Or-N-P (Format "Undo Rollout For %S%S? "
                                Typename
                                (If Rev (Format " To Rev %D" Rev) "")))
              (Kubed-Ext-Rollout-Undo Kubed-Ext-Rollout-Type Kubed-Ext-Rollout-Name
                                      Rev Kubed-Ext-Rollout-Context
                                      Kubed-Ext-Rollout-Namespace))))

(Defun Kubed-Ext-Rollout-Refresh ()
  "Refresh Rollout History Buffer."
  (Interactive Nil Kubed-Ext-Rollout-History-Mode)
  (Kubed-Ext-Rollout-History Kubed-Ext-Rollout-Type Kubed-Ext-Rollout-Name
                             Kubed-Ext-Rollout-Context
                             Kubed-Ext-Rollout-Namespace))

(Define-Derived-Mode Kubed-Ext-Rollout-History-Mode Special-Mode "Kubed Rollout"
  "Mode For Viewing Kubernetes Rollout History."
  :Interactive Nil)

(Keymap-Set Kubed-Ext-Rollout-History-Mode-Map "R"
            #'Kubed-Ext-Rollout-Show-Revision)
(Keymap-Set Kubed-Ext-Rollout-History-Mode-Map "U"
            #'Kubed-Ext-Rollout-Undo-From-History)
(Keymap-Set Kubed-Ext-Rollout-History-Mode-Map "G"
            #'Kubed-Ext-Rollout-Refresh)
(Keymap-Set Kubed-Ext-Rollout-History-Mode-Map "Q" #'Quit-Window)

(Defun Kubed-Ext-Deployments-Rollout-History (Click)
  "Show Rollout History For Deployment At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Deployments-Mode)
  (If-Let ((Dep (Kubed-Ext--Resource-At-Event Click)))
          (Kubed-Ext-Rollout-History "Deployment" Dep
                                     Kubed-List-Context Kubed-List-Namespace)
          (User-Error "No Kubernetes Deployment At Point")))

(Defun Kubed-Ext-Deployments-Rollout-Undo (Click)
  "Undo Rollout For Deployment At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Deployments-Mode)
  (If-Let ((Dep (Kubed-Ext--Resource-At-Event Click)))
          (Let ((Rev (When (Y-Or-N-P "Specify Target Revision? ")
                           (Read-Number "To Revision: "))))
               (When (Y-Or-N-P (Format "Undo Rollout For %S?" Dep))
                     (Kubed-Ext-Rollout-Undo "Deployment" Dep Rev
                                             Kubed-List-Context Kubed-List-Namespace)
                     (Kubed-List-Update T)))
          (User-Error "No Kubernetes Deployment At Point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 14.  Jab / Bounce Deployment
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Jab-Deployment (Deployment &Optional Context Namespace)
  "Force Rolling Update Of Deployment In Context/Namespace."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List (Kubed-Read-Deployment "Jab (Bounce) Deployment" Nil Nil C N) C N)))
  (Let* ((Ctx (Or Context (Kubed-Local-Context)))
         (Ns (Or Namespace (Kubed--Namespace Ctx)))
         (Q Kubed-Ext--Dq)
         (Ts (Number-To-String (Floor (Float-Time))))
         (Patch (Concat "{" Q "Spec" Q ":{" Q "Template" Q ":{" Q "Metadata" Q
                        ":{" Q "Labels" Q ":{" Q "Date" Q ":" Q Ts Q "}}}}}")))
        (Unless (Zerop (Apply #'Call-Process Kubed-Kubectl-Program Nil Nil Nil
                              "Patch" "Deployment" Deployment "-P" Patch
                              (Append (When Ns (List "-N" Ns))
                                      (When Ctx (List "--Context" Ctx)))))
                (User-Error "Failed To Jab Deployment %S" Deployment))
        (Message "Jabbed Deployment %S (Timestamp %S)." Deployment Ts)))

(Defun Kubed-Ext-Deployments-Jab (Click)
  "Jab Deployment At Click Position To Force A Rolling Update."
  (Interactive (List Last-Nonmenu-Event) Kubed-Deployments-Mode)
  (If-Let ((Dep (Kubed-Ext--Resource-At-Event Click)))
          (Progn (Kubed-Ext-Jab-Deployment Dep Kubed-List-Context
                                           Kubed-List-Namespace)
                 (Kubed-List-Update T))
          (User-Error "No Kubernetes Deployment At Point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 15.  Copy / Clipboard Operations
;;; ═══════════════════════════════════════════════════════════════

(Defvar Kubed-Ext--Last-Kubectl-Command Nil
  "Last Kubectl Command Tracked By Kubed-Ext.")

(Defun Kubed-Ext-List-Copy-Log-Command (Click)
  "Copy `Kubectl Logs -F' Command For Resource At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-List-Mode)
  (If-Let ((Name (Kubed-Ext--Resource-At-Event Click)))
          (Let ((Cmd (Format "%S Logs -F --Tail=100 %S%S%S"
                             Kubed-Kubectl-Program
                             (If (String= Kubed-List-Type "Pods")
                                 ""
                                 (Concat Kubed-List-Type "/"))
                             Name
                             (Concat
                              (When Kubed-List-Namespace
                                    (Format " -N %S" Kubed-List-Namespace))
                              (When Kubed-List-Context
                                    (Format " --Context %S" Kubed-List-Context))))))
               (Kill-New Cmd)
               (Message "Copied: %S" Cmd))
          (User-Error "No Kubernetes Resource At Point")))

(Defun Kubed-Ext-List-Copy-Kubectl-Prefix ()
  "Copy The Current Kubectl Command Prefix."
  (Interactive Nil Kubed-List-Mode)
  (Let ((Prefix (Format "%S%S%S"
                        Kubed-Kubectl-Program
                        (If Kubed-List-Namespace
                            (Format " -N %S" Kubed-List-Namespace) "")
                        (If Kubed-List-Context
                            (Format " --Context %S" Kubed-List-Context) ""))))
       (Kill-New Prefix)
       (Message "Copied: %S" Prefix)))

(Defun Kubed-Ext-List-Copy-Last-Command ()
  "Copy The Last Tracked Kubectl Command."
  (Interactive Nil Kubed-List-Mode)
  (If Kubed-Ext--Last-Kubectl-Command
      (Progn (Kill-New Kubed-Ext--Last-Kubectl-Command)
             (Message "Copied: %S" Kubed-Ext--Last-Kubectl-Command))
      (Message "No Kubectl Command Tracked Yet.")))

(Defun Kubed-Ext-List-Copy-Resource-As-Yaml (Click)
  "Copy Yaml Of Resource At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-List-Mode)
  (If-Let ((Name (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Type Kubed-List-Type)
                 (Namespace Kubed-List-Namespace)
                 (Context Kubed-List-Context)
                 (Yaml (With-Temp-Buffer
                        (Apply #'Call-Process Kubed-Kubectl-Program Nil T Nil
                               "Get" Type Name "-O" "Yaml"
                               (Append
                                (When Namespace (List "-N" Namespace))
                                (When Context (List "--Context" Context))))
                        (Buffer-String))))
                (Kill-New Yaml)
                (Message "Yaml For %S/%S Copied (%D Bytes)." Type Name (Length Yaml)))
          (User-Error "No Kubernetes Resource At Point")))

(Transient-Define-Prefix Kubed-Ext-Copy-Popup ()
                         "Kubed Copy Menu."
                         ["Copy To Kill Ring"
                          ("W" "Resource Name"  Kubed-List-Copy-As-Kill)
                          ("L" "Log Command"    Kubed-Ext-List-Copy-Log-Command)
                          ("P" "Kubectl Prefix" Kubed-Ext-List-Copy-Kubectl-Prefix)
                          ("C" "Last Command"   Kubed-Ext-List-Copy-Last-Command)
                          ("Y" "Resource Yaml"  Kubed-Ext-List-Copy-Resource-As-Yaml)])

;;; ═══════════════════════════════════════════════════════════════
;;; § 16.  Label Selector
;;; ═══════════════════════════════════════════════════════════════

(Defvar Kubed-Ext-Label-Selector-History Nil
  "History List For Label Selectors.")

(Defvar Kubed-Ext--Label-Cache (Make-Hash-Table :Test 'Equal)
  "Label Cache.")

(Defvar Kubed-Ext--Label-Cache-Time (Make-Hash-Table :Test 'Equal)
  "Label Cache Timestamps.")

(Defun Kubed-Ext--Fetch-Labels (Type Context Namespace)
  "Discover Label Key=Value Pairs From Resources Of Type In Context/Namespace."
  (Condition-Case Nil
                  (Let ((Output (With-Temp-Buffer
                                 (Apply #'Call-Process Kubed-Kubectl-Program Nil '(T Nil) Nil
                                        "Get" (Or Type "Pods") "--No-Headers"
                                        "-O" "Custom-Columns=Labels:.Metadata.Labels"
                                        (Append
                                         (When Namespace (List "-N" Namespace))
                                         (When Context (List "--Context" Context))))
                                 (Buffer-String))))
                       (Delete-Dups
                        (Mapcan (Lambda (Line)
                                        (Let ((Trimmed (String-Trim Line)))
                                             (When (String-Match "^Map$$$.+$$$" Trimmed)
                                                   (Mapcar (Lambda (Pair)
                                                                   (Replace-Regexp-In-String ":" "=" Pair))
                                                           (Split-String (Match-String 1 Trimmed) " ")))))
                                (Split-String Output "\N" T))))
                  (Error Nil)))

(Defun Kubed-Ext-Discover-Labels (&Optional Type Context Namespace)
  "Discover Labels Of Type In Context/Namespace With 60-Second Caching."
  (Let* ((Key (List (Or Type "Pods") (Or Context "") (Or Namespace "")))
         (Cached (Gethash Key Kubed-Ext--Label-Cache))
         (Cache-Time (Gethash Key Kubed-Ext--Label-Cache-Time 0)))
        (If (And Cached (< (- (Float-Time) Cache-Time) 60))
            Cached
            (Let ((Labels (Kubed-Ext--Fetch-Labels Type Context Namespace)))
                 (Puthash Key Labels Kubed-Ext--Label-Cache)
                 (Puthash Key (Float-Time) Kubed-Ext--Label-Cache-Time)
                 Labels))))

(Defun Kubed-Ext--Find-Active-Selector (Type Context Namespace)
  "Find The Label Selector For Type/Context/Namespace."
  (Catch 'Found
         (Dolist (Buf (Buffer-List))
                 (When (Buffer-Live-P Buf)
                       (With-Current-Buffer Buf
                                            (When (And (Derived-Mode-P 'Kubed-List-Mode)
                                                       Kubed-Ext-List-Label-Selector
                                                       (Equal Kubed-List-Type Type)
                                                       (Equal Kubed-List-Context Context)
                                                       (Equal Kubed-List-Namespace Namespace))
                                                  (Throw 'Found Kubed-Ext-List-Label-Selector)))))))

(Defun Kubed-Ext-Update-With-Selector (Orig-Fn Type Context &Optional Namespace)
  "Advice For Orig-Fn: Inject --Selector For Type/Context/Namespace."
  (Let ((Sel (Kubed-Ext--Find-Active-Selector Type Context Namespace)))
       (If (Null Sel)
           (Funcall Orig-Fn Type Context Namespace)
           (Cl-Letf* ((Real-Mp (Symbol-Function 'Make-Process))
                      ((Symbol-Function 'Make-Process)
                       (Lambda (&Rest Args)
                               (Let ((Cmd (Plist-Get Args :Command)))
                                    (When (And Cmd (Member "Get" Cmd))
                                          (Plist-Put Args :Command
                                                     (Append Cmd (List "--Selector" Sel)))))
                               (Apply Real-Mp Args))))
                     (Funcall Orig-Fn Type Context Namespace)))))

(Advice-Add 'Kubed-Update :Around #'Kubed-Ext-Update-With-Selector)

(Defun Kubed-Ext-List-Set-Label-Selector (Selector)
  "Set Label Selector For Server-Side Filtering.  Empty Clears It."
  (Interactive
   (List (Completing-Read
          (Format-Prompt "Label Selector" "Clear")
          (Append '("")
                  (Kubed-Ext-Discover-Labels
                   Kubed-List-Type Kubed-List-Context Kubed-List-Namespace)
                  Kubed-Ext-Label-Selector-History)
          Nil Nil Nil 'Kubed-Ext-Label-Selector-History))
   Kubed-List-Mode)
  (Setq-Local Kubed-Ext-List-Label-Selector
              (If (String-Empty-P Selector) Nil Selector))
  (Kubed-List-Update))

;; Replaced In Section § 21
;; (Setq Kubed-List-Mode-Line-Format
;;       '(:Eval
;;         (If (Process-Live-P
;;              (Alist-Get 'Process (Kubed--Alist Kubed-List-Type
;;                                                Kubed-List-Context
;;                                                Kubed-List-Namespace)))
;;             (Propertize " [...]" 'Help-Echo "Updating...")
;;           (Concat
;;            (When Kubed-List-Filter
;;              (Propertize
;;               (Concat " [" (Mapconcat #'Prin1-To-String
;;                                       Kubed-List-Filter " ") "]")
;;               'Help-Echo "Current Filter"))
;;            (When Kubed-Ext-List-Label-Selector
;;              (Propertize
;;               (Concat " {" Kubed-Ext-List-Label-Selector "}")
;;               'Help-Echo "Label Selector" 'Face 'Warning))
;;            (When (And (Bound-And-True-P Kubed-Ext-Resource-Filter)
;;                       (Not (String-Empty-P Kubed-Ext-Resource-Filter)))
;;              (Propertize
;;               (Concat " /" Kubed-Ext-Resource-Filter "/")
;;               'Help-Echo "Substring Filter" 'Face 'Italic))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 17.  Buffer Switching + Command Log
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Switch-Buffer ()
  "Switch To Another Kubed Buffer.

Includes List Buffers, Wide-View Buffers, And Top/Metrics Buffers."
  (Interactive)
  (Let (Bufs)
       (Dolist (Buf (Buffer-List))
               (When (With-Current-Buffer Buf
                                          (Or (Derived-Mode-P 'Kubed-List-Mode)
                                              (Derived-Mode-P 'Kubed-Ext-Wide-Mode)
                                              (Derived-Mode-P 'Kubed-Ext-Top-Nodes-Mode)
                                              (Derived-Mode-P 'Kubed-Ext-Top-Pods-Mode)
                                              (Derived-Mode-P 'Kubed-Ext-Port-Forwards-Mode)))
                     (Push (Cons (Kubed-Ext--Buffer-Switch-Candidate Buf) Buf) Bufs)))
       (Setq Bufs (Nreverse Bufs))
       (If Bufs
           (Let* ((Sel (Completing-Read "Switch To Kubed Buffer: " Bufs Nil T))
                  (Buf (Alist-Get Sel Bufs Nil Nil #'String=)))
                 (Switch-To-Buffer Buf))
           (User-Error "No Kubed Buffers Found"))))

(Defun Kubed-Ext--Buffer-Switch-Candidate (Buf)
  "Return A Human-Readable Completion Candidate String For Buf."
  (With-Current-Buffer Buf
                       (Cond
                        ((Derived-Mode-P 'Kubed-List-Mode)
                         (Format "%-14s  %S"
                                 (Or (Bound-And-True-P Kubed-List-Type) "")
                                 (Buffer-Name Buf)))
                        ((Derived-Mode-P 'Kubed-Ext-Wide-Mode)
                         (Format "%-14s  %S"
                                 (Format "Wide:%S" (Or Kubed-Ext-Wide--Type ""))
                                 (Buffer-Name Buf)))
                        ((Derived-Mode-P 'Kubed-Ext-Top-Nodes-Mode)
                         (Format "%-14s  %S"
                                 (Format "Top:Nodes@%S" (Or Kubed-Ext-Top-Context ""))
                                 (Buffer-Name Buf)))
                        ((Derived-Mode-P 'Kubed-Ext-Top-Pods-Mode)
                         (Format "%-14s  %S"
                                 (Format "Top:Pods@%S" (Or Kubed-Ext-Top-Namespace ""))
                                 (Buffer-Name Buf)))
                        ((Derived-Mode-P 'Kubed-Ext-Port-Forwards-Mode)
                         (Format "%-14s  %S" "Portfwds" (Buffer-Name Buf)))
                        (T (Buffer-Name Buf)))))

(Defun Kubed-Ext--Log-Kubectl-Command (Cmd-Str)
  "Log Cmd-Str To The Kubectl Command Log Buffer.
Creates The Buffer If It Does Not Exist."
  (Setq Kubed-Ext--Last-Kubectl-Command Cmd-Str)
  (Let ((Buf (Get-Buffer-Create "*Kubed-Command-Log*")))
       (With-Current-Buffer Buf
                            (Unless (Derived-Mode-P 'Special-Mode)
                                    (Special-Mode))
                            (Let ((Inhibit-Read-Only T))
                                 (Goto-Char (Point-Max))
                                 (Insert (Format "[%S] %S\N"
                                                 (Format-Time-String "%H:%M:%S") Cmd-Str))
                                 (When (> (Line-Number-At-Pos) 500)
                                       (Goto-Char (Point-Min))
                                       (Forward-Line 100)
                                       (Delete-Region (Point-Min) (Point)))))))

(Defun Kubed-Ext--Make-Process-Logger (Orig-Fn &Rest Args)
  "Log Kubectl `Make-Process' Call, Including Shell-Wrapped Invocations.

Handles Two Forms:
  Direct:  (:Command (Kubectl Get Pods ...))
  Wrapped: (:Command (/Bin/Sh -C \"Kubectl Logs ... | Rg ...\"))

Orig-Fn Is The Advised Function, Args Its Arguments."
  (Let ((Cmd (Plist-Get Args :Command)))
       (When (And (Consp Cmd) (Stringp (Car Cmd)))
             (Let ((Prog (File-Name-Nondirectory
                          (File-Name-Sans-Extension (Car Cmd)))))
                  (Cond
                   ;; Direct Kubectl Invocation.
                   ((String-Suffix-P "Kubectl" Prog)
                    (Kubed-Ext--Log-Kubectl-Command
                     (Mapconcat #'Identity (Seq-Filter #'Stringp Cmd) " ")))
                   ;; Shell Wrapper: /Bin/Sh -C "Kubectl ... | Rg ..."
                   ((Member Prog '("Sh" "Bash" "Zsh"))
                    (Let ((Shell-Arg (Car (Last (Seq-Filter #'Stringp Cmd)))))
                         (When (And (Stringp Shell-Arg)
                                    (String-Match-P "Kubectl" Shell-Arg))
                               (Kubed-Ext--Log-Kubectl-Command Shell-Arg))))))))
  (Apply Orig-Fn Args))

(Advice-Add 'Make-Process :Around #'Kubed-Ext--Make-Process-Logger)

(Defun Kubed-Ext--Call-Process-Logger (Orig-Fn Program &Rest Args)
  "Log Kubectl `Call-Process'.
Orig-Fn Is The Advised Function.  Program And Args Are Passed Through."
  (When (And (Stringp Program)
             (String-Suffix-P
              "Kubectl"
              (File-Name-Sans-Extension
               (File-Name-Nondirectory Program))))
        (Kubed-Ext--Log-Kubectl-Command
         (Mapconcat #'Identity
                    (Cons Program (Seq-Filter #'Stringp (Nthcdr 3 Args)))
                    " ")))
  (Apply Orig-Fn Program Args))

(Advice-Add 'Call-Process :Around #'Kubed-Ext--Call-Process-Logger)

(Defun Kubed-Ext--Call-Process-Region-Logger
  (Orig-Fn Start End Program &Rest Args)
  "Log Kubectl `Call-Process-Region'.
Orig-Fn Is Advised.  Start End Program Args Are Passed Through."
  (When (And (Stringp Program)
             (String-Suffix-P
              "Kubectl"
              (File-Name-Sans-Extension
               (File-Name-Nondirectory Program))))
        (Kubed-Ext--Log-Kubectl-Command
         (Mapconcat #'Identity
                    (Cons Program (Seq-Filter #'Stringp (Nthcdr 3 Args)))
                    " ")))
  (Apply Orig-Fn Start End Program Args))

(Advice-Add 'Call-Process-Region :Around
            #'Kubed-Ext--Call-Process-Region-Logger)

(Defun Kubed-Ext-Show-Command-Log ()
  "Show The Kubed Command Log Buffer."
  (Interactive)
  (Let ((Buf (Get-Buffer-Create "*Kubed-Command-Log*")))
       (With-Current-Buffer Buf
                            (Unless (Derived-Mode-P 'Special-Mode) (Special-Mode)))
       (Display-Buffer Buf)))

;;; ─── Process Buffer Access ────────────────────────────────────

(Defun Kubed-Ext-Show-Process-Buffer ()
  "Show The Kubectl Process/Stderr Buffer For The Current List Resource."
  (Interactive Nil Kubed-List-Mode)
  (Let* ((Type Kubed-List-Type)
         (Buf  (Or (Get-Buffer (Format " *Kubed-Get-%S-Stderr*" Type))
                   (Get-Buffer (Format "*Kubed-Get-%S*"         Type))
                   (Get-Buffer (Format " *Kubed-Get-%S*"        Type)))))
        (If (And Buf (Buffer-Live-P Buf))
            (Display-Buffer Buf)
            (User-Error "No Process Buffer Found For %S" Type))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 18.  Keybindings
;;; ═══════════════════════════════════════════════════════════════

;; ── Pods ──────────────────────────────────────────────────────────
(Keymap-Set Kubed-Pods-Mode-Map  "T" #'Kubed-Ext-Top-Pods)
(Keymap-Set Kubed-Pods-Mode-Map  "M" #'Kubed-Ext-Top-Pods)
(Keymap-Set Kubed-Pods-Mode-Map  "V" #'Kubed-Ext-Pods-Vterm)
(Keymap-Set Kubed-Pods-Mode-Map  "T" #'Kubed-Ext-Pods-Eat)
(Keymap-Set Kubed-Pods-Mode-Map  "I" #'Kubed-Ext-Pods-Logs-Init-Container)
(Keymap-Set Kubed-Pod-Prefix-Map "T" #'Kubed-Ext-Top-Pods)
(Keymap-Set Kubed-Pod-Prefix-Map "V" #'Kubed-Ext-Vterm-Pod)
(Keymap-Set Kubed-Pod-Prefix-Map "T" #'Kubed-Ext-Eat-Pod)
(Keymap-Set Kubed-Pod-Prefix-Map "S" #'Kubed-Ext-Eshell-Pod)
(Keymap-Set Kubed-Pod-Prefix-Map "I" #'Kubed-Ext-Logs-Init-Container)

;; ── Nodes ─────────────────────────────────────────────────────────
(Keymap-Set Kubed-Nodes-Mode-Map  "M" #'Kubed-Ext-Top-Nodes)
(Keymap-Set Kubed-Node-Prefix-Map "T" #'Kubed-Ext-Top-Nodes)

;; ── Services ──────────────────────────────────────────────────────
(Keymap-Set Kubed-Services-Mode-Map  "F" #'Kubed-Ext-Services-Forward-Port)
(Keymap-Set Kubed-Service-Prefix-Map "F" #'Kubed-Ext-Forward-Port-To-Service)

;; ── Deployments ───────────────────────────────────────────────────
(Keymap-Set Kubed-Deployments-Mode-Map "F" #'Kubed-Ext-Deployments-Forward-Port)
(Keymap-Set Kubed-Deployments-Mode-Map "R" #'Kubed-Ext-Deployments-Rollout-History)
(Keymap-Set Kubed-Deployments-Mode-Map "J" #'Kubed-Ext-Deployments-Jab)
(Keymap-Set Kubed-Deployments-Mode-Map "U" #'Kubed-Ext-Deployments-Rollout-Undo)
(Keymap-Set Kubed-Deployments-Mode-Map "M-U" #'Kubed-Ext-Unmark-All)
(Keymap-Set Kubed-Deployment-Prefix-Map "F" #'Kubed-Ext-Forward-Port-To-Deployment)
(Keymap-Set Kubed-Deployment-Prefix-Map "R" #'Kubed-Ext-Rollout-History)
(Keymap-Set Kubed-Deployment-Prefix-Map "J" #'Kubed-Ext-Jab-Deployment)
(Keymap-Set Kubed-Deployment-Prefix-Map "U" #'Kubed-Ext-Rollout-Undo)

;; ── Kubed-List-Mode (Parent Of All List Modes) ────────────────────
;;   "D" Intentionally Overrides Kubed'S Mark-For-Deletion (K9s-Style).
;;   "%" Preserves The Old Mark-For-Deletion Workflow.
(Keymap-Set Kubed-List-Mode-Map "D"   #'Kubed-Ext-List-Describe-Resource)
(Keymap-Set Kubed-List-Mode-Map "%"   #'Kubed-List-Mark-For-Deletion)
(Keymap-Set Kubed-List-Mode-Map "S"   #'Kubed-Ext-List-Set-Label-Selector)
(Keymap-Set Kubed-List-Mode-Map "C"   #'Kubed-Ext-Copy-Popup)
(Keymap-Set Kubed-List-Mode-Map "B"   #'Kubed-Ext-Switch-Buffer)
(Keymap-Set Kubed-List-Mode-Map "F"   #'Kubed-Ext-Set-Filter)
(Keymap-Set Kubed-List-Mode-Map "V"   #'Kubed-Ext-List-Wide)
(Keymap-Set Kubed-List-Mode-Map "M"   #'Kubed-Ext-Mark-Item)
(Keymap-Set Kubed-List-Mode-Map "M-M" #'Kubed-Ext-Mark-All)
(Keymap-Set Kubed-List-Mode-Map "M-U" #'Kubed-Ext-Unmark-All)
(Keymap-Set Kubed-List-Mode-Map "M-N" #'Kubed-Ext-Jump-To-Next-Highlight)
(Keymap-Set Kubed-List-Mode-Map "M-P" #'Kubed-Ext-Jump-To-Previous-Highlight)
(Keymap-Set Kubed-List-Mode-Map "X"   #'Kubed-Ext-Delete-Marked)
(Keymap-Set Kubed-List-Mode-Map "A"   #'Kubed-Ext-Auto-Refresh-Mode)
(Keymap-Set Kubed-List-Mode-Map "R"   #'Kubed-Ext-Switch-Resource)
(Keymap-Set Kubed-List-Mode-Map "$"   #'Kubed-Ext-Show-Process-Buffer)

;; ── Kubed-Prefix-Map ──────────────────────────────────────────────
;;   "D" Belongs To Deployments; Use "K" (Kubectl Describe Mnemonic).
(Keymap-Set Kubed-Prefix-Map "K" #'Kubed-Ext-Describe-Resource)
(Keymap-Set Kubed-Prefix-Map "F" #'Kubed-Ext-List-Port-Forwards)
(Keymap-Set Kubed-Prefix-Map "B" #'Kubed-Ext-Switch-Buffer)
(Keymap-Set Kubed-Prefix-Map "#" #'Kubed-Ext-Show-Command-Log)
(Keymap-Set Kubed-Prefix-Map "L" #'Kubed-Ext-Logs-By-Label)

(With-Eval-After-Load 'Kubed
                      (When (Boundp 'Kubed-Deployments-Mode-Map)
                            (Keymap-Set Kubed-Deployments-Mode-Map "M-U" #'Kubed-Ext-Unmark-All)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 19.  Transient Navigation + Resource Actions
;;; ═══════════════════════════════════════════════════════════════

(Defvar Kubed-Ext-Extra-Transient-Suffixes
  '(("Pods"        . [("T" "Top (Metrics)"     Kubed-Ext-Top-Pods)
                      ("V" "Vterm"              Kubed-Ext-Pods-Vterm)
                      ("T" "Eat Terminal"       Kubed-Ext-Pods-Eat)
                      ("E" "Eshell"             Kubed-Ext-Pods-Eshell)
                      ("A" "Ansi-Term"          Kubed-Ext-Pods-Ansi-Term)
                      ("&" "Shell Command"      Kubed-Ext-Pods-Shell-Command)
                      ("I" "Init Container Log" Kubed-Ext-Pods-Logs-Init-Container)])
    ("Nodes"       . [("T" "Top (Metrics)"  Kubed-Ext-Top-Nodes)])
    ("Services"    . [("F" "Forward Port"   Kubed-Ext-Services-Forward-Port)])
    ("Deployments" . [("F" "Forward Port"       Kubed-Ext-Deployments-Forward-Port)
                      ("H" "Rollout History"    Kubed-Ext-Deployments-Rollout-History)
                      ("J" "Jab (Bounce)"       Kubed-Ext-Deployments-Jab)
                      ("U" "Rollout Undo"       Kubed-Ext-Deployments-Rollout-Undo)]))
  "Additional Per-Resource-Type Transient Suffixes.")

(Defun Kubed-Ext-Switch-Context ()
  "Switch Kubectl Context And Refresh Current Resource List."
  (Interactive)
  (When (Derived-Mode-P 'Kubed-List-Mode)
        (Let* ((Type Kubed-List-Type)
               (Ns Kubed-List-Namespace)
               (Ctx (Kubed-Read-Context "Switch To Context")))
              ;; Immediately Start Prefetching Namespaces For The New Context.
              (Kubed-Ext-Prefetch-Namespaces Ctx)
              (When Type
                    (Let ((Fn (Intern (Format "Kubed-List-%S" Type))))
                         (When (Fboundp Fn)
                               (Cond
                                ;; Non-Namespaced Resource Type.
                                ((Not (Kubed-Namespaced-P Type Ctx))
                                 (Funcall Fn Ctx))
                                ;; Reuse Current Namespace.
                                (Ns (Funcall Fn Ctx Ns))
                                ;; Must Determine Namespace — Ensure List Is Ready First.
                                (T
                                 (Kubed-Ext--Ensure-Namespaces-Ready Ctx)
                                 (Funcall Fn Ctx (Kubed--Namespace Ctx))))))))))

(Defun Kubed-Ext-Switch-Namespace ()
  "Switch Namespace And Refresh Current Resource List."
  (Interactive)
  (When (Derived-Mode-P 'Kubed-List-Mode)
        ;; Ensure Namespace Completions Are Available Before Prompting.
        (Kubed-Ext--Ensure-Namespaces-Ready Kubed-List-Context)
        (Let ((Type Kubed-List-Type)
              (Ctx Kubed-List-Context)
              (Ns (Kubed-Read-Namespace "Switch To Namespace" Nil Nil
                                        Kubed-List-Context)))
             (When Type
                   (Let ((Fn (Intern (Format "Kubed-List-%S" Type))))
                        (When (Fboundp Fn)
                              (If (Kubed-Namespaced-P Type Ctx)
                                  (Funcall Fn Ctx Ns)
                                  (Funcall Fn Ctx))))))))

(Defvar Kubed-Ext-Common-Resources
  '(("Pods" . "Pods")
    ("Deployments" . "Deployments")
    ("Services" . "Services")
    ("Configmaps" . "Configmaps")
    ("Secrets" . "Secrets")
    ("Ingresses" . "Ingresses")
    ("Jobs" . "Jobs")
    ("Cronjobs" . "Cronjobs")
    ("Statefulsets" . "Statefulsets")
    ("Daemonsets" . "Daemonsets")
    ("Replicasets" . "Replicasets")
    ("Persistentvolumeclaims" . "Pvcs")
    ("Persistentvolumes" . "Pvs")
    ("Nodes" . "Nodes")
    ("Namespaces" . "Namespaces")
    ("Events" . "Events")
    ("Networkpolicies" . "Networkpolicies")
    ("Horizontalpodautoscalers" . "Hpas")
    ("Kafkas" . "Kafkas")
    ("Kafkaconnects" . "Kafkaconnects")
    ("Strimzipodsets" . "Strimzipodsets")
    ("Kafkanodepools" . "Kafkanodepools")
    ("Kafkarebalances" . "Kafkarebalances")
    ("Kafkaconnectors" . "Kafkaconnectors")
    ("Kafkatopics" . "Kafkatopics"))
  "Common Resources For Quick Switching.")

(Defun Kubed-Ext-Switch-Resource ()
  "Switch To A Different Resource Type In The Same Context/Namespace."
  (Interactive)
  (When (Derived-Mode-P 'Kubed-List-Mode)
        (Let* ((Ctx Kubed-List-Context)
               (Ns Kubed-List-Namespace)
               (Choices (Mapcar (Lambda (R) (Cons (Cdr R) (Car R)))
                                Kubed-Ext-Common-Resources))
               (Sel (Completing-Read "Switch To Resource: " Choices Nil T))
               (Type (Alist-Get Sel Choices Nil Nil #'String=)))
              (When Type
                    (Let ((Fn (Intern (Format "Kubed-List-%S" Type))))
                         (When (Fboundp Fn)
                               (If (Kubed-Namespaced-P Type Ctx)
                                   (Funcall Fn Ctx (Or Ns (Kubed--Namespace Ctx)))
                                   (Funcall Fn Ctx))))))))

(Defun Kubed-Ext-Jump-Pods ()
  "Jump To Pods."
  (Interactive)
  (Kubed-List-Pods Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext-Jump-Deployments ()
  "Jump To Deployments."
  (Interactive)
  (Kubed-List-Deployments Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext-Jump-Services ()
  "Jump To Services."
  (Interactive)
  (Kubed-List-Services Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext-Jump-Jobs ()
  "Jump To Jobs."
  (Interactive)
  (Kubed-List-Jobs Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext-Jump-Configmaps ()
  "Jump To Configmaps."
  (Interactive)
  (Kubed-List-Configmaps Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext-Jump-Secrets ()
  "Jump To Secrets."
  (Interactive)
  (Kubed-List-Secrets Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext-Jump-Ingresses ()
  "Jump To Ingresses."
  (Interactive)
  (Kubed-List-Ingresses Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext-Jump-Pvcs ()
  "Jump To Pvcs."
  (Interactive)
  (Kubed-List-Persistentvolumeclaims Kubed-List-Context Kubed-List-Namespace))

(Defun Kubed-Ext--Collect-Action-Groups ()
  "Build Transient Group Vectors For The Current Resource Type."
  (Let* ((Type (Or Kubed-List-Type ""))
         (Type-Label (Capitalize Type))
         (Upstream (Bound-And-True-P Kubed-List-Transient-Extra-Suffixes))
         (Custom (Alist-Get Type Kubed-Ext-Extra-Transient-Suffixes
                            Nil Nil #'String=))
         (All (Append (When Upstream Upstream) (When Custom (List Custom))))
         (Result Nil)
         (Gidx 0))
        (Dolist (Gv All)
                (Cl-Incf Gidx)
                (Let* ((Entries (If (Vectorp Gv) (Append Gv Nil) Gv))
                       (Kw-Args Nil)
                       (Suffixes Nil)
                       (Rest Entries))
                      (While Rest
                             (Cond
                              ((Keywordp (Car Rest))
                               (Push (Pop Rest) Kw-Args)
                               (When Rest (Push (Pop Rest) Kw-Args)))
                              ((And (Listp (Car Rest))
                                    (>= (Length (Car Rest)) 3)
                                    (Stringp (Nth 0 (Car Rest)))
                                    (Stringp (Nth 1 (Car Rest))))
                               (Push (Pop Rest) Suffixes))
                              (T (Pop Rest))))
                      (Setq Kw-Args (Nreverse Kw-Args)
                            Suffixes (Nreverse Suffixes))
                      (When Suffixes
                            (Push (Apply #'Vector
                                         (Format "%S Actions%S" Type-Label
                                                 (If (> (Length All) 1)
                                                     (Format " (%D)" Gidx) ""))
                                         (Append Kw-Args Suffixes))
                                  Result))))
        (Nreverse Result)))

(Declare-Function Kubed-Ext--Dynamic-Menu "Kubed-Ext")

(Defun Kubed-Ext-Transient-Menu ()
  "Show Transient Combining Navigation, Actions, And Utilities."
  (Interactive)
  (Require 'Transient)
  (Require 'Kubed-Transient Nil T)
  (Let* ((In-List (Derived-Mode-P 'Kubed-List-Mode))
         (Type (And In-List Kubed-List-Type))
         (Agroups (And In-List (Kubed-Ext--Collect-Action-Groups))))
        (Condition-Case Err
                        (Progn
                         (Eval
                          `(Transient-Define-Prefix Kubed-Ext--Dynamic-Menu ()
                                                    ,(Format "Kubed %S" (Or Type "Navigation"))
                                                    ,@(If In-List
                                                          '([["Navigate"
                                                              ("C" "Switch Context"   Kubed-Ext-Switch-Context)
                                                              ("N" "Switch Namespace" Kubed-Ext-Switch-Namespace)
                                                              ("R" "Switch Resource"  Kubed-Ext-Switch-Resource)]
                                                             ["Jump Workloads"
                                                              ("1" "Pods"        Kubed-Ext-Jump-Pods)
                                                              ("2" "Deployments" Kubed-Ext-Jump-Deployments)
                                                              ("3" "Services"    Kubed-Ext-Jump-Services)
                                                              ("4" "Jobs"        Kubed-Ext-Jump-Jobs)]
                                                             ["Jump Config+Net"
                                                              ("5" "Configmaps"  Kubed-Ext-Jump-Configmaps)
                                                              ("6" "Secrets"     Kubed-Ext-Jump-Secrets)
                                                              ("7" "Ingresses"   Kubed-Ext-Jump-Ingresses)
                                                              ("8" "Pvcs"        Kubed-Ext-Jump-Pvcs)]])
                                                          '([["Open Resource List"
                                                              ("1" "Pods"        Kubed-List-Pods)
                                                              ("2" "Deployments" Kubed-List-Deployments)
                                                              ("3" "Services"    Kubed-List-Services)
                                                              ("4" "Jobs"        Kubed-List-Jobs)
                                                              ("5" "Configmaps"  Kubed-List-Configmaps)
                                                              ("6" "Secrets"     Kubed-List-Secrets)]]))
                                                    ,@(Cond
                                                       ((Null Agroups) Nil)
                                                       ((= (Length Agroups) 1) Agroups)
                                                       ((<= (Length Agroups) 3)
                                                        (List (Apply #'Vector Agroups)))
                                                       (T (Let ((Rows Nil) (Rest Agroups))
                                                               (While Rest
                                                                      (Let ((Chunk (Seq-Take Rest 3)))
                                                                           (Push (If (= (Length Chunk) 1) (Car Chunk)
                                                                                     (Apply #'Vector Chunk))
                                                                                 Rows)
                                                                           (Setq Rest (Seq-Drop Rest 3))))
                                                               (Nreverse Rows))))
                                                    ,@(When In-List
                                                            '([["General"
                                                                ("G" "Refresh"          Kubed-List-Update :Transient T)
                                                                ("D" "Describe"         Kubed-Ext-List-Describe-Resource)
                                                                ("/" "Filter (S-Expr)"  Kubed-List-Set-Filter)
                                                                ("F" "Filter (Substr)"  Kubed-Ext-Set-Filter)
                                                                ("S" "Label Selector"   Kubed-Ext-List-Set-Label-Selector)
                                                                ("V" "Wide View"        Kubed-Ext-List-Wide)
                                                                ("A" "Auto-Refresh"     Kubed-Ext-Auto-Refresh-Mode)
                                                                ("Q" "Quit"             Quit-Window)]
                                                               ["Utilities"
                                                                ("B" "Switch Buffer"    Kubed-Ext-Switch-Buffer)
                                                                ("W" "Copy Name"        Kubed-List-Copy-As-Kill)
                                                                ("Y" "Copy Menu"        Kubed-Ext-Copy-Popup)
                                                                ("#" "Command Log"      Kubed-Ext-Show-Command-Log)
                                                                ("M" "Mark Item"        Kubed-Ext-Mark-Item)
                                                                ("X" "Delete Marked"    Kubed-Ext-Delete-Marked)]])))
                          T)
                         (Funcall-Interactively #'Kubed-Ext--Dynamic-Menu))
                        (Error
                         (Message "Dynamic Transient Error: %S" (Error-Message-String Err))
                         (When (Fboundp 'Kubed-List-Transient)
                               (Call-Interactively #'Kubed-List-Transient))))))

(Keymap-Set Kubed-List-Mode-Map "?" #'Kubed-Ext-Transient-Menu)
(Keymap-Set Kubed-List-Mode-Map "C" #'Kubed-Ext-Switch-Context)
(Keymap-Set Kubed-List-Mode-Map "N" #'Kubed-Ext-Switch-Namespace)
(Keymap-Set Kubed-List-Mode-Map "R" #'Kubed-Ext-Switch-Resource)

;;; ═══════════════════════════════════════════════════════════════
;;; § 20.  Kill All Kubed Buffers
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Kill-All-Buffers (&Optional No-Confirm)
  "Kill All Kubed-Related Buffers.

Buffers Are Matched Either By Their Major/Minor Mode (E.G.
`Kubed-List-Mode', `Kubed-Ext-Wide-Mode', Etc.) Or By Their Name
Starting With \"*Kubed\" Or \"*Kubed\" (Case-Insensitive), Including
Internal Space-Prefixed Scratch Buffers Such As \" *Kubed-Get-*\".

With A Prefix Argument No-Confirm, Skip The Confirmation Prompt And
Kill Immediately.

Also Stops Any Active Port-Forward Processes And Cancels The
Auto-Refresh Timer If It Is Running."
  (Interactive "P")
  (Let ((Kubed-Bufs
         (Seq-Filter
          (Lambda (Buf)
                  (Or
                   ;; ── Detect By Major / Minor Mode ──────────────────
                   (With-Current-Buffer Buf
                                        (Or (Derived-Mode-P 'Kubed-List-Mode)
                                            (Derived-Mode-P 'Kubed-Ext-Wide-Mode)
                                            (Derived-Mode-P 'Kubed-Ext-Top-Nodes-Mode)
                                            (Derived-Mode-P 'Kubed-Ext-Top-Pods-Mode)
                                            (Derived-Mode-P 'Kubed-Ext-Port-Forwards-Mode)
                                            (Derived-Mode-P 'Kubed-Ext-Rollout-History-Mode)
                                            (Bound-And-True-P Kubed-Display-Resource-Mode)))
                   ;; ── Detect By Buffer-Name Pattern ─────────────────
                   ;; Matches: *Kubed ...  *Kubed-...  *Kubed Rollout...
                   ;;          " *Kubed-Get-*" (Internal Scratch Buffers)
                   (String-Match-P
                    "\\`[[:Space:]]*\\*[Kk]Ubed"
                    (Buffer-Name Buf))))
          (Buffer-List))))
       (If (Null Kubed-Bufs)
           (Message "No Kubed Buffers To Kill.")
           (When (Or No-Confirm
                     (Yes-Or-No-P
                      (Format "Kill %D Kubed Buffer%S? "
                              (Length Kubed-Bufs)
                              (If (= (Length Kubed-Bufs) 1) "" "S"))))
                 ;; ── Cancel Auto-Refresh Timer ──────────────────────────
                 (When (Fboundp 'Kubed-Ext--Auto-Refresh-Cancel-Timer)
                       (Kubed-Ext--Auto-Refresh-Cancel-Timer))
                 ;; ── Kill Buffers ───────────────────────────────────────
                 (Let ((Killed 0))
                      (Dolist (Buf Kubed-Bufs)
                              (When (Buffer-Live-P Buf)
                                    (Kill-Buffer Buf)
                                    (Cl-Incf Killed)))
                      (Message "Killed %D Kubed Buffer%S."
                               Killed
                               (If (= Killed 1) "" "S")))))))

(Keymap-Set Kubed-Prefix-Map "Q" #'Kubed-Ext-Kill-All-Buffers)
(Keymap-Set Kubed-List-Mode-Map "C-C C-K" #'Kubed-Ext-Kill-All-Buffers)

;;; ═══════════════════════════════════════════════════════════════
;;; § 21.  Circuit Breaker — Intelligent Vpn / Network Recovery
;;; ═══════════════════════════════════════════════════════════════
;;
;; Each Kubectl Context Has An Independent Circuit In One Of Three States:
;;
;;   Closed  — Normal; Auto-Refresh Runs Freely.
;;   Network — Cluster Unreachable (Vpn, Dns, Tls, Timeout).
;;             Trips After `Kubed-Ext-Cb-Threshold' Consecutive Network
;;             Errors.  Recovery Probes With Exponential Back-Off Are
;;             Scheduled Automatically; Circuit Closes On Probe Success.
;;   Auth    — Credentials Expired (Kubelogin, Aadsts, Az-Cli).
;;             Trips After `Kubed-Ext-Cb-Auth-Threshold' Consecutive Auth
;;             Errors.  No Automatic Probing — User Must Re-Authenticate
;;             Then Press `G' Or `Z' To Resume.
;;
;; Both Open States Suppress Auto-Refresh.  Mode-Line Indicators:
;;   ⚡~Xs     Network Outage  (X = Seconds Until Next Probe)
;;   🔑 Re-Auth  Credential Issue

(Defgroup Kubed-Ext-Circuit-Breaker Nil
  "Per-Context Circuit Breaker For Kubed-Ext Auto-Refresh."
  :Group 'Kubed-Ext)

(Defcustom Kubed-Ext-Cb-Threshold 3
  "Consecutive Network Failures Before The Circuit Opens For A Context."
  :Type 'Natnum
  :Group 'Kubed-Ext-Circuit-Breaker)

(Defcustom Kubed-Ext-Cb-Auth-Threshold 2
  "Consecutive Auth Failures Before Auto-Refresh Is Paused For A Context."
  :Type 'Natnum
  :Group 'Kubed-Ext-Circuit-Breaker)

(Defcustom Kubed-Ext-Cb-Initial-Backoff 30
  "Seconds Before The First Network-Recovery Probe After The Circuit Opens."
  :Type 'Natnum
  :Group 'Kubed-Ext-Circuit-Breaker)

(Defcustom Kubed-Ext-Cb-Backoff-Factor 2.0
  "Multiplier Applied To The Probe Interval After Each Failed Probe."
  :Type 'Number
  :Group 'Kubed-Ext-Circuit-Breaker)

(Defcustom Kubed-Ext-Cb-Max-Backoff 300
  "Maximum Probe Interval In Seconds (Default: 5 Minutes)."
  :Type 'Natnum
  :Group 'Kubed-Ext-Circuit-Breaker)

;; ── Per-Context State (All Tables Keyed By Context String) ───────

(Defvar Kubed-Ext--Cb-Open
  (Make-Hash-Table :Test 'Equal)
  "Circuit State Per Context: Nil (Closed), `Network', Or `Auth'.")

(Defvar Kubed-Ext--Cb-Net-Failures
  (Make-Hash-Table :Test 'Equal)
  "Consecutive Network-Error Count Per Context.")

(Defvar Kubed-Ext--Cb-Auth-Failures
  (Make-Hash-Table :Test 'Equal)
  "Consecutive Auth-Error Count Per Context.")

(Defvar Kubed-Ext--Cb-Backoff
  (Make-Hash-Table :Test 'Equal)
  "Current Probe Back-Off Interval In Seconds Per Context.")

(Defvar Kubed-Ext--Cb-Probe-Timers
  (Make-Hash-Table :Test 'Equal)
  "Pending Network-Recovery Probe Timer Per Context.")

;; ── Error Pattern Matching ────────────────────────────────────────

(Defconst Kubed-Ext--Network-Error-Patterns
  '("Eof" "Unexpected Eof" "Connection Reset By Peer" "Connection Refused"
    "Network Is Unreachable" "No Route To Host" "Broken Pipe"
    "No Such Host" "Name Or Service Not Known"
    "Temporary Failure In Name Resolution"
    "I/O Timeout" "Tls Handshake Timeout" "Context Deadline Exceeded"
    "Request Timeout" "Unable To Connect To The Server"
    "Transport: Error While Dialing" "Dial Tcp" "Proxy: "
    "Azmk8s.Io" "Googleapis.Com" "Eks.Amazonaws.Com"
    "Certificate Has Expired")
  "Kubectl Error Substrings Indicating Transient Network Or Vpn Failures.")

(Defconst Kubed-Ext--Auth-Error-Patterns
  '("Azureclicredential" "Kubelogin Failed" "Aadsts"
    "Failed To Get Token" "Getting Credentials: Exec"
    "Exec: Executable Kubelogin" "Tokenrequesterror"
    "Invalid_Client" "Unauthorized" "Unauthorized"
    "Az Login" "Az Logout")
  "Kubectl Error Substrings Indicating Expired Or Invalid Credentials.")

(Defun Kubed-Ext--Network-Error-P (Msg)
  "Return Non-Nil If Msg Indicates A Transient Network Or Vpn Failure."
  (And (Stringp Msg)
       (Seq-Some (Lambda (Pat)
                         (String-Match-P (Regexp-Quote Pat) Msg))
                 Kubed-Ext--Network-Error-Patterns)))

(Defun Kubed-Ext--Auth-Error-P (Msg)
  "Return Non-Nil If Msg Indicates An Expired Or Invalid Credential."
  (And (Stringp Msg)
       (Seq-Some (Lambda (Pat)
                         (String-Match-P (Regexp-Quote Pat) Msg))
                 Kubed-Ext--Auth-Error-Patterns)))

;; ── State Accessors ───────────────────────────────────────────────

(Defun Kubed-Ext--Cb-Open-P (Context)
  "Return Open Reason For Context (`Network' Or `Auth'), Or Nil If Closed."
  (Gethash (Or Context "") Kubed-Ext--Cb-Open))

(Defun Kubed-Ext--Cb-Backoff-For (Context)
  "Return The Current Probe Back-Off Interval In Seconds For Context."
  (Gethash (Or Context "") Kubed-Ext--Cb-Backoff
           Kubed-Ext-Cb-Initial-Backoff))

;; ── Stderr Content Discovery ──────────────────────────────────────

(Defun Kubed-Ext--Cb-Read-Stderr (Resource-Type)
  "Return Stderr Content For A `Kubectl Get Resource-Type' Process, Or Nil.
Tries Several Buffer-Naming Conventions That Kubed May Use."
  (Catch 'Found
         (Dolist (Name (List (Format " *Kubed-Get-%S-Stderr*" Resource-Type)
                             (Format "*Kubed-Get-%S*"         Resource-Type)
                             (Format " *Kubed-Get-%S*"        Resource-Type)))
                 (Let ((Buf (Get-Buffer Name)))
                      (When (And Buf (Buffer-Live-P Buf))
                            (Let ((Content (With-Current-Buffer Buf
                                                                (String-Trim (Buffer-String)))))
                                 (Unless (String-Empty-P Content)
                                         (Throw 'Found Content))))))))

;; ── List-Buffer Helpers ───────────────────────────────────────────

(Defun Kubed-Ext--Cb-In-Context-Bufs (Context Fn)
  "Call Fn With No Args In Every Live Kubed List Buffer Using Context."
  (Let ((Ctx (Or Context "")))
       (Dolist (Buf (Buffer-List))
               (When (Buffer-Live-P Buf)
                     (With-Current-Buffer Buf
                                          (When (And (Derived-Mode-P 'Kubed-List-Mode)
                                                     (Equal Kubed-List-Context Ctx))
                                                (Funcall Fn)))))))

(Defun Kubed-Ext--Cb-Set-Header (Context Message)
  "Display Message As The Error Header In Every List Buffer For Context."
  (Kubed-Ext--Cb-In-Context-Bufs
   Context (Lambda () (Kubed-Ext--Set-Header-Error Message))))

(Defun Kubed-Ext--Cb-Clear-Headers (Context)
  "Clear Circuit-Breaker Header Overlays In Every List Buffer For Context."
  (Kubed-Ext--Cb-In-Context-Bufs
   Context #'Kubed-Ext--Clear-Header-Error))

(Defun Kubed-Ext--Cb-Refresh-Mode-Lines (Context)
  "Force Mode-Line Update In Every Kubed List Buffer For Context."
  (Kubed-Ext--Cb-In-Context-Bufs
   Context #'Force-Mode-Line-Update))

;; ── Auth-Error Summary Extraction ────────────────────────────────

(Defun Kubed-Ext--Cb-Auth-Summary (Err-Msg)
  "Extract A Short Human-Readable Summary From An Auth Error Err-Msg."
  (Let* ((Lines (Split-String (String-Trim Err-Msg) "\N" T))
         (Hint  (Seq-Find (Lambda (L)
                                  (String-Match-P "Az Login\\|Az Logout" L))
                          Lines)))
        (String-Trim (Or Hint (Car Lines) "Credentials Expired."))))

;; ── Core State Machine ────────────────────────────────────────────

(Defun Kubed-Ext--Cb-Reset-State (Context)
  "Clear All Circuit-Breaker Counters And Cancel Any Probe Timer For Context."
  (Let* ((Ctx   (Or Context ""))
         (Timer (Gethash Ctx Kubed-Ext--Cb-Probe-Timers)))
        (Puthash Ctx 0   Kubed-Ext--Cb-Net-Failures)
        (Puthash Ctx 0   Kubed-Ext--Cb-Auth-Failures)
        (Puthash Ctx Nil Kubed-Ext--Cb-Open)
        (Puthash Ctx Kubed-Ext-Cb-Initial-Backoff Kubed-Ext--Cb-Backoff)
        (When (Timerp Timer) (Cancel-Timer Timer))
        (Remhash Ctx Kubed-Ext--Cb-Probe-Timers)))

(Defun Kubed-Ext--Cb-On-Success (Context)
  "Record A Successful Kubectl Call; Close The Circuit For Context If Open."
  (Let* ((Ctx      (Or Context ""))
         (Was-Open (Kubed-Ext--Cb-Open-P Ctx)))
        (Kubed-Ext--Cb-Reset-State Ctx)
        (When Was-Open
              (Message "Kubed ✓  [%S]: Connection Restored — Auto-Refresh Resumed." Ctx)
              (Kubed-Ext--Cb-Clear-Headers Ctx)
              (Kubed-Ext--Cb-Refresh-Mode-Lines Ctx)
              (Kubed-Ext--Cb-In-Context-Bufs
               Ctx (Lambda ()
                           (Condition-Case Nil (Kubed-List-Update T)
                                           (Error Nil)))))))

(Defun Kubed-Ext--Cb-Trip-Network (Context Err-Msg)
  "Open The Circuit With Reason `Network' For Context/Err-Msg."
  (Let* ((Ctx     (Or Context ""))
         (Delay   Kubed-Ext-Cb-Initial-Backoff)
         (Summary (Truncate-String-To-Width
                   (Replace-Regexp-In-String "\N" " " (String-Trim Err-Msg))
                   60 Nil Nil "…")))
        (Puthash Ctx 'Network Kubed-Ext--Cb-Open)
        (Puthash Ctx Delay    Kubed-Ext--Cb-Backoff)
        (Message "Kubed ⚡ [%S]: Unreachable — %S  [Probing In %Ds; `G'/`Z' To Retry]"
                 Ctx Summary Delay)
        (Kubed-Ext--Cb-Refresh-Mode-Lines Ctx)
        (Kubed-Ext--Cb-Schedule-Probe Ctx Delay)))

(Defun Kubed-Ext--Cb-Trip-Auth (Context Err-Msg)
  "Open The Circuit With Reason `Auth' For Context/Err-Msg."
  (Let* ((Ctx     (Or Context ""))
         (Summary (Kubed-Ext--Cb-Auth-Summary Err-Msg)))
        (Puthash Ctx 'Auth Kubed-Ext--Cb-Open)
        (Puthash Ctx Kubed-Ext-Cb-Initial-Backoff Kubed-Ext--Cb-Backoff)
        (Message "Kubed 🔑 [%S]: Auth Paused — %S  [Re-Auth Then Press `G' Or `Z']"
                 Ctx Summary)
        (Kubed-Ext--Cb-Set-Header
         Ctx (Format "🔑 Auth Expired — %S  [Press `G' Or `Z' To Resume]" Summary))
        (Kubed-Ext--Cb-Refresh-Mode-Lines Ctx)))

(Defun Kubed-Ext--Cb-On-Failure (Context Err-Msg)
  "Record A Kubectl Failure For Context With Error Message Err-Msg.
Auth And Network Errors Are Counted Toward Separate Thresholds.
When Either Threshold Is Reached The Circuit Opens, Suppressing
Auto-Refresh.  Unknown Errors Are Silently Ignored."
  (Let ((Ctx (Or Context "")))
       (Cond
        ;; ── Auth / Credential Error ───────────────────────────────
        ;; Only Act While The Circuit Is Not Already In `Auth' State.
        ((And (Kubed-Ext--Auth-Error-P Err-Msg)
              (Not (Eq (Kubed-Ext--Cb-Open-P Ctx) 'Auth)))
         (Let* ((N   (1+ (Gethash Ctx Kubed-Ext--Cb-Auth-Failures 0)))
                (Thr Kubed-Ext-Cb-Auth-Threshold))
               (Puthash Ctx N Kubed-Ext--Cb-Auth-Failures)
               (If (>= N Thr)
                   (Kubed-Ext--Cb-Trip-Auth Ctx Err-Msg)
                   (Message "Kubed 🔑 [%S] (%D/%D): %S"
                            Ctx N Thr (Kubed-Ext--Cb-Auth-Summary Err-Msg)))))

        ;; ── Network / Transport Error ─────────────────────────────
        ;; Only Act While The Circuit Is Fully Closed.
        ((And (Kubed-Ext--Network-Error-P Err-Msg)
              (Not (Kubed-Ext--Cb-Open-P Ctx)))
         (Let* ((N   (1+ (Gethash Ctx Kubed-Ext--Cb-Net-Failures 0)))
                (Thr Kubed-Ext-Cb-Threshold))
               (Puthash Ctx N Kubed-Ext--Cb-Net-Failures)
               (If (>= N Thr)
                   (Kubed-Ext--Cb-Trip-Network Ctx Err-Msg)
                   (Message "Kubed [%S]: Connection Check Failed (%D/%D)"
                            Ctx N Thr)))))))

;; ── Network Recovery Probe ────────────────────────────────────────

(Defun Kubed-Ext--Cb-Schedule-Probe (Context Delay)
  "Schedule A Connectivity Probe For Context After Delay Seconds."
  (Let ((Ctx (Or Context "")))
       (Let ((Old (Gethash Ctx Kubed-Ext--Cb-Probe-Timers)))
            (When (Timerp Old) (Cancel-Timer Old)))
       (Puthash Ctx
                (Run-With-Timer Delay Nil #'Kubed-Ext--Cb-Run-Probe Ctx)
                Kubed-Ext--Cb-Probe-Timers)))

(Defun Kubed-Ext--Cb-Run-Probe (Context)
  "Run A `Kubectl Api-Versions' Connectivity Probe For Context.

Exit 0         → Success; Close Circuit.
Auth Error     → Network Is Up But Credentials Expired; Switch To
                 `Auth' State And Stop Scheduling Further Probes.
Network Error  → Still Unreachable; Double Back-Off And Reschedule.
Other Error    → Server Responded; Close Circuit."
  (Let ((Ctx (Or Context "")))
       (When (Kubed-Ext--Cb-Open-P Ctx)
             (Let ((Probe-Buf (Generate-New-Buffer " *Kubed-Ext-Cb-Probe*")))
                  (Make-Process
                   :Name     "Kubed-Ext-Cb-Probe"
                   :Buffer   Probe-Buf
                   :Noquery  T
                   :Command  (List Kubed-Kubectl-Program
                                   "Api-Versions"
                                   "--Context"         Ctx
                                   "--Request-Timeout" "5s")
                   :Sentinel
                   (Lambda (Proc _Event)
                           (When (Memq (Process-Status Proc) '(Exit Signal))
                                 (Let* ((Code   (Process-Exit-Status Proc))
                                        (Output (If (Buffer-Live-P Probe-Buf)
                                                    (Prog1 (With-Current-Buffer Probe-Buf
                                                                                (String-Trim (Buffer-String)))
                                                           (Kill-Buffer Probe-Buf))
                                                    "")))
                                       (Remhash Ctx Kubed-Ext--Cb-Probe-Timers)
                                       (Cond
                                        ;; Network And Auth Are Both Working.
                                        ((Zerop Code)
                                         (Kubed-Ext--Cb-On-Success Ctx))

                                        ;; Network Is Up But Credentials Have Expired.
                                        ;; Switch To `Auth' State — No More Probes Until Manual Reset.
                                        ((Kubed-Ext--Auth-Error-P Output)
                                         (Puthash Ctx 'Auth Kubed-Ext--Cb-Open)
                                         (Kubed-Ext--Cb-Set-Header
                                          Ctx (Concat "🔑 Network Restored But Credentials Expired"
                                                      "  [Re-Auth, Then Press `G' Or `Z']"))
                                         (Kubed-Ext--Cb-Refresh-Mode-Lines Ctx)
                                         (Message
                                          "Kubed 🔑 [%S]: Network Ok But Credentials Expired — Re-Auth Then Press `G'."
                                          Ctx))

                                        ;; Cluster Still Unreachable — Exponential Back-Off.
                                        ((Kubed-Ext--Network-Error-P Output)
                                         (Let* ((Cur  (Kubed-Ext--Cb-Backoff-For Ctx))
                                                (Next (Min Kubed-Ext-Cb-Max-Backoff
                                                           (Round (* Cur Kubed-Ext-Cb-Backoff-Factor)))))
                                               (Puthash Ctx Next Kubed-Ext--Cb-Backoff)
                                               (Message "Kubed ⚡ [%S]: Still Unreachable — Probing In %Ds."
                                                        Ctx Next)
                                               (Kubed-Ext--Cb-Refresh-Mode-Lines Ctx)
                                               (Kubed-Ext--Cb-Schedule-Probe Ctx Next)))

                                        ;; Unexpected Non-Zero Exit But Server Responded — Close Circuit.
                                        (T
                                         (Kubed-Ext--Cb-On-Success Ctx)))))))))))

;; ── Wiring: Feed Kubed-Update Outcomes Into The State Machine ────

(Defun Kubed-Ext--Cb-Update-Advice (Orig-Fn Type Context &Optional Namespace)
  "Around-Advice On `Kubed-Update': Wire Circuit-Breaker Accounting.
Orig-Fn Is The Original Function; Type, Context, And Namespace Are
Forwarded Unchanged."
  (Funcall Orig-Fn Type Context Namespace)
  (When-Let ((Proc (Alist-Get 'Process
                              (Kubed--Alist Type Context Namespace))))
            (When (Process-Live-P Proc)
                  (Let* ((Ctx        (Or Context ""))
                         (Rtype      Type)
                         (Inner-Sent (Process-Sentinel Proc)))
                        (Set-Process-Sentinel
                         Proc
                         (Lambda (P Status)
                                 (Funcall Inner-Sent P Status)
                                 (Cond
                                  ((String= Status "Finished\N")
                                   (Kubed-Ext--Cb-On-Success Ctx))
                                  ((String= Status "Exited Abnormally With Code 1\N")
                                   (Kubed-Ext--Cb-On-Failure
                                    Ctx (Or (Kubed-Ext--Cb-Read-Stderr Rtype)
                                            "Kubectl Exited With Code 1"))))))))))

(Advice-Add 'Kubed-Update :Around #'Kubed-Ext--Cb-Update-Advice)

;; ── `G' Clears The Circuit Before The Manual Refresh Fires ───────

(Defun Kubed-Ext--Cb-Before-Manual-Refresh (&Rest _)
  "Before-Advice On `Kubed-List-Update': Clear An Open Circuit On `G'.
This Gives The Cluster An Immediate Retry Without Waiting For A Probe."
  (When (And (Derived-Mode-P 'Kubed-List-Mode)
             (Kubed-Ext--Cb-Open-P Kubed-List-Context))
        (Kubed-Ext--Cb-Reset-State Kubed-List-Context)
        (Kubed-Ext--Cb-Clear-Headers Kubed-List-Context)
        (Kubed-Ext--Cb-Refresh-Mode-Lines Kubed-List-Context)))

(Advice-Add 'Kubed-List-Update :Before #'Kubed-Ext--Cb-Before-Manual-Refresh)

;; ── Manual Reset Command ──────────────────────────────────────────

(Defun Kubed-Ext-Circuit-Breaker-Reset (&Optional Context)
  "Reset The Circuit Breaker For Context And Resume Auto-Refresh.
Call This After Reconnecting To Vpn Or Refreshing Expired Credentials.
With A Prefix Argument, Prompt For Context; Otherwise Use The Context
Of The Current Buffer Or The Configured Default."
  (Interactive
   (List (If Current-Prefix-Arg
             (Kubed-Read-Context "Reset Circuit For Context"
                                 (Kubed-Local-Context))
             (Kubed-Local-Context))))
  (Let ((Ctx (Or Context (Kubed-Local-Context))))
       (Kubed-Ext--Cb-Reset-State Ctx)
       (Kubed-Ext--Cb-Clear-Headers Ctx)
       (Kubed-Ext--Cb-Refresh-Mode-Lines Ctx)
       (Kubed-Ext--Cb-In-Context-Bufs
        Ctx (Lambda ()
                    (Condition-Case Nil (Kubed-List-Update T)
                                    (Error Nil))))
       (Message "Kubed [%S]: Circuit Reset — Auto-Refresh Resumed." Ctx)))

(Keymap-Set Kubed-List-Mode-Map "Z" #'Kubed-Ext-Circuit-Breaker-Reset)
(Keymap-Set Kubed-Prefix-Map    "Z" #'Kubed-Ext-Circuit-Breaker-Reset)

;; ── Auto-Refresh Tick ─────────────────────────────────────────────

(Defun Kubed-Ext--Auto-Refresh-Tick ()
  "Refresh Visible Kubed List Buffers; Skip Any With An Open Circuit.
Reschedules Itself After Each Tick With A Fresh Random Delay."
  (Unless (Active-Minibuffer-Window)
          (Let ((Refreshed 0)
                (Skipped   0))
               (Dolist (Win (Window-List))
                       (Let ((Buf (Window-Buffer Win)))
                            (When (Buffer-Live-P Buf)
                                  (With-Current-Buffer Buf
                                                       (When (Derived-Mode-P 'Kubed-List-Mode)
                                                             (Cond
                                                              ;; Circuit Is Open — Skip And Count.
                                                              ((Kubed-Ext--Cb-Open-P Kubed-List-Context)
                                                               (Cl-Incf Skipped))
                                                              ;; An Update Is Already In Progress — Skip Silently.
                                                              ((Process-Live-P
                                                                (Alist-Get 'Process
                                                                           (Kubed--Alist Kubed-List-Type
                                                                                         Kubed-List-Context
                                                                                         Kubed-List-Namespace))))
                                                              ;; Normal Case — Refresh.
                                                              (T
                                                               (Condition-Case Nil
                                                                               (Progn (Kubed-List-Update T) (Cl-Incf Refreshed))
                                                                               (Error Nil)))))))))
               (Cond
                ((And (> Refreshed 0) (> Skipped 0))
                 (Message "Kubed: Refreshed %D Buffer%S; %D Paused (Circuit Open)."
                          Refreshed (If (= Refreshed 1) "" "S") Skipped))
                ((> Refreshed 0)
                 (Message "Kubed: Auto-Refreshed %D Buffer%S."
                          Refreshed (If (= Refreshed 1) "" "S"))))))
  (Kubed-Ext--Auto-Refresh-Schedule))

;; ── Mode-Line ─────────────────────────────────────────────────────

(Setq Kubed-List-Mode-Line-Format
      '(:Eval
        (Let* ((Ctx      Kubed-List-Context)
               (Reason   (And Ctx (Kubed-Ext--Cb-Open-P Ctx)))
               (Updating (Process-Live-P
                          (Alist-Get 'Process
                                     (Kubed--Alist Kubed-List-Type
                                                   Ctx
                                                   Kubed-List-Namespace))))
               (Is-Prod  (And Ctx
                              (Bound-And-True-P
                               Kubed-Ext-Production-Context-Regexp)
                              (Stringp
                               Kubed-Ext-Production-Context-Regexp)
                              (String-Match-P
                               Kubed-Ext-Production-Context-Regexp Ctx))))
              (Concat
               ;; ── Context Badge ──────────────────────────────────
               (When Ctx
                     (Propertize
                      (If Is-Prod
                          (Format " [☢ %S ☢]" Ctx)
                          (Format " [%S]" Ctx))
                      'Face      (If Is-Prod 'Error 'Success)
                      'Help-Echo (Format "Context: %S  (%S)"
                                         Ctx
                                         (If Is-Prod "Production" "Non-Prod"))))
               ;; ── Status Indicators ──────────────────────────────
               (Cond
                (Updating
                 (Propertize " [...]"
                             'Help-Echo "Kubectl Update In Progress…"))

                ((Eq Reason 'Network)
                 (Let ((Secs (Round (Kubed-Ext--Cb-Backoff-For Ctx))))
                      (Propertize (Format " ⚡~%Ds" Secs)
                                  'Face      'Error
                                  'Help-Echo
                                  (Format (Concat "Network Unreachable For `%S'.\N"
                                                  "Auto-Refresh Paused; Probing In ~%Ds.\N"
                                                  "Press `G' Or `Z' To Retry Immediately.")
                                          Ctx Secs))))

                ((Eq Reason 'Auth)
                 (Propertize " 🔑 Re-Auth"
                             'Face      'Warning
                             'Help-Echo
                             (Format (Concat "Credentials Expired For `%S'.\N"
                                             "Re-Authenticate, Then Press `G' Or `Z'.")
                                     Ctx)))

                (T
                 (Concat
                  (When Kubed-List-Filter
                        (Propertize
                         (Format " [%S]"
                                 (Mapconcat #'Prin1-To-String
                                            Kubed-List-Filter " "))
                         'Help-Echo "Active S-Expression Filter"))
                  (When (Bound-And-True-P Kubed-Ext-List-Label-Selector)
                        (Propertize
                         (Format " {%S}" Kubed-Ext-List-Label-Selector)
                         'Help-Echo "Active Label Selector"
                         'Face      'Warning))
                  (When (And (Bound-And-True-P Kubed-Ext-Resource-Filter)
                             (Not (String-Empty-P Kubed-Ext-Resource-Filter)))
                        (Propertize
                         (Format " /%S/" Kubed-Ext-Resource-Filter)
                         'Help-Echo "Active Substring Filter"
                         'Face      'Italic)))))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 22.  Enhanced Log Filtering And Parallel Streaming
;;; ═══════════════════════════════════════════════════════════════

(Defgroup Kubed-Ext-Logs Nil
  "Kubernetes Log Filtering And Parallel Streaming."
  :Group 'Kubed-Ext)

(Defcustom Kubed-Ext-Log-Error-Pattern
  "Exception\\|Error\\|Critical\\|Failure\\|Fatal\\|Panic"
  "Case-Insensitive Regex For Filtering Error Log Lines."
  :Type 'String
  :Group 'Kubed-Ext-Logs)

(Defcustom Kubed-Ext-Log-Parallel-Limit 5
  "Maximum Simultaneous Kubectl Log Processes For Parallel Streaming."
  :Type 'Natnum
  :Group 'Kubed-Ext-Logs)

(Defcustom Kubed-Ext-Log-Default-Since "1h"
  "Default --Since Duration For Filtered Log Commands (E.G. \"30m\", \"24h\")."
  :Type 'String
  :Group 'Kubed-Ext-Logs)

(Defcustom Kubed-Ext-Log-Use-External-Filter 'Auto
  "Controls Whether Rg/Grep Or Emacs-Native Filtering Is Used.

`Auto'  — Prefer Rg, Then Grep, Then Emacs-Native.
`Emacs' — Always Emacs-Native (Portable, No Colour).
`Shell' — Always Shell Pipeline; Error If Neither Rg Nor Grep Found."
  :Type '(Choice (Const :Tag "Auto-Detect (Rg > Grep > Emacs)" Auto)
                 (Const :Tag "Emacs-Native Only"               Emacs)
                 (Const :Tag "Shell Pipeline Only"             Shell))
  :Group 'Kubed-Ext-Logs)

(Defvar Kubed-Ext-Log-Filter-History Nil
  "Minibuffer History For Log-Filter Pattern Prompts.")

;;; ─── Tool Selection ───────────────────────────────────────────

(Defun Kubed-Ext--Log-Shell-Filter-Cmd (Pattern)
  "Return A Shell Command String Filtering Stdin For Pattern, Or Nil."
  (Cond
   ((Executable-Find "Rg")
    (Concat "Rg --Color=Always --Line-Buffered -I "
            (Shell-Quote-Argument Pattern)))
   ((Executable-Find "Grep")
    (Concat "Grep --Line-Buffered -Ei "
            (Shell-Quote-Argument Pattern)))
   (T Nil)))

(Defun Kubed-Ext--Log-Use-Shell-P (Pattern)
  "Return Non-Nil When A Shell Pipeline Should Be Used To Filter Pattern."
  (When (And (Stringp Pattern) (Not (String-Empty-P Pattern)))
        (Pcase Kubed-Ext-Log-Use-External-Filter
               ('Emacs Nil)
               ('Shell
                (Unless (Kubed-Ext--Log-Shell-Filter-Cmd Pattern)
                        (User-Error
                         "No Rg Or Grep Found; Set `Kubed-Ext-Log-Use-External-Filter' To `Emacs'"))
                T)
               (_ (Not (Null (Kubed-Ext--Log-Shell-Filter-Cmd Pattern)))))))

;;; ─── Filter Prompt ────────────────────────────────────────────

(Defun Kubed-Ext--Read-Log-Filter (Prompt &Optional Default)
  "Prompt With Prompt For A Log-Filter Regex, Defaulting To Default."
  (Let* ((Opt-Err    (Concat "(Errors)  " Kubed-Ext-Log-Error-Pattern))
         (Opt-None   "(None)    Show All Lines")
         (Opt-Custom "(Custom)  Enter Pattern…")
         (Choice     (Completing-Read
                      (Format-Prompt Prompt (Or Default ""))
                      (List Opt-Err Opt-None Opt-Custom)
                      Nil Nil Nil
                      'Kubed-Ext-Log-Filter-History
                      (Or Default ""))))
        (Cond
         ((String= Choice Opt-Err)    Kubed-Ext-Log-Error-Pattern)
         ((String= Choice Opt-None)   "")
         ((String= Choice Opt-Custom)
          (Read-String (Format-Prompt "Pattern" Default)
                       Default 'Kubed-Ext-Log-Filter-History))
         (T Choice))))

;;; ─── Emacs-Native Line Filter ─────────────────────────────────

(Defun Kubed-Ext--Make-Line-Filter (Pattern)
  "Return A `Process-Filter' Closure Keeping Only Lines Matching Pattern.
Case-Insensitive.  Partial Lines Are Buffered Across Output Chunks."
  (Let ((Acc ""))
       (Lambda (Proc String)
               (When (Buffer-Live-P (Process-Buffer Proc))
                     (Let* ((Combined        (Concat Acc String))
                            (Lines           (Split-String Combined "\N"))
                            (Complete        (Butlast Lines))
                            (Tail            (Car (Last Lines)))
                            (Case-Fold-Search T))
                           (Setq Acc (Or Tail ""))
                           (With-Current-Buffer (Process-Buffer Proc)
                                                (Let ((Inhibit-Read-Only T))
                                                     (Goto-Char (Point-Max))
                                                     (Dolist (Line Complete)
                                                             (When (Or (Null Pattern)
                                                                       (String-Empty-P Pattern)
                                                                       (String-Match-P Pattern Line))
                                                                   (Insert Line "\N"))))))))))

;;; ─── Ansi-Color Filter ────────────────────────────────────────

(Defun Kubed-Ext--Log-Ansi-Filter (Proc String)
  "Process Filter Inserting String Into Proc'S Buffer With Ansi Decoding."
  (Require 'Ansi-Color)
  (When (Buffer-Live-P (Process-Buffer Proc))
        (With-Current-Buffer (Process-Buffer Proc)
                             (Let ((Inhibit-Read-Only T)
                                   (Beg               (Point-Max)))
                                  (Goto-Char Beg)
                                  (Insert String)
                                  (Ansi-Color-Apply-On-Region Beg (Point-Max))))))

;;; ─── Core Launch Helper ───────────────────────────────────────

(Defun Kubed-Ext--Launch-Log-Process (Buf Kubectl-Args Filter-Pattern)
  "Start A Kubectl Process Writing To Buf, Optionally Filtered.

Kubectl-Args Must Contain Only Strings — Callers Are Responsible For
Omitting Nil Values Via `Append' + `When' Guards.
Filter-Pattern Is A Regex String, Or Nil/Empty For No Filtering."
  (Let* ((Pattern   (And (Stringp Filter-Pattern)
                         (Not (String-Empty-P Filter-Pattern))
                         Filter-Pattern))
         (Use-Shell (Kubed-Ext--Log-Use-Shell-P Pattern)))
        (If Use-Shell
            (Let* ((Filt-Cmd (Kubed-Ext--Log-Shell-Filter-Cmd Pattern))
                   (Kube-Cmd (Mapconcat #'Shell-Quote-Argument
                                        (Cons Kubed-Kubectl-Program Kubectl-Args)
                                        " "))
                   (Full-Cmd (Format "%S | %S" Kube-Cmd Filt-Cmd))
                   (Proc     (Start-Process-Shell-Command
                              "*Kubed-Logs-Filtered*" Buf Full-Cmd)))
                  (When (Executable-Find "Rg")
                        (Set-Process-Filter Proc #'Kubed-Ext--Log-Ansi-Filter))
                  (Set-Process-Sentinel
                   Proc
                   (Lambda (_P Status)
                           (When (Buffer-Live-P Buf)
                                 (With-Current-Buffer Buf
                                                      (Let ((Inhibit-Read-Only T))
                                                           (Cond
                                                            ((String= Status "Finished\N") Nil)
                                                            ((String= Status "Exited Abnormally With Code 1\N")
                                                             (Goto-Char (Point-Max))
                                                             (Insert (Propertize "(No Matching Lines Found)\N"
                                                                                 'Face 'Shadow)))
                                                            (T
                                                             (Goto-Char (Point-Max))
                                                             (Insert (Propertize
                                                                      (Format "[Process: %S]\N" (String-Trim Status))
                                                                      'Face 'Error)))))))))
                  Proc)
            (Let ((Proc (Apply #'Start-Process "*Kubed-Logs*" Buf
                               Kubed-Kubectl-Program Kubectl-Args)))
                 (When Pattern
                       (Set-Process-Filter Proc (Kubed-Ext--Make-Line-Filter Pattern)))
                 Proc))))

;;; ─── Pod: Error Logs ──────────────────────────────────────────

(Defun Kubed-Ext-Pods-Logs-Errors (Click)
  "Show Error-Filtered Logs For The Pod At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Ctx  Kubed-List-Context)
                 (Ns   Kubed-List-Namespace)
                 (Args (Append
                        (List "Logs" Pod
                              "--All-Containers"
                              "--Prefix"
                              "--Since" Kubed-Ext-Log-Default-Since)
                        (When Ctx (List "--Context" Ctx))
                        (When Ns  (List "-N" Ns))))
                 (Buf  (Generate-New-Buffer
                        (Format "*Kubed-Logs-Errors Pods/%S@%S[%S]*"
                                Pod (Or Ns "Default") (Or Ctx "Current")))))
                (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
                (Kubed-Ext--Launch-Log-Process Buf Args Kubed-Ext-Log-Error-Pattern)
                (Display-Buffer Buf))
          (User-Error "No Kubernetes Pod At Point")))

;;; ─── Pod: Custom-Filtered Logs ────────────────────────────────

(Defun Kubed-Ext-Pods-Logs-Custom-Filter (Click)
  "Show Filtered Logs For The Pod At Click, Prompting For Options."
  (Interactive (List Last-Nonmenu-Event) Kubed-Pods-Mode)
  (If-Let ((Pod (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Ctx       Kubed-List-Context)
                 (Ns        Kubed-List-Namespace)
                 (Filt      (Kubed-Ext--Read-Log-Filter
                             "Filter Pattern" Kubed-Ext-Log-Error-Pattern))
                 (Since     (Read-String (Format-Prompt "Since"
                                                        Kubed-Ext-Log-Default-Since)
                                         Nil Nil Kubed-Ext-Log-Default-Since))
                 (Follow    (Y-Or-N-P "Follow (Stream) Logs? "))
                 ;; Ask All-Containers First; Only Prompt For A Specific
                 ;; Container When The User Says No.
                 (All-Con   (Y-Or-N-P "All Containers? "))
                 (Container (Unless All-Con
                                    (Kubed-Read-Container Pod "Container" T Ctx Ns)))
                 (Args      (Append
                             (List "Logs" Pod "--Prefix" "--Since" Since)
                             (When Ctx     (List "--Context" Ctx))
                             (When Ns      (List "-N" Ns))
                             (When Follow  '("--Follow"))
                             (If All-Con
                                 '("--All-Containers")
                                 (List "-C" Container))))
                 (Buf       (Generate-New-Buffer
                             (Format "*Kubed-Logs-Filtered Pods/%S@%S[%S]*"
                                     Pod (Or Ns "Default") (Or Ctx "Current")))))
                (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
                (Kubed-Ext--Launch-Log-Process Buf Args Filt)
                (Display-Buffer Buf))
          (User-Error "No Kubernetes Pod At Point")))

;;; ─── Standalone: Error Logs For Any Resource ──────────────────

;;;###Autoload
(Defun Kubed-Ext-Logs-Errors (&Optional Context Namespace)
  "Show Error-Filtered Logs For A Kubernetes Resource (Context And Namespace)."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List C N)))
  (Let* ((Ctx    (Or Context (Kubed-Local-Context)))
         (Ns     (Or Namespace (Kubed--Namespace Ctx)))
         (Type   (Kubed-Read-Resource-Type "Resource Type" Nil Ctx))
         (Res    (Kubed-Read-Resource-Name Type "Error Logs For" Nil Nil Ctx Ns))
         (Since  (Read-String (Format-Prompt "Since" Kubed-Ext-Log-Default-Since)
                              Nil Nil Kubed-Ext-Log-Default-Since))
         (Follow (Y-Or-N-P "Follow (Stream) Logs? "))
         (Args   (Append
                  (List "Logs" (Concat Type "/" Res)
                        "--Prefix"
                        "--Since" Since)
                  (When Ctx    (List "--Context" Ctx))
                  (When Ns     (List "-N" Ns))
                  (When Follow '("--Follow"))))
         (Buf    (Generate-New-Buffer
                  (Format "*Kubed-Logs-Errors %S/%S@%S[%S]*"
                          Type Res (Or Ns "Default") Ctx))))
        (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
        (Kubed-Ext--Launch-Log-Process Buf Args Kubed-Ext-Log-Error-Pattern)
        (Display-Buffer Buf)))

;;; ─── Standalone: Logs By Label With Filtering ─────────────────

;;;###Autoload
(Defun Kubed-Ext-Logs-By-Label-Filtered (&Optional Context Namespace)
  "Stream Filtered Logs For Pods Matching A Label Selector (Context And Namespace)."
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List C N)))
  (Let* ((Ctx      (Or Context (Kubed-Local-Context)))
         (Ns       (Or Namespace (Kubed--Namespace Ctx)))
         (Labels   (Kubed-Ext-Discover-Labels "Pods" Ctx Ns))
         (Selector (Completing-Read "Label Selector: " Labels Nil Nil
                                    Nil 'Kubed-Ext-Label-Selector-History))
         (Filt     (Kubed-Ext--Read-Log-Filter
                    "Filter Pattern" Kubed-Ext-Log-Error-Pattern))
         (Since    (Read-String (Format-Prompt "Since" Kubed-Ext-Log-Default-Since)
                                Nil Nil Kubed-Ext-Log-Default-Since))
         (Follow   (Y-Or-N-P "Follow (Stream) Logs? "))
         (Args     (Append
                    (List "Logs" "-L" Selector
                          "--All-Containers=True"
                          "--Prefix"
                          "--Since" Since)
                    (When Ctx    (List "--Context" Ctx))
                    (When Ns     (List "-N" Ns))
                    (When Follow '("--Follow"))))
         (Buf      (Generate-New-Buffer
                    (Format "*Kubed-Logs-Filtered -L %S@%S[%S]*"
                            Selector (Or Ns "Default") Ctx))))
        (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
        (Kubed-Ext--Launch-Log-Process Buf Args Filt)
        (Display-Buffer Buf)))

;;; ─── Pod Discovery By Selector ────────────────────────────────

(Defun Kubed-Ext--Pods-By-Selector (Selector Context Namespace)
  "Return Sorted Pod Names Matching Selector In Context/Namespace, Or Nil."
  (Condition-Case Err
                  (Let ((Output
                         (With-Temp-Buffer
                          (Apply #'Call-Process
                                 Kubed-Kubectl-Program Nil '(T Nil) Nil
                                 (Append
                                  (List "Get" "Pods"
                                        "-L"           Selector
                                        "--No-Headers"
                                        "-O"           "Custom-Columns=Name:.Metadata.Name")
                                  (When Context   (List "--Context" Context))
                                  (When Namespace (List "-N"        Namespace))))
                          (Buffer-String))))
                       (Sort (Seq-Filter (Lambda (S) (Not (String-Empty-P (String-Trim S))))
                                         (Split-String Output "\N" T))
                             #'String<))
                  (Error
                   (Message "Kubed-Ext: Cannot List Pods: %S" (Error-Message-String Err))
                   Nil)))

;;; ─── Post-Hoc Filter On Existing Log Buffer ───────────────────

(Defun Kubed-Ext-Filter-Log-Buffer (Pattern)
  "Show Lines From The Current Buffer Matching Pattern Using `Occur'."
  (Interactive
   (List (Kubed-Ext--Read-Log-Filter
          "Filter Log Buffer" Kubed-Ext-Log-Error-Pattern)))
  (When (Or (Null Pattern) (String-Empty-P (String-Trim Pattern)))
        (User-Error "No Filter Pattern Specified"))
  (Let ((Case-Fold-Search T))
       (Occur Pattern)))

;;; ─── Keybindings § 22 ─────────────────────────────────────────

(Keymap-Set Kubed-Pods-Mode-Map "E"   #'Kubed-Ext-Pods-Logs-Errors)
(Keymap-Set Kubed-Pods-Mode-Map "H"   #'Kubed-Ext-Pods-Logs-Custom-Filter)
(Keymap-Set Kubed-Prefix-Map    "E"   #'Kubed-Ext-Logs-Errors)
(Keymap-Set Kubed-Prefix-Map    "L F" #'Kubed-Ext-Logs-By-Label-Filtered)
(Keymap-Set Kubed-Prefix-Map    "L P" #'Kubed-Ext-Logs-Parallel-By-Label)
(Keymap-Set Kubed-Prefix-Map    "L O" #'Kubed-Ext-Filter-Log-Buffer)

;;; ─── Extend Pod Transient Suffixes ────────────────────────────

(Let ((Entry (Assoc "Pods" Kubed-Ext-Extra-Transient-Suffixes)))
     (When Entry
           (Setcdr Entry
                   (Append (Cdr Entry)
                           '(("Le" "Error Logs"
                              Kubed-Ext-Pods-Logs-Errors)
                             ("Lf" "Filter Logs (Custom)"
                              Kubed-Ext-Pods-Logs-Custom-Filter)
                             ("Lp" "Parallel Logs By Label"
                              Kubed-Ext-Logs-Parallel-By-Label))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 22b.  Log Filtering For Deployments And Generic Workloads
;;; ═══════════════════════════════════════════════════════════════

;;; ─── Matchexpression → Selector Segment ───────────────────────

(Defun Kubed-Ext--Matchexpr-To-Selector (Expr)
  "Convert A Kubernetes Matchexpression Expr Alist To A Selector String.
Returns Nil For Unsupported Operators."
  (Let ((Key  (Alist-Get 'Key      Expr))
        (Op   (Alist-Get 'Operator Expr))
        (Vals (Alist-Get 'Values   Expr)))
       (And (Stringp Key)
            (Stringp Op)
            (Pcase Op
                   ("Exists"       Key)
                   ("Doesnotexist" (Concat "!" Key))
                   ("In"
                    (When (And (Listp Vals) Vals)
                          (Format "%S In (%S)" Key (Mapconcat #'Identity Vals ","))))
                   ("Notin"
                    (When (And (Listp Vals) Vals)
                          (Format "%S Notin (%S)" Key (Mapconcat #'Identity Vals ","))))
                   (_ Nil)))))

;;; ─── Workload → Pod Label Selector ───────────────────────────

(Defun Kubed-Ext--Workload-Pod-Selector (Type Name Context Namespace)
  "Return A Kubectl --Selector String For Pods.
Owned By Type/Name In Context/Namespace.
Returns Nil When The Selector Cannot Be Determined."
  (Condition-Case Err
                  (Let ((Json-Str
                         (With-Temp-Buffer
                          (When (Zerop
                                 (Apply #'Call-Process
                                        Kubed-Kubectl-Program Nil '(T Nil) Nil
                                        (Append
                                         (List "Get" Type Name "-O" "Json")
                                         (When Context   (List "--Context" Context))
                                         (When Namespace (List "-N"        Namespace)))))
                                (String-Trim (Buffer-String))))))
                       (When (And Json-Str
                                  (Not (String-Empty-P Json-Str))
                                  (String-Prefix-P "{" Json-Str))
                             (Let* ((Obj      (Json-Parse-String Json-Str
                                                                 :Object-Type 'Alist
                                                                 :Array-Type  'List))
                                    (Spec     (And (Listp Obj)  (Alist-Get 'Spec     Obj)))
                                    (Selector (And (Listp Spec) (Alist-Get 'Selector Spec)))
                                    (Result
                                     (Cond
                                      ;; Replicationcontrollers Use A Flat Label Map.
                                      ((Member Type '("Replicationcontroller"
                                                      "Replicationcontrollers"))
                                       (When (And Selector (Listp Selector))
                                             (Mapconcat (Lambda (Kv)
                                                                (Format "%S=%S" (Car Kv) (Cdr Kv)))
                                                        Selector ",")))
                                      ;; All Other Workload Types Use Matchlabels/Matchexpressions.
                                      ((And Selector
                                            (Listp Selector)
                                            (Not (Eq Selector :Null)))
                                       (Let* ((Ml     (Alist-Get 'Matchlabels      Selector))
                                              (Me     (Alist-Get 'Matchexpressions Selector))
                                              (Lparts (When (And Ml (Listp Ml) (Not (Eq Ml :Null)))
                                                            (Mapcar (Lambda (Kv)
                                                                            (Format "%S=%S" (Car Kv) (Cdr Kv)))
                                                                    Ml)))
                                              (Eparts (When (And Me (Listp Me) (Not (Eq Me :Null)))
                                                            (Delq Nil (Mapcar
                                                                       #'Kubed-Ext--Matchexpr-To-Selector
                                                                       Me))))
                                              (All    (Append Lparts Eparts)))
                                             (When All (Mapconcat #'Identity All ","))))
                                      (T Nil))))
                                   ;; Guard Against Empty String — That Would Match All Pods.
                                   (And (Stringp Result) (Not (String-Empty-P Result)) Result))))
                  (Error
                   (Message "Kubed-Ext: Selector For %S/%S: %S"
                            Type Name (Error-Message-String Err))
                   Nil)))

;;; ─── Shared Parallel Log Runner ───────────────────────────────

(Defun Kubed-Ext--Parallel-Logs-Run
  (Pods Context Namespace Filter Since Follow All-Containers Buf)
  "Stream Logs From Pods Concurrently Into Buf.

At Most `Kubed-Ext-Log-Parallel-Limit' Kubectl Processes Run At Once;
New Ones Start As Slots Free.  Per-Process Emacs-Native Line Filters
Are Always Used So N Independent Streams Do Not Corrupt Shared Output.

Pods           — List Of Pod-Name Strings.
Context        — Kubectl Context, Or Nil For Current Context.
Namespace      — Kubernetes Namespace, Or Nil For Current Namespace.
Filter         — Case-Insensitive Regex; Nil Or Empty Passes All Lines.
Since          — Kubectl --Since Value (E.G. \"1h\", \"30m\").
Follow         — Non-Nil Appends --Follow Flag.
All-Containers — Non-Nil Appends --All-Containers Flag.
Buf            — Pre-Existing Output Buffer."
  (If (Null Pods)
      (With-Current-Buffer Buf
                           (Let ((Inhibit-Read-Only T))
                                (Insert "No Pods Found For The Given Selector.\N")))
      (Message "Streaming From %D Pod%S (<=%D Parallel)..."
               (Length Pods)
               (If (= (Length Pods) 1) "" "S")
               Kubed-Ext-Log-Parallel-Limit)
      (Let ((Remaining (Copy-Sequence Pods))
            (Active    0)
            (Limit     Kubed-Ext-Log-Parallel-Limit))
           (Cl-Labels
            ((Maybe-Start ()
                          (While (And Remaining (< Active Limit))
                                 (Let ((Pod (Pop Remaining)))
                                      (Condition-Case Start-Err
                                                      (Let* ((Args (Append
                                                                    (List "Logs" Pod
                                                                          "--Prefix"
                                                                          "--Since" Since)
                                                                    (When Context        (List "--Context" Context))
                                                                    (When Namespace      (List "-N" Namespace))
                                                                    (When Follow         '("--Follow"))
                                                                    (When All-Containers '("--All-Containers"))))
                                                             (Proc (Apply #'Start-Process
                                                                          (Format "*Kubed-Plog-%S*" Pod)
                                                                          Buf
                                                                          Kubed-Kubectl-Program Args)))
                                                            (When (And (Stringp Filter) (Not (String-Empty-P Filter)))
                                                                  (Set-Process-Filter Proc
                                                                                      (Kubed-Ext--Make-Line-Filter Filter)))
                                                            (Cl-Incf Active)
                                                            (Set-Process-Sentinel Proc
                                                                                  (Lambda (_P _Status)
                                                                                          (Cl-Decf Active)
                                                                                          (Maybe-Start))))
                                                      (Error
                                                       (Message "Kubed-Ext: Log Process For %S: %S"
                                                                Pod (Error-Message-String Start-Err))))))))
            (Maybe-Start)))))

;;; ─── Standalone: Parallel By Label ───────────────────────────

;;;###Autoload
(Defun Kubed-Ext-Logs-Parallel-By-Label (&Optional Context Namespace)
  "Stream Logs From Pods Matching A Label Selector, Up To N In Parallel.

Context And Namespace Are Optional; Defaults To Current Context/Namespace."
  (Declare (Advertised-Calling-Convention (Context Namespace) "1.0"))
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List C N)))
  (Let* ((Ctx      (Or Context (Kubed-Local-Context)))
         (Ns       (Or Namespace (Kubed--Namespace Ctx)))
         (Labels   (Kubed-Ext-Discover-Labels "Pods" Ctx Ns))
         (Selector (Completing-Read "Label Selector: " Labels Nil Nil
                                    Nil 'Kubed-Ext-Label-Selector-History))
         (Filt     (Kubed-Ext--Read-Log-Filter
                    "Filter Pattern" Kubed-Ext-Log-Error-Pattern))
         (Since    (Read-String (Format-Prompt "Since" Kubed-Ext-Log-Default-Since)
                                Nil Nil Kubed-Ext-Log-Default-Since))
         (Follow   (Y-Or-N-P "Follow (Stream) Logs? "))
         (All-Con  (Y-Or-N-P "All Containers? "))
         (Pods     (Kubed-Ext--Pods-By-Selector Selector Ctx Ns))
         (Buf      (Generate-New-Buffer
                    (Format "*Kubed-Parallel-Logs -L %S@%S[%S]*"
                            Selector (Or Ns "Default") Ctx))))
        (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
        (Kubed-Ext--Parallel-Logs-Run Pods Ctx Ns Filt Since Follow All-Con Buf)
        (Display-Buffer Buf)))

;;; ─── Deployment: Error Logs ───────────────────────────────────

(Defun Kubed-Ext-Deployments-Logs-Errors (Click)
  "Show Error-Filtered Logs For The Deployment At Click Position."
  (Interactive (List Last-Nonmenu-Event) Kubed-Deployments-Mode)
  (If-Let ((Dep (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Ctx  Kubed-List-Context)
                 (Ns   Kubed-List-Namespace)
                 (Args (Append
                        (List "Logs" (Concat "Deployment/" Dep)
                              "--All-Containers"
                              "--Prefix"
                              "--Since" Kubed-Ext-Log-Default-Since)
                        (When Ctx (List "--Context" Ctx))
                        (When Ns  (List "-N" Ns))))
                 (Buf  (Generate-New-Buffer
                        (Format "*Kubed-Logs-Errors Deployments/%S@%S[%S]*"
                                Dep (Or Ns "Default") (Or Ctx "Current")))))
                (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
                (Kubed-Ext--Launch-Log-Process Buf Args Kubed-Ext-Log-Error-Pattern)
                (Display-Buffer Buf))
          (User-Error "No Kubernetes Deployment At Point")))

;;; ─── Deployment: Custom-Filtered Logs ────────────────────────

(Defun Kubed-Ext-Deployments-Logs-Custom-Filter (Click)
  "Show Filtered Logs For The Deployment At Click, Prompting For Options."
  (Interactive (List Last-Nonmenu-Event) Kubed-Deployments-Mode)
  (If-Let ((Dep (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Ctx    Kubed-List-Context)
                 (Ns     Kubed-List-Namespace)
                 (Filt   (Kubed-Ext--Read-Log-Filter
                          "Filter Pattern" Kubed-Ext-Log-Error-Pattern))
                 (Since  (Read-String (Format-Prompt "Since" Kubed-Ext-Log-Default-Since)
                                      Nil Nil Kubed-Ext-Log-Default-Since))
                 (Follow (Y-Or-N-P "Follow (Stream) Logs? "))
                 (Args   (Append
                          (List "Logs" (Concat "Deployment/" Dep)
                                "--All-Containers"
                                "--Prefix"
                                "--Since" Since)
                          (When Ctx    (List "--Context" Ctx))
                          (When Ns     (List "-N" Ns))
                          (When Follow '("--Follow"))))
                 (Buf    (Generate-New-Buffer
                          (Format "*Kubed-Logs-Filtered Deployments/%S@%S[%S]*"
                                  Dep (Or Ns "Default") (Or Ctx "Current")))))
                (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
                (Kubed-Ext--Launch-Log-Process Buf Args Filt)
                (Display-Buffer Buf))
          (User-Error "No Kubernetes Deployment At Point")))

;;; ─── Deployment: Parallel Logs ────────────────────────────────

(Defun Kubed-Ext-Deployments-Logs-Parallel (Click)
  "Stream Filtered Logs From All Pods Of The Deployment At Click."
  (Interactive (List Last-Nonmenu-Event) Kubed-Deployments-Mode)
  (If-Let ((Dep (Kubed-Ext--Resource-At-Event Click)))
          (Let* ((Ctx Kubed-List-Context)
                 (Ns  Kubed-List-Namespace)
                 (Sel (Kubed-Ext--Workload-Pod-Selector "Deployments" Dep Ctx Ns)))
                (Unless Sel
                        (User-Error "Cannot Determine Pod Selector For Deployment `%S'" Dep))
                (Let* ((Filt   (Kubed-Ext--Read-Log-Filter
                                "Filter Pattern" Kubed-Ext-Log-Error-Pattern))
                       (Since  (Read-String (Format-Prompt "Since" Kubed-Ext-Log-Default-Since)
                                            Nil Nil Kubed-Ext-Log-Default-Since))
                       (Follow (Y-Or-N-P "Follow (Stream) Logs? "))
                       (All-C  (Y-Or-N-P "All Containers Per Pod? "))
                       (Pods   (Kubed-Ext--Pods-By-Selector Sel Ctx Ns))
                       (Buf    (Generate-New-Buffer
                                (Format "*Kubed-Parallel-Logs Deployments/%S@%S[%S]*"
                                        Dep (Or Ns "Default") (Or Ctx "Current")))))
                      (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
                      (Kubed-Ext--Parallel-Logs-Run Pods Ctx Ns Filt Since Follow All-C Buf)
                      (Display-Buffer Buf)))
          (User-Error "No Kubernetes Deployment At Point")))

;;; ─── Standalone: Parallel For Any Workload ────────────────────

;;;###Autoload
(Defun Kubed-Ext-Logs-Parallel-For-Workload (&Optional Context Namespace)
  "Stream Parallel Filtered Logs From Pods Of Any Workload Resource.

Context And Namespace Are Optional; Defaults To Current Context/Namespace."
  (Declare (Advertised-Calling-Convention (Context Namespace) "1.0"))
  (Interactive
   (Let* ((C (Kubed-Local-Context))
          (C (If (Equal Current-Prefix-Arg '(16))
                 (Kubed-Read-Context "Context" C) C))
          (N (Kubed--Namespace C Current-Prefix-Arg)))
         (List C N)))
  (Let* ((Ctx  (Or Context (Kubed-Local-Context)))
         (Ns   (Or Namespace (Kubed--Namespace Ctx)))
         (Type (Completing-Read "Workload Type: "
                                '("Deployments" "Statefulsets" "Daemonsets"
                                  "Replicasets" "Jobs")
                                Nil T Nil Nil "Deployments"))
         (Name (Kubed-Read-Resource-Name Type "Parallel Logs For" Nil Nil Ctx Ns))
         (Sel  (Kubed-Ext--Workload-Pod-Selector Type Name Ctx Ns)))
        (Unless Sel
                (User-Error "Cannot Determine Pod Selector For %S/%S" Type Name))
        (Let* ((Filt   (Kubed-Ext--Read-Log-Filter
                        "Filter Pattern" Kubed-Ext-Log-Error-Pattern))
               (Since  (Read-String (Format-Prompt "Since" Kubed-Ext-Log-Default-Since)
                                    Nil Nil Kubed-Ext-Log-Default-Since))
               (Follow (Y-Or-N-P "Follow (Stream) Logs? "))
               (All-C  (Y-Or-N-P "All Containers Per Pod? "))
               (Pods   (Kubed-Ext--Pods-By-Selector Sel Ctx Ns))
               (Buf    (Generate-New-Buffer
                        (Format "*Kubed-Parallel-Logs %S/%S@%S[%S]*"
                                Type Name (Or Ns "Default") Ctx))))
              (With-Current-Buffer Buf (Run-Hooks 'Kubed-Logs-Setup-Hook))
              (Kubed-Ext--Parallel-Logs-Run Pods Ctx Ns Filt Since Follow All-C Buf)
              (Display-Buffer Buf))))

;;; ─── Keybindings § 22b ────────────────────────────────────────

(Keymap-Set Kubed-Deployments-Mode-Map "E" #'Kubed-Ext-Deployments-Logs-Errors)
(Keymap-Set Kubed-Deployments-Mode-Map "H" #'Kubed-Ext-Deployments-Logs-Custom-Filter)
(Keymap-Set Kubed-Prefix-Map           "W" #'Kubed-Ext-Logs-Parallel-For-Workload)

;;; ─── Extend Deployment Transient Suffixes ─────────────────────

(Let ((Entry (Assoc "Deployments" Kubed-Ext-Extra-Transient-Suffixes)))
     (When Entry
           (Setcdr Entry
                   (Append (Cdr Entry)
                           '(("Le" "Error Logs (Stream)"
                              Kubed-Ext-Deployments-Logs-Errors)
                             ("Lf" "Filter Logs (Custom)"
                              Kubed-Ext-Deployments-Logs-Custom-Filter)
                             ("Lp" "Parallel Logs (All Pods)"
                              Kubed-Ext-Deployments-Logs-Parallel))))))

;;;; ═══════════════════════════════════════════════════════════════
;;; § 23.  Setup
;;; ═══════════════════════════════════════════════════════════════

(Defun Kubed-Ext-Setup ()
  "Initialize Kubed-Ext Functionality."
  (Message "Kubed-Ext: Setup Complete."))

(Provide 'Kubed-Ext)
;;; Kubed-Ext.El Ends Here
