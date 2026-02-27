;;; kubed-ext.el --- Extensions for Kubed with Async Metrics -*- lexical-binding: t; -*-

;; Author: Chetan Koneru
;; Version: 0.4.0
;; URL: https://github.com/CsBigDataHub/kubed-ext
;; Keywords: tools, kubernetes
;; Package-Requires: ((emacs "29.1") (kubed "0.5.1") (transient "0.4.0"))

;;; Commentary:
;; A comprehensive extension suite for Kubed, transforming Emacs into a
;; production-grade Kubernetes dashboard.

;;; Code:

(require 'kubed)
(require 'kubed-transient)
(require 'transient)
(require 'cl-lib)
(require 'json)

(defgroup kubed-ext nil
  "Extensions for Kubed."
  :group 'kubed)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0.  Autoload Fixes + Full-Screen List Buffers
;;; ═══════════════════════════════════════════════════════════════

(defalias 'kubed-ext-list-ingresss      #'kubed-list-ingresses)
(defalias 'kubed-ext-list-ingressclasss #'kubed-list-ingressclasses)

(add-to-list 'display-buffer-alist
             '("\\`\\*Kubed [a-z][a-z0-9]*[@[]"
               (display-buffer-same-window)))
(add-to-list 'display-buffer-alist
             '("\\`\\*kubed-top-"
               (display-buffer-same-window)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 0a.  Utility Functions
;;; ═══════════════════════════════════════════════════════════════

(defsubst kubed-ext-none-p (s)
  "Return non-nil if S is nil, empty, or a kubectl placeholder value."
  (or (null s)
      (not (stringp s))
      (string-empty-p s)
      (string= s "<none>")
      (string= s "")
      (string= s "nil")
      (string-match-p "\\`[ \t]*\'" s)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 0b.  Defcustoms: Status Faces, Output Format, Shell
;;; ═══════════════════════════════════════════════════════════════

(defcustom kubed-ext-status-faces
  '(("Running"                      . success)
    ("Succeeded"                    . shadow)
    ("Completed"                    . (:foreground "yellow"))
    ("Pending"                      . warning)
    ("ContainerCreating"            . warning)
    ("PodInitializing"              . warning)
    ("Unknown"                      . warning)
    ("Terminating"                  . (:foreground "blue"))
    ("Failed"                       . error)
    ("Error"                        . error)
    ("OOMKilled"                    . error)
    ("CrashLoopBackOff"             . error)
    ("ImagePullBackOff"             . error)
    ("ErrImagePull"                 . error)
    ("CreateContainerConfigError"   . error)
    ("InvalidImageName"             . error)
    ("Evicted"                      . error)
    ("RunContainerError"            . error)
    ("StartError"                   . error)
    ("Ready"                        . success)
    ("NotReady"                     . error)
    ("Active"                       . success)
    ("Bound"                        . success)
    ("Released"                     . shadow)
    ("Healthy"                      . success))
  "Association list mapping pod/resource status strings to faces.
Customize this to change colors for different Kubernetes statuses."
  :type '(alist :key-type string :value-type (choice face sexp))
  :group 'kubed-ext)

(defcustom kubed-ext-output-format "yaml"
  "Default output format for resource display buffers (yaml or json)."
  :type '(choice (const "yaml") (const "json"))
  :group 'kubed-ext)

(defcustom kubed-ext-pod-shell "/bin/sh"
  "Shell to run when opening a terminal in a Kubernetes pod."
  :type 'string
  :group 'kubed-ext)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0c.  Buffer-Local Variables
;;; ═══════════════════════════════════════════════════════════════

(defvar-local kubed-ext-resource-filter ""
  "Substring filter for highlighting matching resources in list buffers.")

(defvar-local kubed-ext--header-error nil
  "Current error message to display, or nil if no error.")

(defvar-local kubed-ext--error-overlay nil
  "Overlay used to display error message at top of buffer.")

(defvar-local kubed-ext-list-label-selector nil
  "Active label selector for server-side kubectl filtering.")

;; External package variable declarations (suppress byte-compiler warnings).
(defvar vterm-shell)
(defvar vterm-buffer-name)
(defvar eat-buffer-name)
(defvar eshell-buffer-name)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0d.  Namespace Prefetch Cache
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext--namespace-prefetch-contexts (make-hash-table :test 'equal)
  "Contexts for which namespace prefetch has been triggered.")

(defun kubed-ext-prefetch-namespaces (&optional context)
  "Asynchronously prefetch namespace list for CONTEXT.
Does nothing if namespaces are already cached or a fetch is in progress.
This ensures `kubed-read-namespace' completions appear without delay."
  (let ((ctx (or context (ignore-errors (kubed-local-context)))))
    (when ctx
      (condition-case nil
          (when (and (not (alist-get 'resources
                                     (kubed--alist "namespaces" ctx nil)))
                     (not (process-live-p
                           (alist-get 'process
                                      (kubed--alist "namespaces" ctx nil)))))
            (puthash ctx t kubed-ext--namespace-prefetch-contexts)
            (kubed-update "namespaces" ctx nil))
        (error nil)))))

(defun kubed-ext--ensure-namespaces-ready (context)
  "Ensure namespace list for CONTEXT is available, waiting if necessary.
If a fetch is in progress, wait for it.  If no fetch has started and no
cached data exists, start one and wait.  If data is already cached,
return immediately."
  (let ((ctx (or context (kubed-local-context))))
    (when ctx
      (unless (alist-get 'resources (kubed--alist "namespaces" ctx nil))
        (let ((proc (alist-get 'process
                               (kubed--alist "namespaces" ctx nil))))
          ;; No live fetch in progress — start one.
          (unless (and proc (process-live-p proc))
            (condition-case nil
                (kubed-update "namespaces" ctx nil)
              (error nil))
            (setq proc (alist-get 'process
                                  (kubed--alist "namespaces" ctx nil))))
          ;; Wait for the running fetch to finish.
          (when (and proc (process-live-p proc))
            (message "Loading namespaces for `%s'..." ctx)
            (while (process-live-p proc)
              (accept-process-output proc 1))))))))

(defun kubed-ext--prefetch-namespaces-hook ()
  "Prefetch namespaces when a kubed list buffer is created."
  (when (bound-and-true-p kubed-list-context)
    (kubed-ext-prefetch-namespaces kubed-list-context)))

(add-hook 'kubed-list-mode-hook #'kubed-ext--prefetch-namespaces-hook)

(defun kubed-ext--prefetch-after-use-context (&rest _)
  "Prefetch namespaces after `kubed-use-context' change the default context."
  (when (and (consp kubed-default-context-and-namespace)
             (car kubed-default-context-and-namespace))
    ;; Clear prefetch flag so the new context gets a fresh fetch.
    (remhash (car kubed-default-context-and-namespace)
             kubed-ext--namespace-prefetch-contexts)
    (kubed-ext-prefetch-namespaces
     (car kubed-default-context-and-namespace))))

(advice-add 'kubed-use-context :after #'kubed-ext--prefetch-after-use-context)

;;; ═══════════════════════════════════════════════════════════════
;;; § 0e.  Auto-Refresh Visible List Buffers
;;; ═══════════════════════════════════════════════════════════════

(defcustom kubed-ext-auto-refresh-interval '(15 . 30)
  "Interval range in seconds for auto-refreshing visible kubed list buffers.

A cons cell (MIN . MAX).  Each cycle picks a random delay between MIN
and MAX seconds before the next refresh.  Set to nil to disable even
when the mode is on."
  :type '(choice (const :tag "Disabled" nil)
                 (cons :tag "Interval range (seconds)"
                   (natnum :tag "Minimum")
                   (natnum :tag "Maximum")))
  :group 'kubed-ext)

(defvar kubed-ext--auto-refresh-timer nil
  "Timer for auto-refreshing visible kubed list buffers.")

(defun kubed-ext--auto-refresh-random-delay ()
  "Return a random delay in seconds, or nil if auto-refresh is disabled."
  (when (consp kubed-ext-auto-refresh-interval)
    (let ((lo (car kubed-ext-auto-refresh-interval))
          (hi (cdr kubed-ext-auto-refresh-interval)))
      (+ lo (random (max 1 (1+ (- hi lo))))))))


(defun kubed-ext--auto-refresh-schedule ()
  "Schedule the next auto-refresh tick with a random delay."
  (kubed-ext--auto-refresh-cancel-timer)
  (when-let ((delay (kubed-ext--auto-refresh-random-delay)))
    (setq kubed-ext--auto-refresh-timer
          (run-with-timer delay nil #'kubed-ext--auto-refresh-tick))))

(defun kubed-ext--auto-refresh-cancel-timer ()
  "Cancel the pending auto-refresh timer if any."
  (when (timerp kubed-ext--auto-refresh-timer)
    (cancel-timer kubed-ext--auto-refresh-timer)
    (setq kubed-ext--auto-refresh-timer nil)))

;;;###autoload
(define-minor-mode kubed-ext-auto-refresh-mode
  "Periodically refresh visible kubed list buffers.

Refresh interval is randomized between the bounds in
`kubed-ext-auto-refresh-interval' (default 15-30 s).  Only buffers
currently shown in a window are refreshed, and a refresh is skipped
when an update is already in progress or the minibuffer is active."
  :global t
  :lighter " KRef"
  :group 'kubed-ext
  (if kubed-ext-auto-refresh-mode
      (progn
        (kubed-ext--auto-refresh-schedule)
        (message "Kubed auto-refresh enabled (%d-%ds)."
                 (car kubed-ext-auto-refresh-interval)
                 (cdr kubed-ext-auto-refresh-interval)))
    (kubed-ext--auto-refresh-cancel-timer)
    (message "Kubed auto-refresh disabled.")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 0f.  Upstream Bug Fix — kubed-update Column Offset Overrun
;;; ═══════════════════════════════════════════════════════════════
;;
;; `kubed-update' calculates column offsets from the header line and
;; applies them to every data row.  When a trailing column is absent
;; (e.g. a pod with no IP, no node, or no deletion timestamp) the data
;; row is shorter than the header, so the fixed offset exceeds the line
;; length and triggers "Args out of range".
;;
;; The fix wraps the process sentinel that kubed-update registers so
;; that every buffer-substring call is clamped to pos-eol.

(defun kubed-ext--safe-update (orig-fn type context &optional namespace)
  "Around advice for `kubed-update': clamp `buffer-substring' in the sentinel.
ORIG-FN TYPE CONTEXT/NAMESPACE."
  (cl-letf* ((orig-mp (symbol-function 'make-process))
             ((symbol-function 'make-process)
              (lambda (&rest args)
                (let* ((orig-sentinel (plist-get args :sentinel))
                       (safe-sentinel
                        (lambda (proc status)
                          (cl-letf* ((orig-bs (symbol-function 'buffer-substring))
                                     ((symbol-function 'buffer-substring)
                                      (lambda (start end)
                                        (with-current-buffer (process-buffer proc)
                                          (let ((limit (point-max)))
                                            (funcall orig-bs
                                                     (min start limit)
                                                     (min end   limit)))))))
                            (funcall orig-sentinel proc status)))))
                  (apply orig-mp (plist-put args :sentinel safe-sentinel))))))
    (funcall orig-fn type context namespace)))

(advice-add 'kubed-update :around #'kubed-ext--safe-update)

;;; ═══════════════════════════════════════════════════════════════
;;; § 1.  CRD Auto-Column Discovery
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext-enriched-resources (make-hash-table :test 'equal)
  "Track resource type+context pairs already enriched with CRD columns.")

(defun kubed-ext--crd-printer-columns (resource-plural context)
  "Fetch additionalPrinterColumns from CRD for RESOURCE-PLURAL in CONTEXT."
  (condition-case err
      (let* ((ctx-args (when context (list "--context" context)))
             (crd-name
              (with-temp-buffer
                (when (zerop (apply #'call-process
                                    kubed-kubectl-program nil '(t nil) nil
                                    "api-resources" "--no-headers" "-o" "name"
                                    ctx-args))
                  (goto-char (point-min))
                  (when (re-search-forward
                         (concat "^" (regexp-quote resource-plural) "\\.")
                         nil t)
                    (string-trim (thing-at-point 'line t)))))))
        (when crd-name
          (let ((json-str
                 (with-temp-buffer
                   (when (zerop
                          (apply #'call-process
                                 kubed-kubectl-program nil '(t nil) nil
                                 "get" "crd" crd-name "-o"
                                 "jsonpath={.spec.versions[0].additionalPrinterColumns}"
                                 ctx-args))
                     (string-trim (buffer-string))))))
            (when (and json-str
                       (not (string-empty-p json-str))
                       (not (string= json-str "null")))
              (let ((all-cols (json-parse-string
                               json-str
                               :array-type 'list :object-type 'alist)))
                (seq-remove
                 (lambda (c) (string= (alist-get 'name c) "Age"))
                 all-cols))))))
    (error (message "Kubed-ext CRD error: %s" err) nil)))

(defun kubed-ext-enrich-columns (resource-plural context)
  "Inject CRD printer columns into kubed for RESOURCE-PLURAL in CONTEXT."
  (when-let ((cols (kubed-ext--crd-printer-columns resource-plural context)))
    (setf (alist-get resource-plural kubed--columns nil nil #'string=)
          (cons '("NAME:.metadata.name")
                (mapcar
                 (lambda (c)
                   (cons (format "%s:%s"
                                 (upcase (replace-regexp-in-string
                                          "[ /]" "_" (alist-get 'name c)))
                                 (alist-get 'jsonPath c))
                         nil))
                 cols)))
    (let ((fmt-var (intern (format "kubed-%s-columns" resource-plural))))
      (when (boundp fmt-var)
        (set fmt-var
             (mapcar (lambda (c)
                       (list (alist-get 'name c)
                             (max (+ 2 (length (alist-get 'name c))) 14)
                             t))
                     cols))))
    t))

(defun kubed-ext--maybe-enrich-columns (&rest _)
  "Before-advice: lazily discover CRD columns on first list."
  (when-let ((type kubed-list-type))
    (let ((key (cons type (or kubed-list-context ""))))
      (unless (gethash key kubed-ext-enriched-resources)
        (puthash key t kubed-ext-enriched-resources)
        (let ((existing (alist-get type kubed--columns nil nil #'string=)))
          (when (or (null existing) (<= (length existing) 1))
            (when (kubed-ext-enrich-columns type kubed-list-context)
              (let ((fmt-var (intern (format "kubed-%s-columns" type))))
                (when (and (boundp fmt-var) (symbol-value fmt-var))
                  (setq tabulated-list-format
                        (apply #'vector
                               (cons kubed-name-column
                                     (symbol-value fmt-var))))
                  (tabulated-list-init-header))))))))))

(advice-add 'kubed-list-update :before #'kubed-ext--maybe-enrich-columns)

(defun kubed-ext-refresh-crd-columns ()
  "Force re-discovery of CRD columns after switching context."
  (interactive)
  (clrhash kubed-ext-enriched-resources)
  (message "CRD column cache cleared."))

;;; ═══════════════════════════════════════════════════════════════
;;; § 2.  Port-Forward with Auto-Complete
;;; ═══════════════════════════════════════════════════════════════

(defconst kubed-ext--dq (string 34)
  "Double-quote character as a one-char string.")

(defconst kubed-ext--jq-tab
  (concat "{" kubed-ext--dq "\\t" kubed-ext--dq "}")
  "Kubectl jsonpath tab separator token.")

(defconst kubed-ext--jq-nl
  (concat "{" kubed-ext--dq "\\n" kubed-ext--dq "}")
  "Kubectl jsonpath newline separator token.")

(defun kubed-ext--json-patch (key value)
  "Return JSON string {spec:{KEY:VALUE}} for kubectl patch."
  (let ((q kubed-ext--dq))
    (concat "{" q "spec" q ":{" q key q ":" value "}}")))

(defun kubed-ext-resource-container-ports (type name &optional context namespace)
  "Discover container ports for resource NAME of TYPE in CONTEXT and NAMESPACE."
  (let ((jsonpath
         (pcase type
           ((or "pod" "pods")
            ".spec.containers[*].ports[*]")
           ((or "deployment" "deployments"
                "statefulset" "statefulsets"
                "daemonset" "daemonsets"
                "replicaset" "replicasets")
            ".spec.template.spec.containers[*].ports[*]")
           (_ nil))))
    (when jsonpath
      (let ((output
             (with-temp-buffer
               (apply #'call-process
                      kubed-kubectl-program nil '(t nil) nil
                      "get" type name "-o"
                      (concat "jsonpath={range " jsonpath "}"
                              "{.containerPort}" kubed-ext--jq-tab
                              "{.name}" kubed-ext--jq-tab
                              "{.protocol}" kubed-ext--jq-nl
                              "{end}")
                      (append
                       (when namespace (list "-n" namespace))
                       (when context (list "--context" context))))
               (buffer-string))))
        (delq nil
              (mapcar (lambda (line)
                        (let ((parts (split-string line "\t")))
                          (when (and (car parts)
                                     (not (string-empty-p (car parts))))
                            (list (string-to-number (nth 0 parts))
                                  (or (nth 1 parts) "")
                                  (or (nth 2 parts) "TCP")))))
                      (split-string output "\n" t)))))))

(defun kubed-ext-service-ports (service &optional context namespace)
  "Discover ports for SERVICE in CONTEXT and NAMESPACE."
  (let ((output
         (with-temp-buffer
           (apply #'call-process
                  kubed-kubectl-program nil '(t nil) nil
                  "get" "service" service "-o"
                  (concat "jsonpath={range .spec.ports[*]}"
                          "{.port}" kubed-ext--jq-tab
                          "{.targetPort}" kubed-ext--jq-tab
                          "{.name}" kubed-ext--jq-tab
                          "{.protocol}" kubed-ext--jq-nl
                          "{end}")
                  (append
                   (when namespace (list "-n" namespace))
                   (when context (list "--context" context))))
           (buffer-string))))
    (delq nil
          (mapcar (lambda (line)
                    (let ((parts (split-string line "\t")))
                      (when (and (>= (length parts) 4)
                                 (not (string-empty-p (car parts))))
                        (list (string-to-number (nth 0 parts))
                              (nth 1 parts)
                              (nth 2 parts)
                              (nth 3 parts)))))
                  (split-string output "\n" t)))))

(defun kubed-ext--format-port-candidate (port-num name protocol)
  "Format a port completion candidate with PORT-NUM, NAME, and PROTOCOL."
  (format "%d%s%s" port-num
          (if (or (null name) (string-empty-p name)) ""
            (format " (%s)" name))
          (if (or (null protocol) (string= protocol "TCP")) ""
            (format " [%s]" protocol))))

(defun kubed-ext-read-resource-port (type name prompt &optional context namespace)
  "Read port from container ports of TYPE/NAME with PROMPT in CONTEXT/NAMESPACE."
  (let* ((ports (kubed-ext-resource-container-ports
                 type name context namespace))
         (candidates
          (mapcar (lambda (p)
                    (kubed-ext--format-port-candidate
                     (nth 0 p) (nth 1 p) (nth 2 p)))
                  ports)))
    (string-to-number
     (if candidates
         (completing-read (format-prompt prompt nil) candidates nil nil)
       (read-string (format-prompt prompt nil))))))

(defun kubed-ext-read-service-port (service prompt &optional context namespace)
  "Read port from SERVICE ports with PROMPT in CONTEXT and NAMESPACE."
  (let* ((ports (kubed-ext-service-ports service context namespace))
         (candidates
          (mapcar (lambda (p)
                    (kubed-ext--format-port-candidate
                     (nth 0 p) (nth 2 p) (nth 3 p)))
                  ports)))
    (string-to-number
     (if candidates
         (completing-read (format-prompt prompt nil) candidates nil nil)
       (read-string (format-prompt prompt nil))))))

(defun kubed-ext-forward-port (type name local-port remote-port context namespace)
  "Forward LOCAL-PORT to REMOTE-PORT of TYPE/NAME in CONTEXT/NAMESPACE."
  (let ((res-spec (pcase type
                    ((or "pod" "pods") name)
                    ((or "service" "services") (concat "svc/" name))
                    (_ (concat (replace-regexp-in-string "s\'" "" type)
                               "/" name))))
        (desc (format "%s/%s %d:%d in %s[%s]"
                      type name local-port remote-port
                      (or namespace "default") (or context "current"))))
    (message "Forwarding localhost:%d -> %s:%d..."
             local-port res-spec remote-port)
    (push (cons desc
                (apply #'start-process
                       "*kubed-port-forward*" nil
                       kubed-kubectl-program "port-forward"
                       res-spec (format "%d:%d" local-port remote-port)
                       (append
                        (when namespace (list "-n" namespace))
                        (when context (list "--context" context)))))
          kubed-port-forward-process-alist)
    (kubed-ext-refresh-pf-markers-in-visible-buffers)))

(defun kubed-ext-forward-port-to-pod
    (pod local-port remote-port context namespace)
  "Forward LOCAL-PORT to REMOTE-PORT of pod POD in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (p (kubed-read-pod "Forward port to pod" nil nil c n))
          (r (kubed-ext-read-resource-port "pods" p "Remote port" c n))
          (l (read-number (format "Local port (remote=%d): " r) r)))
     (list p l r c n)))
  (kubed-ext-forward-port "pods" pod local-port remote-port context namespace))

(defun kubed-ext-pods-forward-port (click)
  "Forward local network port to remote port of pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((r (kubed-ext-read-resource-port
                 "pods" pod "Remote port"
                 kubed-list-context kubed-list-namespace))
             (l (read-number (format "Local port (remote=%d): " r) r)))
        (kubed-ext-forward-port "pods" pod l r
                                kubed-list-context kubed-list-namespace))
    (user-error "No Kubernetes pod at point")))

(defun kubed-ext-forward-port-to-service
    (service local-port remote-port context namespace)
  "Forward LOCAL-PORT to REMOTE-PORT of service SERVICE in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (s (kubed-read-service "Forward port to service" nil nil c n))
          (r (kubed-ext-read-service-port s "Remote port" c n))
          (l (read-number (format "Local port (remote=%d): " r) r)))
     (list s l r c n)))
  (kubed-ext-forward-port "services" service local-port remote-port
                          context namespace))

(defun kubed-ext-services-forward-port (click)
  "Forward local port to Kubernetes service at CLICK position."
  (interactive (list last-nonmenu-event) kubed-services-mode)
  (if-let ((service (tabulated-list-get-id (mouse-set-point click))))
      (let* ((r (kubed-ext-read-service-port
                 service "Remote port"
                 kubed-list-context kubed-list-namespace))
             (l (read-number (format "Local port (remote=%d): " r) r)))
        (kubed-ext-forward-port "services" service l r
                                kubed-list-context kubed-list-namespace))
    (user-error "No Kubernetes service at point")))

(defun kubed-ext-forward-port-to-deployment
    (deployment local-port remote-port context namespace)
  "Forward LOCAL-PORT to REMOTE-PORT of DEPLOYMENT in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (d (kubed-read-deployment "Forward port to deployment" nil nil c n))
          (r (kubed-ext-read-resource-port "deployments" d "Remote port" c n))
          (l (read-number (format "Local port (remote=%d): " r) r)))
     (list d l r c n)))
  (kubed-ext-forward-port "deployments" deployment local-port remote-port
                          context namespace))

(defun kubed-ext-deployments-forward-port (click)
  "Forward local port to Kubernetes deployment at CLICK position."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((deployment (tabulated-list-get-id (mouse-set-point click))))
      (let* ((r (kubed-ext-read-resource-port
                 "deployments" deployment "Remote port"
                 kubed-list-context kubed-list-namespace))
             (l (read-number (format "Local port (remote=%d): " r) r)))
        (kubed-ext-forward-port "deployments" deployment l r
                                kubed-list-context kubed-list-namespace))
    (user-error "No Kubernetes deployment at point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 3.  Port-Forward Markers + Port-Forward List Buffer
;;; ═══════════════════════════════════════════════════════════════

(defface kubed-ext-port-forward-face
  '((((background dark))  :background "#1a2a1a")
    (((background light)) :background "#e8f5e8"))
  "Face for resources with active port-forwards.")

(defun kubed-ext-has-port-forward-p (name context namespace)
  "Return non-nil if NAME has active port-forward in NAMESPACE and CONTEXT."
  (seq-some (lambda (pair)
              (and (process-live-p (cdr pair))
                   (string-match-p (regexp-quote name) (car pair))
                   (or (null namespace)
                       (string-match-p (regexp-quote namespace) (car pair)))
                   (or (null context)
                       (string-match-p (regexp-quote context) (car pair)))))
            kubed-port-forward-process-alist))

(defun kubed-ext-mark-port-forwards (&rest _)
  "Highlight list rows with active port-forwards."
  (when (derived-mode-p 'kubed-list-mode)
    (remove-overlays (point-min) (point-max) 'kubed-pf t)
    (when (kubed-port-forward-process-alist)
      (save-excursion
        (goto-char (point-min))
        (while (not (eobp))
          (when-let ((id (tabulated-list-get-id)))
            (when (kubed-ext-has-port-forward-p
                   id kubed-list-context kubed-list-namespace)
              (let ((ov (make-overlay (line-beginning-position)
                                      (line-end-position))))
                (overlay-put ov 'kubed-pf t)
                (overlay-put ov 'face 'kubed-ext-port-forward-face)
                (overlay-put ov 'help-echo "Port-forwarding active"))))
          (forward-line))))))

(advice-add 'kubed-list-revert :after #'kubed-ext-mark-port-forwards)

(defun kubed-ext-refresh-pf-markers-in-visible-buffers (&rest _)
  "Refresh port-forward markers in all visible kubed list buffers."
  (dolist (win (window-list))
    (with-current-buffer (window-buffer win)
      (when (derived-mode-p 'kubed-list-mode)
        (kubed-ext-mark-port-forwards)))))

(advice-add 'kubed-stop-port-forward :after
            #'kubed-ext-refresh-pf-markers-in-visible-buffers)

(defun kubed-ext-parse-pf-descriptor (desc)
  "Parse port-forward descriptor DESC into list.
DESC format: type/name local:remote in namespace[context]."
  (if (string-match
       "$.+?$ $[0-9]+:[0-9]+$ in $.+?$$$$.+?$$$" desc)
      (list (match-string 1 desc) (match-string 2 desc)
            (match-string 3 desc) (match-string 4 desc))
    (list desc "" "" "")))

(defun kubed-ext-port-forward-entries ()
  "Return entries for port-forward list buffer."
  (mapcar (lambda (pair)
            (let* ((desc (car pair))
                   (parsed (kubed-ext-parse-pf-descriptor desc))
                   (status (if (process-live-p (cdr pair))
                               (propertize "Active" 'face 'success)
                             (propertize "Dead" 'face 'error))))
              (list desc (vector (nth 0 parsed) (nth 1 parsed)
                                 (nth 2 parsed) (nth 3 parsed) status))))
          (kubed-port-forward-process-alist)))

(defun kubed-ext-port-forwards-stop (click)
  "Stop port-forward at CLICK position."
  (interactive (list last-nonmenu-event) kubed-ext-port-forwards-mode)
  (if-let ((desc (tabulated-list-get-id (mouse-set-point click))))
      (when (y-or-n-p (format "Stop port-forward: %s?" desc))
        (kubed-stop-port-forward desc)
        (tabulated-list-print t))
    (user-error "No port-forward at point")))

(defun kubed-ext-port-forwards-stop-all ()
  "Stop all active port-forwards."
  (interactive nil kubed-ext-port-forwards-mode)
  (when (y-or-n-p (format "Stop all %d port-forwards?"
                          (length kubed-port-forward-process-alist)))
    (dolist (pair (kubed-port-forward-process-alist))
      (when (process-live-p (cdr pair))
        (delete-process (cdr pair))))
    (setq kubed-port-forward-process-alist nil)
    (tabulated-list-print t)
    (kubed-ext-refresh-pf-markers-in-visible-buffers)
    (message "All port-forwards stopped.")))

(defun kubed-ext-port-forwards-refresh ()
  "Refresh port-forward list."
  (interactive nil kubed-ext-port-forwards-mode)
  (kubed-port-forward-process-alist)
  (tabulated-list-print t))

(define-derived-mode kubed-ext-port-forwards-mode tabulated-list-mode
  "Kubed PF"
  "Major mode for listing active Kubernetes port-forwards."
  (setq tabulated-list-format
        [("Resource" 32 t) ("Ports" 14 t)
         ("Namespace" 20 t) ("Context" 24 t) ("Status" 8 t)]
        tabulated-list-padding 2
        tabulated-list-entries #'kubed-ext-port-forward-entries)
  (tabulated-list-init-header))

(keymap-set kubed-ext-port-forwards-mode-map "D"
            #'kubed-ext-port-forwards-stop)
(keymap-set kubed-ext-port-forwards-mode-map "K"
            #'kubed-ext-port-forwards-stop-all)
(keymap-set kubed-ext-port-forwards-mode-map "g"
            #'kubed-ext-port-forwards-refresh)
(keymap-set kubed-ext-port-forwards-mode-map "q" #'quit-window)

(defun kubed-ext-list-port-forwards ()
  "Display buffer listing all active port-forwards."
  (interactive)
  (let ((buf (get-buffer-create "*Kubed Port Forwards*")))
    (with-current-buffer buf
      (kubed-ext-port-forwards-mode)
      (tabulated-list-print t))
    (switch-to-buffer buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 3a.  Error Overlay Header
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext--set-header-error (msg)
  "Set error MSG as an overlay at the top of the current buffer."
  (kubed-ext--clear-header-error)
  (when (and msg (not (string-empty-p (string-trim msg))))
    (setq kubed-ext--header-error (string-trim msg))
    (when (> (buffer-size) 0)
      (let ((ov (make-overlay (point-min) (point-min))))
        (overlay-put ov 'before-string
                     (propertize
                      (format "⚠ %s  [Type `$' for process buffer]\n"
                              kubed-ext--header-error)
                      'face 'error))
        (overlay-put ov 'kubed-ext-error t)
        (overlay-put ov 'priority 100)
        (setq kubed-ext--error-overlay ov)))))

(defun kubed-ext--clear-header-error ()
  "Clear any error overlay from the current buffer."
  (when kubed-ext--error-overlay
    (delete-overlay kubed-ext--error-overlay)
    (setq kubed-ext--error-overlay nil))
  (setq kubed-ext--header-error nil))

(defun kubed-ext--reapply-overlays (&rest _)
  "Re-apply error overlay and filter highlight after buffer revert."
  (when (derived-mode-p 'kubed-list-mode)
    (when kubed-ext--header-error
      (let ((msg kubed-ext--header-error))
        (setq kubed-ext--error-overlay nil)
        (kubed-ext--set-header-error msg)))
    (kubed-ext--apply-filter-highlights)))

(advice-add 'kubed-list-revert :after #'kubed-ext--reapply-overlays)

(defun kubed-ext--update-error-advice (orig-fn type context &optional namespace)
  "Around advice for `kubed-update' to capture errors as header overlays.
ORIG-FN is the original function, TYPE CONTEXT NAMESPACE are its args."
  (funcall orig-fn type context namespace)
  ;; Wrap the sentinel of the process that was just created.
  (when-let ((proc (alist-get 'process
                              (kubed--alist type context namespace))))
    (when (process-live-p proc)
      (let ((orig-sentinel (process-sentinel proc))
            (err-buf-name (format " *kubed-get-%s-stderr*" type)))
        (set-process-sentinel
         proc
         (lambda (p status)
           (funcall orig-sentinel p status)
           (dolist (buf (buffer-list))
             (when (buffer-live-p buf)
               (with-current-buffer buf
                 (when (and (derived-mode-p 'kubed-list-mode)
                            (equal kubed-list-type type)
                            (equal kubed-list-context context)
                            (equal kubed-list-namespace namespace))
                   (if (string= status "exited abnormally with code 1\n")
                       (let ((err-buf (get-buffer err-buf-name)))
                         (kubed-ext--set-header-error
                          (if (and err-buf (buffer-live-p err-buf))
                              (with-current-buffer err-buf
                                (string-trim (buffer-string)))
                            "kubectl command failed")))
                     (kubed-ext--clear-header-error))))))))))))

(advice-add 'kubed-update :around #'kubed-ext--update-error-advice)

;;; ═══════════════════════════════════════════════════════════════
;;; § 3b.  Mark / Unmark System (kubel-style)
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-marked-items ()
  "Return list of resource IDs marked with `*' in current buffer."
  (let (items)
    (save-excursion
      (goto-char (point-min))
      (while (not (eobp))
        (when (eq (char-after) ?*)
          (when-let ((id (tabulated-list-get-id)))
            (push id items)))
        (forward-line)))
    (nreverse items)))

(defun kubed-ext-mark-item ()
  "Mark the resource at point with `*'."
  (interactive nil kubed-list-mode)
  (tabulated-list-put-tag
   (propertize "*" 'face 'dired-marked 'help-echo "Selected") t))

(defun kubed-ext-unmark-item ()
  "Unmark the resource at point."
  (interactive nil kubed-list-mode)
  (tabulated-list-put-tag " " t))

(defun kubed-ext-mark-all ()
  "Mark all visible resources in the current buffer."
  (interactive nil kubed-list-mode)
  (save-excursion
    (goto-char (point-min))
    (while (not (eobp))
      (when (tabulated-list-get-id)
        (tabulated-list-put-tag
         (propertize "*" 'face 'dired-marked)))
      (forward-line)))
  (message "Marked all items."))

(defun kubed-ext-unmark-all ()
  "Unmark all resources in the current buffer."
  (interactive nil kubed-list-mode)
  (save-excursion
    (goto-char (point-min))
    (while (not (eobp))
      (when (memq (char-after) '(?* ?D))
        (tabulated-list-put-tag " "))
      (forward-line)))
  (message "Unmarked all items."))

;;; ═══════════════════════════════════════════════════════════════
;;; § 3c.  Substring Filter with Highlight (kubel-style)
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext--apply-filter-highlights ()
  "Apply filter highlighting to current buffer.
Matching rows stay normal; non-matching rows get `shadow' face."
  (when (derived-mode-p 'kubed-list-mode)
    (remove-overlays (point-min) (point-max) 'kubed-ext-filter t)
    (when (and (bound-and-true-p kubed-ext-resource-filter)
               (not (string-empty-p kubed-ext-resource-filter)))
      (save-excursion
        (goto-char (point-min))
        (while (not (eobp))
          (when-let ((entry (tabulated-list-get-entry)))
            (let ((line-text (mapconcat #'identity
                                        (append entry nil) " ")))
              (unless (string-match-p kubed-ext-resource-filter line-text)
                (let ((ov (make-overlay (line-beginning-position)
                                        (line-end-position))))
                  (overlay-put ov 'kubed-ext-filter t)
                  (overlay-put ov 'face 'shadow)))))
          (forward-line))))))

(defun kubed-ext-set-filter (filter)
  "Set substring FILTER for highlighting resources.
Empty string clears the filter."
  (interactive
   (list (read-string (format-prompt "Filter" "clear")
                      kubed-ext-resource-filter))
   kubed-list-mode)
  (setq-local kubed-ext-resource-filter (or filter ""))
  (kubed-ext--apply-filter-highlights)
  (force-mode-line-update)
  (if (string-empty-p kubed-ext-resource-filter)
      (message "Filter cleared.")
    (message "Filter: %s" kubed-ext-resource-filter)))

(defun kubed-ext-jump-to-next-highlight ()
  "Jump to the next resource matching `kubed-ext-resource-filter'."
  (interactive nil kubed-list-mode)
  (unless (and kubed-ext-resource-filter
               (not (string-empty-p kubed-ext-resource-filter)))
    (user-error "No filter set (use `f' to set one)"))
  (let ((found nil) (start (point)))
    (end-of-line)
    (while (and (not found) (not (eobp)))
      (forward-line)
      (when-let ((entry (tabulated-list-get-entry)))
        (let ((text (mapconcat #'identity (append entry nil) " ")))
          (when (string-match-p kubed-ext-resource-filter text)
            (setq found t)))))
    (unless found
      (goto-char (point-min))
      (while (and (not found) (< (point) start))
        (when-let ((entry (tabulated-list-get-entry)))
          (let ((text (mapconcat #'identity (append entry nil) " ")))
            (when (string-match-p kubed-ext-resource-filter text)
              (setq found t))))
        (unless found (forward-line))))
    (if found
        (beginning-of-line)
      (goto-char start)
      (message "No match for `%s'" kubed-ext-resource-filter))))

(defun kubed-ext-jump-to-previous-highlight ()
  "Jump to the previous resource matching `kubed-ext-resource-filter'."
  (interactive nil kubed-list-mode)
  (unless (and kubed-ext-resource-filter
               (not (string-empty-p kubed-ext-resource-filter)))
    (user-error "No filter set (use `f' to set one)"))
  (let ((found nil) (start (point)))
    (beginning-of-line)
    (while (and (not found) (not (bobp)))
      (forward-line -1)
      (when-let ((entry (tabulated-list-get-entry)))
        (let ((text (mapconcat #'identity (append entry nil) " ")))
          (when (string-match-p kubed-ext-resource-filter text)
            (setq found t)))))
    (unless found
      (goto-char (point-max))
      (while (and (not found) (> (point) start))
        (forward-line -1)
        (when-let ((entry (tabulated-list-get-entry)))
          (let ((text (mapconcat #'identity (append entry nil) " ")))
            (when (string-match-p kubed-ext-resource-filter text)
              (setq found t))))))
    (if found
        (beginning-of-line)
      (goto-char start)
      (message "No match for `%s'" kubed-ext-resource-filter))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 3d.  Wide View + Output Format Toggle
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext--parse-kubectl-table (text)
  "Parse TEXT from kubectl output into (HEADERS . ROWS).
HEADERS is a list of strings, ROWS is a list of lists of strings."
  (let* ((lines (split-string text "\n" t))
         (header-line (car lines))
         (data-lines (cdr lines))
         (starts nil)
         (pos 0))
    (push 0 starts)
    (while (string-match "\\S-+$\\s-+$\\S-" header-line pos)
      (let ((gap-end (match-end 1)))
        (push gap-end starts)
        (setq pos gap-end)))
    (setq starts (nreverse starts))
    (let* ((parse-line
            (lambda (line)
              (let ((cols nil)
                    (offsets starts))
                (while offsets
                  (let* ((beg (car offsets))
                         (end (if (cdr offsets) (cadr offsets) (length line)))
                         (end (min end (length line)))
                         (str (if (>= beg (length line)) ""
                                (string-trim
                                 (substring line beg end)))))
                    (push str cols))
                  (setq offsets (cdr offsets)))
                (nreverse cols))))
           (headers (funcall parse-line header-line))
           (rows (delq nil
                       (mapcar (lambda (line)
                                 (when (not (string-empty-p
                                             (string-trim line)))
                                   (funcall parse-line line)))
                               data-lines))))
      (cons headers rows))))

(defvar-local kubed-ext-wide--type      nil "Resource type shown in this wide view buffer.")
(defvar-local kubed-ext-wide--context   nil "`kubectl' context used in this wide view buffer.")
(defvar-local kubed-ext-wide--namespace nil "Namespace used in this wide view buffer.")
(defvar-local kubed-ext-wide--name nil "Resource name shown in this wide view buffer, or nil for all.")

(dolist (sym '(kubed-ext-wide--type kubed-ext-wide--context kubed-ext-wide--namespace kubed-ext-wide--name))
  (put sym 'permanent-local t))

(defun kubed-ext--colorize-wide-cell (header value)
  "Return VALUE propertized with a face based on column HEADER name."
  (let ((h (upcase (string-trim header))))
    (cond
     ((or (string= h "STATUS") (string= h "PHASE"))
      (let ((face (cdr (assoc value kubed-ext-status-faces))))
        (if face (propertize value 'face face) value)))
     ((string= h "READY")
      (if (string-match "$[0-9]+$/$[0-9]+$" value)
          (let* ((r    (string-to-number (match-string 1 value)))
                 (tot  (string-to-number (match-string 2 value)))
                 (face (cond ((and (= r 0) (= tot 0)) 'shadow)
                             ((= r tot)               'success)
                             ((= r 0)                 'error)
                             (t                       'warning))))
            (propertize value 'face face))
        value))
     ((string= h "RESTARTS")
      (let* ((n    (string-to-number value))
             (face (cond ((= n 0) nil)
                         ((< n 5) 'warning)
                         (t       'error))))
        (if face (propertize value 'face face) value)))
     (t value))))

;; NOTE: Callees defined BEFORE the caller to satisfy the byte-compiler.

(defun kubed-ext--wide-row-primary-p ()
  "Return non-nil if point is on a primary wide-view row.

In the multi-row deployments wide view, continuation rows use an empty
Name column.  We treat rows with a non-empty Name column as primary."
  (when-let ((ent (tabulated-list-get-entry)))
    (let ((name (aref ent 0)))
      (and (stringp name)
           (not (string-empty-p (string-trim name)))))))

(defun kubed-ext--wide-populate-kubectl-wide (type ctx ns &optional name)
  "Populate current buffer using raw `kubectl get -o wide' for TYPE, CTX, NS.
Optional NAME limits output to a single named resource."
  (let* ((raw     (with-temp-buffer
                    (apply #'call-process kubed-kubectl-program nil t nil
                           `("get" ,type ,@(when name (list name))
                             "-o" "wide"
                             ,@(when ns  `("-n"        ,ns))
                             ,@(when ctx `("--context" ,ctx))))
                    (buffer-string)))
         (parsed (kubed-ext--parse-kubectl-table raw))
         (headers (car parsed))
         (rows (cdr parsed)))
    (unless headers
      (user-error "`kubectl' returned no output for %s" type))
    (let* ((content-widths
            (cl-loop for h in headers
                     for i from 0
                     collect (apply #'max
                                    (length h)
                                    4
                                    (mapcar (lambda (row)
                                              (length (or (nth i row) "")))
                                            rows))))
           (min-widths
            (cl-loop for h in headers
                     for i from 0
                     collect (max 6 (min (nth i content-widths)
                                         (max (length h) 6)))))
           (available
            (max 20 (- (window-body-width (selected-window)) 2)))
           (padding 2)
           (total (lambda (widths)
                    (+ (apply #'+ widths)
                       (* padding (max 0 (1- (length widths)))))))
           (widths (copy-sequence content-widths)))
      (while (> (funcall total widths) available)
        (let* ((idx (cl-position (apply #'max widths) widths))
               (w (nth idx widths))
               (minw (nth idx min-widths)))
          (if (<= w minw)
              (setq widths nil)
            (setf (nth idx widths) (1- w)))))
      (unless widths
        (setq widths min-widths))
      (setq tabulated-list-format
            (apply #'vector
                   (cl-loop for h in headers
                            for w in widths
                            collect (list h w t)))))
    (setq tabulated-list-entries
          (mapcar (lambda (row)
                    (list (or (car row) "")
                          (apply #'vector
                                 (cl-loop for h in headers
                                          for i from 0
                                          collect (kubed-ext--colorize-wide-cell
                                                   h (or (nth i row) ""))))))
                  rows))
    (setq tabulated-list-padding 2)
    (tabulated-list-init-header)
    (tabulated-list-print t)))

(defun kubed-ext--wide-populate-deployments (ctx ns &optional name)
  "Populate a multi-row, intuitive wide view for Deployments.

CTX is the kubectl context, NS is the namespace.
Optional NAME limits output to a single named deployment."
  (let* ((json-str
          (with-temp-buffer
            (apply #'call-process kubed-kubectl-program nil t nil
                   (append (list "get" "deployments")
                           (when name (list name))
                           (list "-o" "json")
                           (when ns (list "-n" ns))
                           (when ctx (list "--context" ctx))))
            (buffer-string)))
         (obj (json-parse-string json-str :object-type 'alist :array-type 'list))
         (items (if name
                    (if (alist-get 'items obj)
                        (alist-get 'items obj)
                      (list obj))
                  (or (alist-get 'items obj) '())))
         (entries nil)
         (seen-detail (make-hash-table :test 'equal)))
    (setq tabulated-list-padding 2)
    (setq tabulated-list-format
          (vector
           (list "Name" 34 t)
           (list "Ready" 10 t)
           (list "Up-to-date" 12 t)
           (list "Available" 12 t)
           (list "Age" 10 t)
           (list "Detail" 70 t)))
    (dolist (it items)
      (let* ((meta (alist-get 'metadata it))
             (spec (alist-get 'spec it))
             (status (alist-get 'status it))
             (dep-name (or (alist-get 'name meta) ""))
             (labels (alist-get 'labels meta))
             (selector (alist-get 'selector spec))
             (match-labels (alist-get 'matchLabels selector))
             (replicas (number-to-string (or (alist-get 'replicas spec) 0)))
             (ready (number-to-string (or (alist-get 'readyReplicas status) 0)))
             (updated (number-to-string (or (alist-get 'updatedReplicas status) 0)))
             (available (number-to-string (or (alist-get 'availableReplicas status) 0)))
             (creation (or (alist-get 'creationTimestamp meta) ""))
             (age (kubed-ext--format-age creation))
             (ready-str (kubed-ext--format-ready-total ready replicas))
             (updated-str (kubed-ext--format-deployment-count updated replicas))
             (avail-str (kubed-ext--format-deployment-count available replicas))
             (tmpl (alist-get 'template spec))
             (podspec (and tmpl (alist-get 'spec tmpl)))
             (containers (and podspec (alist-get 'containers podspec))))
        (let* ((pairs
                (mapcar (lambda (c)
                          (let ((cname (or (alist-get 'name c) ""))
                                (img (or (alist-get 'image c) "")))
                            (cons cname img)))
                        (or containers '())))
               (summary
                (if (null pairs)
                    ""
                  (mapconcat
                   (lambda (p)
                     (let ((cname (car p))
                           (img (cdr p))
                           (img-face 'font-lock-constant-face))
                       (concat
                        (propertize cname 'face 'shadow)
                        (propertize " → " 'face 'shadow)
                        (propertize img 'face img-face))))
                   pairs
                   (propertize " | " 'face 'shadow)))))
          (push (list dep-name
                      (vector dep-name ready-str updated-str avail-str age summary))
                entries))
        (cl-flet
            ((push-detail
              (detail)
              (when (and (stringp detail) (not (string-empty-p (string-trim detail))))
                (let ((k (cons dep-name detail)))
                  (unless (gethash k seen-detail)
                    (puthash k t seen-detail)
                    (push (list dep-name
                                (vector "" "" "" "" ""
                                        (propertize detail 'face 'shadow)))
                          entries))))))
          (dolist (c (or containers '()))
            (let* ((cname (or (alist-get 'name c) ""))
                   (img (or (alist-get 'image c) ""))
                   (img-face 'font-lock-constant-face)
                   (detail (concat "container: " cname "  image: " img)))
              (let ((k (cons dep-name detail)))
                (unless (gethash k seen-detail)
                  (puthash k t seen-detail)
                  (push (list dep-name
                              (vector "" "" "" "" ""
                                      (concat
                                       (propertize "container: " 'face 'shadow)
                                       (propertize cname 'face 'shadow)
                                       (propertize "  image: " 'face 'shadow)
                                       (propertize img 'face img-face))))
                        entries)))))
          (when (and match-labels (listp match-labels))
            (dolist (pair match-labels)
              (push-detail (format "selector: %s=%s" (car pair) (cdr pair)))))
          (when (and labels (listp labels))
            (dolist (pair labels)
              (push-detail (format "label: %s=%s" (car pair) (cdr pair))))))))
    (setq tabulated-list-entries (nreverse entries))
    (tabulated-list-init-header)
    (tabulated-list-print t)))

(defun kubed-ext--wide-populate (type ctx ns &optional name)
  "Fill the current buffer with a wide-format table for TYPE in CTX/NS.

Optional argument NAME limits output to a single named resource.

For most types, this shells out to:
  kubectl get TYPE -o wide

For deployments, we render a more intuitive multi-row view based on JSON.

This runs `kubectl' synchronously."
  (setq kubed-ext-wide--type      type
        kubed-ext-wide--context   ctx
        kubed-ext-wide--namespace ns
        kubed-ext-wide--name      name)
  (if (string= type "deployments")
      (kubed-ext--wide-populate-deployments ctx ns name)
    (kubed-ext--wide-populate-kubectl-wide type ctx ns name)))

(defun kubed-ext-list-wide ()
  "Display the current resource list in kubectl wide format with color coding.
When point is on a resource, show only that resource."
  (interactive nil kubed-list-mode)
  (let* ((type kubed-list-type)
         (ctx  kubed-list-context)
         (ns   kubed-list-namespace)
         (name (tabulated-list-get-id))
         (buf  (get-buffer-create
                (format "*Kubed wide %s/%s/%s%s*"
                        type (or ns "cluster") (or ctx "current")
                        (if name (concat "/" name) "")))))
    (pop-to-buffer buf)
    (kubed-ext-wide-mode)
    (let ((inhibit-read-only t))
      (kubed-ext--wide-populate type ctx ns name))))

(defun kubed-ext-wide-describe-resource ()
  "Describe the Kubernetes resource at point in the wide view buffer."
  (interactive nil kubed-ext-wide-mode)
  (unless (kubed-ext--wide-row-primary-p)
    (user-error "No primary resource row at point"))
  (if-let ((name (tabulated-list-get-id)))
      (let* ((type kubed-ext-wide--type)
             (ns   kubed-ext-wide--namespace)
             (ctx  kubed-ext-wide--context))
        (unless type
          (user-error "No resource type context in this buffer"))
        (let ((buf (get-buffer-create
                    (format "*Kubed describe %s/%s/%s/%s*"
                            type name
                            (or ns "default")
                            (or ctx "current")))))
          (with-current-buffer buf
            (let ((inhibit-read-only t))
              (erase-buffer)
              (apply #'call-process kubed-kubectl-program nil t nil
                     (append (list "describe" type name)
                             (when ns (list "-n" ns))
                             (when ctx (list "--context" ctx))))
              (goto-char (point-min))
              (special-mode)))
          (display-buffer buf)))
    (user-error "No Kubernetes resource at point")))

(defun kubed-ext-wide-fit-column (n)
  "Fit width of Nth table column to its content in wide view.
If N is negative, fit all columns.  Interactively, N is the column
number at point, or the numeric prefix argument if you provide one."
  (interactive
   (list (if current-prefix-arg
             (prefix-numeric-value current-prefix-arg)
           (kubed-list-column-number-at-point)))
   kubed-ext-wide-mode)
  (kubed-list-fit-column-width-to-content n))

(define-derived-mode kubed-ext-wide-mode tabulated-list-mode "Kubed Wide"
  "Major mode for displaying kubectl wide output with color coding.
\\{kubed-ext-wide-mode-map}"
  (setq truncate-lines t)
  (setq-local word-wrap nil)
  (setq-local truncate-string-ellipsis (propertize "…" 'face 'shadow))
  (keymap-set kubed-ext-wide-mode-map "g"   #'kubed-ext-list-wide-refresh)
  (keymap-set kubed-ext-wide-mode-map "q"   #'quit-window)
  (keymap-set kubed-ext-wide-mode-map "b"   #'kubed-ext-switch-buffer)
  (keymap-set kubed-ext-wide-mode-map "<"   #'kubed-ext-wide-fit-column)
  (keymap-set kubed-ext-wide-mode-map "|"   #'kubed-ext-wide-fit-column)
  (keymap-set kubed-ext-wide-mode-map "}"   #'tabulated-list-widen-current-column)
  (keymap-set kubed-ext-wide-mode-map "{"   #'tabulated-list-narrow-current-column)
  (keymap-set kubed-ext-wide-mode-map "?"   #'kubed-ext-transient-menu)
  (keymap-set kubed-ext-wide-mode-map "d"   #'kubed-ext-wide-describe-resource)
  (keymap-set kubed-ext-wide-mode-map "RET" #'kubed-ext-wide-describe-resource))

(defun kubed-ext-list-wide-refresh ()
  "Refresh the wide view buffer by re-running kubectl."
  (interactive nil kubed-ext-wide-mode)
  (unless kubed-ext-wide--type
    (user-error "No resource context; open wide view from a resource list buffer"))
  (message "Refreshing %s wide view..." kubed-ext-wide--type)
  (let ((inhibit-read-only t))
    (kubed-ext--wide-populate
     kubed-ext-wide--type
     kubed-ext-wide--context
     kubed-ext-wide--namespace
     kubed-ext-wide--name)))

(defun kubed-ext-toggle-output-format ()
  "Toggle resource display format between YAML and JSON and revert."
  (interactive)
  (setq kubed-ext-output-format
        (if (string= kubed-ext-output-format "yaml") "json" "yaml"))
  (when (bound-and-true-p kubed-display-resource-mode)
    (revert-buffer))
  (message "Output format: %s" kubed-ext-output-format))

(defun kubed-ext-set-output-format ()
  "Prompt for and set the output format for resource display."
  (interactive)
  (setq kubed-ext-output-format
        (completing-read "Output format: " '("yaml" "json") nil t
                         nil nil kubed-ext-output-format))
  (when (bound-and-true-p kubed-display-resource-mode)
    (revert-buffer))
  (message "Output format set to %s." kubed-ext-output-format))

(defun kubed-ext--display-revert-format (orig-fn &rest args)
  "Around advice for `kubed-display-resource-revert' for configurable format.
ORIG-FN is the original function, ARGS its arguments."
  (if (string= kubed-ext-output-format "yaml")
      (apply orig-fn args)
    (seq-let (type name context namespace)
        kubed-display-resource-info
      (let ((inhibit-read-only t)
            (target (current-buffer)))
        (buffer-disable-undo)
        (with-temp-buffer
          (unless (zerop
                   (apply #'call-process
                          kubed-kubectl-program nil t nil "get"
                          type (concat "--output=" kubed-ext-output-format)
                          name
                          (append (when namespace (list "-n" namespace))
                                  (when context (list "--context" context)))))
            (error "Failed to display Kubernetes resource `%s'" name))
          (let ((source (current-buffer)))
            (with-current-buffer target
              (replace-buffer-contents source)
              (set-buffer-modified-p nil)
              (buffer-enable-undo))))))))

(advice-add 'kubed-display-resource-revert :around
            #'kubed-ext--display-revert-format)

;;; ═══════════════════════════════════════════════════════════════
;;; § 3e.  Batch Operations on Marked Items
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-delete-marked ()
  "Delete all resources marked with `*'."
  (interactive nil kubed-list-mode)
  (let ((items (kubed-ext-marked-items)))
    (unless items
      (user-error "No resources marked with `*'"))
    (when (y-or-n-p (format "Delete %d marked %s?"
                            (length items) kubed-list-type))
      (kubed-delete-resources kubed-list-type items
                              kubed-list-context kubed-list-namespace)
      (kubed-list-update t))))

(defun kubed-ext-logs-marked ()
  "Show logs for all pods marked with `*'."
  (interactive nil kubed-list-mode)
  (let ((items (kubed-ext-marked-items)))
    (unless items
      (user-error "No resources marked with `*'"))
    (dolist (pod items)
      (kubed-logs kubed-list-type pod kubed-list-context kubed-list-namespace
                  t t nil nil nil nil nil))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 4.  Strimzi Operation Helpers
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-strimzi-pause-reconciliation
    (type name &optional context namespace)
  "Pause Strimzi reconciliation for resource NAME of TYPE in CONTEXT."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (type (completing-read "Resource type: "
                                 '("kafka" "kafkaconnect" "kafkabridge"
                                   "kafkamirrormaker2" "kafkamirrormaker")
                                 nil t))
          (name (kubed-read-resource-name
                 (concat type "s") "Pause reconciliation for" nil nil c n)))
     (list type name c n)))
  (let ((context (or context (kubed-local-context)))
        (namespace (or namespace
                       (kubed--namespace (or context
                                             (kubed-local-context))))))
    (unless (zerop
             (apply #'call-process
                    kubed-kubectl-program nil nil nil
                    "annotate" type name
                    "strimzi.io/pause-reconciliation=true" "--overwrite"
                    (append
                     (when namespace (list "-n" namespace))
                     (when context (list "--context" context)))))
      (user-error "Failed to pause reconciliation for %s/%s" type name))
    (message "Paused reconciliation for %s/%s." type name)))

(defun kubed-ext-strimzi-resume-reconciliation
    (type name &optional context namespace)
  "Resume Strimzi reconciliation for resource NAME of TYPE in CONTEXT."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (type (completing-read "Resource type: "
                                 '("kafka" "kafkaconnect" "kafkabridge"
                                   "kafkamirrormaker2" "kafkamirrormaker")
                                 nil t))
          (name (kubed-read-resource-name
                 (concat type "s") "Resume reconciliation for" nil nil c n)))
     (list type name c n)))
  (let ((context (or context (kubed-local-context)))
        (namespace (or namespace
                       (kubed--namespace (or context
                                             (kubed-local-context))))))
    (unless (zerop
             (apply #'call-process
                    kubed-kubectl-program nil nil nil
                    "annotate" type name
                    "strimzi.io/pause-reconciliation-"
                    (append
                     (when namespace (list "-n" namespace))
                     (when context (list "--context" context)))))
      (user-error "Failed to resume reconciliation for %s/%s" type name))
    (message "Resumed reconciliation for %s/%s." type name)))

(defun kubed-ext-strimzi-restart-connector
    (name &optional context namespace)
  "Trigger restart of Strimzi KafkaConnector NAME in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list (kubed-read-resource-name
            "kafkaconnectors" "Restart connector" nil nil c n)
           c n)))
  (let ((context (or context (kubed-local-context)))
        (namespace (or namespace
                       (kubed--namespace (or context
                                             (kubed-local-context))))))
    (unless (zerop
             (apply #'call-process
                    kubed-kubectl-program nil nil nil
                    "annotate" "kafkaconnector" name
                    "strimzi.io/restart=true" "--overwrite"
                    (append
                     (when namespace (list "-n" namespace))
                     (when context (list "--context" context)))))
      (user-error "Failed to restart connector %s" name))
    (message "Triggered restart of connector %s." name)))

(defun kubed-ext-scale-kafkaconnect
    (name replicas &optional context namespace)
  "Scale Strimzi KafkaConnect NAME to REPLICAS in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list (kubed-read-resource-name
            "kafkaconnects" "Scale KafkaConnect" nil nil c n)
           (read-number "Number of replicas: ")
           c n)))
  (let* ((context (or context (kubed-local-context)))
         (namespace (or namespace (kubed--namespace context))))
    (kubed-patch "kafkaconnects" name
                 (kubed-ext--json-patch "replicas" (number-to-string replicas))
                 context namespace "merge")
    (message "Scaled KafkaConnect %s to %d replicas." name replicas)
    (kubed-update "kafkaconnects" context namespace)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 5.  Column Helpers, Metrics, Pod/Deployment Formatting
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-append-columns (resource-plural fetch-columns display-columns)
  "Append extra FETCH-COLUMNS and DISPLAY-COLUMNS to RESOURCE-PLURAL."
  (let ((existing (alist-get resource-plural kubed--columns nil nil #'string=))
        (fmt-var (intern (format "kubed-%s-columns" resource-plural))))
    (setf (alist-get resource-plural kubed--columns nil nil #'string=)
          (append existing fetch-columns))
    (when (boundp fmt-var)
      (set fmt-var (append (symbol-value fmt-var) display-columns)))))

(defun kubed-ext-set-columns (resource-plural fetch-columns display-columns)
  "Replace columns for RESOURCE-PLURAL with FETCH-COLUMNS and DISPLAY-COLUMNS."
  (setf (alist-get resource-plural kubed--columns nil nil #'string=)
        (cons '("NAME:.metadata.name") fetch-columns))
  (let ((fmt-var (intern (format "kubed-%s-columns" resource-plural))))
    (when (boundp fmt-var)
      (set fmt-var display-columns))))

(defun kubed-ext-parse-cpu (s)
  "Parse Kubernetes CPU quantity S to millicores (number)."
  (cond
   ((kubed-ext-none-p s) 0)
   ((string-suffix-p "m" s)  (string-to-number s))
   ((string-suffix-p "n" s)  (/ (string-to-number s) 1000000.0))
   ((string-suffix-p "u" s)  (/ (string-to-number s) 1000.0))
   (t (* 1000.0 (string-to-number s)))))

(defun kubed-ext-parse-mem (s)
  "Parse Kubernetes memory quantity S to MiB (number)."
  (cond
   ((kubed-ext-none-p s) 0)
   ((string-suffix-p "Ti" s) (* 1048576.0 (string-to-number s)))
   ((string-suffix-p "Gi" s) (* 1024.0    (string-to-number s)))
   ((string-suffix-p "Mi" s) (string-to-number s))
   ((string-suffix-p "Ki" s) (/ (string-to-number s) 1024.0))
   (t (/ (string-to-number s) 1048576.0))))

(defun kubed-ext-sum-cpu (s)
  "Sum comma-separated CPU values in S."
  (if (kubed-ext-none-p s) 0
    (apply #'+ (mapcar #'kubed-ext-parse-cpu
                       (split-string s "," t "[ \t]")))))

(defun kubed-ext-sum-mem (s)
  "Sum comma-separated memory values in S."
  (if (kubed-ext-none-p s) 0
    (apply #'+ (mapcar #'kubed-ext-parse-mem
                       (split-string s "," t "[ \t]")))))

(defun kubed-ext-format-cpu (millicores)
  "Format MILLICORES as human-readable CPU string."
  (let ((str (cond
              ((<= millicores 0) "0")
              ((>= millicores 1000) (format "%.1f" (/ millicores 1000.0)))
              (t (format "%dm" (round millicores))))))
    (propertize str 'kubed-sort-value millicores)))

(defun kubed-ext-format-mem (mib)
  "Format MIB as human-readable memory string."
  (let ((str (cond
              ((<= mib 0) "0")
              ((>= mib 1024) (format "%.1fGi" (/ mib 1024.0)))
              (t (format "%.0fMi" mib)))))
    (propertize str 'kubed-sort-value mib)))

(defun kubed-ext-format-pct (used total)
  "Format percentage USED/TOTAL with color coding."
  (if (<= total 0)
      (propertize "n/a" 'face 'shadow 'kubed-sort-value -1)
    (let* ((pct (/ (* 100.0 used) total))
           (str (format "%.0f%%" pct)))
      (propertize str
                  'face (cond ((>= pct 90) 'error)
                              ((>= pct 70) 'warning)
                              (t 'success))
                  'kubed-sort-value pct))))

(defun kubed-ext-top-numeric-sorter (col-idx)
  "Return a sort predicate comparing column COL-IDX by `kubed-sort-value'."
  (lambda (a b)
    (let ((va (get-text-property 0 'kubed-sort-value (aref (cadr a) col-idx)))
          (vb (get-text-property 0 'kubed-sort-value (aref (cadr b) col-idx))))
      (< (or va 0) (or vb 0)))))

;; ── Kubernetes Timestamp ──

(defun kubed-ext--parse-k8s-timestamp (s)
  "Parse Kubernetes RFC3339/ISO8601 timestamp S to an Emacs time value.

Supports the common Kubernetes forms:
- YYYY-MM-DDTHH:MM:SSZ
- YYYY-MM-DDTHH:MM:SS.sssZ
- YYYY-MM-DDTHH:MM:SS±HH:MM

Return nil if S cannot be parsed."
  (when (and (stringp s)
             (not (string-empty-p s))
             ;; Quick guard: must start with YYYY-MM-DDTHH:MM:SS
             (string-match-p
              "\\`[0-9]\\{4\\}-[0-9]\\{2\\}-[0-9]\\{2\\}T[0-9]\\{2\\}:[0-9]\\{2\\}:[0-9]\\{2\\}"
              s))
    (condition-case nil
        (parse-iso8601-time-string s)
      (error nil))))

(defun kubed-ext--format-age (timestamp)
  "Convert TIMESTAMP to human-readable relative age like kubectl.

If TIMESTAMP already carries a `kubed-sort-value' text property (i.e. it
was already formatted by a previous call), return it unchanged.
Returns `n/a' for nil, empty, or placeholder values like `<none>'."
  (cond
   ;; Nil, empty, or kubectl placeholder.
   ((kubed-ext-none-p timestamp)
    (propertize "n/a" 'face 'shadow 'kubed-sort-value 0))
   ;; Already formatted — return as-is (idempotency guard).
   ((and (stringp timestamp)
         (get-text-property 0 'kubed-sort-value timestamp))
    timestamp)
   ;; Try to parse as ISO 8601 timestamp.
   (t
    (let ((parsed (kubed-ext--parse-k8s-timestamp timestamp)))
      (if (null parsed)
          ;; Not a recognizable timestamp — pass through.
          (propertize (if (stringp timestamp) timestamp
                        (format "%s" timestamp))
                      'kubed-sort-value 0)
        (let* ((secs (max 0 (floor (float-time (time-subtract nil parsed)))))
               (total-minutes (/ secs 60))
               (hours (/ secs 3600))
               (days (/ secs 86400))
               (years (/ days 365))
               (str (cond
                     ((< secs 120)
                      (format "%ds" secs))
                     ((< total-minutes 10)
                      (let ((s (mod secs 60)))
                        (if (= s 0) (format "%dm" total-minutes)
                          (format "%dm%ds" total-minutes s))))
                     ((< hours 3)
                      (format "%dm" total-minutes))
                     ((< hours 8)
                      (let ((m (mod total-minutes 60)))
                        (if (= m 0) (format "%dh" hours)
                          (format "%dh%dm" hours m))))
                     ((< hours 48)
                      (format "%dh" hours))
                     ((< days 8)
                      (let ((h (mod hours 24)))
                        (if (= h 0) (format "%dd" days)
                          (format "%dd%dh" days h))))
                     ((< years 2)
                      (format "%dd" days))
                     ((< years 8)
                      (format "%dy%dd" years (mod days 365)))
                     (t (format "%dy" years)))))
          (propertize str 'kubed-sort-value (float secs))))))))

;; ── Pod Status Computation (kubel-style) ──

(defun kubed-ext--compute-pod-status (phase waiting terminated deletion)
  "Compute kubel-style pod status from PHASE, WAITING, TERMINATED, DELETION.
Returns a propertized string with face from `kubed-ext-status-faces'.
Deletion is detected by positive timestamp match."
  (let* ((waiting-reason (unless (kubed-ext-none-p waiting)
                           (car (split-string waiting ","))))
         (terminated-reason (unless (kubed-ext-none-p terminated)
                              (car (split-string terminated ","))))
         (deleting (and (stringp deletion)
                        (string-match-p
                         "\\`[0-9]\\{4\\}-[0-9]\\{2\\}-[0-9]\\{2\\}T"
                         deletion)))
         (status (cond
                  (deleting "Terminating")
                  (waiting-reason waiting-reason)
                  (terminated-reason terminated-reason)
                  (t (or phase "Unknown"))))
         (face (cdr (assoc status kubed-ext-status-faces))))
    (if face (propertize status 'face face) status)))

(defun kubed-ext--format-pod-ready (ready-count total-count)
  "Format READY-COUNT/TOTAL-COUNT as kubel-style `x/y' with color."
  (let* ((r (string-to-number (or ready-count "0")))
         (tot (string-to-number (or total-count "0")))
         (str (format "%d/%d" r tot))
         (face (cond
                ((and (= r 0) (= tot 0)) 'shadow)
                ((= r tot)                'success)
                ((> r 0)                  'warning)
                (t                        'error))))
    (propertize str 'face face 'kubed-sort-value r)))

(defun kubed-ext--format-pod-restarts (count)
  "Format pod restart COUNT with color coding."
  (let* ((str (number-to-string count))
         (face (cond
                ((= count 0) nil)
                ((< count 5) 'warning)
                (t 'error))))
    (if face
        (propertize str 'face face 'kubed-sort-value count)
      (propertize str 'kubed-sort-value count))))

;; ── Deployment Formatting ──

(defun kubed-ext--format-ready-total (ready replicas)
  "Format READY/REPLICAS as ready/total with color coding."
  (let* ((r (string-to-number (or ready "0")))
         (tot (string-to-number (or replicas "0")))
         (str (format "%d/%d" r tot))
         (face (cond
                ((and (= r 0) (= tot 0)) 'shadow)
                ((= r tot)                'success)
                ((> r 0)                  'warning)
                (t                        'error))))
    (propertize str 'face face 'kubed-sort-value r)))

(defun kubed-ext--format-deployment-count (count replicas)
  "Format deployment COUNT relative to REPLICAS with color coding."
  (let* ((c (string-to-number (or count "0")))
         (tot (string-to-number (or replicas "0")))
         (str (number-to-string c))
         (face (cond
                ((and (= c 0) (= tot 0)) 'shadow)
                ((>= c tot)               'success)
                ((> c 0)                  'warning)
                (t                        'error))))
    (propertize str 'face face 'kubed-sort-value c)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 6.  Enhance Upstream Resource Columns + Mode Hooks
;;; ═══════════════════════════════════════════════════════════════

(kubed-ext-set-columns
 "pods"
 (list
  '("PHASE:.status.phase")
  '("READY_NAMES:.status.containerStatuses[?(@.ready==true)].name")
  '("ALL_NAMES:.status.containerStatuses[*].name")
  '("STARTTIME:.status.startTime")
  '("RESTARTS:.status.containerStatuses[*].restartCount")
  '("IP:.status.podIP")
  '("NODE:.spec.nodeName")
  '("WAITING:.status.containerStatuses[*].state.waiting.reason")
  '("TERMINATED:.status.containerStatuses[*].state.terminated.reason")
  '("DELETION:.metadata.deletionTimestamp"))
 nil)

(defun kubed-ext--count-csv-items (s)
  "Count comma-separated items in S.  Empty/none=0, single item=1."
  (cond
   ((kubed-ext-none-p s)             0)
   ((string-empty-p (string-trim s)) 0)
   (t (1+ (seq-count (lambda (c) (= c ?,)) s)))))

(defun kubed-ext--pod-entries ()
  "Custom entries for pods with kubel-style status and color coding.
Reads 11-element fetch vectors and produces 7-column display vectors:
Name, Ready(x/y), Status, Restarts, Age, IP, Node."
  (let* ((raw  (alist-get 'resources (kubed--alist kubed-list-type
                                                   kubed-list-context
                                                   kubed-list-namespace)))
         (pred (kubed-list-interpret-filter))
         (result nil))
    (dolist (entry raw)
      (let* ((vec (cadr entry))
             (new-entry
              (if (and (vectorp vec) (>= (length vec) 11))
                  (let* ((ready-names  (aref vec 2))
                         (all-names    (aref vec 3))
                         (restart-raw  (aref vec 5))
                         (ready-count  (kubed-ext--count-csv-items ready-names))
                         (total-count  (kubed-ext--count-csv-items all-names))
                         (restart-n    (if (kubed-ext-none-p restart-raw) 0
                                         (apply #'+
                                                (mapcar #'string-to-number
                                                        (split-string
                                                         restart-raw "," t " "))))))
                    (list (car entry)
                          (vector
                           (aref vec 0)
                           (kubed-ext--format-pod-ready
                            (number-to-string ready-count)
                            (number-to-string total-count))
                           (kubed-ext--compute-pod-status
                            (aref vec 1) (aref vec 8) (aref vec 9) (aref vec 10))
                           (kubed-ext--format-pod-restarts restart-n)
                           (kubed-ext--format-age (aref vec 4))
                           (if (kubed-ext-none-p (aref vec 6)) "" (aref vec 6))
                           (if (kubed-ext-none-p (aref vec 7)) "" (aref vec 7)))))
                entry)))
        (when (funcall pred new-entry)
          (push new-entry result))))
    (nreverse result)))

(defun kubed-ext--pod-mode-setup ()
  "Set up pod list with kubel-style Ready, Status, Restarts, Age columns."
  (setq-local tabulated-list-format
              (vector
               kubed-name-column
               (list "Ready" 7 (kubed-ext-top-numeric-sorter 1))
               (list "Status" 22 t)
               (list "Restarts" 9
                     (kubed-ext-top-numeric-sorter 3) :right-align t)
               (list "Age" 10 (kubed-ext-top-numeric-sorter 4))
               (list "IP" 16 t)
               (list "Node" 32 t)))
  (setq-local tabulated-list-entries #'kubed-ext--pod-entries)
  (tabulated-list-init-header))

(add-hook 'kubed-pods-mode-hook #'kubed-ext--pod-mode-setup)

(kubed-ext-append-columns
 "services"
 (list
  (cons "SELECTOR:.spec.selector"
        (lambda (s)
          (cond
           ((kubed-ext-none-p s) "")
           ((string-prefix-p "map[" s) (substring s 4 -1))
           (t s)))))
 (list (list "Selector" 40 t)))

(kubed-ext-set-columns
 "ingresses"
 (list
  '("CLASS:.spec.ingressClassName")
  '("HOSTS:.spec.rules[*].host")
  (cons "ADDRESS:.status.loadBalancer.ingress[0]"
        (lambda (s)
          (cond
           ((kubed-ext-none-p s) "")
           ((string-match "ip:$[^] :]+$" s) (match-string 1 s))
           ((string-match "hostname:$[^] :]+$" s) (match-string 1 s))
           (t s))))
  (cons "PORTS:.spec.tls[0].hosts"
        (lambda (s)
          (if (kubed-ext-none-p s) "80" "80, 443")))
  (cons "AGE:.metadata.creationTimestamp" #'kubed-ext--format-age))
 (list
  (list "Class" 20 t)
  (list "Hosts" 40 t)
  (list "Address" 40 t)
  (list "Ports" 10 t)
  (list "Age" 20 t)))

(kubed-ext-set-columns
 "persistentvolumes"
 (list
  '("CAPACITY:.spec.capacity.storage")
  '("ACCESS-MODES:.spec.accessModes[*]")
  '("RECLAIM:.spec.persistentVolumeReclaimPolicy")
  (cons "STATUS:.status.phase"
        (lambda (ph)
          (let ((face (cdr (assoc ph kubed-ext-status-faces))))
            (if face (propertize ph 'face face) ph))))
  '("CLAIM-NS:.spec.claimRef.namespace")
  '("CLAIM-NAME:.spec.claimRef.name")
  '("STORAGECLASS:.spec.storageClassName"))
 (list
  (list "Capacity" 10 t)
  (list "Access-modes" 18 t)
  (list "Reclaim" 14 t)
  (list "Status" 10 t)
  (list "Claim-ns" 16 t)
  (list "Claim-name" 28 t)
  (list "Storageclass" 20 t)))

(defun kubed-ext--deployment-entries ()
  "Custom entries for deployments with kubel-style Ready and Age."
  (let* ((raw (alist-get 'resources (kubed--alist kubed-list-type
                                                  kubed-list-context
                                                  kubed-list-namespace)))
         (pred (kubed-list-interpret-filter))
         (result nil))
    (dolist (entry raw)
      (let* ((vec (cadr entry))
             (new-entry
              (if (and (vectorp vec) (>= (length vec) 6))
                  (list (car entry)
                        (vector
                         (aref vec 0)
                         (kubed-ext--format-ready-total
                          (let ((v (aref vec 1)))
                            (if (kubed-ext-none-p v) "0" v))
                          (let ((v (aref vec 4)))
                            (if (kubed-ext-none-p v) "0" v)))
                         (kubed-ext--format-deployment-count
                          (let ((v (aref vec 2)))
                            (if (kubed-ext-none-p v) "0" v))
                          (let ((v (aref vec 4)))
                            (if (kubed-ext-none-p v) "0" v)))
                         (kubed-ext--format-deployment-count
                          (let ((v (aref vec 3)))
                            (if (kubed-ext-none-p v) "0" v))
                          (let ((v (aref vec 4)))
                            (if (kubed-ext-none-p v) "0" v)))
                         (kubed-ext--format-age (aref vec 5))))
                entry)))
        (when (funcall pred new-entry)
          (push new-entry result))))
    (nreverse result)))

(defun kubed-ext--deployment-mode-setup ()
  "Set up deployment list with kubel-style Ready and Age columns."
  (setq-local tabulated-list-format
              (vector
               kubed-name-column
               (list "Ready" 10 (kubed-ext-top-numeric-sorter 1))
               (list "Up-to-date" 12
                     (kubed-ext-top-numeric-sorter 2) :right-align t)
               (list "Available" 12
                     (kubed-ext-top-numeric-sorter 3) :right-align t)
               (list "Age" 12 (kubed-ext-top-numeric-sorter 4))))
  (setq-local tabulated-list-entries #'kubed-ext--deployment-entries)
  (tabulated-list-init-header))

(add-hook 'kubed-deployments-mode-hook #'kubed-ext--deployment-mode-setup)

;;; ═══════════════════════════════════════════════════════════════
;;; § 7.  New Built-in Resources
;;; ═══════════════════════════════════════════════════════════════

(eval '(kubed-define-resource node
           ((status ".status.conditions[-1:].status" 10
                    nil
                    (lambda (s)
                      (cond
                       ((string= s "True")    (propertize "Ready"    'face 'success))
                       ((string= s "False")   (propertize "NotReady" 'face 'error))
                       ((string= s "Unknown") (propertize "Unknown"  'face 'warning))
                       (t s))))
            (roles ".metadata.labels" 16
                   nil
                   (lambda (s)
                     (let ((roles nil) (pos 0))
                       (while (string-match
                               "node-role\\.kubernetes\\.io/$[^:=, ]+$"
                               s pos)
                         (push (match-string 1 s) roles)
                         (setq pos (match-end 0)))
                       (if roles (string-join (nreverse roles) ",") ""))))
            (taints ".spec.taints[*].key" 7
                    (lambda (l r)
                      (< (string-to-number l) (string-to-number r)))
                    (lambda (s)
                      (if (or (string-empty-p s) (string= s "")) "0"
                        (number-to-string
                         (1+ (seq-count (lambda (c) (= c ?,)) s)))))
                    :right-align t)
            (version ".status.nodeInfo.kubeletVersion" 14)
            (cpu-alloc ".status.allocatable.cpu" 6
                       (lambda (l r)
                         (< (string-to-number l) (string-to-number r)))
                       nil :right-align t)
            (mem-alloc ".status.allocatable.memory" 8
                       nil
                       (lambda (s)
                         (let ((mib (kubed-ext-parse-mem s)))
                           (if (>= mib 1024)
                               (format "%.1fGi" (/ mib 1024.0))
                             (format "%.0fMi" mib))))
                       :right-align t)
            (internal-ip ".status.addresses[0].address" 16)
            (creationtimestamp ".metadata.creationTimestamp" 20))
         :namespaced nil
         (top "T" "Show resource metrics for"
              (let* ((ctx kubed-list-context)
                     (top-out (with-temp-buffer
                                (call-process kubed-kubectl-program nil t nil
                                              "top" "node" node
                                              "--no-headers" "--context" ctx)
                                (buffer-string)))
                     (top-f (split-string (string-trim top-out) nil t))
                     (cpu-used (kubed-ext-parse-cpu (nth 1 top-f)))
                     (mem-used (kubed-ext-parse-mem (nth 3 top-f)))
                     (alloc-out (with-temp-buffer
                                  (call-process
                                   kubed-kubectl-program nil t nil
                                   "get" "node" node "--no-headers"
                                   "--context" ctx "-o"
                                   "custom-columns=CPU:.status.allocatable.cpu,MEM:.status.allocatable.memory")
                                  (buffer-string)))
                     (alloc-f (split-string (string-trim alloc-out) nil t))
                     (cpu-a (kubed-ext-parse-cpu (nth 0 alloc-f)))
                     (mem-a (kubed-ext-parse-mem (nth 1 alloc-f)))
                     (buf (get-buffer-create
                           (format "*kubed-top node/%s[%s]*" node ctx))))
                (with-current-buffer buf
                  (let ((inhibit-read-only t))
                    (erase-buffer)
                    (insert (propertize (format "Node Metrics: %s\n" node)
                                        'face 'bold))
                    (insert (make-string 60 ?-) "\n\n")
                    (insert (format "  CPU:  %s used  /  %s allocatable  (%s)\n"
                                    (kubed-ext-format-cpu cpu-used)
                                    (kubed-ext-format-cpu cpu-a)
                                    (kubed-ext-format-pct cpu-used cpu-a)))
                    (insert (format "  MEM:  %s used  /  %s allocatable  (%s)\n"
                                    (kubed-ext-format-mem mem-used)
                                    (kubed-ext-format-mem mem-a)
                                    (kubed-ext-format-pct mem-used mem-a)))
                    (goto-char (point-min))
                    (special-mode)))
                (display-buffer buf))))
      t)

(eval '(kubed-define-resource persistentvolumeclaim
           ((status ".status.phase" 10 nil
                    (lambda (ph)
                      (propertize ph 'face
                                  (pcase ph
                                    ("Bound"   'success)
                                    ("Pending" 'warning)
                                    ("Lost"    'error)
                                    (_         'default)))))
            (volume ".spec.volumeName" 36)
            (capacity ".status.capacity.storage" 10)
            (access-modes ".spec.accessModes[*]" 18)
            (storageclass ".spec.storageClassName" 20)))
      t)

(eval '(kubed-define-resource configmap
           ((creationtimestamp ".metadata.creationTimestamp" 20)))
      t)

(eval '(kubed-define-resource event
           ((type ".type" 8 nil
                  (lambda (s)
                    (propertize s 'face
                                (pcase s
                                  ("Normal"  'success)
                                  ("Warning" 'warning)
                                  (_         'default)))))
            (reason ".reason" 20)
            (object-kind ".involvedObject.kind" 12)
            (object-name ".involvedObject.name" 28)
            (count ".count" 6
                   (lambda (l r)
                     (< (string-to-number l) (string-to-number r)))
                   nil :right-align t)
            (message ".message" 50)
            (last-seen ".lastTimestamp" 20)))
      t)

(eval '(kubed-define-resource networkpolicy
           ((pod-selector ".spec.podSelector.matchLabels" 50 nil
                          (lambda (s)
                            (cond
                             ((or (string-empty-p s) (string= s ""))
                              "(all pods)")
                             ((string-prefix-p "map[" s) (substring s 4 -1))
                             (t s))))
            (policy-types ".spec.policyTypes[*]" 18)
            (creationtimestamp ".metadata.creationTimestamp" 20))
         :plural networkpolicies)
      t)

(eval '(kubed-define-resource horizontalpodautoscaler
           ((reference ".spec.scaleTargetRef.name" 28)
            (ref-kind ".spec.scaleTargetRef.kind" 14)
            (min-pods ".spec.minReplicas" 8
                      (lambda (l r)
                        (< (string-to-number l) (string-to-number r)))
                      nil :right-align t)
            (max-pods ".spec.maxReplicas" 8
                      (lambda (l r)
                        (< (string-to-number l) (string-to-number r)))
                      nil :right-align t)
            (replicas ".status.currentReplicas" 8
                      (lambda (l r)
                        (< (string-to-number l) (string-to-number r)))
                      nil :right-align t)))
      t)

(eval '(kubed-define-resource poddisruptionbudget
           ((min-available ".spec.minAvailable" 14)
            (max-unavailable ".spec.maxUnavailable" 16)
            (allowed-disruptions ".status.disruptionsAllowed" 20
                                 (lambda (l r)
                                   (< (string-to-number l)
                                      (string-to-number r)))
                                 nil :right-align t)
            (current-healthy ".status.currentHealthy" 16
                             (lambda (l r)
                               (< (string-to-number l)
                                  (string-to-number r)))
                             nil :right-align t)
            (desired-healthy ".status.desiredHealthy" 16
                             (lambda (l r)
                               (< (string-to-number l)
                                  (string-to-number r)))
                             nil :right-align t)))
      t)

(eval '(kubed-define-resource customresourcedefinition
           ((group ".spec.group" 34)
            (kind ".spec.names.kind" 24)
            (scope ".spec.scope" 12 nil
                   (lambda (s)
                     (propertize s 'face
                                 (pcase s
                                   ("Namespaced" 'success)
                                   ("Cluster"    'warning)
                                   (_            'default)))))
            (versions ".spec.versions[*].name" 16)
            (creationtimestamp ".metadata.creationTimestamp" 20))
         :namespaced nil
         (list-instances "I" "List instances of"
                         (let* ((plural (car (split-string
                                              customresourcedefinition "\\.")))
                                (entry  (tabulated-list-get-entry))
                                (scope  (and entry
                                             (substring-no-properties
                                              (aref entry 3))))
                                (ns-p   (string= scope "Namespaced"))
                                (ctx    kubed-list-context)
                                (buf    (get-buffer-create
                                         (format "*kubed %s[%s]*" plural ctx))))
                           (with-current-buffer buf
                             (let ((inhibit-read-only t))
                               (erase-buffer)
                               (apply #'call-process
                                      kubed-kubectl-program nil t nil
                                      "get" plural "-o" "wide"
                                      (append
                                       (when ctx (list "--context" ctx))
                                       (when ns-p (list "--all-namespaces"))))
                               (goto-char (point-min))
                               (special-mode)))
                           (display-buffer buf))))
      t)

;;; ═══════════════════════════════════════════════════════════════
;;; § 8–9.  Strimzi + Plain Resources
;;; ═══════════════════════════════════════════════════════════════

(eval '(kubed-define-resource kafka ()
         :suffixes ([("P" "Pause Reconciliation"  kubed-kafkas-pause)
                     ("R" "Resume Reconciliation" kubed-kafkas-resume)])
         (pause "P" "Pause reconciliation for"
                (kubed-ext-strimzi-pause-reconciliation
                 "kafka" kafka kubed-list-context kubed-list-namespace)
                (kubed-list-update t))
         (resume "R" "Resume reconciliation for"
                 (kubed-ext-strimzi-resume-reconciliation
                  "kafka" kafka kubed-list-context kubed-list-namespace)
                 (kubed-list-update t)))
      t)

(eval '(kubed-define-resource kafkaconnect ()
         :plural kafkaconnects
         :prefix (("$" "Scale" kubed-scale-kafkaconnect))
         :suffixes ([("$" "Scale"                 kubed-kafkaconnects-scale)
                     ("P" "Pause Reconciliation"  kubed-kafkaconnects-pause)
                     ("R" "Resume Reconciliation" kubed-kafkaconnects-resume)])
         (scale "$" "Scale"
                (kubed-ext-scale-kafkaconnect
                 kafkaconnect
                 (if current-prefix-arg
                     (prefix-numeric-value current-prefix-arg)
                   (read-number "Number of replicas: "))
                 kubed-list-context kubed-list-namespace))
         (pause "P" "Pause reconciliation for"
                (kubed-ext-strimzi-pause-reconciliation
                 "kafkaconnect" kafkaconnect
                 kubed-list-context kubed-list-namespace)
                (kubed-list-update t))
         (resume "R" "Resume reconciliation for"
                 (kubed-ext-strimzi-resume-reconciliation
                  "kafkaconnect" kafkaconnect
                  kubed-list-context kubed-list-namespace)
                 (kubed-list-update t)))
      t)

(eval '(kubed-define-resource kafkaconnector ()
         :suffixes ([("P" "Pause"   kubed-kafkaconnectors-pause)
                     ("R" "Resume"  kubed-kafkaconnectors-resume)
                     ("X" "Restart" kubed-kafkaconnectors-restart)])
         (pause "P" "Pause"
                (kubed-patch "kafkaconnectors" kafkaconnector
                             (kubed-ext--json-patch "pause" "true")
                             kubed-list-context kubed-list-namespace "merge")
                (kubed-list-update t))
         (resume "R" "Resume"
                 (kubed-patch "kafkaconnectors" kafkaconnector
                              (kubed-ext--json-patch "pause" "false")
                              kubed-list-context kubed-list-namespace "merge")
                 (kubed-list-update t))
         (restart "X" "Restart"
                  (kubed-ext-strimzi-restart-connector
                   kafkaconnector kubed-list-context kubed-list-namespace)
                  (kubed-list-update t)))
      t)

(eval '(kubed-define-resource kafkatopic ()
         :suffixes ([("p" "Set Partitions" kubed-kafkatopics-set-partitions)])
         (set-partitions "p" "Set partitions for"
                         (let ((n (read-number "Number of partitions: ")))
                           (kubed-patch "kafkatopics" kafkatopic
                                        (kubed-ext--json-patch "partitions"
                                                               (number-to-string n))
                                        kubed-list-context kubed-list-namespace
                                        "merge"))
                         (kubed-list-update t)))
      t)

(dolist (spec '((kafkabridge) (kafkamirrormaker2) (kafkamirrormaker)
                (kafkanodepool) (kafkarebalance) (kafkauser) (strimzipodset)))
  (eval (cons 'kubed-define-resource spec) t))

(dolist (spec '((endpoint) (limitrange) (podtemplate) (replicationcontroller)
                (resourcequota) (serviceaccount) (controllerrevision) (lease)
                (endpointslice) (rolebinding) (role)
                (azureapplicationgatewayrewrite) (podmonitor) (servicemonitor)))
  (eval (cons 'kubed-define-resource spec) t))

;;; ═══════════════════════════════════════════════════════════════
;;; § 9a.  Human-Readable Age for All Timestamp Columns
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext--timestamp-patched-types
  (make-hash-table :test 'equal)
  "Resource types whose timestamp columns carry a formatter.")

(defun kubed-ext--timestamp-column-spec-p (spec)
  "Non-nil if SPEC names a Kubernetes timestamp column.
SPEC is a fetch-column string such as
`CREATIONTIMESTAMP:.metadata.creationTimestamp'."
  (and
   (stringp spec)
   (or (string-match-p "\\.creationTimestamp" spec)
       (string-match-p "\\.startTime" spec)
       (string-match-p "\\.lastTimestamp" spec)
       (string-match-p "\\.lastScheduleTime" spec)
       (string-match-p "\\.lastSuccessfulTime" spec)
       (string-match-p "\\.deletionTimestamp" spec)
       (string-match-p "\\`AGE:" spec)
       (string-match-p "\\`LAST.SEEN:" spec)
       (string-match-p "\\`LASTSCHEDULE:" spec)
       (string-match-p "\\`LASTSUCCESS:" spec)
       (string-match-p "\\`STARTTIME:" spec)
       (string-match-p "TIMESTAMP:" spec))))

(defun kubed-ext--patch-type-timestamp-columns (type)
  "Attach age formatter to timestamp columns for TYPE.
Pods and deployments are skipped because they format
ages inside their custom entries functions."
  (unless (or (gethash type kubed-ext--timestamp-patched-types)
              (member type '("pods" "deployments")))
    (puthash type t kubed-ext--timestamp-patched-types)
    (let ((columns (alist-get type kubed--columns
                              nil nil #'string=)))
      (when columns
        (setf (alist-get type kubed--columns
                         nil nil #'string=)
              (mapcar
               (lambda (col)
                 (let ((spec (if (consp col)
                                 (car col)
                               col)))
                   (if (and (kubed-ext--timestamp-column-spec-p
                             spec)
                            (not (and (consp col) (cdr col))))
                       (cons spec #'kubed-ext--format-age)
                     col)))
               columns))))))

(defun kubed-ext--patch-all-timestamp-columns ()
  "Patch timestamp columns for all known resource types."
  (dolist (entry kubed--columns)
    (kubed-ext--patch-type-timestamp-columns (car entry))))

(defun kubed-ext--fix-timestamp-display-columns ()
  "Rename timestamp display columns to short labels.
Install numeric sort via the `kubed-sort-value' text
property that `kubed-ext--format-age' attaches."
  (dolist (type-name '("services" "secrets" "jobs"
                       "replicasets" "daemonsets"
                       "statefulsets" "ingressclasses"
                       "namespaces" "nodes" "events"
                       "cronjobs"))
    (let ((fmt-var (intern
                    (format "kubed-%s-columns"
                            type-name))))
      (when (and (boundp fmt-var)
                 (symbol-value fmt-var))
        (let ((idx 0))
          (set fmt-var
               (mapcar
                (lambda (col)
                  (cl-incf idx)
                  (let ((nm (downcase (car col))))
                    (cond
                     ((or (string= nm "creationtimestamp")
                          (string= nm "starttime"))
                      (append
                       (list "Age" 10
                             (kubed-ext-top-numeric-sorter
                              idx))
                       (nthcdr 3 col)))
                     ((string= nm "last-seen")
                      (append
                       (list "Last Seen" 10
                             (kubed-ext-top-numeric-sorter
                              idx))
                       (nthcdr 3 col)))
                     ((string= nm "lastschedule")
                      (append
                       (list "Last Sched" 10
                             (kubed-ext-top-numeric-sorter
                              idx))
                       (nthcdr 3 col)))
                     ((string= nm "lastsuccess")
                      (append
                       (list "Last OK" 10
                             (kubed-ext-top-numeric-sorter
                              idx))
                       (nthcdr 3 col)))
                     (t col))))
                (symbol-value fmt-var))))))))

(defun kubed-ext--ensure-age-formatting
    (orig-fn type context &optional namespace)
  "Around-advice for `kubed-update'.
Patch timestamp columns for TYPE before calling
ORIG-FN with TYPE, CONTEXT, and NAMESPACE."
  (kubed-ext--patch-type-timestamp-columns type)
  (funcall orig-fn type context namespace))

(advice-add 'kubed-update :around
            #'kubed-ext--ensure-age-formatting)

;; Apply patches now -- all kubed-define-resource forms
;; have already been evaluated.
(kubed-ext--patch-all-timestamp-columns)
(kubed-ext--fix-timestamp-display-columns)

;;; ═══════════════════════════════════════════════════════════════
;;; § 10.  Async Helpers
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext--async-kubectl (args callback &optional errback)
  "Run kubectl with ARGS asynchronously.
Call CALLBACK with output string on success.
Call ERRBACK with error string on failure."
  (let* ((output-buf (generate-new-buffer " *kubed-ext-async*"))
         (default-directory temporary-file-directory))
    (make-process
     :name "kubed-ext-kubectl"
     :buffer output-buf
     :command (cons kubed-kubectl-program args)
     :noquery t
     :sentinel
     (lambda (proc _event)
       (when (memq (process-status proc) '(exit signal))
         (let ((exit-code (process-exit-status proc))
               (output ""))
           (when (buffer-live-p output-buf)
             (setq output (with-current-buffer output-buf (buffer-string)))
             (kill-buffer output-buf))
           (if (zerop exit-code)
               (funcall callback output)
             (when errback
               (funcall errback
                        (format "kubectl exited %d" exit-code))))))))))

(defun kubed-ext--async-kubectl-2 (args1 args2 callback &optional errback)
  "Run two kubectl commands concurrently.
ARGS1 and ARGS2 are arg lists.
CALLBACK is called with (list out1 out2) on success.
ERRBACK is called with error message on failure."
  (let ((results (cons nil nil))
        (count 0)
        (failed nil))
    (kubed-ext--async-kubectl
     args1
     (lambda (out)
       (unless failed
         (setcar results out)
         (cl-incf count)
         (when (= count 2)
           (funcall callback (list (car results) (cdr results))))))
     (lambda (err)
       (unless failed (setq failed t) (when errback (funcall errback err)))))
    (kubed-ext--async-kubectl
     args2
     (lambda (out)
       (unless failed
         (setcdr results out)
         (cl-incf count)
         (when (= count 2)
           (funcall callback (list (car results) (cdr results))))))
     (lambda (err)
       (unless failed (setq failed t) (when errback (funcall errback err)))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 10b.  Top Commands
;;; ═══════════════════════════════════════════════════════════════

(defvar-local kubed-ext-top-context nil "Context for top buffer.")
(defvar-local kubed-ext-top-namespace nil "Namespace for top buffer.")
(defvar-local kubed-ext-top--fetching nil "Non-nil when async fetch in progress.")

(defun kubed-ext-top-refresh ()
  "Refresh the current top buffer asynchronously."
  (interactive)
  (if kubed-ext-top--fetching
      (message "Metrics fetch already in progress...")
    (setq kubed-ext-top--fetching t)
    (setq tabulated-list-entries nil)
    (let ((inhibit-read-only t))
      (erase-buffer)
      (insert (propertize "\n  Fetching metrics from API...\n" 'face 'shadow)))
    (cond
     ((derived-mode-p 'kubed-ext-top-nodes-mode) (kubed-ext--top-nodes-fetch))
     ((derived-mode-p 'kubed-ext-top-pods-mode)  (kubed-ext--top-pods-fetch)))))

(defun kubed-ext--top-nodes-fetch ()
  "Fetch node metrics asynchronously and populate the table."
  (let ((ctx kubed-ext-top-context) (buf (current-buffer)))
    (kubed-ext--async-kubectl-2
     (list "top" "nodes" "--no-headers" "--context" ctx)
     (list "get" "nodes" "--no-headers" "--context" ctx "-o"
           (concat "custom-columns=NAME:.metadata.name,"
                   "CPU:.status.allocatable.cpu,MEM:.status.allocatable.memory"))
     (lambda (results)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (setq kubed-ext-top--fetching nil)
           (let ((alloc-map (make-hash-table :test 'equal)) (entries nil))
             (dolist (line (split-string (nth 1 results) "\n" t))
               (let ((f (split-string line nil t)))
                 (puthash (nth 0 f) (list (kubed-ext-parse-cpu (nth 1 f))
                                          (kubed-ext-parse-mem (nth 2 f)))
                          alloc-map)))
             (dolist (line (split-string (nth 0 results) "\n" t))
               (let* ((f (split-string line nil t))
                      (name (nth 0 f))
                      (cpu-u (kubed-ext-parse-cpu (nth 1 f)))
                      (mem-u (kubed-ext-parse-mem (nth 3 f)))
                      (alloc (gethash name alloc-map '(0 0)))
                      (cpu-a (nth 0 alloc)) (mem-a (nth 1 alloc)))
                 (push (list name (vector name
                                          (kubed-ext-format-cpu cpu-u)
                                          (kubed-ext-format-cpu cpu-a)
                                          (kubed-ext-format-pct cpu-u cpu-a)
                                          (kubed-ext-format-mem mem-u)
                                          (kubed-ext-format-mem mem-a)
                                          (kubed-ext-format-pct mem-u mem-a)))
                       entries)))
             (setq tabulated-list-entries (nreverse entries))
             (tabulated-list-print t)))))
     (lambda (err)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (setq kubed-ext-top--fetching nil)
           (message "Node metrics failed: %s" err)))))))

(defun kubed-ext--top-pods-fetch ()
  "Fetch pod metrics asynchronously and populate the table."
  (let ((ctx kubed-ext-top-context) (ns kubed-ext-top-namespace)
        (buf (current-buffer)))
    (kubed-ext--async-kubectl-2
     (append (list "top" "pods" "--no-headers" "--context" ctx)
             (when ns (list "-n" ns)))
     (append (list "get" "pods" "--no-headers" "--context" ctx "-o"
                   (concat "custom-columns=NAME:.metadata.name,"
                           "CR:.spec.containers[*].resources.requests.cpu,"
                           "CL:.spec.containers[*].resources.limits.cpu,"
                           "MR:.spec.containers[*].resources.requests.memory,"
                           "ML:.spec.containers[*].resources.limits.memory"))
             (when ns (list "-n" ns)))
     (lambda (results)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (setq kubed-ext-top--fetching nil)
           (let ((spec-map (make-hash-table :test 'equal)) (entries nil))
             (dolist (line (split-string (nth 1 results) "\n" t))
               (let ((f (split-string line nil t)))
                 (puthash (nth 0 f) (list (kubed-ext-sum-cpu (nth 1 f))
                                          (kubed-ext-sum-cpu (nth 2 f))
                                          (kubed-ext-sum-mem (nth 3 f))
                                          (kubed-ext-sum-mem (nth 4 f)))
                          spec-map)))
             (dolist (line (split-string (nth 0 results) "\n" t))
               (let* ((f (split-string line nil t))
                      (name (nth 0 f))
                      (cpu-u (kubed-ext-parse-cpu (nth 1 f)))
                      (mem-u (kubed-ext-parse-mem (nth 2 f)))
                      (spec (gethash name spec-map '(0 0 0 0))))
                 (push (list name
                             (vector name
                                     (kubed-ext-format-cpu cpu-u)
                                     (kubed-ext-format-mem mem-u)
                                     (kubed-ext-format-cpu (nth 0 spec))
                                     (kubed-ext-format-cpu (nth 1 spec))
                                     (kubed-ext-format-pct cpu-u (nth 0 spec))
                                     (kubed-ext-format-pct cpu-u (nth 1 spec))
                                     (kubed-ext-format-mem (nth 2 spec))
                                     (kubed-ext-format-mem (nth 3 spec))
                                     (kubed-ext-format-pct mem-u (nth 2 spec))
                                     (kubed-ext-format-pct mem-u (nth 3 spec))))
                       entries)))
             (setq tabulated-list-entries (nreverse entries))
             (tabulated-list-print t)))))
     (lambda (err)
       (when (buffer-live-p buf)
         (with-current-buffer buf
           (setq kubed-ext-top--fetching nil)
           (message "Pod metrics failed: %s" err)))))))

(define-derived-mode kubed-ext-top-nodes-mode tabulated-list-mode
  "Kubed Top Nodes" "Major mode for displaying node resource usage."
  (setq tabulated-list-format
        (vector (list "Name" 40 t)
                (list "CPU"   8 (kubed-ext-top-numeric-sorter 1) :right-align t)
                (list "CPU/A" 8 (kubed-ext-top-numeric-sorter 2) :right-align t)
                (list "%CPU"  6 (kubed-ext-top-numeric-sorter 3) :right-align t)
                (list "MEM"   8 (kubed-ext-top-numeric-sorter 4) :right-align t)
                (list "MEM/A" 8 (kubed-ext-top-numeric-sorter 5) :right-align t)
                (list "%MEM"  6 (kubed-ext-top-numeric-sorter 6) :right-align t))
        tabulated-list-padding 2
        tabulated-list-entries nil)
  (tabulated-list-init-header))

(define-derived-mode kubed-ext-top-pods-mode tabulated-list-mode
  "Kubed Top Pods" "Major mode for displaying pod resource usage."
  (setq tabulated-list-format
        (vector (list "Name"   48 t)
                (list "CPU"     7 (kubed-ext-top-numeric-sorter 1)  :right-align t)
                (list "MEM"     8 (kubed-ext-top-numeric-sorter 2)  :right-align t)
                (list "CPU/R"   7 (kubed-ext-top-numeric-sorter 3)  :right-align t)
                (list "CPU/L"   7 (kubed-ext-top-numeric-sorter 4)  :right-align t)
                (list "%CPU/R"  7 (kubed-ext-top-numeric-sorter 5)  :right-align t)
                (list "%CPU/L"  7 (kubed-ext-top-numeric-sorter 6)  :right-align t)
                (list "MEM/R"   8 (kubed-ext-top-numeric-sorter 7)  :right-align t)
                (list "MEM/L"   8 (kubed-ext-top-numeric-sorter 8)  :right-align t)
                (list "%MEM/R"  7 (kubed-ext-top-numeric-sorter 9)  :right-align t)
                (list "%MEM/L"  7 (kubed-ext-top-numeric-sorter 10) :right-align t))
        tabulated-list-padding 2
        tabulated-list-entries nil)
  (tabulated-list-init-header))

(keymap-set kubed-ext-top-nodes-mode-map "g" #'kubed-ext-top-refresh)
(keymap-set kubed-ext-top-nodes-mode-map "q" #'quit-window)
(keymap-set kubed-ext-top-nodes-mode-map "b" #'kubed-ext-switch-buffer)
(keymap-set kubed-ext-top-pods-mode-map  "g" #'kubed-ext-top-refresh)
(keymap-set kubed-ext-top-pods-mode-map  "q" #'quit-window)
(keymap-set kubed-ext-top-pods-mode-map  "b" #'kubed-ext-switch-buffer)

(defun kubed-ext-top-nodes (&optional context)
  "Display node resource usage in CONTEXT."
  (interactive
   (list (if current-prefix-arg
             (kubed-read-context "Context" (kubed-local-context))
           (kubed-local-context))))
  (let* ((context (or context (kubed-local-context)))
         (buf (get-buffer-create (format "*kubed-top-nodes[%s]*" context))))
    (with-current-buffer buf
      (kubed-ext-top-nodes-mode)
      (setq-local kubed-ext-top-context context)
      (kubed-ext-top-refresh))
    (display-buffer buf)))

(defun kubed-ext-top-pods (&optional context namespace)
  "Display pod resource usage in CONTEXT and NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list c n)))
  (let* ((context (or context (kubed-local-context)))
         (namespace (or namespace (kubed--namespace context)))
         (buf (get-buffer-create
               (format "*kubed-top-pods@%s[%s]*" namespace context))))
    (with-current-buffer buf
      (kubed-ext-top-pods-mode)
      (setq-local kubed-ext-top-context context)
      (setq-local kubed-ext-top-namespace namespace)
      (kubed-ext-top-refresh))
    (display-buffer buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 11.  Terminal Support (vterm, eat, eshell, ansi-term)
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext--tramp-optimized nil
  "Non-nil when TRAMP has already been optimized for kubed.")

(defun kubed-ext-optimize-tramp ()
  "Configure TRAMP to cache connections, improving Eshell/Dired speed."
  (unless kubed-ext--tramp-optimized
    (setq kubed-ext--tramp-optimized t)
    (require 'tramp)
    (when (require 'kubed-tramp nil t)
      (when (boundp 'kubed-tramp-method)
        (add-to-list 'tramp-connection-properties
                     (list (regexp-quote (format "/%s:" kubed-tramp-method))
                           "direct-async-process" t))))
    (when (boundp 'tramp-completion-reread-directory-timeout)
      (setq tramp-completion-reread-directory-timeout nil))))

(add-hook 'kubed-list-mode-hook #'kubed-ext-optimize-tramp)

(defun kubed-ext--kubectl-exec-command (pod container context namespace shell)
  "Build kubectl exec command for POD/CONTAINER/CONTEXT/NAMESPACE/SHELL."
  (mapconcat #'shell-quote-argument
             (append (list kubed-kubectl-program "exec" "-it" pod
                           "-c" container)
                     (when namespace (list "-n" namespace))
                     (when context   (list "--context" context))
                     (list "--" shell))
             " "))

(defun kubed-ext-pods-vterm (click)
  "Open vterm in Kubernetes pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (unless (require 'vterm nil t)
    (user-error "This command requires the `vterm' package"))
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((container (kubed-read-container pod "Container" t
                                              kubed-list-context kubed-list-namespace))
             (vterm-shell (kubed-ext--kubectl-exec-command
                           pod container kubed-list-context kubed-list-namespace
                           kubed-ext-pod-shell))
             (vterm-buffer-name
              (format "*Kubed vterm %s*"
                      (kubed-display-resource-short-description
                       "pods" pod kubed-list-context kubed-list-namespace))))
        (vterm vterm-buffer-name))
    (user-error "No Kubernetes pod at point")))

(defun kubed-ext-vterm-pod (pod &optional context namespace)
  "Open vterm in Kubernetes POD in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list (kubed-read-pod "Open vterm in pod" nil nil c n) c n)))
  (unless (require 'vterm nil t)
    (user-error "This command requires the `vterm' package"))
  (let* ((context (or context (kubed-local-context)))
         (namespace (or namespace (kubed--namespace context)))
         (container (kubed-read-container pod "Container" t context namespace))
         (vterm-shell (kubed-ext--kubectl-exec-command
                       pod container context namespace kubed-ext-pod-shell))
         (vterm-buffer-name
          (format "*Kubed vterm %s*"
                  (kubed-display-resource-short-description
                   "pods" pod context namespace))))
    (vterm vterm-buffer-name)))

(defun kubed-ext-pods-eat (click)
  "Open eat terminal in Kubernetes pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (unless (require 'eat nil t)
    (user-error "This command requires the `eat' package"))
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((container (kubed-read-container pod "Container" t
                                              kubed-list-context kubed-list-namespace))
             (cmd (kubed-ext--kubectl-exec-command
                   pod container kubed-list-context kubed-list-namespace
                   kubed-ext-pod-shell))
             (eat-buffer-name
              (format "*Kubed eat %s*"
                      (kubed-display-resource-short-description
                       "pods" pod kubed-list-context kubed-list-namespace))))
        (eat cmd t))
    (user-error "No Kubernetes pod at point")))

(defun kubed-ext-eat-pod (pod &optional context namespace)
  "Open eat terminal in Kubernetes POD in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list (kubed-read-pod "Open eat in pod" nil nil c n) c n)))
  (unless (require 'eat nil t)
    (user-error "This command requires the `eat' package"))
  (let* ((context (or context (kubed-local-context)))
         (namespace (or namespace (kubed--namespace context)))
         (container (kubed-read-container pod "Container" t context namespace))
         (cmd (kubed-ext--kubectl-exec-command
               pod container context namespace kubed-ext-pod-shell))
         (eat-buffer-name
          (format "*Kubed eat %s*"
                  (kubed-display-resource-short-description
                   "pods" pod context namespace))))
    (eat cmd t)))

(defun kubed-ext-pods-eshell (click)
  "Open eshell in Kubernetes pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (require 'kubed-tramp) (kubed-tramp-assert-support)
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((default-directory
               (kubed-remote-file-name
                kubed-list-context kubed-list-namespace pod))
             (eshell-buffer-name
              (format "*Kubed eshell %s*"
                      (kubed-display-resource-short-description
                       "pods" pod kubed-list-context kubed-list-namespace))))
        (eshell t))
    (user-error "No Kubernetes pod at point")))

(defun kubed-ext-eshell-pod (pod &optional context namespace)
  "Open eshell in Kubernetes POD in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list (kubed-read-pod "Open eshell in pod" nil nil c n) c n)))
  (require 'kubed-tramp) (kubed-tramp-assert-support)
  (let* ((context (or context (kubed-local-context)))
         (namespace (or namespace (kubed--namespace context)))
         (default-directory (kubed-remote-file-name context namespace pod))
         (eshell-buffer-name
          (format "*Kubed eshell %s*"
                  (kubed-display-resource-short-description
                   "pods" pod context namespace))))
    (eshell t)))

(defun kubed-ext-pods-ansi-term (click)
  "Open `ansi-term' in Kubernetes pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((container (kubed-read-container pod "Container" t
                                              kubed-list-context kubed-list-namespace))
             (cmd (kubed-ext--kubectl-exec-command
                   pod container kubed-list-context kubed-list-namespace
                   kubed-ext-pod-shell))
             (buf-name (format "Kubed term %s"
                               (kubed-display-resource-short-description
                                "pods" pod kubed-list-context kubed-list-namespace))))
        (with-current-buffer (ansi-term "/bin/bash" buf-name)
          (process-send-string (get-buffer-process (current-buffer))
                               (concat cmd "\n"))))
    (user-error "No Kubernetes pod at point")))

(defun kubed-ext-pods-shell-command (click)
  "Run a shell command via kubectl exec in pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((container (kubed-read-container pod "Container" t
                                              kubed-list-context kubed-list-namespace))
             (prefix (concat
                      (mapconcat #'shell-quote-argument
                                 (append (list kubed-kubectl-program
                                               "exec" "-it" pod "-c" container)
                                         (when kubed-list-namespace
                                           (list "-n" kubed-list-namespace))
                                         (when kubed-list-context
                                           (list "--context" kubed-list-context))
                                         (list "--"))
                                 " ")
                      " "))
             (command (read-string "Shell command: " prefix)))
        (shell-command command))
    (user-error "No Kubernetes pod at point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 12.  Describe Resource
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-list-describe-resource (click)
  "Describe Kubernetes resource at CLICK position using kubectl describe."
  (interactive (list last-nonmenu-event) kubed-list-mode)
  (if-let ((name (tabulated-list-get-id (mouse-set-point click))))
      (let* ((type kubed-list-type)
             (namespace kubed-list-namespace)
             (context kubed-list-context)
             (buf (get-buffer-create
                   (format "*Kubed describe %s/%s@%s[%s]*"
                           type name
                           (or namespace "default")
                           (or context "current")))))
        (with-current-buffer buf
          (let ((inhibit-read-only t))
            (erase-buffer)
            (apply #'call-process kubed-kubectl-program nil t nil
                   (append (list "describe" type name)
                           (when namespace (list "-n" namespace))
                           (when context (list "--context" context))))
            (goto-char (point-min))
            (special-mode)))
        (display-buffer buf))
    (user-error "No Kubernetes resource at point")))

(defun kubed-ext-describe-resource (type name &optional context namespace)
  "Describe resource NAME of TYPE in CONTEXT and NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (type (kubed-read-resource-type "Type to describe" nil c))
          (ns (when (kubed-namespaced-p type c)
                (kubed--namespace c current-prefix-arg)))
          (name (kubed-read-resource-name type "Describe" nil nil c ns)))
     (list type name c ns)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace
                 (when (kubed-namespaced-p type ctx)
                   (kubed--namespace ctx))))
         (buf (get-buffer-create
               (format "*Kubed describe %s/%s@%s[%s]*"
                       type name (or ns "cluster") ctx))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (apply #'call-process kubed-kubectl-program nil t nil
               (append (list "describe" type name)
                       (when ns (list "-n" ns))
                       (when ctx (list "--context" ctx))))
        (goto-char (point-min))
        (special-mode)))
    (display-buffer buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 12a.  initContainer Logs
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-pod-init-containers (pod &optional context namespace)
  "Return list of init container names in POD in CONTEXT/NAMESPACE, or nil."
  (condition-case nil
      (let ((output (car (apply #'process-lines
                                kubed-kubectl-program "get" "pod" pod
                                "-o" "jsonpath={.spec.initContainers[*].name}"
                                (append
                                 (when namespace (list "--namespace" namespace))
                                 (when context (list "--context" context)))))))
        (when (and output
                   (not (kubed-ext-none-p output)))
          (split-string output " " t)))
    (error nil)))

(defun kubed-ext-logs-init-container (&optional context namespace)
  "Show logs for an init container of a pod in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list c n)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace (kubed--namespace ctx)))
         (pod (kubed-read-pod "Pod" nil nil ctx ns))
         (init-containers (kubed-ext-pod-init-containers pod ctx ns)))
    (unless init-containers
      (user-error "Pod `%s' has no init containers" pod))
    (let ((container (if (= (length init-containers) 1)
                         (car init-containers)
                       (completing-read "Init container: "
                                        init-containers nil t))))
      (kubed-logs "pods" pod ctx ns container nil nil nil nil nil nil))))

(defun kubed-ext-pods-logs-init-container (click)
  "Show init container logs for pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let ((init-containers (kubed-ext-pod-init-containers
                              pod kubed-list-context kubed-list-namespace)))
        (unless init-containers
          (user-error "Pod `%s' has no init containers" pod))
        (let ((container (if (= (length init-containers) 1)
                             (car init-containers)
                           (completing-read "Init container: "
                                            init-containers nil t))))
          (kubed-logs "pods" pod kubed-list-context kubed-list-namespace
                      container nil nil nil nil nil nil)))
    (user-error "No Kubernetes pod at point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 12b.  Logs by Label Selector
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-logs-by-label (&optional context namespace)
  "Show logs for pods matching a label selector in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list c n)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace (kubed--namespace ctx)))
         (labels (kubed-ext-discover-labels "pods" ctx ns))
         (label (completing-read "Label selector: " labels nil nil
                                 nil 'kubed-ext-label-selector-history))
         (tail (read-number "Tail lines: " 100))
         (buf (generate-new-buffer
               (format "*kubed-logs -l %s@%s[%s]*" label ns ctx))))
    (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
    (start-process "*kubed-logs-by-label*" buf
                   kubed-kubectl-program "logs" "-l" label
                   "--tail" (number-to-string tail)
                   "-n" ns "--context" ctx)
    (display-buffer buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 13.  Rollout Management
;;; ═══════════════════════════════════════════════════════════════

(defvar-local kubed-ext-rollout-type nil)
(defvar-local kubed-ext-rollout-name nil)
(defvar-local kubed-ext-rollout-context nil)
(defvar-local kubed-ext-rollout-namespace nil)

(defun kubed-ext-rollout-history (type name &optional context namespace)
  "Show rollout history for TYPE/NAME in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (type (completing-read "Resource type: "
                                 '("deployment" "statefulset" "daemonset") nil t))
          (name (kubed-read-resource-name
                 (concat type "s") "Rollout history for" nil nil c n)))
     (list type name c n)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace (kubed--namespace ctx)))
         (typename (format "%s/%s" type name))
         (buf (get-buffer-create
               (format "*kubed rollout %s@%s[%s]*"
                       typename (or ns "default") ctx))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "Rollout History: %s\n" typename)
                            'face 'bold))
        (insert (format "Namespace: %s  Context: %s\n" (or ns "default") ctx))
        (insert (make-string 60 ?─) "\n\n")
        (apply #'call-process kubed-kubectl-program nil t nil
               "rollout" "history" typename
               (append (when ns (list "-n" ns))
                       (when ctx (list "--context" ctx))))
        (insert "\n" (make-string 60 ?─) "\n")
        (insert (propertize
                 "\nKeys: r = view revision  u = undo  g = refresh  q = quit\n"
                 'face 'shadow))
        (goto-char (point-min))
        (setq-local kubed-ext-rollout-type type
                    kubed-ext-rollout-name name
                    kubed-ext-rollout-context ctx
                    kubed-ext-rollout-namespace ns)
        (kubed-ext-rollout-history-mode)))
    (display-buffer buf)))

(defun kubed-ext-rollout-show-revision ()
  "Show a specific revision from the rollout history buffer."
  (interactive nil kubed-ext-rollout-history-mode)
  (let* ((typename (format "%s/%s" kubed-ext-rollout-type
                           kubed-ext-rollout-name))
         (ctx kubed-ext-rollout-context)
         (ns kubed-ext-rollout-namespace)
         (rev (read-number "Revision: "))
         (buf (get-buffer-create
               (format "*kubed rollout %s rev %d*" typename rev))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "Revision %d: %s\n" rev typename)
                            'face 'bold))
        (insert (make-string 60 ?─) "\n\n")
        (apply #'call-process kubed-kubectl-program nil t nil
               "rollout" "history" typename (format "--revision=%d" rev)
               (append (when ns (list "-n" ns))
                       (when ctx (list "--context" ctx))))
        (goto-char (point-min))
        (special-mode)))
    (display-buffer buf)))

(defun kubed-ext-rollout-undo (type name &optional revision context namespace)
  "Undo rollout of TYPE/NAME to REVISION in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (type (completing-read "Resource type: "
                                 '("deployment" "statefulset" "daemonset") nil t))
          (name (kubed-read-resource-name
                 (concat type "s") "Undo rollout for" nil nil c n))
          (rev (when (y-or-n-p "Specify target revision? ")
                 (read-number "To revision: "))))
     (list type name rev c n)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace (kubed--namespace ctx)))
         (typename (format "%s/%s" type name)))
    (unless (zerop (apply #'call-process kubed-kubectl-program nil nil nil
                          "rollout" "undo" typename
                          (append
                           (when revision
                             (list (format "--to-revision=%d" revision)))
                           (when ns (list "-n" ns))
                           (when ctx (list "--context" ctx)))))
      (user-error "Failed to undo rollout for %s" typename))
    (message "Rolled back %s%s." typename
             (if revision (format " to revision %d" revision) ""))))

(defun kubed-ext-rollout-undo-from-history ()
  "Undo rollout from history buffer."
  (interactive nil kubed-ext-rollout-history-mode)
  (let* ((typename (format "%s/%s" kubed-ext-rollout-type
                           kubed-ext-rollout-name))
         (rev (when (y-or-n-p "Specify revision? ")
                (read-number "To revision: "))))
    (when (y-or-n-p (format "Undo rollout for %s%s? "
                            typename
                            (if rev (format " to rev %d" rev) "")))
      (kubed-ext-rollout-undo kubed-ext-rollout-type kubed-ext-rollout-name
                              rev kubed-ext-rollout-context
                              kubed-ext-rollout-namespace))))

(defun kubed-ext-rollout-refresh ()
  "Refresh rollout history buffer."
  (interactive nil kubed-ext-rollout-history-mode)
  (kubed-ext-rollout-history kubed-ext-rollout-type kubed-ext-rollout-name
                             kubed-ext-rollout-context
                             kubed-ext-rollout-namespace))

(define-derived-mode kubed-ext-rollout-history-mode special-mode "Kubed Rollout"
  "Mode for viewing Kubernetes rollout history."
  :interactive nil)

(keymap-set kubed-ext-rollout-history-mode-map "r"
            #'kubed-ext-rollout-show-revision)
(keymap-set kubed-ext-rollout-history-mode-map "u"
            #'kubed-ext-rollout-undo-from-history)
(keymap-set kubed-ext-rollout-history-mode-map "g"
            #'kubed-ext-rollout-refresh)
(keymap-set kubed-ext-rollout-history-mode-map "q" #'quit-window)

(defun kubed-ext-deployments-rollout-history (click)
  "Show rollout history for deployment at CLICK position."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (kubed-ext-rollout-history "deployment" dep
                                 kubed-list-context kubed-list-namespace)
    (user-error "No Kubernetes deployment at point")))

(defun kubed-ext-deployments-rollout-undo (click)
  "Undo rollout for deployment at CLICK position."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (let ((rev (when (y-or-n-p "Specify target revision? ")
                   (read-number "To revision: "))))
        (when (y-or-n-p (format "Undo rollout for %s?" dep))
          (kubed-ext-rollout-undo "deployment" dep rev
                                  kubed-list-context kubed-list-namespace)
          (kubed-list-update t)))
    (user-error "No Kubernetes deployment at point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 14.  Jab / Bounce Deployment
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-jab-deployment (deployment &optional context namespace)
  "Force rolling update of DEPLOYMENT in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list (kubed-read-deployment "Jab (bounce) deployment" nil nil c n) c n)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace (kubed--namespace ctx)))
         (q kubed-ext--dq)
         (ts (number-to-string (floor (float-time))))
         (patch (concat "{" q "spec" q ":{" q "template" q ":{" q "metadata" q
                        ":{" q "labels" q ":{" q "date" q ":" q ts q "}}}}}")))
    (unless (zerop (apply #'call-process kubed-kubectl-program nil nil nil
                          "patch" "deployment" deployment "-p" patch
                          (append (when ns (list "-n" ns))
                                  (when ctx (list "--context" ctx)))))
      (user-error "Failed to jab deployment %s" deployment))
    (message "Jabbed deployment %s (timestamp %s)." deployment ts)))

(defun kubed-ext-deployments-jab (click)
  "Jab deployment at CLICK position to force a rolling update."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (progn (kubed-ext-jab-deployment dep kubed-list-context
                                       kubed-list-namespace)
             (kubed-list-update t))
    (user-error "No Kubernetes deployment at point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 15.  Copy / Clipboard Operations
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext--last-kubectl-command nil
  "Last kubectl command tracked by kubed-ext.")

(defun kubed-ext-list-copy-log-command (click)
  "Copy `kubectl logs -f' command for resource at CLICK position."
  (interactive (list last-nonmenu-event) kubed-list-mode)
  (if-let ((name (tabulated-list-get-id (mouse-set-point click))))
      (let ((cmd (format "%s logs -f --tail=100 %s%s%s"
                         kubed-kubectl-program
                         (if (string= kubed-list-type "pods")
                             ""
                           (concat kubed-list-type "/"))
                         name
                         (concat
                          (when kubed-list-namespace
                            (format " -n %s" kubed-list-namespace))
                          (when kubed-list-context
                            (format " --context %s" kubed-list-context))))))
        (kill-new cmd)
        (message "Copied: %s" cmd))
    (user-error "No Kubernetes resource at point")))

(defun kubed-ext-list-copy-kubectl-prefix ()
  "Copy the current kubectl command prefix."
  (interactive nil kubed-list-mode)
  (let ((prefix (format "%s%s%s"
                        kubed-kubectl-program
                        (if kubed-list-namespace
                            (format " -n %s" kubed-list-namespace) "")
                        (if kubed-list-context
                            (format " --context %s" kubed-list-context) ""))))
    (kill-new prefix)
    (message "Copied: %s" prefix)))

(defun kubed-ext-list-copy-last-command ()
  "Copy the last tracked kubectl command."
  (interactive nil kubed-list-mode)
  (if kubed-ext--last-kubectl-command
      (progn (kill-new kubed-ext--last-kubectl-command)
             (message "Copied: %s" kubed-ext--last-kubectl-command))
    (message "No kubectl command tracked yet.")))

(defun kubed-ext-list-copy-resource-as-yaml (click)
  "Copy YAML of resource at CLICK position."
  (interactive (list last-nonmenu-event) kubed-list-mode)
  (if-let ((name (tabulated-list-get-id (mouse-set-point click))))
      (let* ((type kubed-list-type)
             (namespace kubed-list-namespace)
             (context kubed-list-context)
             (yaml (with-temp-buffer
                     (apply #'call-process kubed-kubectl-program nil t nil
                            "get" type name "-o" "yaml"
                            (append
                             (when namespace (list "-n" namespace))
                             (when context (list "--context" context))))
                     (buffer-string))))
        (kill-new yaml)
        (message "YAML for %s/%s copied (%d bytes)." type name (length yaml)))
    (user-error "No Kubernetes resource at point")))

(transient-define-prefix kubed-ext-copy-popup ()
  "Kubed Copy Menu."
  ["Copy to kill ring"
   ("w" "Resource name"  kubed-list-copy-as-kill)
   ("l" "Log command"    kubed-ext-list-copy-log-command)
   ("p" "kubectl prefix" kubed-ext-list-copy-kubectl-prefix)
   ("C" "Last command"   kubed-ext-list-copy-last-command)
   ("y" "Resource YAML"  kubed-ext-list-copy-resource-as-yaml)])

;;; ═══════════════════════════════════════════════════════════════
;;; § 16.  Label Selector
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext-label-selector-history nil
  "History list for label selectors.")

(defvar kubed-ext--label-cache (make-hash-table :test 'equal)
  "Label cache.")

(defvar kubed-ext--label-cache-time (make-hash-table :test 'equal)
  "Label cache timestamps.")

(defun kubed-ext--fetch-labels (type context namespace)
  "Discover label key=value pairs from resources of TYPE in CONTEXT/NAMESPACE."
  (condition-case nil
      (let ((output (with-temp-buffer
                      (apply #'call-process kubed-kubectl-program nil '(t nil) nil
                             "get" (or type "pods") "--no-headers"
                             "-o" "custom-columns=LABELS:.metadata.labels"
                             (append
                              (when namespace (list "-n" namespace))
                              (when context (list "--context" context))))
                      (buffer-string))))
        (delete-dups
         (mapcan (lambda (line)
                   (let ((trimmed (string-trim line)))
                     (when (string-match "^map$$$.+$$$" trimmed)
                       (mapcar (lambda (pair)
                                 (replace-regexp-in-string ":" "=" pair))
                               (split-string (match-string 1 trimmed) " ")))))
                 (split-string output "\n" t))))
    (error nil)))

(defun kubed-ext-discover-labels (&optional type context namespace)
  "Discover labels of TYPE in CONTEXT/NAMESPACE with 60-second caching."
  (let* ((key (list (or type "pods") (or context "") (or namespace "")))
         (cached (gethash key kubed-ext--label-cache))
         (cache-time (gethash key kubed-ext--label-cache-time 0)))
    (if (and cached (< (- (float-time) cache-time) 60))
        cached
      (let ((labels (kubed-ext--fetch-labels type context namespace)))
        (puthash key labels kubed-ext--label-cache)
        (puthash key (float-time) kubed-ext--label-cache-time)
        labels))))

(defun kubed-ext--find-active-selector (type context namespace)
  "Find the label selector for TYPE/CONTEXT/NAMESPACE."
  (catch 'found
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and (derived-mode-p 'kubed-list-mode)
                     kubed-ext-list-label-selector
                     (equal kubed-list-type type)
                     (equal kubed-list-context context)
                     (equal kubed-list-namespace namespace))
            (throw 'found kubed-ext-list-label-selector)))))))

(defun kubed-ext-update-with-selector (orig-fn type context &optional namespace)
  "Advice for ORIG-FN: inject --selector for TYPE/CONTEXT/NAMESPACE."
  (let ((sel (kubed-ext--find-active-selector type context namespace)))
    (if (null sel)
        (funcall orig-fn type context namespace)
      (cl-letf* ((real-mp (symbol-function 'make-process))
                 ((symbol-function 'make-process)
                  (lambda (&rest args)
                    (let ((cmd (plist-get args :command)))
                      (when (and cmd (member "get" cmd))
                        (plist-put args :command
                                   (append cmd (list "--selector" sel)))))
                    (apply real-mp args))))
        (funcall orig-fn type context namespace)))))

(advice-add 'kubed-update :around #'kubed-ext-update-with-selector)

(defun kubed-ext-list-set-label-selector (selector)
  "Set label SELECTOR for server-side filtering.  Empty clears it."
  (interactive
   (list (completing-read
          (format-prompt "Label selector" "clear")
          (append '("")
                  (kubed-ext-discover-labels
                   kubed-list-type kubed-list-context kubed-list-namespace)
                  kubed-ext-label-selector-history)
          nil nil nil 'kubed-ext-label-selector-history))
   kubed-list-mode)
  (setq-local kubed-ext-list-label-selector
              (if (string-empty-p selector) nil selector))
  (kubed-list-update))

;; replaced in section § 21
;; (setq kubed-list-mode-line-format
;;       '(:eval
;;         (if (process-live-p
;;              (alist-get 'process (kubed--alist kubed-list-type
;;                                                kubed-list-context
;;                                                kubed-list-namespace)))
;;             (propertize " [...]" 'help-echo "Updating...")
;;           (concat
;;            (when kubed-list-filter
;;              (propertize
;;               (concat " [" (mapconcat #'prin1-to-string
;;                                       kubed-list-filter " ") "]")
;;               'help-echo "Current filter"))
;;            (when kubed-ext-list-label-selector
;;              (propertize
;;               (concat " {" kubed-ext-list-label-selector "}")
;;               'help-echo "Label selector" 'face 'warning))
;;            (when (and (bound-and-true-p kubed-ext-resource-filter)
;;                       (not (string-empty-p kubed-ext-resource-filter)))
;;              (propertize
;;               (concat " /" kubed-ext-resource-filter "/")
;;               'help-echo "Substring filter" 'face 'italic))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 17.  Buffer Switching + Command Log
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-switch-buffer ()
  "Switch to another Kubed buffer.

Includes list buffers, wide-view buffers, and top/metrics buffers."
  (interactive)
  (let (bufs)
    (dolist (buf (buffer-list))
      (when (with-current-buffer buf
              (or (derived-mode-p 'kubed-list-mode)
                  (derived-mode-p 'kubed-ext-wide-mode)
                  (derived-mode-p 'kubed-ext-top-nodes-mode)
                  (derived-mode-p 'kubed-ext-top-pods-mode)
                  (derived-mode-p 'kubed-ext-port-forwards-mode)))
        (push (cons (kubed-ext--buffer-switch-candidate buf) buf) bufs)))
    (setq bufs (nreverse bufs))
    (if bufs
        (let* ((sel (completing-read "Switch to kubed buffer: " bufs nil t))
               (buf (alist-get sel bufs nil nil #'string=)))
          (switch-to-buffer buf))
      (user-error "No kubed buffers found"))))

(defun kubed-ext--buffer-switch-candidate (buf)
  "Return a human-readable completion candidate string for BUF."
  (with-current-buffer buf
    (cond
     ((derived-mode-p 'kubed-list-mode)
      (format "%-14s  %s"
              (or (bound-and-true-p kubed-list-type) "")
              (buffer-name buf)))
     ((derived-mode-p 'kubed-ext-wide-mode)
      (format "%-14s  %s"
              (format "wide:%s" (or kubed-ext-wide--type ""))
              (buffer-name buf)))
     ((derived-mode-p 'kubed-ext-top-nodes-mode)
      (format "%-14s  %s"
              (format "top:nodes@%s" (or kubed-ext-top-context ""))
              (buffer-name buf)))
     ((derived-mode-p 'kubed-ext-top-pods-mode)
      (format "%-14s  %s"
              (format "top:pods@%s" (or kubed-ext-top-namespace ""))
              (buffer-name buf)))
     ((derived-mode-p 'kubed-ext-port-forwards-mode)
      (format "%-14s  %s" "portfwds" (buffer-name buf)))
     (t (buffer-name buf)))))

(defun kubed-ext--log-kubectl-command (cmd-str)
  "Log CMD-STR to the kubectl command log buffer.
Creates the buffer if it does not exist."
  (setq kubed-ext--last-kubectl-command cmd-str)
  (let ((buf (get-buffer-create "*kubed-command-log*")))
    (with-current-buffer buf
      (unless (derived-mode-p 'special-mode)
        (special-mode))
      (let ((inhibit-read-only t))
        (goto-char (point-max))
        (insert (format "[%s] %s\n"
                        (format-time-string "%H:%M:%S") cmd-str))
        (when (> (line-number-at-pos) 500)
          (goto-char (point-min))
          (forward-line 100)
          (delete-region (point-min) (point)))))))

(defun kubed-ext--make-process-logger (orig-fn &rest args)
  "Log kubectl `make-process' call, including shell-wrapped invocations.

Handles two forms:
  Direct:  (:command (kubectl get pods ...))
  Wrapped: (:command (/bin/sh -c \"kubectl logs ... | rg ...\"))

ORIG-FN is the advised function, ARGS its arguments."
  (let ((cmd (plist-get args :command)))
    (when (and (consp cmd) (stringp (car cmd)))
      (let ((prog (file-name-nondirectory
                   (file-name-sans-extension (car cmd)))))
        (cond
         ;; Direct kubectl invocation.
         ((string-suffix-p "kubectl" prog)
          (kubed-ext--log-kubectl-command
           (mapconcat #'identity (seq-filter #'stringp cmd) " ")))
         ;; Shell wrapper: /bin/sh -c "kubectl ... | rg ..."
         ((member prog '("sh" "bash" "zsh"))
          (let ((shell-arg (car (last (seq-filter #'stringp cmd)))))
            (when (and (stringp shell-arg)
                       (string-match-p "kubectl" shell-arg))
              (kubed-ext--log-kubectl-command shell-arg))))))))
  (apply orig-fn args))

(advice-add 'make-process :around #'kubed-ext--make-process-logger)

(defun kubed-ext--call-process-logger (orig-fn program &rest args)
  "Log kubectl `call-process'.
ORIG-FN is the advised function.  PROGRAM and ARGS are passed through."
  (when (and (stringp program)
             (string-suffix-p
              "kubectl"
              (file-name-sans-extension
               (file-name-nondirectory program))))
    (kubed-ext--log-kubectl-command
     (mapconcat #'identity
                (cons program (seq-filter #'stringp (nthcdr 3 args)))
                " ")))
  (apply orig-fn program args))

(advice-add 'call-process :around #'kubed-ext--call-process-logger)

(defun kubed-ext--call-process-region-logger
    (orig-fn start end program &rest args)
  "Log kubectl `call-process-region'.
ORIG-FN is advised.  START END PROGRAM ARGS are passed through."
  (when (and (stringp program)
             (string-suffix-p
              "kubectl"
              (file-name-sans-extension
               (file-name-nondirectory program))))
    (kubed-ext--log-kubectl-command
     (mapconcat #'identity
                (cons program (seq-filter #'stringp (nthcdr 3 args)))
                " ")))
  (apply orig-fn start end program args))

(advice-add 'call-process-region :around
            #'kubed-ext--call-process-region-logger)

(defun kubed-ext-show-command-log ()
  "Show the kubed command log buffer."
  (interactive)
  (let ((buf (get-buffer-create "*kubed-command-log*")))
    (with-current-buffer buf
      (unless (derived-mode-p 'special-mode) (special-mode)))
    (display-buffer buf)))

;;; ─── Process buffer access ────────────────────────────────────

(defun kubed-ext-show-process-buffer ()
  "Show the kubectl process/stderr buffer for the current list resource."
  (interactive nil kubed-list-mode)
  (let* ((type kubed-list-type)
         (buf  (or (get-buffer (format " *kubed-get-%s-stderr*" type))
                   (get-buffer (format "*kubed-get-%s*"         type))
                   (get-buffer (format " *kubed-get-%s*"        type)))))
    (if (and buf (buffer-live-p buf))
        (display-buffer buf)
      (user-error "No process buffer found for %s" type))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 18.  Keybindings
;;; ═══════════════════════════════════════════════════════════════

;; ── Pods ──────────────────────────────────────────────────────────
(keymap-set kubed-pods-mode-map  "T" #'kubed-ext-top-pods)
(keymap-set kubed-pods-mode-map  "M" #'kubed-ext-top-pods)
(keymap-set kubed-pods-mode-map  "v" #'kubed-ext-pods-vterm)
(keymap-set kubed-pods-mode-map  "t" #'kubed-ext-pods-eat)
(keymap-set kubed-pods-mode-map  "i" #'kubed-ext-pods-logs-init-container)
(keymap-set kubed-pod-prefix-map "T" #'kubed-ext-top-pods)
(keymap-set kubed-pod-prefix-map "v" #'kubed-ext-vterm-pod)
(keymap-set kubed-pod-prefix-map "t" #'kubed-ext-eat-pod)
(keymap-set kubed-pod-prefix-map "S" #'kubed-ext-eshell-pod)
(keymap-set kubed-pod-prefix-map "i" #'kubed-ext-logs-init-container)

;; ── Nodes ─────────────────────────────────────────────────────────
(keymap-set kubed-nodes-mode-map  "M" #'kubed-ext-top-nodes)
(keymap-set kubed-node-prefix-map "T" #'kubed-ext-top-nodes)

;; ── Services ──────────────────────────────────────────────────────
(keymap-set kubed-services-mode-map  "F" #'kubed-ext-services-forward-port)
(keymap-set kubed-service-prefix-map "F" #'kubed-ext-forward-port-to-service)

;; ── Deployments ───────────────────────────────────────────────────
(keymap-set kubed-deployments-mode-map "F" #'kubed-ext-deployments-forward-port)
(keymap-set kubed-deployments-mode-map "r" #'kubed-ext-deployments-rollout-history)
(keymap-set kubed-deployments-mode-map "j" #'kubed-ext-deployments-jab)
(keymap-set kubed-deployments-mode-map "U" #'kubed-ext-deployments-rollout-undo)
(keymap-set kubed-deployments-mode-map "M-u" #'kubed-ext-unmark-all)
(keymap-set kubed-deployment-prefix-map "F" #'kubed-ext-forward-port-to-deployment)
(keymap-set kubed-deployment-prefix-map "r" #'kubed-ext-rollout-history)
(keymap-set kubed-deployment-prefix-map "j" #'kubed-ext-jab-deployment)
(keymap-set kubed-deployment-prefix-map "U" #'kubed-ext-rollout-undo)

;; ── kubed-list-mode (parent of all list modes) ────────────────────
;;   "d" intentionally overrides kubed's mark-for-deletion (k9s-style).
;;   "%" preserves the old mark-for-deletion workflow.
(keymap-set kubed-list-mode-map "d"   #'kubed-ext-list-describe-resource)
(keymap-set kubed-list-mode-map "%"   #'kubed-list-mark-for-deletion)
(keymap-set kubed-list-mode-map "S"   #'kubed-ext-list-set-label-selector)
(keymap-set kubed-list-mode-map "c"   #'kubed-ext-copy-popup)
(keymap-set kubed-list-mode-map "b"   #'kubed-ext-switch-buffer)
(keymap-set kubed-list-mode-map "f"   #'kubed-ext-set-filter)
(keymap-set kubed-list-mode-map "V"   #'kubed-ext-list-wide)
(keymap-set kubed-list-mode-map "m"   #'kubed-ext-mark-item)
(keymap-set kubed-list-mode-map "M-m" #'kubed-ext-mark-all)
(keymap-set kubed-list-mode-map "M-u" #'kubed-ext-unmark-all)
(keymap-set kubed-list-mode-map "M-n" #'kubed-ext-jump-to-next-highlight)
(keymap-set kubed-list-mode-map "M-p" #'kubed-ext-jump-to-previous-highlight)
(keymap-set kubed-list-mode-map "X"   #'kubed-ext-delete-marked)
(keymap-set kubed-list-mode-map "A"   #'kubed-ext-auto-refresh-mode)
(keymap-set kubed-list-mode-map "r"   #'kubed-ext-switch-resource)
(keymap-set kubed-list-mode-map "$"   #'kubed-ext-show-process-buffer)

;; ── kubed-prefix-map ──────────────────────────────────────────────
;;   "d" belongs to deployments; use "k" (kubectl describe mnemonic).
(keymap-set kubed-prefix-map "k" #'kubed-ext-describe-resource)
(keymap-set kubed-prefix-map "F" #'kubed-ext-list-port-forwards)
(keymap-set kubed-prefix-map "b" #'kubed-ext-switch-buffer)
(keymap-set kubed-prefix-map "#" #'kubed-ext-show-command-log)
(keymap-set kubed-prefix-map "L" #'kubed-ext-logs-by-label)

(with-eval-after-load 'kubed
  (when (boundp 'kubed-deployments-mode-map)
    (keymap-set kubed-deployments-mode-map "M-u" #'kubed-ext-unmark-all)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 19.  Transient Navigation + Resource Actions
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext-extra-transient-suffixes
  '(("pods"        . [("T" "Top (metrics)"     kubed-ext-top-pods)
                      ("v" "Vterm"              kubed-ext-pods-vterm)
                      ("t" "Eat terminal"       kubed-ext-pods-eat)
                      ("E" "Eshell"             kubed-ext-pods-eshell)
                      ("A" "Ansi-term"          kubed-ext-pods-ansi-term)
                      ("&" "Shell command"      kubed-ext-pods-shell-command)
                      ("i" "Init container log" kubed-ext-pods-logs-init-container)])
    ("nodes"       . [("T" "Top (metrics)"  kubed-ext-top-nodes)])
    ("services"    . [("F" "Forward Port"   kubed-ext-services-forward-port)])
    ("deployments" . [("F" "Forward Port"       kubed-ext-deployments-forward-port)
                      ("H" "Rollout History"    kubed-ext-deployments-rollout-history)
                      ("j" "Jab (bounce)"       kubed-ext-deployments-jab)
                      ("u" "Rollout Undo"       kubed-ext-deployments-rollout-undo)]))
  "Additional per-resource-type transient suffixes.")

(defun kubed-ext-switch-context ()
  "Switch kubectl context and refresh current resource list."
  (interactive)
  (when (derived-mode-p 'kubed-list-mode)
    (let* ((type kubed-list-type)
           (ns kubed-list-namespace)
           (ctx (kubed-read-context "Switch to context")))
      ;; Immediately start prefetching namespaces for the new context.
      (kubed-ext-prefetch-namespaces ctx)
      (when type
        (let ((fn (intern (format "kubed-list-%s" type))))
          (when (fboundp fn)
            (cond
             ;; Non-namespaced resource type.
             ((not (kubed-namespaced-p type ctx))
              (funcall fn ctx))
             ;; Reuse current namespace.
             (ns (funcall fn ctx ns))
             ;; Must determine namespace — ensure list is ready first.
             (t
              (kubed-ext--ensure-namespaces-ready ctx)
              (funcall fn ctx (kubed--namespace ctx))))))))))

(defun kubed-ext-switch-namespace ()
  "Switch namespace and refresh current resource list."
  (interactive)
  (when (derived-mode-p 'kubed-list-mode)
    ;; Ensure namespace completions are available before prompting.
    (kubed-ext--ensure-namespaces-ready kubed-list-context)
    (let ((type kubed-list-type)
          (ctx kubed-list-context)
          (ns (kubed-read-namespace "Switch to namespace" nil nil
                                    kubed-list-context)))
      (when type
        (let ((fn (intern (format "kubed-list-%s" type))))
          (when (fboundp fn)
            (if (kubed-namespaced-p type ctx)
                (funcall fn ctx ns)
              (funcall fn ctx))))))))

(defvar kubed-ext-common-resources
  '(("pods" . "Pods")
    ("deployments" . "Deployments")
    ("services" . "Services")
    ("configmaps" . "ConfigMaps")
    ("secrets" . "Secrets")
    ("ingresses" . "Ingresses")
    ("jobs" . "Jobs")
    ("cronjobs" . "CronJobs")
    ("statefulsets" . "StatefulSets")
    ("daemonsets" . "DaemonSets")
    ("replicasets" . "ReplicaSets")
    ("persistentvolumeclaims" . "PVCs")
    ("persistentvolumes" . "PVs")
    ("nodes" . "Nodes")
    ("namespaces" . "Namespaces")
    ("events" . "Events")
    ("networkpolicies" . "NetworkPolicies")
    ("horizontalpodautoscalers" . "HPAs")
    ("kafkas" . "Kafkas")
    ("kafkaconnects" . "KafkaConnects")
    ("strimzipodsets" . "StrimziPodSets")
    ("kafkanodepools" . "KafkaNodePools")
    ("kafkarebalances" . "KafkaRebalances")
    ("kafkaconnectors" . "KafkaConnectors")
    ("kafkatopics" . "KafkaTopics"))
  "Common resources for quick switching.")

(defun kubed-ext-switch-resource ()
  "Switch to a different resource type in the same context/namespace."
  (interactive)
  (when (derived-mode-p 'kubed-list-mode)
    (let* ((ctx kubed-list-context)
           (ns kubed-list-namespace)
           (choices (mapcar (lambda (r) (cons (cdr r) (car r)))
                            kubed-ext-common-resources))
           (sel (completing-read "Switch to resource: " choices nil t))
           (type (alist-get sel choices nil nil #'string=)))
      (when type
        (let ((fn (intern (format "kubed-list-%s" type))))
          (when (fboundp fn)
            (if (kubed-namespaced-p type ctx)
                (funcall fn ctx (or ns (kubed--namespace ctx)))
              (funcall fn ctx))))))))

(defun kubed-ext-jump-pods ()
  "Jump to Pods."
  (interactive)
  (kubed-list-pods kubed-list-context kubed-list-namespace))

(defun kubed-ext-jump-deployments ()
  "Jump to Deployments."
  (interactive)
  (kubed-list-deployments kubed-list-context kubed-list-namespace))

(defun kubed-ext-jump-services ()
  "Jump to Services."
  (interactive)
  (kubed-list-services kubed-list-context kubed-list-namespace))

(defun kubed-ext-jump-jobs ()
  "Jump to Jobs."
  (interactive)
  (kubed-list-jobs kubed-list-context kubed-list-namespace))

(defun kubed-ext-jump-configmaps ()
  "Jump to ConfigMaps."
  (interactive)
  (kubed-list-configmaps kubed-list-context kubed-list-namespace))

(defun kubed-ext-jump-secrets ()
  "Jump to Secrets."
  (interactive)
  (kubed-list-secrets kubed-list-context kubed-list-namespace))

(defun kubed-ext-jump-ingresses ()
  "Jump to Ingresses."
  (interactive)
  (kubed-list-ingresses kubed-list-context kubed-list-namespace))

(defun kubed-ext-jump-pvcs ()
  "Jump to PVCs."
  (interactive)
  (kubed-list-persistentvolumeclaims kubed-list-context kubed-list-namespace))

(defun kubed-ext--collect-action-groups ()
  "Build transient group vectors for the current resource type."
  (let* ((type (or kubed-list-type ""))
         (type-label (capitalize type))
         (upstream (bound-and-true-p kubed-list-transient-extra-suffixes))
         (custom (alist-get type kubed-ext-extra-transient-suffixes
                            nil nil #'string=))
         (all (append (when upstream upstream) (when custom (list custom))))
         (result nil)
         (gidx 0))
    (dolist (gv all)
      (cl-incf gidx)
      (let* ((entries (if (vectorp gv) (append gv nil) gv))
             (kw-args nil)
             (suffixes nil)
             (rest entries))
        (while rest
          (cond
           ((keywordp (car rest))
            (push (pop rest) kw-args)
            (when rest (push (pop rest) kw-args)))
           ((and (listp (car rest))
                 (>= (length (car rest)) 3)
                 (stringp (nth 0 (car rest)))
                 (stringp (nth 1 (car rest))))
            (push (pop rest) suffixes))
           (t (pop rest))))
        (setq kw-args (nreverse kw-args)
              suffixes (nreverse suffixes))
        (when suffixes
          (push (apply #'vector
                       (format "%s Actions%s" type-label
                               (if (> (length all) 1)
                                   (format " (%d)" gidx) ""))
                       (append kw-args suffixes))
                result))))
    (nreverse result)))

(declare-function kubed-ext--dynamic-menu "kubed-ext")

(defun kubed-ext-transient-menu ()
  "Show transient combining navigation, actions, and utilities."
  (interactive)
  (require 'transient)
  (require 'kubed-transient nil t)
  (let* ((in-list (derived-mode-p 'kubed-list-mode))
         (type (and in-list kubed-list-type))
         (agroups (and in-list (kubed-ext--collect-action-groups))))
    (condition-case err
        (progn
          (eval
           `(transient-define-prefix kubed-ext--dynamic-menu ()
              ,(format "Kubed %s" (or type "Navigation"))
              ,@(if in-list
                    '([["Navigate"
                        ("c" "Switch Context"   kubed-ext-switch-context)
                        ("n" "Switch Namespace" kubed-ext-switch-namespace)
                        ("r" "Switch Resource"  kubed-ext-switch-resource)]
                       ["Jump Workloads"
                        ("1" "Pods"        kubed-ext-jump-pods)
                        ("2" "Deployments" kubed-ext-jump-deployments)
                        ("3" "Services"    kubed-ext-jump-services)
                        ("4" "Jobs"        kubed-ext-jump-jobs)]
                       ["Jump Config+Net"
                        ("5" "ConfigMaps"  kubed-ext-jump-configmaps)
                        ("6" "Secrets"     kubed-ext-jump-secrets)
                        ("7" "Ingresses"   kubed-ext-jump-ingresses)
                        ("8" "PVCs"        kubed-ext-jump-pvcs)]])
                  '([["Open Resource List"
                      ("1" "Pods"        kubed-list-pods)
                      ("2" "Deployments" kubed-list-deployments)
                      ("3" "Services"    kubed-list-services)
                      ("4" "Jobs"        kubed-list-jobs)
                      ("5" "ConfigMaps"  kubed-list-configmaps)
                      ("6" "Secrets"     kubed-list-secrets)]]))
              ,@(cond
                 ((null agroups) nil)
                 ((= (length agroups) 1) agroups)
                 ((<= (length agroups) 3)
                  (list (apply #'vector agroups)))
                 (t (let ((rows nil) (rest agroups))
                      (while rest
                        (let ((chunk (seq-take rest 3)))
                          (push (if (= (length chunk) 1) (car chunk)
                                  (apply #'vector chunk))
                                rows)
                          (setq rest (seq-drop rest 3))))
                      (nreverse rows))))
              ,@(when in-list
                  '([["General"
                      ("g" "Refresh"          kubed-list-update :transient t)
                      ("d" "Describe"         kubed-ext-list-describe-resource)
                      ("/" "Filter (S-expr)"  kubed-list-set-filter)
                      ("f" "Filter (substr)"  kubed-ext-set-filter)
                      ("S" "Label Selector"   kubed-ext-list-set-label-selector)
                      ("V" "Wide view"        kubed-ext-list-wide)
                      ("A" "Auto-refresh"     kubed-ext-auto-refresh-mode)
                      ("q" "Quit"             quit-window)]
                     ["Utilities"
                      ("b" "Switch Buffer"    kubed-ext-switch-buffer)
                      ("w" "Copy Name"        kubed-list-copy-as-kill)
                      ("y" "Copy Menu"        kubed-ext-copy-popup)
                      ("#" "Command Log"      kubed-ext-show-command-log)
                      ("m" "Mark item"        kubed-ext-mark-item)
                      ("X" "Delete marked"    kubed-ext-delete-marked)]])))
           t)
          (funcall-interactively #'kubed-ext--dynamic-menu))
      (error
       (message "Dynamic transient error: %s" (error-message-string err))
       (when (fboundp 'kubed-list-transient)
         (call-interactively #'kubed-list-transient))))))

(keymap-set kubed-list-mode-map "?" #'kubed-ext-transient-menu)
(keymap-set kubed-list-mode-map "C" #'kubed-ext-switch-context)
(keymap-set kubed-list-mode-map "N" #'kubed-ext-switch-namespace)
(keymap-set kubed-list-mode-map "R" #'kubed-ext-switch-resource)

;;; ═══════════════════════════════════════════════════════════════
;;; § 20.  Kill All Kubed Buffers
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-kill-all-buffers (&optional no-confirm)
  "Kill all Kubed-related buffers.

Buffers are matched either by their major/minor mode (e.g.
`kubed-list-mode', `kubed-ext-wide-mode', etc.) or by their name
starting with \"*Kubed\" or \"*kubed\" (case-insensitive), including
internal space-prefixed scratch buffers such as \" *kubed-get-*\".

With a prefix argument NO-CONFIRM, skip the confirmation prompt and
kill immediately.

Also stops any active port-forward processes and cancels the
auto-refresh timer if it is running."
  (interactive "P")
  (let ((kubed-bufs
         (seq-filter
          (lambda (buf)
            (or
             ;; ── Detect by major / minor mode ──────────────────
             (with-current-buffer buf
               (or (derived-mode-p 'kubed-list-mode)
                   (derived-mode-p 'kubed-ext-wide-mode)
                   (derived-mode-p 'kubed-ext-top-nodes-mode)
                   (derived-mode-p 'kubed-ext-top-pods-mode)
                   (derived-mode-p 'kubed-ext-port-forwards-mode)
                   (derived-mode-p 'kubed-ext-rollout-history-mode)
                   (bound-and-true-p kubed-display-resource-mode)))
             ;; ── Detect by buffer-name pattern ─────────────────
             ;; Matches: *Kubed ...  *kubed-...  *kubed rollout...
             ;;          " *kubed-get-*" (internal scratch buffers)
             (string-match-p
              "\\`[[:space:]]*\\*[Kk]ubed"
              (buffer-name buf))))
          (buffer-list))))
    (if (null kubed-bufs)
        (message "No Kubed buffers to kill.")
      (when (or no-confirm
                (yes-or-no-p
                 (format "Kill %d Kubed buffer%s? "
                         (length kubed-bufs)
                         (if (= (length kubed-bufs) 1) "" "s"))))
        ;; ── Cancel auto-refresh timer ──────────────────────────
        (when (fboundp 'kubed-ext--auto-refresh-cancel-timer)
          (kubed-ext--auto-refresh-cancel-timer))
        ;; ── Kill buffers ───────────────────────────────────────
        (let ((killed 0))
          (dolist (buf kubed-bufs)
            (when (buffer-live-p buf)
              (kill-buffer buf)
              (cl-incf killed)))
          (message "Killed %d Kubed buffer%s."
                   killed
                   (if (= killed 1) "" "s")))))))

(keymap-set kubed-prefix-map "Q" #'kubed-ext-kill-all-buffers)
(keymap-set kubed-list-mode-map "C-c C-k" #'kubed-ext-kill-all-buffers)

;;; ═══════════════════════════════════════════════════════════════
;;; § 21.  Circuit Breaker — Intelligent VPN / Network Recovery
;;; ═══════════════════════════════════════════════════════════════
;;
;; Each kubectl context has an independent circuit in one of three states:
;;
;;   closed  — normal; auto-refresh runs freely.
;;   network — cluster unreachable (VPN, DNS, TLS, timeout).
;;             Trips after `kubed-ext-cb-threshold' consecutive network
;;             errors.  Recovery probes with exponential back-off are
;;             scheduled automatically; circuit closes on probe success.
;;   auth    — credentials expired (kubelogin, AADSTS, az-cli).
;;             Trips after `kubed-ext-cb-auth-threshold' consecutive auth
;;             errors.  No automatic probing — user must re-authenticate
;;             then press `g' or `Z' to resume.
;;
;; Both open states suppress auto-refresh.  Mode-line indicators:
;;   ⚡~Xs     network outage  (X = seconds until next probe)
;;   🔑 re-auth  credential issue

(defgroup kubed-ext-circuit-breaker nil
  "Per-context circuit breaker for kubed-ext auto-refresh."
  :group 'kubed-ext)

(defcustom kubed-ext-cb-threshold 3
  "Consecutive network failures before the circuit opens for a context."
  :type 'natnum
  :group 'kubed-ext-circuit-breaker)

(defcustom kubed-ext-cb-auth-threshold 2
  "Consecutive auth failures before auto-refresh is paused for a context."
  :type 'natnum
  :group 'kubed-ext-circuit-breaker)

(defcustom kubed-ext-cb-initial-backoff 30
  "Seconds before the first network-recovery probe after the circuit opens."
  :type 'natnum
  :group 'kubed-ext-circuit-breaker)

(defcustom kubed-ext-cb-backoff-factor 2.0
  "Multiplier applied to the probe interval after each failed probe."
  :type 'number
  :group 'kubed-ext-circuit-breaker)

(defcustom kubed-ext-cb-max-backoff 300
  "Maximum probe interval in seconds (default: 5 minutes)."
  :type 'natnum
  :group 'kubed-ext-circuit-breaker)

;; ── Per-context state (all tables keyed by context string) ───────

(defvar kubed-ext--cb-open
  (make-hash-table :test 'equal)
  "Circuit state per context: nil (closed), `network', or `auth'.")

(defvar kubed-ext--cb-net-failures
  (make-hash-table :test 'equal)
  "Consecutive network-error count per context.")

(defvar kubed-ext--cb-auth-failures
  (make-hash-table :test 'equal)
  "Consecutive auth-error count per context.")

(defvar kubed-ext--cb-backoff
  (make-hash-table :test 'equal)
  "Current probe back-off interval in seconds per context.")

(defvar kubed-ext--cb-probe-timers
  (make-hash-table :test 'equal)
  "Pending network-recovery probe timer per context.")

;; ── Error pattern matching ────────────────────────────────────────

(defconst kubed-ext--network-error-patterns
  '("EOF" "unexpected EOF" "connection reset by peer" "connection refused"
    "network is unreachable" "no route to host" "broken pipe"
    "no such host" "Name or service not known"
    "Temporary failure in name resolution"
    "i/o timeout" "TLS handshake timeout" "context deadline exceeded"
    "request timeout" "Unable to connect to the server"
    "transport: Error while dialing" "dial tcp" "proxy: "
    "azmk8s.io" "googleapis.com" "eks.amazonaws.com"
    "certificate has expired")
  "Kubectl error substrings indicating transient network or VPN failures.")

(defconst kubed-ext--auth-error-patterns
  '("AzureCLICredential" "kubelogin failed" "AADSTS"
    "failed to get token" "getting credentials: exec"
    "exec: executable kubelogin" "TokenRequestError"
    "invalid_client" "Unauthorized" "unauthorized"
    "az login" "az logout")
  "Kubectl error substrings indicating expired or invalid credentials.")

(defun kubed-ext--network-error-p (msg)
  "Return non-nil if MSG indicates a transient network or VPN failure."
  (and (stringp msg)
       (seq-some (lambda (pat)
                   (string-match-p (regexp-quote pat) msg))
                 kubed-ext--network-error-patterns)))

(defun kubed-ext--auth-error-p (msg)
  "Return non-nil if MSG indicates an expired or invalid credential."
  (and (stringp msg)
       (seq-some (lambda (pat)
                   (string-match-p (regexp-quote pat) msg))
                 kubed-ext--auth-error-patterns)))

;; ── State accessors ───────────────────────────────────────────────

(defun kubed-ext--cb-open-p (context)
  "Return open reason for CONTEXT (`network' or `auth'), or nil if closed."
  (gethash (or context "") kubed-ext--cb-open))

(defun kubed-ext--cb-backoff-for (context)
  "Return the current probe back-off interval in seconds for CONTEXT."
  (gethash (or context "") kubed-ext--cb-backoff
           kubed-ext-cb-initial-backoff))

;; ── Stderr content discovery ──────────────────────────────────────

(defun kubed-ext--cb-read-stderr (resource-type)
  "Return stderr content for a `kubectl get RESOURCE-TYPE' process, or nil.
Tries several buffer-naming conventions that kubed may use."
  (catch 'found
    (dolist (name (list (format " *kubed-get-%s-stderr*" resource-type)
                        (format "*kubed-get-%s*"         resource-type)
                        (format " *kubed-get-%s*"        resource-type)))
      (let ((buf (get-buffer name)))
        (when (and buf (buffer-live-p buf))
          (let ((content (with-current-buffer buf
                           (string-trim (buffer-string)))))
            (unless (string-empty-p content)
              (throw 'found content))))))))

;; ── List-buffer helpers ───────────────────────────────────────────

(defun kubed-ext--cb-in-context-bufs (context fn)
  "Call FN with no args in every live kubed list buffer using CONTEXT."
  (let ((ctx (or context "")))
    (dolist (buf (buffer-list))
      (when (buffer-live-p buf)
        (with-current-buffer buf
          (when (and (derived-mode-p 'kubed-list-mode)
                     (equal kubed-list-context ctx))
            (funcall fn)))))))

(defun kubed-ext--cb-set-header (context message)
  "Display MESSAGE as the error header in every list buffer for CONTEXT."
  (kubed-ext--cb-in-context-bufs
   context (lambda () (kubed-ext--set-header-error message))))

(defun kubed-ext--cb-clear-headers (context)
  "Clear circuit-breaker header overlays in every list buffer for CONTEXT."
  (kubed-ext--cb-in-context-bufs
   context #'kubed-ext--clear-header-error))

(defun kubed-ext--cb-refresh-mode-lines (context)
  "Force mode-line update in every kubed list buffer for CONTEXT."
  (kubed-ext--cb-in-context-bufs
   context #'force-mode-line-update))

;; ── Auth-error summary extraction ────────────────────────────────

(defun kubed-ext--cb-auth-summary (err-msg)
  "Extract a short human-readable summary from an auth error ERR-MSG."
  (let* ((lines (split-string (string-trim err-msg) "\n" t))
         (hint  (seq-find (lambda (l)
                            (string-match-p "az login\\|az logout" l))
                          lines)))
    (string-trim (or hint (car lines) "Credentials expired."))))

;; ── Core state machine ────────────────────────────────────────────

(defun kubed-ext--cb-reset-state (context)
  "Clear all circuit-breaker counters and cancel any probe timer for CONTEXT."
  (let* ((ctx   (or context ""))
         (timer (gethash ctx kubed-ext--cb-probe-timers)))
    (puthash ctx 0   kubed-ext--cb-net-failures)
    (puthash ctx 0   kubed-ext--cb-auth-failures)
    (puthash ctx nil kubed-ext--cb-open)
    (puthash ctx kubed-ext-cb-initial-backoff kubed-ext--cb-backoff)
    (when (timerp timer) (cancel-timer timer))
    (remhash ctx kubed-ext--cb-probe-timers)))

(defun kubed-ext--cb-on-success (context)
  "Record a successful kubectl call; close the circuit for CONTEXT if open."
  (let* ((ctx      (or context ""))
         (was-open (kubed-ext--cb-open-p ctx)))
    (kubed-ext--cb-reset-state ctx)
    (when was-open
      (message "Kubed ✓  [%s]: connection restored — auto-refresh resumed." ctx)
      (kubed-ext--cb-clear-headers ctx)
      (kubed-ext--cb-refresh-mode-lines ctx)
      (kubed-ext--cb-in-context-bufs
       ctx (lambda ()
             (condition-case nil (kubed-list-update t)
               (error nil)))))))

(defun kubed-ext--cb-trip-network (context err-msg)
  "Open the circuit with reason `network' for CONTEXT/ERR-MSG."
  (let* ((ctx     (or context ""))
         (delay   kubed-ext-cb-initial-backoff)
         (summary (truncate-string-to-width
                   (replace-regexp-in-string "\n" " " (string-trim err-msg))
                   60 nil nil "…")))
    (puthash ctx 'network kubed-ext--cb-open)
    (puthash ctx delay    kubed-ext--cb-backoff)
    (message "Kubed ⚡ [%s]: unreachable — %s  [probing in %ds; `g'/`Z' to retry]"
             ctx summary delay)
    (kubed-ext--cb-refresh-mode-lines ctx)
    (kubed-ext--cb-schedule-probe ctx delay)))

(defun kubed-ext--cb-trip-auth (context err-msg)
  "Open the circuit with reason `auth' for CONTEXT/ERR-MSG."
  (let* ((ctx     (or context ""))
         (summary (kubed-ext--cb-auth-summary err-msg)))
    (puthash ctx 'auth kubed-ext--cb-open)
    (puthash ctx kubed-ext-cb-initial-backoff kubed-ext--cb-backoff)
    (message "Kubed 🔑 [%s]: auth paused — %s  [re-auth then press `g' or `Z']"
             ctx summary)
    (kubed-ext--cb-set-header
     ctx (format "🔑 Auth expired — %s  [Press `g' or `Z' to resume]" summary))
    (kubed-ext--cb-refresh-mode-lines ctx)))

(defun kubed-ext--cb-on-failure (context err-msg)
  "Record a kubectl failure for CONTEXT with error message ERR-MSG.
Auth and network errors are counted toward separate thresholds.
When either threshold is reached the circuit opens, suppressing
auto-refresh.  Unknown errors are silently ignored."
  (let ((ctx (or context "")))
    (cond
     ;; ── Auth / credential error ───────────────────────────────
     ;; Only act while the circuit is not already in `auth' state.
     ((and (kubed-ext--auth-error-p err-msg)
           (not (eq (kubed-ext--cb-open-p ctx) 'auth)))
      (let* ((n   (1+ (gethash ctx kubed-ext--cb-auth-failures 0)))
             (thr kubed-ext-cb-auth-threshold))
        (puthash ctx n kubed-ext--cb-auth-failures)
        (if (>= n thr)
            (kubed-ext--cb-trip-auth ctx err-msg)
          (message "Kubed 🔑 [%s] (%d/%d): %s"
                   ctx n thr (kubed-ext--cb-auth-summary err-msg)))))

     ;; ── Network / transport error ─────────────────────────────
     ;; Only act while the circuit is fully closed.
     ((and (kubed-ext--network-error-p err-msg)
           (not (kubed-ext--cb-open-p ctx)))
      (let* ((n   (1+ (gethash ctx kubed-ext--cb-net-failures 0)))
             (thr kubed-ext-cb-threshold))
        (puthash ctx n kubed-ext--cb-net-failures)
        (if (>= n thr)
            (kubed-ext--cb-trip-network ctx err-msg)
          (message "Kubed [%s]: connection check failed (%d/%d)"
                   ctx n thr)))))))

;; ── Network recovery probe ────────────────────────────────────────

(defun kubed-ext--cb-schedule-probe (context delay)
  "Schedule a connectivity probe for CONTEXT after DELAY seconds."
  (let ((ctx (or context "")))
    (let ((old (gethash ctx kubed-ext--cb-probe-timers)))
      (when (timerp old) (cancel-timer old)))
    (puthash ctx
             (run-with-timer delay nil #'kubed-ext--cb-run-probe ctx)
             kubed-ext--cb-probe-timers)))

(defun kubed-ext--cb-run-probe (context)
  "Run a `kubectl api-versions' connectivity probe for CONTEXT.

exit 0         → success; close circuit.
auth error     → network is up but credentials expired; switch to
                 `auth' state and stop scheduling further probes.
network error  → still unreachable; double back-off and reschedule.
other error    → server responded; close circuit."
  (let ((ctx (or context "")))
    (when (kubed-ext--cb-open-p ctx)
      (let ((probe-buf (generate-new-buffer " *kubed-ext-cb-probe*")))
        (make-process
         :name     "kubed-ext-cb-probe"
         :buffer   probe-buf
         :noquery  t
         :command  (list kubed-kubectl-program
                         "api-versions"
                         "--context"         ctx
                         "--request-timeout" "5s")
         :sentinel
         (lambda (proc _event)
           (when (memq (process-status proc) '(exit signal))
             (let* ((code   (process-exit-status proc))
                    (output (if (buffer-live-p probe-buf)
                                (prog1 (with-current-buffer probe-buf
                                         (string-trim (buffer-string)))
                                  (kill-buffer probe-buf))
                              "")))
               (remhash ctx kubed-ext--cb-probe-timers)
               (cond
                ;; Network and auth are both working.
                ((zerop code)
                 (kubed-ext--cb-on-success ctx))

                ;; Network is up but credentials have expired.
                ;; Switch to `auth' state — no more probes until manual reset.
                ((kubed-ext--auth-error-p output)
                 (puthash ctx 'auth kubed-ext--cb-open)
                 (kubed-ext--cb-set-header
                  ctx (concat "🔑 Network restored but credentials expired"
                              "  [Re-auth, then press `g' or `Z']"))
                 (kubed-ext--cb-refresh-mode-lines ctx)
                 (message
                  "Kubed 🔑 [%s]: network OK but credentials expired — re-auth then press `g'."
                  ctx))

                ;; Cluster still unreachable — exponential back-off.
                ((kubed-ext--network-error-p output)
                 (let* ((cur  (kubed-ext--cb-backoff-for ctx))
                        (next (min kubed-ext-cb-max-backoff
                                   (round (* cur kubed-ext-cb-backoff-factor)))))
                   (puthash ctx next kubed-ext--cb-backoff)
                   (message "Kubed ⚡ [%s]: still unreachable — probing in %ds."
                            ctx next)
                   (kubed-ext--cb-refresh-mode-lines ctx)
                   (kubed-ext--cb-schedule-probe ctx next)))

                ;; Unexpected non-zero exit but server responded — close circuit.
                (t
                 (kubed-ext--cb-on-success ctx)))))))))))

;; ── Wiring: feed kubed-update outcomes into the state machine ────

(defun kubed-ext--cb-update-advice (orig-fn type context &optional namespace)
  "Around-advice on `kubed-update': wire circuit-breaker accounting.
ORIG-FN is the original function; TYPE, CONTEXT, and NAMESPACE are
forwarded unchanged."
  (funcall orig-fn type context namespace)
  (when-let ((proc (alist-get 'process
                              (kubed--alist type context namespace))))
    (when (process-live-p proc)
      (let* ((ctx        (or context ""))
             (rtype      type)
             (inner-sent (process-sentinel proc)))
        (set-process-sentinel
         proc
         (lambda (p status)
           (funcall inner-sent p status)
           (cond
            ((string= status "finished\n")
             (kubed-ext--cb-on-success ctx))
            ((string= status "exited abnormally with code 1\n")
             (kubed-ext--cb-on-failure
              ctx (or (kubed-ext--cb-read-stderr rtype)
                      "kubectl exited with code 1"))))))))))

(advice-add 'kubed-update :around #'kubed-ext--cb-update-advice)

;; ── `g' clears the circuit before the manual refresh fires ───────

(defun kubed-ext--cb-before-manual-refresh (&rest _)
  "Before-advice on `kubed-list-update': clear an open circuit on `g'.
This gives the cluster an immediate retry without waiting for a probe."
  (when (and (derived-mode-p 'kubed-list-mode)
             (kubed-ext--cb-open-p kubed-list-context))
    (kubed-ext--cb-reset-state kubed-list-context)
    (kubed-ext--cb-clear-headers kubed-list-context)
    (kubed-ext--cb-refresh-mode-lines kubed-list-context)))

(advice-add 'kubed-list-update :before #'kubed-ext--cb-before-manual-refresh)

;; ── Manual reset command ──────────────────────────────────────────

(defun kubed-ext-circuit-breaker-reset (&optional context)
  "Reset the circuit breaker for CONTEXT and resume auto-refresh.
Call this after reconnecting to VPN or refreshing expired credentials.
With a prefix argument, prompt for CONTEXT; otherwise use the context
of the current buffer or the configured default."
  (interactive
   (list (if current-prefix-arg
             (kubed-read-context "Reset circuit for context"
                                 (kubed-local-context))
           (kubed-local-context))))
  (let ((ctx (or context (kubed-local-context))))
    (kubed-ext--cb-reset-state ctx)
    (kubed-ext--cb-clear-headers ctx)
    (kubed-ext--cb-refresh-mode-lines ctx)
    (kubed-ext--cb-in-context-bufs
     ctx (lambda ()
           (condition-case nil (kubed-list-update t)
             (error nil))))
    (message "Kubed [%s]: circuit reset — auto-refresh resumed." ctx)))

(keymap-set kubed-list-mode-map "Z" #'kubed-ext-circuit-breaker-reset)
(keymap-set kubed-prefix-map    "Z" #'kubed-ext-circuit-breaker-reset)

;; ── Auto-refresh tick ─────────────────────────────────────────────

(defun kubed-ext--auto-refresh-tick ()
  "Refresh visible kubed list buffers; skip any with an open circuit.
Reschedules itself after each tick with a fresh random delay."
  (unless (active-minibuffer-window)
    (let ((refreshed 0)
          (skipped   0))
      (dolist (win (window-list))
        (let ((buf (window-buffer win)))
          (when (buffer-live-p buf)
            (with-current-buffer buf
              (when (derived-mode-p 'kubed-list-mode)
                (cond
                 ;; Circuit is open — skip and count.
                 ((kubed-ext--cb-open-p kubed-list-context)
                  (cl-incf skipped))
                 ;; An update is already in progress — skip silently.
                 ((process-live-p
                   (alist-get 'process
                              (kubed--alist kubed-list-type
                                            kubed-list-context
                                            kubed-list-namespace))))
                 ;; Normal case — refresh.
                 (t
                  (condition-case nil
                      (progn (kubed-list-update t) (cl-incf refreshed))
                    (error nil)))))))))
      (cond
       ((and (> refreshed 0) (> skipped 0))
        (message "Kubed: refreshed %d buffer%s; %d paused (circuit open)."
                 refreshed (if (= refreshed 1) "" "s") skipped))
       ((> refreshed 0)
        (message "Kubed: auto-refreshed %d buffer%s."
                 refreshed (if (= refreshed 1) "" "s"))))))
  (kubed-ext--auto-refresh-schedule))

;; ── Mode-line ─────────────────────────────────────────────────────

(setq kubed-list-mode-line-format
      '(:eval
        (let* ((ctx      kubed-list-context)
               (reason   (and ctx (kubed-ext--cb-open-p ctx)))
               (updating (process-live-p
                          (alist-get 'process
                                     (kubed--alist kubed-list-type ctx
                                                   kubed-list-namespace)))))
          (cond
           (updating
            (propertize " [...]" 'help-echo "Updating…"))

           ((eq reason 'network)
            (let ((secs (round (kubed-ext--cb-backoff-for ctx))))
              (propertize (format " ⚡~%ds" secs)
                          'face      'error
                          'help-echo (format (concat "Network unreachable for `%s'.\n"
                                                     "Auto-refresh paused; probing every ~%ds.\n"
                                                     "Press `g' or `Z' to retry immediately.")
                                             ctx secs))))

           ((eq reason 'auth)
            (propertize " 🔑 re-auth"
                        'face      'warning
                        'help-echo (format (concat "Credentials expired for `%s'.\n"
                                                   "Re-authenticate, then press `g' or `Z'.")
                                           ctx)))

           (t
            (concat
             (when kubed-list-filter
               (propertize
                (format " [%s]" (mapconcat #'prin1-to-string
                                           kubed-list-filter " "))
                'help-echo "Active filter"))
             (when (bound-and-true-p kubed-ext-list-label-selector)
               (propertize (format " {%s}" kubed-ext-list-label-selector)
                           'help-echo "Label selector"
                           'face      'warning))
             (when (and (bound-and-true-p kubed-ext-resource-filter)
                        (not (string-empty-p kubed-ext-resource-filter)))
               (propertize (format " /%s/" kubed-ext-resource-filter)
                           'help-echo "Substring filter"
                           'face      'italic))))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 22.  Enhanced Log Filtering and Parallel Streaming
;;; ═══════════════════════════════════════════════════════════════

(defgroup kubed-ext-logs nil
  "Kubernetes log filtering and parallel streaming."
  :group 'kubed-ext)

(defcustom kubed-ext-log-error-pattern
  "exception\\|error\\|critical\\|failure\\|fatal\\|panic"
  "Case-insensitive regex for filtering error log lines."
  :type 'string
  :group 'kubed-ext-logs)

(defcustom kubed-ext-log-parallel-limit 5
  "Maximum simultaneous kubectl log processes for parallel streaming."
  :type 'natnum
  :group 'kubed-ext-logs)

(defcustom kubed-ext-log-default-since "1h"
  "Default --since duration for filtered log commands (e.g. \"30m\", \"24h\")."
  :type 'string
  :group 'kubed-ext-logs)

(defcustom kubed-ext-log-use-external-filter 'auto
  "Controls whether rg/grep or Emacs-native filtering is used.

`auto'  — prefer rg, then grep, then Emacs-native.
`emacs' — always Emacs-native (portable, no colour).
`shell' — always shell pipeline; error if neither rg nor grep found."
  :type '(choice (const :tag "Auto-detect (rg > grep > Emacs)" auto)
                 (const :tag "Emacs-native only"               emacs)
                 (const :tag "Shell pipeline only"             shell))
  :group 'kubed-ext-logs)

(defvar kubed-ext-log-filter-history nil
  "Minibuffer history for log-filter pattern prompts.")

;;; ─── Tool selection ───────────────────────────────────────────

(defun kubed-ext--log-shell-filter-cmd (pattern)
  "Return a shell command string filtering stdin for PATTERN, or nil."
  (cond
   ((executable-find "rg")
    (concat "rg --color=always --line-buffered -i "
            (shell-quote-argument pattern)))
   ((executable-find "grep")
    (concat "grep --line-buffered -Ei "
            (shell-quote-argument pattern)))
   (t nil)))

(defun kubed-ext--log-use-shell-p (pattern)
  "Return non-nil when a shell pipeline should be used to filter PATTERN."
  (when (and (stringp pattern) (not (string-empty-p pattern)))
    (pcase kubed-ext-log-use-external-filter
      ('emacs nil)
      ('shell
       (unless (kubed-ext--log-shell-filter-cmd pattern)
         (user-error
          "No rg or grep found; set `kubed-ext-log-use-external-filter' to `emacs'"))
       t)
      (_ (not (null (kubed-ext--log-shell-filter-cmd pattern)))))))

;;; ─── Filter prompt ────────────────────────────────────────────

(defun kubed-ext--read-log-filter (prompt &optional default)
  "Prompt with PROMPT for a log-filter regex, defaulting to DEFAULT."
  (let* ((opt-err    (concat "(errors)  " kubed-ext-log-error-pattern))
         (opt-none   "(none)    show all lines")
         (opt-custom "(custom)  enter pattern…")
         (choice     (completing-read
                      (format-prompt prompt (or default ""))
                      (list opt-err opt-none opt-custom)
                      nil nil nil
                      'kubed-ext-log-filter-history
                      (or default ""))))
    (cond
     ((string= choice opt-err)    kubed-ext-log-error-pattern)
     ((string= choice opt-none)   "")
     ((string= choice opt-custom)
      (read-string (format-prompt "Pattern" default)
                   default 'kubed-ext-log-filter-history))
     (t choice))))

;;; ─── Emacs-native line filter ─────────────────────────────────

(defun kubed-ext--make-line-filter (pattern)
  "Return a `process-filter' closure keeping only lines matching PATTERN.
Case-insensitive.  Partial lines are buffered across output chunks."
  (let ((acc ""))
    (lambda (proc string)
      (when (buffer-live-p (process-buffer proc))
        (let* ((combined        (concat acc string))
               (lines           (split-string combined "\n"))
               (complete        (butlast lines))
               (tail            (car (last lines)))
               (case-fold-search t))
          (setq acc (or tail ""))
          (with-current-buffer (process-buffer proc)
            (let ((inhibit-read-only t))
              (goto-char (point-max))
              (dolist (line complete)
                (when (or (null pattern)
                          (string-empty-p pattern)
                          (string-match-p pattern line))
                  (insert line "\n"))))))))))

;;; ─── ANSI-color filter ────────────────────────────────────────

(defun kubed-ext--log-ansi-filter (proc string)
  "Process filter inserting STRING into PROC's buffer with ANSI decoding."
  (require 'ansi-color)
  (when (buffer-live-p (process-buffer proc))
    (with-current-buffer (process-buffer proc)
      (let ((inhibit-read-only t)
            (beg               (point-max)))
        (goto-char beg)
        (insert string)
        (ansi-color-apply-on-region beg (point-max))))))

;;; ─── Core launch helper ───────────────────────────────────────

(defun kubed-ext--launch-log-process (buf kubectl-args filter-pattern)
  "Start a kubectl process writing to BUF, optionally filtered.

KUBECTL-ARGS must contain only strings — callers are responsible for
omitting nil values via `append' + `when' guards.
FILTER-PATTERN is a regex string, or nil/empty for no filtering."
  (let* ((pattern   (and (stringp filter-pattern)
                         (not (string-empty-p filter-pattern))
                         filter-pattern))
         (use-shell (kubed-ext--log-use-shell-p pattern)))
    (if use-shell
        (let* ((filt-cmd (kubed-ext--log-shell-filter-cmd pattern))
               (kube-cmd (mapconcat #'shell-quote-argument
                                    (cons kubed-kubectl-program kubectl-args)
                                    " "))
               (full-cmd (format "%s | %s" kube-cmd filt-cmd))
               (proc     (start-process-shell-command
                          "*kubed-logs-filtered*" buf full-cmd)))
          (when (executable-find "rg")
            (set-process-filter proc #'kubed-ext--log-ansi-filter))
          (set-process-sentinel
           proc
           (lambda (_p status)
             (when (buffer-live-p buf)
               (with-current-buffer buf
                 (let ((inhibit-read-only t))
                   (cond
                    ((string= status "finished\n") nil)
                    ((string= status "exited abnormally with code 1\n")
                     (goto-char (point-max))
                     (insert (propertize "(no matching lines found)\n"
                                         'face 'shadow)))
                    (t
                     (goto-char (point-max))
                     (insert (propertize
                              (format "[process: %s]\n" (string-trim status))
                              'face 'error)))))))))
          proc)
      (let ((proc (apply #'start-process "*kubed-logs*" buf
                         kubed-kubectl-program kubectl-args)))
        (when pattern
          (set-process-filter proc (kubed-ext--make-line-filter pattern)))
        proc))))

;;; ─── Pod: error logs ──────────────────────────────────────────

(defun kubed-ext-pods-logs-errors (click)
  "Show error-filtered logs for the pod at CLICK position."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((ctx  kubed-list-context)
             (ns   kubed-list-namespace)
             (args (append
                    (list "logs" pod
                          "--all-containers"
                          "--prefix"
                          "--since" kubed-ext-log-default-since)
                    (when ctx (list "--context" ctx))
                    (when ns  (list "-n" ns))))
             (buf  (generate-new-buffer
                    (format "*kubed-logs-errors pods/%s@%s[%s]*"
                            pod (or ns "default") (or ctx "current")))))
        (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
        (kubed-ext--launch-log-process buf args kubed-ext-log-error-pattern)
        (display-buffer buf))
    (user-error "No Kubernetes pod at point")))

;;; ─── Pod: custom-filtered logs ────────────────────────────────

(defun kubed-ext-pods-logs-custom-filter (click)
  "Show filtered logs for the pod at CLICK, prompting for options."
  (interactive (list last-nonmenu-event) kubed-pods-mode)
  (if-let ((pod (tabulated-list-get-id (mouse-set-point click))))
      (let* ((ctx       kubed-list-context)
             (ns        kubed-list-namespace)
             (filt      (kubed-ext--read-log-filter
                         "Filter pattern" kubed-ext-log-error-pattern))
             (since     (read-string (format-prompt "Since"
                                                    kubed-ext-log-default-since)
                                     nil nil kubed-ext-log-default-since))
             (follow    (y-or-n-p "Follow (stream) logs? "))
             ;; Ask all-containers first; only prompt for a specific
             ;; container when the user says no.
             (all-con   (y-or-n-p "All containers? "))
             (container (unless all-con
                          (kubed-read-container pod "Container" t ctx ns)))
             (args      (append
                         (list "logs" pod "--prefix" "--since" since)
                         (when ctx     (list "--context" ctx))
                         (when ns      (list "-n" ns))
                         (when follow  '("--follow"))
                         (if all-con
                             '("--all-containers")
                           (list "-c" container))))
             (buf       (generate-new-buffer
                         (format "*kubed-logs-filtered pods/%s@%s[%s]*"
                                 pod (or ns "default") (or ctx "current")))))
        (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
        (kubed-ext--launch-log-process buf args filt)
        (display-buffer buf))
    (user-error "No Kubernetes pod at point")))

;;; ─── Standalone: error logs for any resource ──────────────────

;;;###autoload
(defun kubed-ext-logs-errors (&optional context namespace)
  "Show error-filtered logs for a Kubernetes resource (CONTEXT and NAMESPACE)."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list c n)))
  (let* ((ctx    (or context (kubed-local-context)))
         (ns     (or namespace (kubed--namespace ctx)))
         (type   (kubed-read-resource-type "Resource type" nil ctx))
         (res    (kubed-read-resource-name type "Error logs for" nil nil ctx ns))
         (since  (read-string (format-prompt "Since" kubed-ext-log-default-since)
                              nil nil kubed-ext-log-default-since))
         (follow (y-or-n-p "Follow (stream) logs? "))
         (args   (append
                  (list "logs" (concat type "/" res)
                        "--prefix"
                        "--since" since)
                  (when ctx    (list "--context" ctx))
                  (when ns     (list "-n" ns))
                  (when follow '("--follow"))))
         (buf    (generate-new-buffer
                  (format "*kubed-logs-errors %s/%s@%s[%s]*"
                          type res (or ns "default") ctx))))
    (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
    (kubed-ext--launch-log-process buf args kubed-ext-log-error-pattern)
    (display-buffer buf)))

;;; ─── Standalone: logs by label with filtering ─────────────────

;;;###autoload
(defun kubed-ext-logs-by-label-filtered (&optional context namespace)
  "Stream filtered logs for pods matching a label selector (CONTEXT and NAMESPACE)."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list c n)))
  (let* ((ctx      (or context (kubed-local-context)))
         (ns       (or namespace (kubed--namespace ctx)))
         (labels   (kubed-ext-discover-labels "pods" ctx ns))
         (selector (completing-read "Label selector: " labels nil nil
                                    nil 'kubed-ext-label-selector-history))
         (filt     (kubed-ext--read-log-filter
                    "Filter pattern" kubed-ext-log-error-pattern))
         (since    (read-string (format-prompt "Since" kubed-ext-log-default-since)
                                nil nil kubed-ext-log-default-since))
         (follow   (y-or-n-p "Follow (stream) logs? "))
         (args     (append
                    (list "logs" "-l" selector
                          "--all-containers=true"
                          "--prefix"
                          "--since" since)
                    (when ctx    (list "--context" ctx))
                    (when ns     (list "-n" ns))
                    (when follow '("--follow"))))
         (buf      (generate-new-buffer
                    (format "*kubed-logs-filtered -l %s@%s[%s]*"
                            selector (or ns "default") ctx))))
    (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
    (kubed-ext--launch-log-process buf args filt)
    (display-buffer buf)))

;;; ─── Pod discovery by selector ────────────────────────────────

(defun kubed-ext--pods-by-selector (selector context namespace)
  "Return sorted pod names matching SELECTOR in CONTEXT/NAMESPACE, or nil."
  (condition-case err
      (let ((output
             (with-temp-buffer
               (apply #'call-process
                      kubed-kubectl-program nil '(t nil) nil
                      (append
                       (list "get" "pods"
                             "-l"           selector
                             "--no-headers"
                             "-o"           "custom-columns=NAME:.metadata.name")
                       (when context   (list "--context" context))
                       (when namespace (list "-n"        namespace))))
               (buffer-string))))
        (sort (seq-filter (lambda (s) (not (string-empty-p (string-trim s))))
                          (split-string output "\n" t))
              #'string<))
    (error
     (message "kubed-ext: cannot list pods: %s" (error-message-string err))
     nil)))

;;; ─── Post-hoc filter on existing log buffer ───────────────────

(defun kubed-ext-filter-log-buffer (pattern)
  "Show lines from the current buffer matching PATTERN using `occur'."
  (interactive
   (list (kubed-ext--read-log-filter
          "Filter log buffer" kubed-ext-log-error-pattern)))
  (when (or (null pattern) (string-empty-p (string-trim pattern)))
    (user-error "No filter pattern specified"))
  (let ((case-fold-search t))
    (occur pattern)))

;;; ─── Keybindings § 22 ─────────────────────────────────────────

(keymap-set kubed-pods-mode-map "E"   #'kubed-ext-pods-logs-errors)
(keymap-set kubed-pods-mode-map "H"   #'kubed-ext-pods-logs-custom-filter)
(keymap-set kubed-prefix-map    "e"   #'kubed-ext-logs-errors)
(keymap-set kubed-prefix-map    "l f" #'kubed-ext-logs-by-label-filtered)
(keymap-set kubed-prefix-map    "l p" #'kubed-ext-logs-parallel-by-label)
(keymap-set kubed-prefix-map    "l o" #'kubed-ext-filter-log-buffer)

;;; ─── Extend pod transient suffixes ────────────────────────────

(let ((entry (assoc "pods" kubed-ext-extra-transient-suffixes)))
  (when entry
    (setcdr entry
            (apply #'vector
                   (append
                    (append (cdr entry) nil)
                    (list
                     '("le" "Error logs"
                       kubed-ext-pods-logs-errors)
                     '("lf" "Filter logs (custom)"
                       kubed-ext-pods-logs-custom-filter)
                     '("lp" "Parallel logs by label"
                       kubed-ext-logs-parallel-by-label)))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 22b.  Log Filtering for Deployments and Generic Workloads
;;; ═══════════════════════════════════════════════════════════════

;;; ─── matchExpression → selector segment ───────────────────────

(defun kubed-ext--matchexpr-to-selector (expr)
  "Convert a Kubernetes matchExpression EXPR alist to a selector string.
Returns nil for unsupported operators."
  (let ((key  (alist-get 'key      expr))
        (op   (alist-get 'operator expr))
        (vals (alist-get 'values   expr)))
    (and (stringp key)
         (stringp op)
         (pcase op
           ("Exists"       key)
           ("DoesNotExist" (concat "!" key))
           ("In"
            (when (and (listp vals) vals)
              (format "%s in (%s)" key (mapconcat #'identity vals ","))))
           ("NotIn"
            (when (and (listp vals) vals)
              (format "%s notin (%s)" key (mapconcat #'identity vals ","))))
           (_ nil)))))

;;; ─── Workload → pod label selector ───────────────────────────

(defun kubed-ext--workload-pod-selector (type name context namespace)
  "Return a kubectl --selector string for pods.
owned by TYPE/NAME in CONTEXT/NAMESPACE.
Returns nil when the selector cannot be determined."
  (condition-case err
      (let ((json-str
             (with-temp-buffer
               (when (zerop
                      (apply #'call-process
                             kubed-kubectl-program nil '(t nil) nil
                             (append
                              (list "get" type name "-o" "json")
                              (when context   (list "--context" context))
                              (when namespace (list "-n"        namespace)))))
                 (string-trim (buffer-string))))))
        (when (and json-str
                   (not (string-empty-p json-str))
                   (string-prefix-p "{" json-str))
          (let* ((obj      (json-parse-string json-str
                                              :object-type 'alist
                                              :array-type  'list))
                 (spec     (and (listp obj)  (alist-get 'spec     obj)))
                 (selector (and (listp spec) (alist-get 'selector spec)))
                 (result
                  (cond
                   ;; ReplicationControllers use a flat label map.
                   ((member type '("replicationcontroller"
                                   "replicationcontrollers"))
                    (when (and selector (listp selector))
                      (mapconcat (lambda (kv)
                                   (format "%s=%s" (car kv) (cdr kv)))
                                 selector ",")))
                   ;; All other workload types use matchLabels/matchExpressions.
                   ((and selector
                         (listp selector)
                         (not (eq selector :null)))
                    (let* ((ml     (alist-get 'matchLabels      selector))
                           (me     (alist-get 'matchExpressions selector))
                           (lparts (when (and ml (listp ml) (not (eq ml :null)))
                                     (mapcar (lambda (kv)
                                               (format "%s=%s" (car kv) (cdr kv)))
                                             ml)))
                           (eparts (when (and me (listp me) (not (eq me :null)))
                                     (delq nil (mapcar
                                                #'kubed-ext--matchexpr-to-selector
                                                me))))
                           (all    (append lparts eparts)))
                      (when all (mapconcat #'identity all ","))))
                   (t nil))))
            ;; Guard against empty string — that would match all pods.
            (and (stringp result) (not (string-empty-p result)) result))))
    (error
     (message "kubed-ext: selector for %s/%s: %s"
              type name (error-message-string err))
     nil)))

;;; ─── Shared parallel log runner ───────────────────────────────

(defun kubed-ext--parallel-logs-run
    (pods context namespace filter since follow all-containers buf)
  "Stream logs from PODS concurrently into BUF.

At most `kubed-ext-log-parallel-limit' kubectl processes run at once;
new ones start as slots free.  Per-process Emacs-native line filters
are always used so N independent streams do not corrupt shared output.

PODS           — list of pod-name strings.
CONTEXT        — kubectl context, or nil for current context.
NAMESPACE      — Kubernetes namespace, or nil for current namespace.
FILTER         — case-insensitive regex; nil or empty passes all lines.
SINCE          — kubectl --since value (e.g. \"1h\", \"30m\").
FOLLOW         — non-nil appends --follow flag.
ALL-CONTAINERS — non-nil appends --all-containers flag.
BUF            — pre-existing output buffer."
  (if (null pods)
      (with-current-buffer buf
        (let ((inhibit-read-only t))
          (insert "No pods found for the given selector.\n")))
    (message "Streaming from %d pod%s (<=%d parallel)..."
             (length pods)
             (if (= (length pods) 1) "" "s")
             kubed-ext-log-parallel-limit)
    (let ((remaining (copy-sequence pods))
          (active    0)
          (limit     kubed-ext-log-parallel-limit))
      (cl-labels
          ((maybe-start ()
                        (while (and remaining (< active limit))
                          (let ((pod (pop remaining)))
                            (condition-case start-err
                                (let* ((args (append
                                              (list "logs" pod
                                                    "--prefix"
                                                    "--since" since)
                                              (when context        (list "--context" context))
                                              (when namespace      (list "-n" namespace))
                                              (when follow         '("--follow"))
                                              (when all-containers '("--all-containers"))))
                                       (proc (apply #'start-process
                                                    (format "*kubed-plog-%s*" pod)
                                                    buf
                                                    kubed-kubectl-program args)))
                                  (when (and (stringp filter) (not (string-empty-p filter)))
                                    (set-process-filter proc
                                                        (kubed-ext--make-line-filter filter)))
                                  (cl-incf active)
                                  (set-process-sentinel proc
                                                        (lambda (_p _status)
                                                          (cl-decf active)
                                                          (maybe-start))))
                              (error
                               (message "kubed-ext: log process for %s: %s"
                                        pod (error-message-string start-err))))))))
        (maybe-start)))))

;;; ─── Standalone: parallel by label ───────────────────────────

;;;###autoload
(defun kubed-ext-logs-parallel-by-label (&optional context namespace)
  "Stream logs from pods matching a label selector, up to N in parallel.

CONTEXT and NAMESPACE are optional; defaults to current context/namespace."
  (declare (advertised-calling-convention (context namespace) "1.0"))
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list c n)))
  (let* ((ctx      (or context (kubed-local-context)))
         (ns       (or namespace (kubed--namespace ctx)))
         (labels   (kubed-ext-discover-labels "pods" ctx ns))
         (selector (completing-read "Label selector: " labels nil nil
                                    nil 'kubed-ext-label-selector-history))
         (filt     (kubed-ext--read-log-filter
                    "Filter pattern" kubed-ext-log-error-pattern))
         (since    (read-string (format-prompt "Since" kubed-ext-log-default-since)
                                nil nil kubed-ext-log-default-since))
         (follow   (y-or-n-p "Follow (stream) logs? "))
         (all-con  (y-or-n-p "All containers? "))
         (pods     (kubed-ext--pods-by-selector selector ctx ns))
         (buf      (generate-new-buffer
                    (format "*kubed-parallel-logs -l %s@%s[%s]*"
                            selector (or ns "default") ctx))))
    (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
    (kubed-ext--parallel-logs-run pods ctx ns filt since follow all-con buf)
    (display-buffer buf)))

;;; ─── Deployment: error logs ───────────────────────────────────

(defun kubed-ext-deployments-logs-errors (click)
  "Show error-filtered logs for the deployment at CLICK position."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (let* ((ctx  kubed-list-context)
             (ns   kubed-list-namespace)
             (args (append
                    (list "logs" (concat "deployment/" dep)
                          "--all-containers"
                          "--prefix"
                          "--since" kubed-ext-log-default-since)
                    (when ctx (list "--context" ctx))
                    (when ns  (list "-n" ns))))
             (buf  (generate-new-buffer
                    (format "*kubed-logs-errors deployments/%s@%s[%s]*"
                            dep (or ns "default") (or ctx "current")))))
        (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
        (kubed-ext--launch-log-process buf args kubed-ext-log-error-pattern)
        (display-buffer buf))
    (user-error "No Kubernetes deployment at point")))

;;; ─── Deployment: custom-filtered logs ────────────────────────

(defun kubed-ext-deployments-logs-custom-filter (click)
  "Show filtered logs for the deployment at CLICK, prompting for options."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (let* ((ctx    kubed-list-context)
             (ns     kubed-list-namespace)
             (filt   (kubed-ext--read-log-filter
                      "Filter pattern" kubed-ext-log-error-pattern))
             (since  (read-string (format-prompt "Since" kubed-ext-log-default-since)
                                  nil nil kubed-ext-log-default-since))
             (follow (y-or-n-p "Follow (stream) logs? "))
             (args   (append
                      (list "logs" (concat "deployment/" dep)
                            "--all-containers"
                            "--prefix"
                            "--since" since)
                      (when ctx    (list "--context" ctx))
                      (when ns     (list "-n" ns))
                      (when follow '("--follow"))))
             (buf    (generate-new-buffer
                      (format "*kubed-logs-filtered deployments/%s@%s[%s]*"
                              dep (or ns "default") (or ctx "current")))))
        (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
        (kubed-ext--launch-log-process buf args filt)
        (display-buffer buf))
    (user-error "No Kubernetes deployment at point")))

;;; ─── Deployment: parallel logs ────────────────────────────────

(defun kubed-ext-deployments-logs-parallel (click)
  "Stream filtered logs from ALL pods of the deployment at CLICK."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (let* ((ctx kubed-list-context)
             (ns  kubed-list-namespace)
             (sel (kubed-ext--workload-pod-selector "deployments" dep ctx ns)))
        (unless sel
          (user-error "Cannot determine pod selector for deployment `%s'" dep))
        (let* ((filt   (kubed-ext--read-log-filter
                        "Filter pattern" kubed-ext-log-error-pattern))
               (since  (read-string (format-prompt "Since" kubed-ext-log-default-since)
                                    nil nil kubed-ext-log-default-since))
               (follow (y-or-n-p "Follow (stream) logs? "))
               (all-c  (y-or-n-p "All containers per pod? "))
               (pods   (kubed-ext--pods-by-selector sel ctx ns))
               (buf    (generate-new-buffer
                        (format "*kubed-parallel-logs deployments/%s@%s[%s]*"
                                dep (or ns "default") (or ctx "current")))))
          (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
          (kubed-ext--parallel-logs-run pods ctx ns filt since follow all-c buf)
          (display-buffer buf)))
    (user-error "No Kubernetes deployment at point")))

;;; ─── Standalone: parallel for any workload ────────────────────

;;;###autoload
(defun kubed-ext-logs-parallel-for-workload (&optional context namespace)
  "Stream parallel filtered logs from pods of any workload resource.

CONTEXT and NAMESPACE are optional; defaults to current context/namespace."
  (declare (advertised-calling-convention (context namespace) "1.0"))
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16))
                 (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg)))
     (list c n)))
  (let* ((ctx  (or context (kubed-local-context)))
         (ns   (or namespace (kubed--namespace ctx)))
         (type (completing-read "Workload type: "
                                '("deployments" "statefulsets" "daemonsets"
                                  "replicasets" "jobs")
                                nil t nil nil "deployments"))
         (name (kubed-read-resource-name type "Parallel logs for" nil nil ctx ns))
         (sel  (kubed-ext--workload-pod-selector type name ctx ns)))
    (unless sel
      (user-error "Cannot determine pod selector for %s/%s" type name))
    (let* ((filt   (kubed-ext--read-log-filter
                    "Filter pattern" kubed-ext-log-error-pattern))
           (since  (read-string (format-prompt "Since" kubed-ext-log-default-since)
                                nil nil kubed-ext-log-default-since))
           (follow (y-or-n-p "Follow (stream) logs? "))
           (all-c  (y-or-n-p "All containers per pod? "))
           (pods   (kubed-ext--pods-by-selector sel ctx ns))
           (buf    (generate-new-buffer
                    (format "*kubed-parallel-logs %s/%s@%s[%s]*"
                            type name (or ns "default") ctx))))
      (with-current-buffer buf (run-hooks 'kubed-logs-setup-hook))
      (kubed-ext--parallel-logs-run pods ctx ns filt since follow all-c buf)
      (display-buffer buf))))

;;; ─── Keybindings § 22b ────────────────────────────────────────

(keymap-set kubed-deployments-mode-map "E" #'kubed-ext-deployments-logs-errors)
(keymap-set kubed-deployments-mode-map "H" #'kubed-ext-deployments-logs-custom-filter)
(keymap-set kubed-prefix-map           "w" #'kubed-ext-logs-parallel-for-workload)

;;; ─── Extend deployment transient suffixes ─────────────────────

(let ((entry (assoc "deployments" kubed-ext-extra-transient-suffixes)))
  (when entry
    (setcdr entry
            (apply #'vector
                   (append
                    (append (cdr entry) nil)
                    (list
                     '("le" "Error logs (stream)"
                       kubed-ext-deployments-logs-errors)
                     '("lf" "Filter logs (custom)"
                       kubed-ext-deployments-logs-custom-filter)
                     '("lp" "Parallel logs (all pods)"
                       kubed-ext-deployments-logs-parallel)))))))

;;;; ═══════════════════════════════════════════════════════════════
;;; § 23.  Setup
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-setup ()
  "Initialize kubed-ext functionality."
  (message "Kubed-ext: setup complete."))

(provide 'kubed-ext)
;;; kubed-ext.el ends here
