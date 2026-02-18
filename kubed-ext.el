;;; kubed-ext.el --- Extensions for Kubed with Async Metrics -*- lexical-binding: t; -*-

;; Author: Chetan Koneru
;; Version: 0.1.0
;; URL: https://github.com/CsBigDataHub/kubed-ext
;; Keywords: tools, kubernetes

;;; Commentary:
;; A comprehensive extension suite for Kubed, transforming Emacs into a
;; production-grade Kubernetes dashboard.
;;
;; Features:
;; - True Async Metrics (Top) using futur.el (Non-blocking)
;; - Advanced Port Forwarding with history and management buffer
;; - Strimzi Kafka support
;; - CRD Auto-discovery
;; - Server-side Label Selectors
;; - Optimized Terminal integration (vterm/eat/eshell)

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
;;; § 2.  Port-Forward with Auto-Complete (k9s Shift-F style)
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
                    (_ (concat (replace-regexp-in-string "s'" "" type)
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
  "Parse port-forward descriptor DESC into list."
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
   ((or (null s) (string-empty-p s) (string= s "<none>")) 0)
   ((string-suffix-p "m" s)  (string-to-number s))
   ((string-suffix-p "n" s)  (/ (string-to-number s) 1000000.0))
   ((string-suffix-p "u" s)  (/ (string-to-number s) 1000.0))
   (t (* 1000.0 (string-to-number s)))))

(defun kubed-ext-parse-mem (s)
  "Parse Kubernetes memory quantity S to MiB (number)."
  (cond
   ((or (null s) (string-empty-p s) (string= s "")) 0)
   ((string-suffix-p "Ti" s) (* 1048576.0 (string-to-number s)))
   ((string-suffix-p "Gi" s) (* 1024.0    (string-to-number s)))
   ((string-suffix-p "Mi" s) (string-to-number s))
   ((string-suffix-p "Ki" s) (/ (string-to-number s) 1024.0))
   (t (/ (string-to-number s) 1048576.0))))

(defun kubed-ext-sum-cpu (s)
  "Sum comma-separated CPU values in S."
  (if (or (null s) (string-empty-p s)) 0
    (apply #'+ (mapcar #'kubed-ext-parse-cpu
                       (split-string s "," t "[ \t]")))))

(defun kubed-ext-sum-mem (s)
  "Sum comma-separated memory values in S."
  (if (or (null s) (string-empty-p s)) 0
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
  "Return a sort predicate comparing column COL-IDX by kubed-sort-value."
  (lambda (a b)
    (let ((va (get-text-property 0 'kubed-sort-value (aref (cadr a) col-idx)))
          (vb (get-text-property 0 'kubed-sort-value (aref (cadr b) col-idx))))
      (< (or va 0) (or vb 0)))))

;; ── Kubernetes Timestamp ──

(defun kubed-ext--parse-k8s-timestamp (s)
  "Parse Kubernetes ISO 8601 timestamp S to an Emacs time value.
Returns nil if S cannot be parsed."
  (condition-case nil
      (when (string-match
             (concat "$[0-9]\\{4\\}$-$[0-9]\\{2\\}$-$[0-9]\\{2\\}$"
                     "T$[0-9]\\{2\\}$:$[0-9]\\{2\\}$:$[0-9]\\{2\\}$Z?")
             s)
        (encode-time
         (string-to-number (match-string 6 s))
         (string-to-number (match-string 5 s))
         (string-to-number (match-string 4 s))
         (string-to-number (match-string 3 s))
         (string-to-number (match-string 2 s))
         (string-to-number (match-string 1 s))
         0))
    (error nil)))

(defun kubed-ext--format-age (timestamp)
  "Convert TIMESTAMP to human-readable age matching kubectl HumanDuration."
  (if (or (null timestamp) (string-empty-p timestamp)
          (string= timestamp ""))
      (propertize "n/a" 'face 'shadow 'kubed-sort-value 0)
    (let ((parsed (kubed-ext--parse-k8s-timestamp timestamp)))
      (if (null parsed)
          (propertize timestamp 'kubed-sort-value 0)
        (let* ((secs (floor (float-time (time-subtract (current-time) parsed))))
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
          (propertize str 'kubed-sort-value (float secs)))))))

;; ── Pod Status Computation (kubel-style) ──

(defun kubed-ext--compute-pod-status (phase waiting terminated deletion)
  "Compute kubel-style pod status from PHASE, WAITING, TERMINATED, DELETION.
Returns a propertized string with face matching the status severity."
  (let* ((none-p (lambda (s)
                   (or (null s) (string-empty-p s) (string= s ""))))
         (waiting-reason (unless (funcall none-p waiting)
                           (car (split-string waiting ","))))
         (terminated-reason (unless (funcall none-p terminated)
                              (car (split-string terminated ","))))
         (deleting (not (funcall none-p deletion)))
         (status (cond
                  (deleting "Terminating")
                  (waiting-reason waiting-reason)
                  (terminated-reason terminated-reason)
                  (t (or phase "Unknown"))))
         (face (pcase status
                 ("Running"                      'success)
                 ("Succeeded"                    'shadow)
                 ("Completed"                    '(:foreground "yellow"))
                 ("Pending"                      'warning)
                 ("ContainerCreating"            'warning)
                 ("PodInitializing"              'warning)
                 ("Unknown"                      'warning)
                 ("Terminating"                  '(:foreground "blue"))
                 ("Failed"                       'error)
                 ("Error"                        'error)
                 ("OOMKilled"                    'error)
                 ("CrashLoopBackOff"             'error)
                 ("ImagePullBackOff"             'error)
                 ("ErrImagePull"                 'error)
                 ("CreateContainerConfigError"   'error)
                 ("InvalidImageName"             'error)
                 ("Evicted"                      'error)
                 ("RunContainerError"            'error)
                 ("StartError"                   'error)
                 (_                              nil))))
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
  "Format READY/REPLICAS as kubel-style ready/total with color coding."
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

;; ── Pods: fetch extra fields, display kubel-style via hook ──
;;
;; We replace all pod fetch columns so we also get waiting/terminated
;; reasons and deletionTimestamp for computing real container status.
;; The mode hook overrides the display format.
;;
;; Fetch vector layout:
;;   [0]=Name [1]=Phase [2]=ReadyCount [3]=TotalCount [4]=StartTime
;;   [5]=Restarts [6]=IP [7]=Node [8]=WaitingReason
;;   [9]=TerminatedReason [10]=DeletionTimestamp

(kubed-ext-set-columns
 "pods"
 (list
  '("PHASE:.status.phase")
  (cons "READY:.status.containerStatuses[?(.ready==true)].name"
        (lambda (cs)
          (if (string= cs "") "0"
            (number-to-string (1+ (seq-count (lambda (c) (= c ?,)) cs))))))
  (cons "TOTAL:.status.containerStatuses[*].name"
        (lambda (cs)
          (if (string= cs "") "0"
            (number-to-string (1+ (seq-count (lambda (c) (= c ?,)) cs))))))
  '("STARTTIME:.status.startTime")
  (cons "RESTARTS:.status.containerStatuses[*].restartCount"
        (lambda (s)
          (if (string-empty-p s) "0"
            (number-to-string
             (apply #'+ (mapcar #'string-to-number
                                (split-string s ",")))))))
  '("IP:.status.podIP")
  '("NODE:.spec.nodeName")
  '("WAITING:.status.containerStatuses[*].state.waiting.reason")
  '("TERMINATED:.status.containerStatuses[*].state.terminated.reason")
  '("DELETION:.metadata.deletionTimestamp"))
 nil)

(defun kubed-ext--pod-entries ()
  "Custom entries for pods with kubel-style status and color coding.
Reads 11-element fetch vectors and produces 7-column display vectors:
Name, Ready(x/y), Status, Restarts, Age, IP, Node."
  (let* ((raw (alist-get 'resources (kubed--alist kubed-list-type
                                                  kubed-list-context
                                                  kubed-list-namespace)))
         (pred (kubed-list-interpret-filter))
         (result nil))
    (dolist (entry raw)
      (let* ((vec (cadr entry))
             (new-entry
              (if (and (vectorp vec) (>= (length vec) 11))
                  (list (car entry)
                        (vector
                         (aref vec 0)
                         (kubed-ext--format-pod-ready
                          (aref vec 2) (aref vec 3))
                         (kubed-ext--compute-pod-status
                          (aref vec 1) (aref vec 8)
                          (aref vec 9) (aref vec 10))
                         (kubed-ext--format-pod-restarts
                          (string-to-number (or (aref vec 5) "0")))
                         (kubed-ext--format-age (aref vec 4))
                         (or (aref vec 6) "")
                         (or (aref vec 7) "")))
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

;; ── Services: append Selector column ──

(kubed-ext-append-columns
 "services"
 (list
  (cons "SELECTOR:.spec.selector"
        (lambda (s)
          (cond
           ((string-empty-p s) "")
           ((string-prefix-p "map[" s) (substring s 4 -1))
           (t s)))))
 (list (list "Selector" 40 t)))

;; ── Ingresses: enhanced columns ──

(kubed-ext-set-columns
 "ingresses"
 (list
  '("CLASS:.spec.ingressClassName")
  '("HOSTS:.spec.rules[*].host")
  (cons "ADDRESS:.status.loadBalancer.ingress[0]"
        (lambda (s)
          (cond
           ((string-empty-p s) "")
           ((string-match "ip:$[^] :]+$" s) (match-string 1 s))
           ((string-match "hostname:$[^] :]+$" s) (match-string 1 s))
           (t s))))
  (cons "PORTS:.spec.tls[0].hosts"
        (lambda (s)
          (if (string-empty-p s) "80" "80, 443")))
  '("AGE:.metadata.creationTimestamp"))
 (list
  (list "Class" 20 t)
  (list "Hosts" 40 t)
  (list "Address" 40 t)
  (list "Ports" 10 t)
  (list "Age" 20 t)))

;; ── Persistent Volumes: enhanced columns ──

(kubed-ext-set-columns
 "persistentvolumes"
 (list
  '("CAPACITY:.spec.capacity.storage")
  '("ACCESS-MODES:.spec.accessModes[*]")
  '("RECLAIM:.spec.persistentVolumeReclaimPolicy")
  (cons "STATUS:.status.phase"
        (lambda (ph)
          (propertize ph 'face
                      (pcase ph
                        ("Bound"     'success)
                        ("Available" 'warning)
                        ("Released"  'shadow)
                        ("Failed"    'error)
                        (_           'default)))))
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

;; ── Deployments: kubel-style Ready/Age via hook ──
;;
;; Upstream fetches: [0]Name [1]Ready [2]Updated [3]Available [4]Reps [5]Timestamp
;; Hook displays:    Name | Ready(x/y) | Up-to-date | Available | Age

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
                          (aref vec 1) (aref vec 4))
                         (kubed-ext--format-deployment-count
                          (aref vec 2) (aref vec 4))
                         (kubed-ext--format-deployment-count
                          (aref vec 3) (aref vec 4))
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
                      (if (string-empty-p s) "0"
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
                             ((string-empty-p s) "(all pods)")
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
;;; § 8–9.  Strimzi + Plain Resources  (unchanged from previous)
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
;;; § 10.  Async Helpers
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext--async-kubectl (args callback &optional errback)
  "Run kubectl with ARGS asynchronously.
Call CALLBACK with output on success, ERRBACK with error on failure."
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
ARGS1 and ARGS2 are arg lists. CALLBACK is called with (out1 out2) on success.
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
        tabulated-list-padding 2 tabulated-list-entries nil)
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
        tabulated-list-padding 2 tabulated-list-entries nil)
  (tabulated-list-init-header))

(keymap-set kubed-ext-top-nodes-mode-map "g" #'kubed-ext-top-refresh)
(keymap-set kubed-ext-top-nodes-mode-map "q" #'quit-window)
(keymap-set kubed-ext-top-pods-mode-map  "g" #'kubed-ext-top-refresh)
(keymap-set kubed-ext-top-pods-mode-map  "q" #'quit-window)

(defun kubed-ext-top-nodes (&optional context)
  "Display k9s-style node resource usage in CONTEXT."
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
  "Display k9s-style pod resource usage in CONTEXT and NAMESPACE."
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

(defcustom kubed-ext-pod-shell "/bin/sh"
  "Shell to run when opening a terminal in a Kubernetes pod."
  :type 'string :group 'kubed-ext)

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

(defvar vterm-shell)

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
          (process-send-string (current-buffer) (concat cmd "\n"))))
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
      (let ((buf (get-buffer-create
                  (format "*Kubed describe %s/%s@%s[%s]*"
                          kubed-list-type name
                          (or kubed-list-namespace "default")
                          (or kubed-list-context "current")))))
        (with-current-buffer buf
          (let ((inhibit-read-only t))
            (erase-buffer)
            (apply #'call-process kubed-kubectl-program nil t nil
                   "describe" kubed-list-type name
                   (append
                    (when kubed-list-namespace (list "-n" kubed-list-namespace))
                    (when kubed-list-context (list "--context" kubed-list-context))))
            (goto-char (point-min)) (special-mode)))
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
         (ns (or namespace (when (kubed-namespaced-p type ctx) (kubed--namespace ctx))))
         (buf (get-buffer-create
               (format "*Kubed describe %s/%s@%s[%s]*"
                       type name (or ns "cluster") ctx))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (apply #'call-process kubed-kubectl-program nil t nil
               "describe" type name
               (append (when ns (list "-n" ns)) (when ctx (list "--context" ctx))))
        (goto-char (point-min)) (special-mode)))
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
          (c (if (equal current-prefix-arg '(16)) (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (type (completing-read "Resource type: "
                                 '("deployment" "statefulset" "daemonset") nil t))
          (name (kubed-read-resource-name (concat type "s") "Rollout history for" nil nil c n)))
     (list type name c n)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace (kubed--namespace ctx)))
         (typename (format "%s/%s" type name))
         (buf (get-buffer-create (format "*kubed rollout %s@%s[%s]*" typename (or ns "default") ctx))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "Rollout History: %s\n" typename) 'face 'bold))
        (insert (format "Namespace: %s  Context: %s\n" (or ns "default") ctx))
        (insert (make-string 60 ?─) "\n\n")
        (apply #'call-process kubed-kubectl-program nil t nil
               "rollout" "history" typename
               (append (when ns (list "-n" ns)) (when ctx (list "--context" ctx))))
        (insert "\n" (make-string 60 ?─) "\n")
        (insert (propertize "\nKeys: r = view revision  u = undo  g = refresh  q = quit\n" 'face 'shadow))
        (goto-char (point-min))
        (setq-local kubed-ext-rollout-type type kubed-ext-rollout-name name
                    kubed-ext-rollout-context ctx kubed-ext-rollout-namespace ns)
        (kubed-ext-rollout-history-mode)))
    (display-buffer buf)))

(defun kubed-ext-rollout-show-revision ()
  "Show a specific revision from the rollout history buffer."
  (interactive nil kubed-ext-rollout-history-mode)
  (let* ((typename (format "%s/%s" kubed-ext-rollout-type kubed-ext-rollout-name))
         (ctx kubed-ext-rollout-context) (ns kubed-ext-rollout-namespace)
         (rev (read-number "Revision: "))
         (buf (get-buffer-create (format "*kubed rollout %s rev %d*" typename rev))))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (erase-buffer)
        (insert (propertize (format "Revision %d: %s\n" rev typename) 'face 'bold))
        (insert (make-string 60 ?─) "\n\n")
        (apply #'call-process kubed-kubectl-program nil t nil
               "rollout" "history" typename (format "--revision=%d" rev)
               (append (when ns (list "-n" ns)) (when ctx (list "--context" ctx))))
        (goto-char (point-min)) (special-mode)))
    (display-buffer buf)))

(defun kubed-ext-rollout-undo (type name &optional revision context namespace)
  "Undo rollout of TYPE/NAME to REVISION in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16)) (kubed-read-context "Context" c) c))
          (n (kubed--namespace c current-prefix-arg))
          (type (completing-read "Resource type: "
                                 '("deployment" "statefulset" "daemonset") nil t))
          (name (kubed-read-resource-name (concat type "s") "Undo rollout for" nil nil c n))
          (rev (when (y-or-n-p "Specify target revision? ") (read-number "To revision: "))))
     (list type name rev c n)))
  (let* ((ctx (or context (kubed-local-context)))
         (ns (or namespace (kubed--namespace ctx)))
         (typename (format "%s/%s" type name)))
    (unless (zerop (apply #'call-process kubed-kubectl-program nil nil nil
                          "rollout" "undo" typename
                          (append (when revision (list (format "--to-revision=%d" revision)))
                                  (when ns (list "-n" ns)) (when ctx (list "--context" ctx)))))
      (user-error "Failed to undo rollout for %s" typename))
    (message "Rolled back %s%s." typename
             (if revision (format " to revision %d" revision) ""))))

(defun kubed-ext-rollout-undo-from-history ()
  "Undo rollout from history buffer."
  (interactive nil kubed-ext-rollout-history-mode)
  (let* ((typename (format "%s/%s" kubed-ext-rollout-type kubed-ext-rollout-name))
         (rev (when (y-or-n-p "Specify revision? ") (read-number "To revision: "))))
    (when (y-or-n-p (format "Undo rollout for %s%s? "
                            typename (if rev (format " to rev %d" rev) "")))
      (kubed-ext-rollout-undo kubed-ext-rollout-type kubed-ext-rollout-name
                              rev kubed-ext-rollout-context kubed-ext-rollout-namespace))))

(defun kubed-ext-rollout-refresh ()
  "Refresh rollout history buffer."
  (interactive nil kubed-ext-rollout-history-mode)
  (kubed-ext-rollout-history kubed-ext-rollout-type kubed-ext-rollout-name
                             kubed-ext-rollout-context kubed-ext-rollout-namespace))

(define-derived-mode kubed-ext-rollout-history-mode special-mode "Kubed Rollout"
  "Mode for viewing Kubernetes rollout history." :interactive nil)
(keymap-set kubed-ext-rollout-history-mode-map "r" #'kubed-ext-rollout-show-revision)
(keymap-set kubed-ext-rollout-history-mode-map "u" #'kubed-ext-rollout-undo-from-history)
(keymap-set kubed-ext-rollout-history-mode-map "g" #'kubed-ext-rollout-refresh)
(keymap-set kubed-ext-rollout-history-mode-map "q" #'quit-window)

(defun kubed-ext-deployments-rollout-history (click)
  "Show rollout history for deployment at CLICK position."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (kubed-ext-rollout-history "deployment" dep kubed-list-context kubed-list-namespace)
    (user-error "No Kubernetes deployment at point")))

(defun kubed-ext-deployments-rollout-undo (click)
  "Undo rollout for deployment at CLICK position."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (let ((rev (when (y-or-n-p "Specify target revision? ") (read-number "To revision: "))))
        (when (y-or-n-p (format "Undo rollout for %s?" dep))
          (kubed-ext-rollout-undo "deployment" dep rev kubed-list-context kubed-list-namespace)
          (kubed-list-update t)))
    (user-error "No Kubernetes deployment at point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 14.  Jab / Bounce Deployment
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-jab-deployment (deployment &optional context namespace)
  "Force rolling update of DEPLOYMENT in CONTEXT/NAMESPACE."
  (interactive
   (let* ((c (kubed-local-context))
          (c (if (equal current-prefix-arg '(16)) (kubed-read-context "Context" c) c))
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
                          (append (when ns (list "-n" ns)) (when ctx (list "--context" ctx)))))
      (user-error "Failed to jab deployment %s" deployment))
    (message "Jabbed deployment %s (timestamp %s)." deployment ts)))

(defun kubed-ext-deployments-jab (click)
  "Jab deployment at CLICK position to force a rolling update."
  (interactive (list last-nonmenu-event) kubed-deployments-mode)
  (if-let ((dep (tabulated-list-get-id (mouse-set-point click))))
      (progn (kubed-ext-jab-deployment dep kubed-list-context kubed-list-namespace)
             (kubed-list-update t))
    (user-error "No Kubernetes deployment at point")))

;;; ═══════════════════════════════════════════════════════════════
;;; § 15.  Copy / Clipboard Operations
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext--last-kubectl-command nil
  "Last kubectl command tracked by kubed-ext.")

(defun kubed-ext-list-copy-log-command (click)
  "Copy `kubectl logs -f` command for resource at CLICK position."
  (interactive (list last-nonmenu-event) kubed-list-mode)
  (if-let ((name (tabulated-list-get-id (mouse-set-point click))))
      (let ((cmd (format "%s logs -f --tail=100 %s%s%s"
                         kubed-kubectl-program
                         (if (string= kubed-list-type "pods") "" (concat kubed-list-type "/"))
                         name
                         (concat (when kubed-list-namespace (format " -n %s" kubed-list-namespace))
                                 (when kubed-list-context (format " --context %s" kubed-list-context))))))
        (kill-new cmd) (message "Copied: %s" cmd))
    (user-error "No Kubernetes resource at point")))

(defun kubed-ext-list-copy-kubectl-prefix ()
  "Copy the current kubectl command prefix."
  (interactive nil kubed-list-mode)
  (let ((prefix (format "%s%s%s" kubed-kubectl-program
                        (if kubed-list-namespace (format " -n %s" kubed-list-namespace) "")
                        (if kubed-list-context (format " --context %s" kubed-list-context) ""))))
    (kill-new prefix) (message "Copied: %s" prefix)))

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
      (let ((yaml (with-temp-buffer
                    (apply #'call-process kubed-kubectl-program nil t nil
                           "get" kubed-list-type name "-o" "yaml"
                           (append (when kubed-list-namespace (list "-n" kubed-list-namespace))
                                   (when kubed-list-context (list "--context" kubed-list-context))))
                    (buffer-string))))
        (kill-new yaml) (message "YAML for %s/%s copied (%d bytes)." kubed-list-type name (length yaml)))
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

(defvar-local kubed-ext-list-label-selector nil
  "Active label selector for server-side kubectl filtering.")
(defvar kubed-ext-label-selector-history nil "History list for label selectors.")
(defvar kubed-ext--label-cache (make-hash-table :test 'equal) "Label cache.")
(defvar kubed-ext--label-cache-time (make-hash-table :test 'equal) "Label cache timestamps.")

(defun kubed-ext--fetch-labels (type context namespace)
  "Discover label key=value pairs from resources of TYPE in CONTEXT/NAMESPACE."
  (condition-case nil
      (let ((output (with-temp-buffer
                      (apply #'call-process kubed-kubectl-program nil '(t nil) nil
                             "get" (or type "pods") "--no-headers"
                             "-o" "custom-columns=LABELS:.metadata.labels"
                             (append (when namespace (list "-n" namespace))
                                     (when context (list "--context" context))))
                      (buffer-string))))
        (delete-dups
         (mapcan (lambda (line)
                   (let ((trimmed (string-trim line)))
                     (when (string-match "^map$$$.+$$$" trimmed)
                       (mapcar (lambda (pair) (replace-regexp-in-string ":" "=" pair))
                               (split-string (match-string 1 trimmed) " ")))))
                 (split-string output "\n" t))))
    (error nil)))

(defun kubed-ext-discover-labels (&optional type context namespace)
  "Discover labels of TYPE in CONTEXT/NAMESPACE with 60-second caching."
  (let* ((key (list (or type "pods") (or context "") (or namespace "")))
         (cached (gethash key kubed-ext--label-cache))
         (cache-time (gethash key kubed-ext--label-cache-time 0)))
    (if (and cached (< (- (float-time) cache-time) 60)) cached
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
          (when (and (derived-mode-p 'kubed-list-mode) kubed-ext-list-label-selector
                     (equal kubed-list-type type) (equal kubed-list-context context)
                     (equal kubed-list-namespace namespace))
            (throw 'found kubed-ext-list-label-selector)))))))

(defun kubed-ext-update-with-selector (orig-fn type context &optional namespace)
  "Advice for ORIG-FN: inject --selector for TYPE/CONTEXT/NAMESPACE."
  (let ((sel (kubed-ext--find-active-selector type context namespace)))
    (if (null sel) (funcall orig-fn type context namespace)
      (cl-letf* ((real-mp (symbol-function 'make-process))
                 ((symbol-function 'make-process)
                  (lambda (&rest args)
                    (let ((cmd (plist-get args :command)))
                      (when (and cmd (member "get" cmd))
                        (plist-put args :command (append cmd (list "--selector" sel)))))
                    (apply real-mp args))))
        (funcall orig-fn type context namespace)))))

(advice-add 'kubed-update :around #'kubed-ext-update-with-selector)

(defun kubed-ext-list-set-label-selector (selector)
  "Set label SELECTOR for server-side filtering.  Empty clears it."
  (interactive
   (list (completing-read
          (format-prompt "Label selector" "clear")
          (append '("") (kubed-ext-discover-labels
                         kubed-list-type kubed-list-context kubed-list-namespace)
                  kubed-ext-label-selector-history)
          nil nil nil 'kubed-ext-label-selector-history))
   kubed-list-mode)
  (setq-local kubed-ext-list-label-selector
              (if (string-empty-p selector) nil selector))
  (kubed-list-update))

(setq kubed-list-mode-line-format
      '(:eval
        (if (process-live-p (alist-get 'process (kubed--alist kubed-list-type
                                                              kubed-list-context
                                                              kubed-list-namespace)))
            (propertize " [...]" 'help-echo "Updating...")
          (concat
           (when kubed-list-filter
             (propertize (concat " [" (mapconcat #'prin1-to-string kubed-list-filter " ") "]")
                         'help-echo "Current filter"))
           (when kubed-ext-list-label-selector
             (propertize (concat " {" kubed-ext-list-label-selector "}")
                         'help-echo "Label selector" 'face 'warning))))))

;;; ═══════════════════════════════════════════════════════════════
;;; § 17.  Buffer Switching + Command Log
;;; ═══════════════════════════════════════════════════════════════

(defun kubed-ext-switch-buffer ()
  "Switch to another Kubed list buffer."
  (interactive)
  (let (bufs)
    (dolist (buf (buffer-list))
      (when (with-current-buffer buf (derived-mode-p 'kubed-list-mode))
        (push (cons (format "%-14s  %s" (buffer-local-value 'kubed-list-type buf)
                            (buffer-name buf))
                    buf) bufs)))
    (if bufs
        (let* ((sel (completing-read "Switch to kubed buffer: " bufs nil t))
               (buf (alist-get sel bufs nil nil #'string=)))
          (switch-to-buffer buf))
      (user-error "No kubed list buffers found"))))

(defun kubed-ext--log-kubectl-command (cmd-str)
  "Log CMD-STR to the kubectl command log buffer."
  (setq kubed-ext--last-kubectl-command cmd-str)
  (when-let ((buf (get-buffer "*kubed-command-log*")))
    (with-current-buffer buf
      (let ((inhibit-read-only t))
        (goto-char (point-max))
        (insert (format "[%s] %s\n" (format-time-string "%H:%M:%S") cmd-str))
        (when (> (line-number-at-pos) 500)
          (goto-char (point-min)) (forward-line 100)
          (delete-region (point-min) (point)))))))

(defun kubed-ext--make-process-logger (orig-fn &rest args)
  "Log kubectl `make-process' call.
ORIG-FN is the advised function, ARGS its arguments."
  (when-let* ((cmd (plist-get args :command))
              ((stringp (car cmd)))
              ((string-suffix-p "kubectl" (file-name-sans-extension
                                           (file-name-nondirectory (car cmd))))))
    (kubed-ext--log-kubectl-command (mapconcat #'identity cmd " ")))
  (apply orig-fn args))
(advice-add 'make-process :around #'kubed-ext--make-process-logger)

(defun kubed-ext--call-process-logger (orig-fn program &rest args)
  "Log kubectl `call-process'.
ORIG-FN is the advised function.  PROGRAM is the executable and ARGS are passed."
  (when (and (stringp program)
             (string-suffix-p "kubectl" (file-name-sans-extension
                                         (file-name-nondirectory program))))
    (kubed-ext--log-kubectl-command
     (mapconcat #'identity (cons program (seq-filter #'stringp (nthcdr 3 args))) " ")))
  (apply orig-fn program args))
(advice-add 'call-process :around #'kubed-ext--call-process-logger)

(defun kubed-ext--call-process-region-logger (orig-fn start end program &rest args)
  "Log kubectl `call-process-region'.
ORIG-FN is advised. START and END are region bounds. PROGRAM and ARGS passed."
  (when (and (stringp program)
             (string-suffix-p "kubectl" (file-name-sans-extension
                                         (file-name-nondirectory program))))
    (kubed-ext--log-kubectl-command
     (mapconcat #'identity (cons program (seq-filter #'stringp (nthcdr 3 args))) " ")))
  (apply orig-fn start end program args))
(advice-add 'call-process-region :around #'kubed-ext--call-process-region-logger)

(defun kubed-ext-show-command-log ()
  "Show the kubed command log buffer."
  (interactive)
  (let ((buf (get-buffer-create "*kubed-command-log*")))
    (with-current-buffer buf (unless (derived-mode-p 'special-mode) (special-mode)))
    (display-buffer buf)))

;;; ═══════════════════════════════════════════════════════════════
;;; § 18.  Keybindings
;;; ═══════════════════════════════════════════════════════════════

(keymap-set kubed-pods-mode-map   "T" #'kubed-ext-top-pods)
(keymap-set kubed-pods-mode-map   "M" #'kubed-ext-top-pods)
(keymap-set kubed-pods-mode-map   "v" #'kubed-ext-pods-vterm)
(keymap-set kubed-pods-mode-map   "t" #'kubed-ext-pods-eat)
(keymap-set kubed-pod-prefix-map  "T" #'kubed-ext-top-pods)
(keymap-set kubed-pod-prefix-map  "v" #'kubed-ext-vterm-pod)
(keymap-set kubed-pod-prefix-map  "t" #'kubed-ext-eat-pod)
(keymap-set kubed-pod-prefix-map  "S" #'kubed-ext-eshell-pod)
(keymap-set kubed-nodes-mode-map  "M" #'kubed-ext-top-nodes)
(keymap-set kubed-node-prefix-map "T" #'kubed-ext-top-nodes)
(keymap-set kubed-services-mode-map  "F" #'kubed-ext-services-forward-port)
(keymap-set kubed-service-prefix-map "F" #'kubed-ext-forward-port-to-service)
(keymap-set kubed-deployments-mode-map  "F" #'kubed-ext-deployments-forward-port)
(keymap-set kubed-deployments-mode-map  "r" #'kubed-ext-deployments-rollout-history)
(keymap-set kubed-deployments-mode-map  "j" #'kubed-ext-deployments-jab)
(keymap-set kubed-deployments-mode-map  "U" #'kubed-ext-deployments-rollout-undo)
(keymap-set kubed-deployment-prefix-map "F" #'kubed-ext-forward-port-to-deployment)
(keymap-set kubed-deployment-prefix-map "r" #'kubed-ext-rollout-history)
(keymap-set kubed-deployment-prefix-map "j" #'kubed-ext-jab-deployment)
(keymap-set kubed-deployment-prefix-map "U" #'kubed-ext-rollout-undo)
(keymap-set kubed-list-mode-map "d" #'kubed-ext-list-describe-resource)
(keymap-set kubed-list-mode-map "S" #'kubed-ext-list-set-label-selector)
(keymap-set kubed-list-mode-map "c" #'kubed-ext-copy-popup)
(keymap-set kubed-list-mode-map "b" #'kubed-ext-switch-buffer)
(keymap-set kubed-prefix-map "F" #'kubed-ext-list-port-forwards)
(keymap-set kubed-prefix-map "b" #'kubed-ext-switch-buffer)
(keymap-set kubed-prefix-map "#" #'kubed-ext-show-command-log)
(keymap-set kubed-prefix-map "d" #'kubed-ext-describe-resource)

;;; ═══════════════════════════════════════════════════════════════
;;; § 19.  Transient Navigation + Resource Actions
;;; ═══════════════════════════════════════════════════════════════

(defvar kubed-ext-extra-transient-suffixes
  '(("pods"        . [("T" "Top (metrics)"  kubed-ext-top-pods)
                      ("v" "Vterm"          kubed-ext-pods-vterm)
                      ("t" "Eat terminal"   kubed-ext-pods-eat)
                      ("E" "Eshell"         kubed-ext-pods-eshell)
                      ("A" "Ansi-term"      kubed-ext-pods-ansi-term)
                      ("&" "Shell command"  kubed-ext-pods-shell-command)])
    ("nodes"       . [("T" "Top (metrics)"  kubed-ext-top-nodes)])
    ("services"    . [("F" "Forward Port"   kubed-ext-services-forward-port)])
    ("deployments" . [("F" "Forward Port"      kubed-ext-deployments-forward-port)
                      ("H" "Rollout History"   kubed-ext-deployments-rollout-history)
                      ("j" "Jab (bounce)"      kubed-ext-deployments-jab)
                      ("u" "Rollout Undo"      kubed-ext-deployments-rollout-undo)]))
  "Additional per-resource-type transient suffixes.")

(defun kubed-ext-switch-context ()
  "Switch kubectl context and refresh current resource list."
  (interactive)
  (when (derived-mode-p 'kubed-list-mode)
    (let ((type kubed-list-type) (ns kubed-list-namespace)
          (ctx (kubed-read-context "Switch to context")))
      (when type
        (let ((fn (intern (format "kubed-list-%s" type))))
          (when (fboundp fn) (if ns (funcall fn ctx ns) (funcall fn ctx))))))))

(defun kubed-ext-switch-namespace ()
  "Switch namespace and refresh current resource list."
  (interactive)
  (when (derived-mode-p 'kubed-list-mode)
    (let ((type kubed-list-type) (ctx kubed-list-context)
          (ns (kubed-read-namespace "Switch to namespace" nil nil kubed-list-context)))
      (when type
        (let ((fn (intern (format "kubed-list-%s" type))))
          (when (fboundp fn) (funcall fn ctx ns)))))))

(defvar kubed-ext-common-resources
  '(("pods" . "Pods") ("deployments" . "Deployments") ("services" . "Services")
    ("configmaps" . "ConfigMaps") ("secrets" . "Secrets") ("ingresses" . "Ingresses")
    ("jobs" . "Jobs") ("cronjobs" . "CronJobs") ("statefulsets" . "StatefulSets")
    ("daemonsets" . "DaemonSets") ("replicasets" . "ReplicaSets")
    ("persistentvolumeclaims" . "PVCs") ("persistentvolumes" . "PVs")
    ("nodes" . "Nodes") ("namespaces" . "Namespaces") ("events" . "Events")
    ("networkpolicies" . "NetworkPolicies") ("horizontalpodautoscalers" . "HPAs")
    ("kafkas" . "Kafkas") ("kafkaconnects" . "KafkaConnects")
    ("kafkaconnectors" . "KafkaConnectors") ("kafkatopics" . "KafkaTopics"))
  "Common resources for quick switching.")

(defun kubed-ext-switch-resource ()
  "Switch to a different resource type in the same context/namespace."
  (interactive)
  (when (derived-mode-p 'kubed-list-mode)
    (let* ((ctx kubed-list-context) (ns kubed-list-namespace)
           (choices (mapcar (lambda (r) (cons (cdr r) (car r))) kubed-ext-common-resources))
           (sel (completing-read "Switch to resource: " choices nil t))
           (type (alist-get sel choices nil nil #'string=)))
      (when type
        (let ((fn (intern (format "kubed-list-%s" type))))
          (when (fboundp fn) (if ns (funcall fn ctx ns) (funcall fn ctx))))))))

(defun kubed-ext-jump-pods ()        "Jump to Pods."        (interactive) (kubed-list-pods kubed-list-context kubed-list-namespace))
(defun kubed-ext-jump-deployments () "Jump to Deployments." (interactive) (kubed-list-deployments kubed-list-context kubed-list-namespace))
(defun kubed-ext-jump-services ()    "Jump to Services."    (interactive) (kubed-list-services kubed-list-context kubed-list-namespace))
(defun kubed-ext-jump-jobs ()        "Jump to Jobs."        (interactive) (kubed-list-jobs kubed-list-context kubed-list-namespace))
(defun kubed-ext-jump-configmaps ()  "Jump to ConfigMaps."  (interactive) (kubed-list-configmaps kubed-list-context kubed-list-namespace))
(defun kubed-ext-jump-secrets ()     "Jump to Secrets."     (interactive) (kubed-list-secrets kubed-list-context kubed-list-namespace))
(defun kubed-ext-jump-ingresses ()   "Jump to Ingresses."   (interactive) (kubed-list-ingresses kubed-list-context kubed-list-namespace))
(defun kubed-ext-jump-pvcs ()        "Jump to PVCs."        (interactive) (kubed-list-persistentvolumeclaims kubed-list-context kubed-list-namespace))

(defun kubed-ext--collect-action-groups ()
  "Build transient group vectors for the current resource type."
  (let* ((type (or kubed-list-type "")) (type-label (capitalize type))
         (upstream (bound-and-true-p kubed-list-transient-extra-suffixes))
         (custom (alist-get type kubed-ext-extra-transient-suffixes nil nil #'string=))
         (all (append (when upstream upstream) (when custom (list custom))))
         (result nil) (gidx 0))
    (dolist (gv all)
      (cl-incf gidx)
      (let* ((entries (if (vectorp gv) (append gv nil) gv))
             (kw-args nil) (suffixes nil) (rest entries))
        (while rest
          (cond
           ((keywordp (car rest)) (push (pop rest) kw-args) (when rest (push (pop rest) kw-args)))
           ((and (listp (car rest)) (>= (length (car rest)) 3)
                 (stringp (nth 0 (car rest))) (stringp (nth 1 (car rest))))
            (push (pop rest) suffixes))
           (t (pop rest))))
        (setq kw-args (nreverse kw-args) suffixes (nreverse suffixes))
        (when suffixes
          (push (apply #'vector
                       (format "%s Actions%s" type-label
                               (if (> (length all) 1) (format " (%d)" gidx) ""))
                       (append kw-args suffixes))
                result))))
    (nreverse result)))

(declare-function kubed-ext--dynamic-menu "kubed-ext")

(defun kubed-ext-transient-menu ()
  "Show transient combining navigation, actions, and utilities."
  (interactive)
  (require 'transient) (require 'kubed-transient nil t)
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
                 ((<= (length agroups) 3) (list (apply #'vector agroups)))
                 (t (let ((rows nil) (rest agroups))
                      (while rest
                        (let ((chunk (seq-take rest 3)))
                          (push (if (= (length chunk) 1) (car chunk)
                                  (apply #'vector chunk)) rows)
                          (setq rest (seq-drop rest 3))))
                      (nreverse rows))))
              ,@(when in-list
                  '([["General"
                      ("g" "Refresh"         kubed-list-update :transient t)
                      ("d" "Describe"        kubed-ext-list-describe-resource)
                      ("/" "Filter"          kubed-list-set-filter)
                      ("S" "Label Selector"  kubed-ext-list-set-label-selector)
                      ("q" "Quit"            quit-window)]
                     ["Utilities"
                      ("b" "Switch Buffer"   kubed-ext-switch-buffer)
                      ("w" "Copy Name"       kubed-list-copy-as-kill)
                      ("y" "Copy Menu"       kubed-ext-copy-popup)
                      ("#" "Command Log"     kubed-ext-show-command-log)]])))
           t)
          (funcall-interactively #'kubed-ext--dynamic-menu))
      (error
       (message "Dynamic transient error: %s" (error-message-string err))
       (when (fboundp 'kubed-list-transient) (call-interactively #'kubed-list-transient))))))

(keymap-set kubed-list-mode-map "?" #'kubed-ext-transient-menu)
(keymap-set kubed-list-mode-map "C" #'kubed-ext-switch-context)
(keymap-set kubed-list-mode-map "N" #'kubed-ext-switch-namespace)
(keymap-set kubed-list-mode-map "R" #'kubed-ext-switch-resource)

(defun kubed-ext-setup ()
  "Initialize kubed-ext functionality."
  (message "Kubed-ext: setup complete."))

(provide 'kubed-ext)
;;; kubed-ext.el ends here
