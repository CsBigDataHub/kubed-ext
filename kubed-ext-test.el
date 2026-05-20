;;; kubed-ext-test.el --- Tests for kubed-ext -*- lexical-binding: t; -*-

;;; Commentary:

;; Regression tests for kubed-ext.

;;; Code:

(require 'ert)
(require 'cl-lib)
(require 'kubed-ext)

(ert-deftest kubed-ext-usable-context-rejects-interactive-auth-prompt ()
  "Do not pass kubelogin interactive-auth instructions as kubectl context."
  (let ((auth-prompt
         "Run the command below to authenticate interactively; additional arguments may be added as needed:"))
    (cl-letf (((symbol-function 'kubed-ext--current-context-safe)
               (lambda () "safe-context")))
      (should (kubed-ext--context-error-string-p auth-prompt))
      (should (equal (kubed-ext--usable-context auth-prompt)
                     "safe-context")))))

(ert-deftest kubed-ext-current-context-safe-filters-auth-prompt-fallback ()
  "Do not fall back to auth-prompt lines from `kubed-contexts'."
  (let ((auth-prompt
         "Run the command below to authenticate interactively; additional arguments may be added as needed:")
        (kubed-default-context-and-namespace-alist nil))
    (cl-letf (((symbol-function 'kubed-contexts)
               (lambda () (list auth-prompt)))
              ((symbol-function 'process-file)
               (lambda (&rest _args)
                 (insert auth-prompt)
                 0)))
      (should-not (kubed-ext--current-context-safe)))))

(ert-deftest kubed-ext-usable-context-allows-current-context-fallback ()
  "Use nil context instead of signaling when only auth noise is available."
  (let ((auth-prompt
         "Run the command below to authenticate interactively; additional arguments may be added as needed:"))
    (cl-letf (((symbol-function 'kubed-ext--current-context-safe)
               (lambda () nil)))
      (should-not (kubed-ext--usable-context auth-prompt)))))

(ert-deftest kubed-ext-expected-crd-discovery-error-recognizes-auth-prompt ()
  "Treat startup kubelogin auth prompts as expected CRD discovery failures."
  (should (kubed-ext--expected-crd-discovery-error-p
           (concat "kubectl exited 1: Run the command below to authenticate "
                   "interactively; additional arguments may be added as needed:"))))

(ert-deftest kubed-ext-parse-crd-compact-list-builds-cache-entry ()
  "Parse compact CRD discovery output without full OpenAPI schema JSON."
  (let* ((text (concat "example.com\tWidget\twidgets\tNamespaced\t"
                       "Ready|||string|||.status.ready;;;"
                       "Warnings|||string|||.status.warnings;;;\t\n"))
         (entries (kubed-ext--parse-crd-compact-list text))
         (entry (car entries)))
    (should (= (length entries) 1))
    (should (equal (plist-get entry :type) "widgets.example.com"))
    (should (eq (plist-get entry :namespaced) t))
    (should (equal (alist-get 'jsonPath
                              (car (plist-get entry :printer-columns)))
                   ".status.ready"))))

(ert-deftest kubed-ext-parse-crd-compact-list-falls-back-to-spec-columns ()
  "Use legacy spec-level printer columns when storage-version columns are empty."
  (let* ((text (concat "example.com\tClusterWidget\tclusterwidgets\tCluster\t\t"
                       "Phase|||string|||.status.phase;;;\n"))
         (entry (car (kubed-ext--parse-crd-compact-list text))))
    (should (equal (plist-get entry :type) "clusterwidgets.example.com"))
    (should-not (plist-get entry :namespaced))
    (should (equal (alist-get 'name (car (plist-get entry :printer-columns)))
                   "Phase"))))

(ert-deftest kubed-ext-list-command-omits-nil-context-and-keeps-chunking ()
  "Server-side list commands should omit nil contexts and not disable chunking."
  (let ((cmd (kubed-ext--list-command
              "secrets" nil "ocdp" nil t nil)))
    (should (equal (seq-take cmd 3) (list (kubed-kubectl-program) "get" "secrets")))
    (should-not (member "--context" cmd))
    (should (member "--server-print=true" cmd))
    (should-not (member "--chunk-size=0" cmd))
    (should (member "--namespace" cmd))
    (should (member "ocdp" cmd))))

(ert-deftest kubed-ext-list-command-keeps-custom-columns-for-normal-resources ()
  "Normal resources should keep the custom-column list path."
  (let ((cmd (kubed-ext--list-command
              "pods" "ctx" nil
              '(("NAME:.metadata.name") ("STATUS:.status.phase"))
              nil "app=demo")))
    (should (member "--context" cmd))
    (should (member "ctx" cmd))
    (should (member "--output=custom-columns=NAME:.metadata.name,STATUS:.status.phase" cmd))
    (should (member "--selector" cmd))
    (should (member "app=demo" cmd))))

(ert-deftest kubed-ext-current-context-safe-does-not-call-process-file ()
  "Safe context lookup must use cached state, not synchronous kubectl calls."
  (let ((called nil)
        (buf (generate-new-buffer " *kubed-current-context-safe-test*")))
    (unwind-protect
        (with-current-buffer buf
          (setq-local kubed-list-context "ctx-a")
          (cl-letf (((symbol-function 'kubed-contexts)
                     (lambda ()
                       (setq called t)
                       '("ctx-a" "ctx-b")))
                    ((symbol-function 'process-file)
                     (lambda (&rest _args)
                       (setq called t)
                       0)))
            (should (equal (kubed-ext--current-context-safe) "ctx-a"))
            (should-not called)))
      (kill-buffer buf))))

(ert-deftest kubed-ext-refresh-list-buffers-marks-hidden-stale ()
  "Successful updates should render visible buffers and mark hidden ones stale."
  (let ((visible (generate-new-buffer " *kubed-visible-test*"))
        (hidden (generate-new-buffer " *kubed-hidden-test*"))
        (reverted nil))
    (unwind-protect
        (progn
          (dolist (buf (list visible hidden))
            (with-current-buffer buf
              (setq-local kubed-list-type "pods")
              (setq-local kubed-list-context "ctx")
              (setq-local kubed-list-namespace "ns")))
          (cl-letf (((symbol-function 'buffer-list)
                     (lambda (&optional _frame) (list visible hidden)))
                    ((symbol-function 'derived-mode-p)
                     (lambda (&rest _modes) t))
                    ((symbol-function 'get-buffer-window)
                     (lambda (&optional buffer _all-frames)
                       (and (eq (or buffer (current-buffer)) visible) 'visible-window)))
                    ((symbol-function 'revert-buffer)
                     (lambda (&rest _args)
                       (push (current-buffer) reverted)))
                    ((symbol-function 'tabulated-list-init-header)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'set-window-point)
                     (lambda (&rest _args) nil))
                    ((symbol-function 'walk-windows)
                     (lambda (&rest _args) nil)))
            (kubed-ext--refresh-list-buffers-after-update
             "pods" "ctx" "ns" nil)
            (should (equal reverted (list visible)))
            (with-current-buffer hidden
              (should (bound-and-true-p kubed-ext--stale-list-buffer-p)))))
      (kill-buffer visible)
      (kill-buffer hidden))))

(ert-deftest kubed-ext-handle-server-table-empty-output-does-not-signal ()
  "Unexpected empty table output should not signal from the process sentinel path."
  (let ((out (generate-new-buffer " *kubed-empty-table-test*")))
    (unwind-protect
        (cl-letf (((symbol-function 'kubed-ext--refresh-list-buffers-after-update)
                   (lambda (&rest _args) nil)))
          (should-not (kubed-ext--handle-server-table-success
                       out nil "widgets.example.com" "ctx" "ns")))
      (kill-buffer out))))

(ert-deftest kubed-ext-large-list-skip-remembers-known-large-lists ()
  "Auto-refresh should skip resources previously observed as large."
  (let ((kubed-ext-auto-refresh-large-list-threshold 2))
    (clrhash kubed-ext--known-large-lists)
    (cl-letf (((symbol-function 'kubed-ext--cached-resource-count)
               (lambda (&rest _args) 0)))
      (should-not (kubed-ext--large-list-auto-refresh-skip-p
                   "secrets" "ctx" "ns" nil))
      (kubed-ext--remember-large-list-if-needed "secrets" "ctx" "ns" nil 5)
      (should (kubed-ext--large-list-auto-refresh-skip-p
               "secrets" "ctx" "ns" nil)))))

(ert-deftest kubed-ext-find-active-selector-is-host-aware ()
  "Label selectors should not leak between local and remote host caches."
  (let ((local (generate-new-buffer " *kubed-selector-local-test*"))
        (remote (generate-new-buffer " *kubed-selector-remote-test*")))
    (unwind-protect
        (progn
          (with-current-buffer local
            (setq-local default-directory "/tmp/")
            (setq-local kubed-list-type "pods")
            (setq-local kubed-list-context "ctx")
            (setq-local kubed-list-namespace "ns")
            (setq-local kubed-ext-list-label-selector "app=local"))
          (with-current-buffer remote
            (setq-local default-directory "/ssh:demo:/tmp/")
            (setq-local kubed-list-type "pods")
            (setq-local kubed-list-context "ctx")
            (setq-local kubed-list-namespace "ns")
            (setq-local kubed-ext-list-label-selector "app=remote"))
          (cl-letf (((symbol-function 'buffer-list)
                     (lambda (&optional _frame) (list local remote)))
                    ((symbol-function 'derived-mode-p)
                     (lambda (&rest _modes) t)))
            (should (equal (kubed-ext--find-active-selector
                            "pods" "ctx" "ns" nil)
                           "app=local"))
            (should (equal (kubed-ext--find-active-selector
                            "pods" "ctx" "ns" "/ssh:demo:")
                           "app=remote"))))
      (kill-buffer local)
      (kill-buffer remote))))

(ert-deftest kubed-ext-auto-refresh-mode-allows-disabled-interval ()
  "Enabling auto-refresh mode with nil interval should not signal."
  (let ((kubed-ext-auto-refresh-interval nil)
        (kubed-ext-auto-refresh-mode nil))
    (should (kubed-ext-auto-refresh-mode 1))
    (kubed-ext-auto-refresh-mode -1)))

(ert-deftest kubed-ext-stern-logs-falls-back-when-stern-missing ()
  "Basic kubed logs should still work when stern is not installed."
  (let ((called nil))
    (cl-letf (((symbol-function 'executable-find)
               (lambda (_program) nil))
              ((symbol-function 'kubed-ext--call-original-kubed-logs)
               (lambda (&rest args)
                 (setq called args)
                 :fallback)))
      (should (eq (kubed-ext-stern-logs
                   "pods" "pod-a" "ctx" "ns" nil nil nil nil nil nil nil)
                  :fallback))
      (should (equal called
                     '("pods" "pod-a" "ctx" "ns" nil nil nil nil nil nil nil))))))

(ert-deftest kubed-ext-marked-items-includes-selected-and-delete-marks ()
  "Delete helpers should honor both `*' and upstream `D' marks."
  (with-temp-buffer
    (insert "* pod-a\nD pod-b\n  pod-c\n")
    (goto-char (point-min))
    (cl-letf (((symbol-function 'tabulated-list-get-id)
               (lambda ()
                 (pcase (line-number-at-pos)
                   (1 "pod-a")
                   (2 "pod-b")
                   (_ nil)))))
      (should (equal (kubed-ext-marked-items)
                     '("pod-a" "pod-b"))))))

(ert-deftest kubed-ext-log-ansi-filter-preserves-colored-output ()
  "Stern ANSI sequences should be rendered, not left raw in the buffer."
  (let ((buf (generate-new-buffer " *kubed-log-ansi-test*")))
    (unwind-protect
        (let ((proc (make-process :name "kubed-ext-log-ansi-test"
                                  :buffer buf
                                  :command '("cat")
                                  :noquery t)))
          (set-marker (process-mark proc) (point-min) buf)
          (with-current-buffer buf
            (kubed-ext--log-ansi-filter proc "\x1b[31mERR\x1b[0m\n")
            (should (equal (buffer-string) "ERR\n"))
            (should (or (get-text-property (point-min) 'face)
                        (get-text-property (point-min) 'font-lock-face)))))
      (when (buffer-live-p buf)
        (kill-buffer buf)))))

(ert-deftest kubed-ext-keybinding-snapshots-restore-old-binding ()
  "Overwritten keybindings should be restored, not merely unset."
  (let ((test-map (make-sparse-keymap))
        (kubed-ext--keybinding-snapshots nil))
    (defvar kubed-ext-test-map nil)
    (setq kubed-ext-test-map test-map)
    (keymap-set kubed-ext-test-map "d" #'ignore)
    (kubed-ext--set-key 'kubed-ext-test-map "d" #'identity)
    (should (eq (keymap-lookup kubed-ext-test-map "d") #'identity))
    (kubed-ext--restore-keybinding-snapshots)
    (should (eq (keymap-lookup kubed-ext-test-map "d") #'ignore))))

(ert-deftest kubed-ext-column-state-restore-resets-kubed-columns ()
  "Column restoration should reset `kubed--columns' and display variables."
  (let ((kubed--columns '(("pods" . (("NAME:.metadata.name")))))
        (kubed-pods-columns '(("Name" 40 t)))
        (kubed-ext--original-kubed-columns '(("pods" . (("NAME:.metadata.name")))))
        (kubed-ext--original-column-variables nil))
    (kubed-ext--snapshot-column-variable 'kubed-pods-columns)
    (setq kubed--columns '(("pods" . (("BROKEN:.spec.broken")))))
    (setq kubed-pods-columns '(("Broken" 10 t)))
    (kubed-ext--restore-column-state)
    (should (equal kubed--columns '(("pods" . (("NAME:.metadata.name"))))))
    (should (equal kubed-pods-columns '(("Name" 40 t))))))

(ert-deftest kubed-ext-cancel-tracked-timers-cancels-pending-timers ()
  "Tracked short-lived timers should be cancellable on unload."
  (let ((kubed-ext--tracked-timers nil))
    (kubed-ext--run-at-time-tracked 3600 nil #'ignore)
    (should (= (length kubed-ext--tracked-timers) 1))
    (kubed-ext--cancel-tracked-timers)
    (should-not kubed-ext--tracked-timers)))

(provide 'kubed-ext-test)
;;; kubed-ext-test.el ends here
