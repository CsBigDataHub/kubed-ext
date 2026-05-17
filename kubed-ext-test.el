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
         "Run the command below to authenticate interactively; additional arguments may be added as needed:"))
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

(provide 'kubed-ext-test)
;;; kubed-ext-test.el ends here
