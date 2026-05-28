# Security Policy

UC Riverside Research Computing is committed to ensuring the safety, privacy, and integrity of our research datasets and compute infrastructure. This document outlines our security policies, built-in security controls, and how to report any security vulnerabilities discovered in the `rds-cli`.

---

## 🛡️ Supported Versions

Only the latest release of `rds-cli` is actively supported for security updates. Please ensure you are running the most up-to-date version:

| Version | Supported |
| :--- | :--- |
| `>= 0.1.8` |  Active Support |
| `< 0.1.8` | Unsupported (Please upgrade immediately) |

To upgrade your local installation:
```bash
uv tool upgrade rds-cli
```

---

## 🔒 Built-In Security Features

The `rds-cli` enforces active security boundaries to protect researcher assets and local workstation directories:

1. **Secure Credential Storage:** 
   Interactive configurations are stored locally inside `~/.config/rds-cli/.env`. The application automatically modifies file permissions on creation to `0600` (Read/Write only by owner) to prevent unauthorized local users from viewing access keys.
2. **Directory Traversal Protection (Guard):**
   Recursive S3 downloads examine all incoming object keys. The client performs full, canonical path resolution on both the local base directory and target output files, actively aborting operations if a remote prefix attempts to write files outside of the designated download directory.
3. **Credentials Fallback Safety:**
   The CLI resolves standard credentials in standard priority order (Interactive local configuration first, falling back cleanly to system-level environment variables like `AWS_ACCESS_KEY_ID`), preventing plain-text hardcoding inside code files.

---

## ✉️ Reporting a Vulnerability

If you discover a security vulnerability (e.g., exposed access credentials, sandbox escapes, or memory leaks), please **do not open a public issue** on GitHub. 

Instead, report all vulnerabilities privately via email to the UCR Research Computing team:

* **Primary Contact:** Chuck Forsyth
* **Email:** [forsythc@ucr.edu](mailto:forsythc@ucr.edu)
* **Subject:** `[SECURITY VULNERABILITY] rds-cli`

### What to Include
Please provide a clear and actionable report containing:
- A detailed description of the vulnerability and its potential impact.
- Step-by-step instructions to reproduce the issue (proof-of-concept scripts or commands).
- System environment details (Python version, operating system, etc.).

We will acknowledge receipt of your report within 48 hours and work diligently to release a patched version as quickly as possible.
