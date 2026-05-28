# Changelog

All notable changes to the `rds-cli` project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.10] - 2026-05-28

### Added
- **MIT License:** Added official licensing to clarify intellectual property and usage.
- **Repository Professionalization suite:** Introduced a comprehensive set of metadata templates, contribution guides, and bug reporting mechanisms:
  - `CONTRIBUTING.md` (detailed developer guidelines for UCR researchers).
  - `SECURITY.md` (security disclosure policy, highlighting credential safety and traversal protections).
  - `.github/pull_request_template.md` (enforcing the Skywalker local gauntlet checks).
  - Issue templates for standardizing bug reports and feature requests.

---

## [0.1.9] - 2026-05-28

### Added
- **Architecture Diagram:** Integrated an interactive Mermaid.js diagram representing UCR CephRDS gateway load-balancing and GCS streaming architecture.
- **Developer Guide:** Documented local gauntlet setup and detailed command references.
- **CI/CD Workflow:** Added `.github/workflows/ci.yml` to automatically run Ruff checks, MyPy typing verification, and pytest suite on pull requests to `main`.

---

## [0.1.8] - 2026-05-28

### Added
- **Multi-Threaded Failure Isolation:** Recursive directory uploads and downloads are now fault-tolerant. A permission or connection error on an individual file will log a summary and continue processing other items rather than aborting the command queue.
- **Safety Listing Bounds:** Introduced the `--limit` option (defaulting to 1000) on recursive lists to prevent memory lockups and connection timeouts when listing large S3 buckets. Specify `-1` or `0` to lift the limit explicitly.
- **Credentials Fallback:** Standard AWS environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_S3_ENDPOINT_URL`) are automatically resolved when custom config files are absent.

### Fixed
- **Click/Typer Signal Absorption:** Added explicit propagation rules for `typer.Exit` signals within exception handlers, fixing bugs where CLI command cancellations and clean terminations were swallowed.

---

## [0.1.7] - 2026-05-28

### Added
- **Directory Traversal Protection:** Implemented strict canonical path resolution to ensure downloaded files are kept strictly within their target directories.
- **Individual Download command:** Added a dedicated `download` command to securely pull remote S3 files.

### Fixed
- **Resource Cleanup:** Resolved potential open file-descriptor leaks during multi-threaded chunk reads.

---

## [0.1.6] - 2026-05-28

### Added
- **Initial Baseline CLI:** Supported fundamental S3 commands (`auth`, `info`, `ls`, `upload`, `rm`, `share`, `stat`).
- **Cross-Cloud GCS copying:** Provided direct streaming copy between S3 endpoints and Google Cloud Storage buckets using memory-buffered chunks.
