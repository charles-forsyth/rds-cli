# CephRDS CLI (`rds-cli`)

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![Built with uv](https://img.shields.io/badge/built%20with-uv-purple.svg)](https://github.com/astral-sh/uv)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-261230.svg)](https://github.com/astral-sh/ruff)

`rds-cli` is the official, high-performance Command Line Interface for **CephRDS**, UC Riverside's 3.2 Petabyte S3-compatible Research Data Service. It is designed specifically for UCR researchers to easily upload, download, manage, and share massive datasets.

---

## 🚀 Features

- **Blazing Fast Uploads:** Supports automatic, concurrent multipart chunking (`--multipart`) for multi-gigabyte files to maximize VPN throughput.
- **Secure by Default:** Credentials are kept strictly out of your source code and safely stored in `~/.config/rds-cli/.env`.
- **Easy Sharing:** Generate temporary, expiring, cryptographically signed public URLs to share data with external collaborators.
- **Rich Metadata:** Tag your research data with custom Key/Value pairs (e.g., `project=Polaris`, `status=raw`) for better organization.
- **Folder Syncing:** Seamlessly upload and download entire nested directory structures recursively.

---

## ⚙️ Installation

The recommended way to install the tool globally on your system is via `uv`:

```bash
uv tool install git+https://github.com/charles-forsyth/rds-cli.git
```

To update to the latest release in the future:
```bash
uv tool upgrade rds-cli
```

---

## 🔑 Authentication

Before using the tool, you must authenticate. Run the `auth` command and paste your CephRDS `Access Key` and `Secret Key` (provided to you by UCR Research Computing).

```bash
rds-cli auth
```
*Note: The default endpoint is `https://rds.ucr.edu`. Keep this unless specifically instructed otherwise.*

---

## 📖 Usage Examples

### 1. View Available Storage
List all the S3 buckets you have access to:
```bash
rds-cli ls
```

Check the total capacity, object count, and quota of a specific bucket:
```bash
rds-cli info -b my-research-bucket
```

### 2. Uploading Data
Upload a single file:
```bash
rds-cli upload ./dataset.csv -b my-research-bucket
```

Upload a massive file using concurrent multipart chunks (Recommended for >1GB):
```bash
rds-cli upload ./huge_model.pt -b my-research-bucket --multipart
```

Upload an entire folder and tag it with custom metadata:
```bash
rds-cli upload ./my_project_folder -b my-research-bucket -m project=Neuroscience -m type=raw
```

### 3. Listing & Inspecting
List all files inside a bucket:
```bash
rds-cli ls -b my-research-bucket
```

Filter files by prefix (folder path):
```bash
rds-cli ls -b my-research-bucket -p my_project_folder/
```

View detailed stats and custom metadata for a specific file:
```bash
rds-cli stat my_project_folder/data.csv -b my-research-bucket
```

### 4. Secure Sharing
Generate a public download link valid for 1 hour (3600 seconds) so external partners can download your data securely:
```bash
rds-cli share my_project_folder/data.csv -b my-research-bucket -e 3600
```

### 5. Downloading Data
Download a file back to your local machine:
```bash
rds-cli download remote_file.txt -b my-research-bucket -d ./local_folder/
```

Recursively download an entire S3 prefix (folder) structure:
```bash
rds-cli download remote_folder/ -b my-research-bucket -r -d ./local_folder/
```

### 6. Deleting Data
Delete a single file:
```bash
rds-cli rm old_file.txt -b my-research-bucket
```

Recursively delete an entire folder structure from S3:
```bash
rds-cli rm old_folder/ -b my-research-bucket -r
```

---

## 🛠️ Development

Built entirely within the UCR **Skywalker Development Workflow**:
1. Written in Python 3.12+
2. Supercharged by [uv](https://github.com/astral-sh/uv)
3. CLI powered by [Typer](https://typer.tiangolo.com/) and [Rich](https://rich.readthedocs.io/)
4. Type-checked with MyPy and linted by Ruff

For support, please contact UCR Research Computing.
