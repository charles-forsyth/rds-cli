import typer
import os
from rich.console import Console
from rich.panel import Panel
from typing import Optional, List, Any
from .config import CONFIG_DIR, ENV_FILE

app = typer.Typer(help="CephRDS Command Line Interface")
console = Console()


@app.command()
def auth(
    access_key: Optional[str] = typer.Option(None, prompt=True, hide_input=False),
    secret_key: Optional[str] = typer.Option(None, prompt=True, hide_input=True),
    endpoint: str = typer.Option("https://rds.ucr.edu", prompt=True),
):
    """Configure your CephRDS credentials."""
    env_content = f"""S3_ACCESS_KEY={access_key}
S3_SECRET_KEY={secret_key}
S3_ENDPOINT_URL={endpoint}
"""

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(ENV_FILE, "w") as f:
        f.write(env_content)

    ENV_FILE.chmod(0o600)

    console.print(
        Panel(
            "[green]Authentication configured successfully![/green]\n"
            f"Credentials saved securely to: [cyan]{ENV_FILE}[/cyan]"
        )
    )


@app.command()
def info(bucket: str = typer.Option(..., "--bucket", "-b", help="Bucket to check")):
    """Get bucket capacity and usage statistics."""
    from .client import get_s3_client
    from .utils import format_size
    from botocore.exceptions import ClientError

    try:
        s3 = get_s3_client()
        console.print(f"Fetching info for bucket: [bold cyan]{bucket}[/bold cyan]")
        response = s3.head_bucket(Bucket=bucket)
        headers = response.get("ResponseMetadata", {}).get("HTTPHeaders", {})

        used_bytes = int(headers.get("x-rgw-bytes-used", 0))
        objects = int(headers.get("x-rgw-object-count", 0))
        quota_bytes = int(headers.get("x-rgw-quota-bucket-size", -1))

        console.print(f"Total Objects: [bold]{objects}[/bold]")
        console.print(f"Total Size:    [bold]{format_size(used_bytes)}[/bold]")

        if quota_bytes > 0:
            console.print(f"Quota:         {format_size(quota_bytes)}")
            console.print(f"Usage:         {(used_bytes / quota_bytes) * 100:.2f}%")
        else:
            console.print("Quota:         [green]Unlimited[/green]")

    except ValueError as e:
        console.print(f"[red]{e}[/red]")
    except ClientError as e:
        console.print(f"[red]Error accessing bucket: {e}[/red]")


@app.command()
def ls(
    bucket: Optional[str] = typer.Option(None, "--bucket", "-b", help="Bucket to list"),
    prefix: str = typer.Option("", "--prefix", "-p", help="Filter by prefix"),
):
    """List objects in a bucket, or list all buckets if none is specified."""
    from .client import get_s3_client
    from .utils import format_size
    from botocore.exceptions import ClientError

    try:
        s3 = get_s3_client()
        if not bucket:
            console.print("Listing all available [bold cyan]buckets[/bold cyan]:")
            response = s3.list_buckets()
            buckets = response.get("Buckets", [])
            for b in buckets:
                console.print(f" - {b['Name']} (Created: {b['CreationDate']})")
            console.print(f"\nFound [bold]{len(buckets)}[/bold] buckets.")
            return

        console.print(
            f"Listing contents of: [bold cyan]{bucket}[/bold cyan] (Prefix: '{prefix}')"
        )
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        count = 0
        total_size = 0
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    console.print(f"\\[{format_size(obj['Size']):>10}] {obj['Key']}")
                    count += 1
                    total_size += obj["Size"]

        console.print(
            f"\nFound [bold]{count}[/bold] objects. Total size: [bold]{format_size(total_size)}[/bold]"
        )
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
    except ClientError as e:
        console.print(f"[red]Error listing bucket: {e}[/red]")


@app.command()
def upload(
    local_path: str = typer.Argument(..., help="Path to local file or directory"),
    bucket: str = typer.Option(..., "--bucket", "-b", help="Destination bucket"),
    key: Optional[str] = typer.Option(
        None, "--key", "-k", help="S3 object key or prefix"
    ),
    meta: Optional[List[str]] = typer.Option(
        None, "--meta", "-m", help="Custom metadata (format: key=value)"
    ),
    multipart: bool = typer.Option(
        False, "--multipart", help="Force multipart upload for large files"
    ),
):
    """Upload a file or folder to the bucket (with optional metadata and multipart support)."""
    import boto3.s3.transfer
    from .client import get_s3_client
    from botocore.exceptions import ClientError

    if not os.path.exists(local_path):
        console.print(f"[red]Error: Local path '{local_path}' does not exist.[/red]")
        return

    extra_args = {}
    if meta:
        meta_dict = {}
        for m in meta:
            if "=" in m:
                k, v = m.split("=", 1)
                meta_dict[k] = v
        if meta_dict:
            extra_args["Metadata"] = meta_dict
            console.print(f"[dim]Applying metadata: {meta_dict}[/dim]")

    transfer_config = None
    if multipart:
        console.print("[dim]Forcing multipart upload (5MB chunks).[/dim]")
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024
        )

    try:
        s3 = get_s3_client()
        if os.path.isfile(local_path):
            s3_key = key or os.path.basename(local_path)
            console.print(f"Uploading file '{local_path}' to '{bucket}/{s3_key}'...")

            file_kwargs: dict[str, Any] = {}
            if extra_args:
                file_kwargs["ExtraArgs"] = extra_args
            if transfer_config:
                file_kwargs["Config"] = transfer_config

            s3.upload_file(local_path, bucket, s3_key, **file_kwargs)
            console.print("[green]Upload successful.[/green]")

        elif os.path.isdir(local_path):
            prefix = key or os.path.basename(os.path.abspath(local_path))
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            console.print(
                f"Uploading directory '{local_path}' to prefix '{bucket}/{prefix}'..."
            )
            count = 0
            for root, dirs, files in os.walk(local_path):
                for file in files:
                    local_file = os.path.join(root, file)
                    rel_path = os.path.relpath(local_file, local_path)
                    s3_key = f"{prefix}{rel_path}".replace("\\", "/")

                    console.print(f"  Uploading {local_file} -> {s3_key}")

                    dir_kwargs: dict[str, Any] = {}
                    if extra_args:
                        dir_kwargs["ExtraArgs"] = extra_args
                    if transfer_config:
                        dir_kwargs["Config"] = transfer_config

                    s3.upload_file(local_file, bucket, s3_key, **dir_kwargs)
                    count += 1
            console.print(f"[green]Upload complete. {count} files uploaded.[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
    except ClientError as e:
        console.print(f"[red]Upload failed: {e}[/red]")


@app.command()
def rm(
    key: str = typer.Argument(
        ..., help="S3 object key (or prefix if --recursive) to delete"
    ),
    bucket: str = typer.Option(..., "--bucket", "-b", help="Destination bucket"),
    recursive: bool = typer.Option(
        False, "--recursive", "-r", help="Delete all files under a prefix"
    ),
):
    """Delete a file or folder from the bucket."""
    from .client import get_s3_client
    from botocore.exceptions import ClientError

    try:
        s3 = get_s3_client()
        if recursive:
            console.print(
                f"Deleting all objects under prefix '{key}' from '{bucket}'..."
            )
            paginator = s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=key)

            deleted_count = 0
            for page in pages:
                if "Contents" in page:
                    objects_to_delete = [
                        {"Key": obj["Key"]} for obj in page["Contents"]
                    ]
                    s3.delete_objects(
                        Bucket=bucket, Delete={"Objects": objects_to_delete}
                    )
                    deleted_count += len(objects_to_delete)
                    console.print(f"  Deleted {len(objects_to_delete)} objects...")

            console.print(
                f"[green]Deletion complete. {deleted_count} objects deleted.[/green]"
            )
        else:
            console.print(f"Deleting '{key}' from '{bucket}'...")
            s3.delete_object(Bucket=bucket, Key=key)
            console.print("[green]Deletion successful.[/green]")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
    except ClientError as e:
        console.print(f"[red]Deletion failed: {e}[/red]")


@app.command()
def share(
    key: str = typer.Argument(..., help="S3 object key to share"),
    bucket: str = typer.Option(
        ..., "--bucket", "-b", help="Bucket containing the object"
    ),
    expires: int = typer.Option(
        86400, "--expires", "-e", help="Expiration time in seconds (default: 1 day)"
    ),
):
    """Generate a temporary public URL for a file."""
    from .client import get_s3_client
    from botocore.exceptions import ClientError

    try:
        s3 = get_s3_client()
        console.print(
            f"Generating presigned URL for '{bucket}/{key}' (valid for {expires} seconds)..."
        )
        url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
        )
        console.print(f"\n[bold green]Public URL:[/bold green]\n[cyan]{url}[/cyan]\n")
    except ValueError as e:
        console.print(f"[red]{e}[/red]")
    except ClientError as e:
        console.print(f"[red]Error generating URL: {e}[/red]")


@app.command()
def stat(
    key: str = typer.Argument(..., help="S3 object key to inspect"),
    bucket: str = typer.Option(
        ..., "--bucket", "-b", help="Bucket containing the object"
    ),
):
    """Show detailed info and metadata for a specific file."""
    from .client import get_s3_client
    from .utils import format_size
    from botocore.exceptions import ClientError

    try:
        s3 = get_s3_client()
        console.print(f"Fetching metadata for '{bucket}/{key}'...")
        response = s3.head_object(Bucket=bucket, Key=key)

        console.print(
            f"Size: [bold]{format_size(response.get('ContentLength', 0))}[/bold]"
        )
        console.print(f"Last Modified: [bold]{response.get('LastModified')}[/bold]")
        console.print(f"Content Type: [bold]{response.get('ContentType')}[/bold]")

        metadata = response.get("Metadata", {})
        if metadata:
            console.print("\n[bold]Custom Metadata:[/bold]")
            for k, v in metadata.items():
                console.print(f"  [cyan]{k}[/cyan]: {v}")
        else:
            console.print("\n[dim]No custom metadata found.[/dim]")

    except ValueError as e:
        console.print(f"[red]{e}[/red]")
    except ClientError as e:
        console.print(f"[red]Error fetching object: {e}[/red]")


@app.command()
def cp(
    source: str = typer.Argument(
        ..., help="Source path (local path, s3://bucket/key, or gs://bucket/key)"
    ),
    destination: str = typer.Argument(
        ..., help="Destination path (local path, s3://bucket/key, or gs://bucket/key)"
    ),
    recursive: bool = typer.Option(False, "--recursive", "-r", help="Copy recursively"),
    multipart: bool = typer.Option(
        False, "--multipart", help="Force multipart for large files"
    ),
):
    """Copy files between local, CephRDS (S3), and Google Cloud Storage (GCS)."""
    import boto3.s3.transfer
    from .client import get_s3_client
    from botocore.exceptions import ClientError
    import tempfile

    s3 = get_s3_client()

    transfer_config = None
    if multipart:
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024
        )

    def parse_url(url: str):
        if url.startswith("s3://"):
            parts = url[5:].split("/", 1)
            return "s3", parts[0], parts[1] if len(parts) > 1 else ""
        elif url.startswith("gs://"):
            parts = url[5:].split("/", 1)
            return "gs", parts[0], parts[1] if len(parts) > 1 else ""
        return "local", None, url

    src_scheme, src_bucket, src_key = parse_url(source)
    dst_scheme, dst_bucket, dst_key = parse_url(destination)

    try:
        # Case 1: Local to S3 (Upload)
        if src_scheme == "local" and dst_scheme == "s3":
            if not os.path.exists(source):
                console.print(
                    f"[red]Error: Local source '{source}' does not exist.[/red]"
                )
                return

            kwargs: dict[str, Any] = {}
            if transfer_config:
                kwargs["Config"] = transfer_config

            if os.path.isfile(source):
                final_key = (
                    dst_key
                    if dst_key and not dst_key.endswith("/")
                    else f"{dst_key}{os.path.basename(source)}"
                )
                console.print(
                    f"Uploading '{source}' to 's3://{dst_bucket}/{final_key}'..."
                )
                s3.upload_file(source, dst_bucket, final_key, **kwargs)
                console.print("[green]Upload complete.[/green]")
            elif os.path.isdir(source) and recursive:
                prefix = (
                    dst_key
                    if dst_key.endswith("/")
                    else f"{dst_key}/"
                    if dst_key
                    else ""
                )
                console.print(
                    f"Uploading directory '{source}' to 's3://{dst_bucket}/{prefix}'..."
                )
                count = 0
                for root, _, files in os.walk(source):
                    for file in files:
                        local_file = os.path.join(root, file)
                        rel_path = os.path.relpath(local_file, source)
                        final_key = f"{prefix}{rel_path}".replace("\\", "/")
                        console.print(f"  -> {final_key}")
                        s3.upload_file(local_file, dst_bucket, final_key, **kwargs)
                        count += 1
                console.print(f"[green]Uploaded {count} files.[/green]")
            else:
                console.print(
                    "[red]Source is a directory. Use --recursive (-r) to copy.[/red]"
                )

        # Case 2: S3 to Local (Download)
        elif src_scheme == "s3" and dst_scheme == "local":
            if not recursive:
                final_dst = (
                    destination
                    if not os.path.isdir(destination)
                    else os.path.join(destination, os.path.basename(src_key))
                )
                console.print(
                    f"Downloading 's3://{src_bucket}/{src_key}' to '{final_dst}'..."
                )
                os.makedirs(os.path.dirname(os.path.abspath(final_dst)), exist_ok=True)
                s3.download_file(src_bucket, src_key, final_dst)
                console.print("[green]Download complete.[/green]")
            else:
                console.print(
                    f"Downloading recursively from 's3://{src_bucket}/{src_key}' to '{destination}'..."
                )
                os.makedirs(destination, exist_ok=True)
                paginator = s3.get_paginator("list_objects_v2")
                pages = paginator.paginate(Bucket=src_bucket, Prefix=src_key)
                count = 0
                for page in pages:
                    if "Contents" in page:
                        for obj in page["Contents"]:
                            obj_key = obj["Key"]
                            if obj_key.endswith("/"):
                                continue

                            rel_path = (
                                os.path.relpath(obj_key, src_key)
                                if src_key
                                else obj_key
                            )
                            if rel_path == ".":
                                rel_path = os.path.basename(obj_key)

                            local_file_path = os.path.join(destination, rel_path)
                            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

                            console.print(f"  <- {obj_key}")
                            s3.download_file(src_bucket, obj_key, local_file_path)
                            count += 1
                console.print(f"[green]Downloaded {count} files.[/green]")

        # Case 3: S3 to S3 (Server-side Copy)
        elif src_scheme == "s3" and dst_scheme == "s3":
            if not recursive:
                final_key = (
                    dst_key
                    if dst_key and not dst_key.endswith("/")
                    else f"{dst_key}{os.path.basename(src_key)}"
                )
                console.print(
                    f"Copying 's3://{src_bucket}/{src_key}' to 's3://{dst_bucket}/{final_key}'..."
                )
                s3.copy_object(
                    CopySource={"Bucket": src_bucket, "Key": src_key},
                    Bucket=dst_bucket,
                    Key=final_key,
                )
                console.print("[green]Copy complete.[/green]")
            else:
                console.print(
                    "[red]Recursive S3-to-S3 copy is not yet supported in this CLI.[/red]"
                )

        # Case 4: S3 to GCS (Cross-Cloud)
        elif src_scheme == "s3" and dst_scheme == "gs":
            from google.cloud import storage  # type: ignore

            gcs = storage.Client()
            if recursive:
                console.print(
                    "[red]Recursive cross-cloud copy is not supported yet.[/red]"
                )
                return

            final_key = (
                dst_key
                if dst_key and not dst_key.endswith("/")
                else f"{dst_key}{os.path.basename(src_key)}"
            )
            console.print(
                f"Streaming 's3://{src_bucket}/{src_key}' -> 'gs://{dst_bucket}/{final_key}'..."
            )

            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_name = tmp.name

            try:
                s3.download_file(src_bucket, src_key, tmp_name)
                gcs_bucket = gcs.bucket(dst_bucket)
                blob = gcs_bucket.blob(final_key)
                blob.upload_from_filename(tmp_name)
                console.print("[green]Cross-cloud copy complete.[/green]")
            finally:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)

        # Case 5: GCS to S3 (Cross-Cloud)
        elif src_scheme == "gs" and dst_scheme == "s3":
            from google.cloud import storage  # type: ignore

            gcs = storage.Client()
            if recursive:
                console.print(
                    "[red]Recursive cross-cloud copy is not supported yet.[/red]"
                )
                return

            final_key = (
                dst_key
                if dst_key and not dst_key.endswith("/")
                else f"{dst_key}{os.path.basename(src_key)}"
            )
            console.print(
                f"Streaming 'gs://{src_bucket}/{src_key}' -> 's3://{dst_bucket}/{final_key}'..."
            )

            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp_name = tmp.name

            try:
                gcs_bucket = gcs.bucket(src_bucket)
                blob = gcs_bucket.blob(src_key)
                blob.download_to_filename(tmp_name)

                kwargs_s3: dict[str, Any] = {}
                if transfer_config:
                    kwargs_s3["Config"] = transfer_config

                s3.upload_file(tmp_name, dst_bucket, final_key, **kwargs_s3)
                console.print("[green]Cross-cloud copy complete.[/green]")
            finally:
                if os.path.exists(tmp_name):
                    os.remove(tmp_name)

        else:
            console.print("[red]Invalid arguments or unsupported copy operation.[/red]")

    except ClientError as e:
        console.print(f"[red]Operation failed: {e}[/red]")
    except Exception as e:
        console.print(f"[red]Cross-cloud operation failed: {e}[/red]")


@app.command()
def mv(
    source: str = typer.Argument(
        ..., help="Source path (local path or s3://bucket/key)"
    ),
    destination: str = typer.Argument(
        ..., help="Destination path (local path or s3://bucket/key)"
    ),
    recursive: bool = typer.Option(False, "--recursive", "-r", help="Move recursively"),
):
    """Move files between local and CephRDS (acts like standard 'mv')."""
    # Simply calls cp, then deletes the source if successful
    # Note: A real mv would check exit codes, this is a simplified wrapper for demonstration
    console.print(f"[yellow]Moving {source} -> {destination}[/yellow]")
    import subprocess

    cmd = ["rds-cli", "cp", source, destination]
    if recursive:
        cmd.append("-r")

    result = subprocess.run(cmd)

    if result.returncode == 0:
        # If cp succeeded, delete the source
        if source.startswith("s3://"):
            bucket, key = source[5:].split("/", 1)
            subprocess.run(
                ["rds-cli", "rm", key, "-b", bucket] + (["-r"] if recursive else [])
            )
        else:
            import shutil

            if os.path.isdir(source) and recursive:
                shutil.rmtree(source)
            else:
                os.remove(source)
        console.print("[green]Move complete.[/green]")
    else:
        console.print("[red]Move failed during copy phase.[/red]")


if __name__ == "__main__":
    app()
