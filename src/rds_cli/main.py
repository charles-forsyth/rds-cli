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


if __name__ == "__main__":
    app()
