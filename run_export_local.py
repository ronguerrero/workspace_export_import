#!/usr/bin/env python3
"""
Standalone runner for workspace export. Use when running outside Databricks.
Authenticate via: databricks auth login <host> --profile=<profile>
Or set env: DATABRICKS_HOST, DATABRICKS_TOKEN (and optionally DATABRICKS_PROFILE).
"""
import argparse
import base64
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import (
    ExportFormat,
    ObjectType,
    WorkspaceObjectPermissionLevel,
)


def _path_to_export_dir_name(path: str) -> str:
    p = path.strip("/")
    return p.replace("/", "_") if p else "root"


def _path_to_file_rel(path: str, language: str) -> str:
    ext = {"PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala", "R": ".r"}.get((language or "PYTHON").upper(), ".py")
    return path.lstrip("/").replace("/", os.sep) + ext


def _get_direct_acl(access_control_list):
    if not access_control_list:
        return []
    result = []
    for entry in access_control_list:
        direct = None
        if entry.all_permissions:
            for p in entry.all_permissions:
                if getattr(p, "inherited", True) is False:
                    direct = p.permission_level
                    break
            if direct is None and entry.all_permissions:
                direct = entry.all_permissions[0].permission_level
        if direct is None:
            continue
        item = {"permission_level": direct.value}
        if entry.user_name:
            item["user_name"] = entry.user_name
        if entry.group_name:
            item["group_name"] = entry.group_name
        if entry.service_principal_name:
            item["service_principal_name"] = entry.service_principal_name
        result.append(item)
    return result


def _get_owner(access_control_list):
    """First direct CAN_MANAGE principal, else first any CAN_MANAGE."""
    if not access_control_list:
        return None
    fallback = None
    for entry in access_control_list:
        if not entry.all_permissions:
            continue
        for p in entry.all_permissions:
            if getattr(p, "permission_level", None) != WorkspaceObjectPermissionLevel.CAN_MANAGE:
                continue
            out = {}
            if entry.user_name:
                out["user_name"] = entry.user_name
            if entry.group_name:
                out["group_name"] = entry.group_name
            if entry.service_principal_name:
                out["service_principal_name"] = entry.service_principal_name
            if not out:
                continue
            if getattr(p, "inherited", True) is False:
                return out
            if fallback is None:
                fallback = out
    return fallback


def list_notebooks_and_dirs_recursive(client: WorkspaceClient, path: str):
    try:
        for obj in client.workspace.list(path=path):
            if obj.object_type == ObjectType.NOTEBOOK:
                yield (
                    "notebook",
                    obj.path,
                    obj.object_id,
                    obj.language.value if obj.language else None,
                )
            elif obj.object_type == ObjectType.DIRECTORY:
                yield ("directory", obj.path, obj.object_id, None)
                yield from list_notebooks_and_dirs_recursive(client, obj.path)
    except Exception as e:
        print(f"Warning: Could not access {path}: {e}", file=sys.stderr)


def run_export(
    client: WorkspaceClient,
    starting_path: str,
    export_base_path: str,
) -> str:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    dir_name = _path_to_export_dir_name(starting_path) + "_" + ts
    export_dir = os.path.join(export_base_path, dir_name)
    files_dir = os.path.join(export_dir, "files")
    os.makedirs(files_dir, exist_ok=True)

    try:
        workspace_url = getattr(client.config, "host", "unknown")
    except Exception:
        workspace_url = "unknown"

    print(f"Exporting from: {workspace_url}")
    print(f"Starting path:   {starting_path}")
    print(f"Export directory: {export_dir}")

    notebooks_manifest = []
    directories_manifest = []
    errors = []

    for kind, path, object_id, language in list_notebooks_and_dirs_recursive(client, starting_path):
        try:
            if kind == "notebook":
                print(f"Exporting notebook: {path}")
                export_resp = client.workspace.export(path=path, format=ExportFormat.SOURCE)
                content_b64 = getattr(export_resp, "content", None) or ""
                if content_b64:
                    rel = _path_to_file_rel(path, language or "PYTHON")
                    nb_path = os.path.join(files_dir, rel)
                    os.makedirs(os.path.dirname(nb_path), exist_ok=True)
                    Path(nb_path).write_bytes(base64.b64decode(content_b64))
                perms = client.workspace.get_permissions("notebooks", str(object_id))
                acl = _get_direct_acl(perms.access_control_list or [])
                owner = _get_owner(perms.access_control_list or [])
                notebooks_manifest.append({"path": path, "language": language or "PYTHON", "acl": acl, "owner": owner})
            else:
                print(f"Exporting directory (ACLs): {path}")
                perms = client.workspace.get_permissions("directories", str(object_id))
                acl = _get_direct_acl(perms.access_control_list or [])
                owner = _get_owner(perms.access_control_list or [])
                directories_manifest.append({"path": path, "acl": acl, "owner": owner})
        except Exception as e:
            errors.append({"path": path, "error": str(e)})
            print(f"Error processing {path}: {e}", file=sys.stderr)

    if errors:
        print(f"\n{len(errors)} error(s) during export (see manifest).", file=sys.stderr)

    manifest = {
        "source_workspace": workspace_url,
        "export_date": datetime.utcnow().isoformat() + "Z",
        "starting_path": starting_path,
        "notebooks": notebooks_manifest,
        "directories": directories_manifest,
        "errors": errors,
    }
    manifest_path = os.path.join(export_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    print(f"Manifest written: {manifest_path}")
    print(f"Notebooks: {len(notebooks_manifest)}, Directories: {len(directories_manifest)}")
    return export_dir


def main():
    ap = argparse.ArgumentParser(description="Run workspace export locally (uses Databricks SDK)")
    ap.add_argument("--host", default=os.environ.get("DATABRICKS_HOST"), help="Databricks workspace URL")
    ap.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN"), help="Databricks token (or use profile)")
    ap.add_argument("--profile", default=os.environ.get("DATABRICKS_PROFILE"), help="Databricks CLI profile name")
    ap.add_argument("--starting-path", required=True, help="Workspace path to export (e.g. /Users/will.chow@databricks.com)")
    ap.add_argument("--export-dir", default=None, help="Base directory for export (default: /tmp/workspace_export)")
    args = ap.parse_args()

    export_base = args.export_dir or "/tmp/workspace_export"
    os.makedirs(export_base, exist_ok=True)
    starting_path = (args.starting_path or "").strip()
    starting_path = starting_path if starting_path.startswith("/") else "/" + starting_path or "/"

    if args.profile:
        client = WorkspaceClient(profile=args.profile)
    elif args.host and args.token:
        client = WorkspaceClient(host=args.host, token=args.token)
    elif args.host:
        client = WorkspaceClient(host=args.host)
    else:
        client = WorkspaceClient()

    run_export(client, starting_path, export_base)


if __name__ == "__main__":
    main()
