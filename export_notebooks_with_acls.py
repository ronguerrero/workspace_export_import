# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Notebook Export (with ACLs)
# MAGIC
# MAGIC Exports a **top-level directory** (or root) as **SOURCE only** (no cell outputs) via the Workspace API, then captures **ACLs** for all notebooks and directories in that tree. SOURCE format is source code only; ACLs are stored separately in the manifest.
# MAGIC
# MAGIC **Output:**
# MAGIC - **files/** — directory structure mirroring workspace paths; each notebook as SOURCE (e.g. `.py`, `.sql`); no cell outputs.
# MAGIC - **manifest.json** — paths and ACLs for notebooks and directories (for reapply on import).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set the **export file path** (e.g. a UC volume path like `/Volumes/main/default/workspace_export`) where the extract will land, and the **user notebook path** to export from the workspace. For large trees, use a subpath to avoid the export size limit (~10MB).

# COMMAND ----------

dbutils.widgets.text("export_file_path", "/Volumes/main/default/workspace_export", "Export file path")
dbutils.widgets.text("starting_path", "/", "User notebook path to export")

# COMMAND ----------

EXPORT_BASE_PATH = dbutils.widgets.get("export_file_path").strip()
STARTING_PATH = dbutils.widgets.get("starting_path").strip()

if not EXPORT_BASE_PATH:
    raise ValueError("Set the 'Export file path' widget (e.g. /Volumes/catalog/schema/volume)")

print(f"Export will be written to: {EXPORT_BASE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dependencies and helpers
# MAGIC
# MAGIC Uses Workspace API (per-notebook SOURCE export) and Permissions API for ACLs.

# COMMAND ----------

import base64
import json
import os
from datetime import datetime
from pathlib import Path

def _path_to_export_dir_name(path: str) -> str:
    """Workspace path to export directory name: '/' -> 'root', '/Shared/Proj' -> 'Shared_Proj'."""
    p = path.strip("/")
    return p.replace("/", "_") if p else "root"


def _path_to_file_rel(path: str, language: str) -> str:
    """Workspace path and language to relative file path under files/ (e.g. Shared/proj/notebook.py)."""
    ext = {"PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala", "R": ".r"}.get((language or "PYTHON").upper(), ".py")
    return path.lstrip("/").replace("/", os.sep) + ext

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import (
    ExportFormat,
    ObjectType,
    WorkspaceObjectAccessControlRequest,
    WorkspaceObjectPermissionLevel,
)

# COMMAND ----------

def _get_direct_acl(access_control_list):
    """Build ACL list with only direct (non-inherited) permissions for export."""
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
    """Return owner as a single principal dict (user_name, group_name, or service_principal_name) for the first direct CAN_MANAGE principal, else None."""
    if not access_control_list:
        return None
    for entry in access_control_list:
        if not entry.all_permissions:
            continue
        for p in entry.all_permissions:
            if getattr(p, "inherited", True) is False and getattr(p, "permission_level", None) == WorkspaceObjectPermissionLevel.CAN_MANAGE:
                out = {}
                if entry.user_name:
                    out["user_name"] = entry.user_name
                if entry.group_name:
                    out["group_name"] = entry.group_name
                if entry.service_principal_name:
                    out["service_principal_name"] = entry.service_principal_name
                if out:
                    return out
                break
    return None


def list_notebooks_and_dirs_recursive(client: WorkspaceClient, path: str):
    """Recursively list notebooks and directories from path. Yields (object_type, path, object_id, language)."""
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
        print(f"Warning: Could not access {path}: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Run export
# MAGIC
# MAGIC Extract each notebook as SOURCE into a **directory structure** under `files/`, and collect ACLs for every notebook and directory.

# COMMAND ----------

ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
dir_name = _path_to_export_dir_name(STARTING_PATH) + "_" + ts
export_dir = os.path.join(EXPORT_BASE_PATH, dir_name)
files_dir = os.path.join(export_dir, "files")
os.makedirs(files_dir, exist_ok=True)

client = WorkspaceClient()
try:
    user = client.current_user.me()
    workspace_url = getattr(client.config, "host", "unknown")
except Exception:
    user = None
    workspace_url = "unknown"

print(f"Exporting from: {workspace_url}")
print(f"Starting path:   {STARTING_PATH}")
print(f"Export directory: {export_dir}")

# COMMAND ----------

notebooks_manifest = []
directories_manifest = []
errors = []

for kind, path, object_id, language in list_notebooks_and_dirs_recursive(client, STARTING_PATH):
    try:
        if kind == "notebook":
            # Export single notebook as SOURCE (no cell outputs)
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
            perms = client.workspace.get_permissions("directories", str(object_id))
            acl = _get_direct_acl(perms.access_control_list or [])
            owner = _get_owner(perms.access_control_list or [])
            directories_manifest.append({"path": path, "acl": acl, "owner": owner})
    except Exception as e:
        errors.append({"path": path, "error": str(e)})
        print(f"Error processing {path}: {e}")

if errors:
    print(f"\n{len(errors)} error(s) during export (see manifest).")

# COMMAND ----------

manifest = {
    "source_workspace": workspace_url,
    "export_date": datetime.utcnow().isoformat() + "Z",
    "starting_path": STARTING_PATH,
    "notebooks": notebooks_manifest,
    "directories": directories_manifest,
    "errors": errors,
}

manifest_path = os.path.join(export_dir, "manifest.json")
with open(manifest_path, "w") as f:
    json.dump(manifest, f, indent=2)

print(f"Manifest written: {manifest_path}")
print(f"Notebooks: {len(notebooks_manifest)}, Directories (ACLs): {len(directories_manifest)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC - **Export directory:** `{export_dir}`
# MAGIC - **files/** — directory structure with each notebook as SOURCE (e.g. `.py`, `.sql`); no cell outputs.
# MAGIC - **manifest.json** — paths and ACLs for notebooks and directories.
# MAGIC
# MAGIC Use the separate **import_notebooks_with_acls.py** notebook (in this repo) to import from this export folder and apply ACLs in the target workspace.

# COMMAND ----------

display(f"Export complete. {len(notebooks_manifest)} notebooks in files/, {len(directories_manifest)} directories.")
