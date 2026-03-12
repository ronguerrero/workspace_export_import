# Databricks notebook source
# MAGIC %md
# MAGIC # Import Workspace Export and ACLs
# MAGIC
# MAGIC Imports notebooks from the export **directory structure** (files/) and applies ACLs from the manifest. Use with export produced by the Workspace Notebook Export (with ACLs) notebook.
# MAGIC
# MAGIC **Use:** Set the **Export directory path** (e.g. UC volume path to the export folder). Import runs in the *current* workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set **Notebook path to import** to the workspace path that was exported (e.g. `/Shared/MyProject`). It is translated to a name with `/` replaced by `_` (e.g. `Shared_MyProject`). Set **Export path** to the base path where exports live (e.g. `/Volumes/main/default/workspace_export`). The notebook finds the **latest** directory under that path whose name starts with the translated name (e.g. `Shared_MyProject_20250303_120000`) and imports from it.

# COMMAND ----------

dbutils.widgets.text("notebook_path_to_import", "/", "Notebook path to import")
dbutils.widgets.text("export_path", "", "Export path")
dbutils.widgets.dropdown("skip_acls", "false", ["true", "false"], "Skip applying ACLs")

# COMMAND ----------

from pathlib import Path

notebook_path_to_import = dbutils.widgets.get("notebook_path_to_import").strip()
export_path = dbutils.widgets.get("export_path").strip()
skip_acls = dbutils.widgets.get("skip_acls") == "true"

if not export_path:
    raise ValueError("Set the 'Export path' widget (e.g. /Volumes/catalog/schema/volume)")

# Translate notebook path to directory name: '/' -> 'root', '/Shared/Proj' -> 'Shared_Proj'
def path_to_export_dir_name(path: str) -> str:
    p = path.strip("/")
    return p.replace("/", "_") if p else "root"

name_prefix = path_to_export_dir_name(notebook_path_to_import) + "_"
export_path_obj = Path(export_path)
if not export_path_obj.is_dir():
    raise FileNotFoundError(f"Export path is not a directory: {export_path}")

# Find directories whose name starts with name_prefix (e.g. Shared_Proj_20250303_120000), pick latest by name (timestamp)
matching = [d for d in export_path_obj.iterdir() if d.is_dir() and d.name.startswith(name_prefix)]
if notebook_path_to_import.strip("/") in ["Users"]:
    matching = [d for d in matching if "@" not in d.name]
if not matching:
    raise FileNotFoundError(f"No export directory found under {export_path} matching '{name_prefix}*'. Run the export notebook first.")

matching.sort(key=lambda d: d.name, reverse=True)
export_dir = str(matching[0])
print(f"Using latest export: {export_dir}")

# COMMAND ----------

import base64
import json
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import (
    ImportFormat,
    Language,
    WorkspaceObjectAccessControlRequest,
    WorkspaceObjectPermissionLevel,
)

def load_manifest(export_dir: str):
    path = Path(export_dir) / "manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"manifest.json not found in {export_dir}")
    return json.loads(path.read_text())

def path_to_file(export_dir: str, nb_path: str, language: str) -> Path:
    ext = {"PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala", "R": ".r"}.get((language or "PYTHON").upper(), ".py")
    rel = nb_path.lstrip("/").replace("/", os.sep) + ext
    return Path(export_dir) / "files" / rel

# COMMAND ----------

manifest = load_manifest(export_dir)
notebooks = manifest.get("notebooks", [])
directories = manifest.get("directories", [])

client = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create directories and import notebooks
# MAGIC
# MAGIC Create directory structure, then import each notebook from the files/ tree.

# COMMAND ----------

# Create all directories from the manifest (parents first). mkdirs creates parent dirs as needed.
dir_paths = sorted({d["path"] for d in directories})
dir_paths.sort(key=lambda p: (p.count("/"), p))
for path in dir_paths:
    print(f"Creating directory: {path}")
    try:
        client.workspace.mkdirs(path)
    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" not in str(e):
            print(f"mkdirs {path}: {e}")

for nb in notebooks:
    path = nb["path"]
    print(f"Importing notebook: {path}")
    # Ensure this notebook's parent directory exists (defensive: import API requires it)
    parent = path.rsplit("/", 1)[0]
    if parent:
        try:
            client.workspace.mkdirs(parent)
        except Exception as e:
            if "RESOURCE_ALREADY_EXISTS" not in str(e):
                print(f"mkdirs {parent}: {e}")
    lang_str = (nb.get("language") or "PYTHON").upper()
    try:
        lang = getattr(Language, lang_str, Language.PYTHON)
    except AttributeError:
        lang = Language.PYTHON
    content_path = path_to_file(export_dir, path, nb.get("language"))
    if not content_path.exists():
        print(f"Skip (file not found): {path}")
        continue
    content_b64 = base64.b64encode(content_path.read_bytes()).decode("ascii")
    try:
        client.workspace.import_(path=path, content=content_b64, format=ImportFormat.SOURCE, language=lang, overwrite=True)
    except Exception as e:
        print(f"Import failed {path}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply ACLs
# MAGIC
# MAGIC Apply stored ACLs to directories and notebooks (skip if **Skip applying ACLs** is true).

# COMMAND ----------

if skip_acls:
    print("ACLs skipped (widget set to true).")
else:

    def _req_principal_eq(req, owner: dict) -> bool:
        """True if req is the same principal as owner (user, group, or service principal)."""
        return (
            (owner.get("user_name") and req.user_name == owner["user_name"])
            or (owner.get("group_name") and req.group_name == owner["group_name"])
            or (owner.get("service_principal_name") and req.service_principal_name == owner["service_principal_name"])
        )

    def set_acl(object_type: str, path: str, acl: list, owner: dict = None):
        print(f"Applying ACLs ({object_type}): {path}")
        try:
            status = client.workspace.get_status(path=path)
            oid = getattr(status, "object_id", None)
            if not oid:
                return
            reqs = []
            for entry in (acl or []):
                pl = (entry.get("permission_level") or "CAN_READ").upper().replace("-", "_")
                try:
                    perm = WorkspaceObjectPermissionLevel[pl]
                except KeyError:
                    perm = WorkspaceObjectPermissionLevel.CAN_READ
                reqs.append(
                    WorkspaceObjectAccessControlRequest(
                        user_name=entry.get("user_name"),
                        group_name=entry.get("group_name"),
                        service_principal_name=entry.get("service_principal_name"),
                        permission_level=perm,
                    )
                )
            # Apply stored owner with CAN_MANAGE so ownership is restored on import
            if owner and not any(_req_principal_eq(r, owner) for r in reqs):
                reqs.append(
                    WorkspaceObjectAccessControlRequest(
                        user_name=owner.get("user_name"),
                        group_name=owner.get("group_name"),
                        service_principal_name=owner.get("service_principal_name"),
                        permission_level=WorkspaceObjectPermissionLevel.CAN_MANAGE,
                    )
                )
            if reqs:
                client.workspace.set_permissions(object_type, str(oid), access_control_list=reqs)
        except Exception as e:
            print(f"set_permissions {object_type} {path}: {e}")

    for d in directories:
        set_acl("directories", d["path"], d.get("acl", []), d.get("owner"))
    for nb in notebooks:
        set_acl("notebooks", nb["path"], nb.get("acl", []), nb.get("owner"))

    print("ACLs applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Done.** Notebooks and directories have been imported and ACLs applied (unless skipped).
