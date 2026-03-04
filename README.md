# Workspace Notebook Export/Import with ACLs

Exports a **top-level directory** (or root) as **SOURCE only** (no cell outputs) using the Workspace API, captures **ACLs** for all notebooks and directories in that tree, and provides a script to import the archive into another workspace and reapply ACLs.

**Why SOURCE:** Export uses **SOURCE** format so the archive contains notebook and file **source code only**—no cell outputs. ACLs are not included in the archive, so we capture them separately in a manifest and reapply them on import.

## Contents

| File | Description |
|------|-------------|
| `export_notebooks_with_acls.py` | Databricks notebook to run in the **source** workspace. Exports the chosen path as SOURCE (no outputs), collects ACLs, and writes the bundle + import script. |
| `import_notebooks_with_acls.py` | **Databricks notebook** to run in the target workspace. The export also generates a copy in each export folder. |

## 1. Export (in source workspace)

1. Import `export_notebooks_with_acls.py` into your **source** Databricks workspace as a notebook.
2. Ensure a **Unity Catalog volume** exists (e.g. `main.default.workspace_export` or your catalog/schema/volume). The notebook writes the extract there.
3. Configure (or use the default widgets):
   - **UC Catalog** / **UC Schema** / **UC Volume name**: used to build the path `/Volumes/<catalog>/<schema>/<volume>/workspace_export`.
   - **Workspace path to export**: e.g. `"/"` for full workspace, or `"/Shared"`, `"/Users/you@company.com"`. For large workspaces, use a subpath to avoid the export size limit (~10MB).
4. Run all cells.

**Output** under the UC volume at `.../workspace_export/export_<timestamp>/`:

- **files/** — directory structure mirroring workspace paths; each notebook as SOURCE (e.g. `.py`, `.sql`); no cell outputs.
- **manifest.json** — Paths and ACLs for every notebook and directory.
- **import_notebooks_with_acls.py** — **Databricks notebook**: import this into the target workspace and run it with the **Export directory path** widget set to this export folder.

## 2. Import (into target workspace)

1. In the **target** workspace (or any workspace that can read the UC volume), **import** the notebook `import_notebooks_with_acls.py`—either from this repo or from the export folder on the volume (e.g. `/Volumes/.../workspace_export/export_YYYYMMDD_HHMMSS/import_notebooks_with_acls.py`). Databricks will open it as a notebook.
2. Set the **Export directory path** widget to the export folder (e.g. `/Volumes/main/default/workspace_export/workspace_export/export_20250303_120000`).
3. Leave **Target host** and **Target token** empty to import into the *current* workspace. To import into a different workspace, set those widgets.
4. Run all cells.

The notebook imports the archive to the path from the manifest and applies ACLs (unless **Skip applying ACLs** is set to true).

## Requirements

- **Export:** Run inside Databricks (notebook); uses `databricks-sdk` (included in runtimes).
- **Import:** Python 3.7+ with `databricks-sdk`; target workspace URL and token (PAT or OAuth).

## Notes

- **UC volume:** The extract is written to a Unity Catalog volume at `/Volumes/<catalog>/<schema>/<volume>/workspace_export/export_<timestamp>/`. Create the volume first (e.g. `CREATE VOLUME main.default.workspace_export;`) and ensure the cluster has access to the catalog/schema.
- **SOURCE = no outputs:** Each notebook is exported as SOURCE (no cell outputs) into the `files/` directory structure. The import notebook reads from `files/` and imports each notebook.
- **Size limit:** The export API has a size limit (~10MB). If you hit `MAX_NOTEBOOK_SIZE_EXCEEDED`, use a narrower `STARTING_PATH` (e.g. `/Shared` or `/Users/you@company.com`) or export multiple folders separately.
- **ACLs:** Only **direct** (non-inherited) permissions are exported. Principals (users, groups, service principals) must exist in the target workspace for permissions to apply.
- **Owner:** The principal with direct **CAN_MANAGE** is recorded as `owner` in the manifest per notebook and directory. On import, that owner is applied with CAN_MANAGE so ownership is restored (or set) on the target object.
- **Paths:** The archive is imported to the same path as in the source (`starting_path`). The manifest stores full paths for ACL application; no path rewriting is done.
