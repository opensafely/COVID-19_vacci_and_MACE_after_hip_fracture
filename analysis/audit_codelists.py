"""Write a reproducible inventory of downloaded codelists (standard library only).

Run from the repository root: python analysis/audit_codelists.py
This checks file structure and provenance, not clinical validity. Review candidate
definitions separately; they are intentionally outside the downloaded manifest.
"""
import ast
import csv
import hashlib
import json
from collections import defaultdict
from pathlib import Path


def main():
    manifest = json.loads(Path("codelists/codelists.json").read_text())["files"]
    tree = ast.parse(Path("analysis/codelists.py").read_text())
    imports = defaultdict(list)
    references = set()
    for path in Path("analysis").glob("*.py"):
        if path.name == "codelists.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "codelists":
                references.add(node.attr)
    # Include aliases such as ICD prefix expansions in the reference graph.
    changed = True
    while changed:
        before = set(references)
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in references for t in node.targets):
                references.update(n.id for n in ast.walk(node.value) if isinstance(n, ast.Name))
        changed = before != references
    for node in tree.body:
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name) and node.value.func.id == "codelist_from_csv"):
            imports[Path(node.value.args[0].value).name].append(node.targets[0].id)

    rows = []
    for name, meta in sorted(manifest.items()):
        path = Path("codelists") / name
        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fields = reader.fieldnames
            data = list(reader)
        code_col = next((c for c in ["code", "id", "icd", "dmd_id", "snomed_id"] if c in fields), None)
        if code_col is None:
            raise ValueError(f"No code column in {name}")
        codes = [r[code_col] for r in data]
        if any(not c.strip() for c in codes):
            raise ValueError(f"Blank code in {name}")
        symbols = imports[name]
        used = [s for s in symbols if s in references]
        rows.append({
            "file": name, "code_column": code_col, "row_count": len(data),
            "unique_codes": len(set(codes)), "duplicate_code_rows": len(codes)-len(set(codes)),
            "loaded_symbols": ";".join(symbols), "referenced_symbols": ";".join(used),
            "usage": "referenced" if used else "loaded_not_referenced" if symbols else "not_loaded",
            "version_url": meta["url"], "downloaded_at": meta["downloaded_at"],
            "file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
    Path("codelists/audit").mkdir(exist_ok=True)
    with Path("codelists/audit/inventory.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Inventoried {len(rows)} manifest files; {sum(r['usage'] == 'referenced' for r in rows)} referenced by analysis.")


if __name__ == "__main__":
    main()
