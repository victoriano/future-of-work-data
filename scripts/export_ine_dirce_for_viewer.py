from __future__ import annotations

import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fow.workflow_scoring import SCORE_METADATA, compute_priority_breakdown, compute_scores

SOURCE_PATH = ROOT / "data/processed/ine_dirce/ine_dirce_workflows_enriched_top20.csv"
OUTPUT_PATH = ROOT / "viewer/public/data/ine_dirce_workflows_enriched_top20.json"


def clean_json_value(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None

    if isinstance(value, dict):
        return {key: clean_json_value(item) for key, item in value.items()}

    if isinstance(value, list):
        return [clean_json_value(item) for item in value]

    return value


def enrich_row(row: dict) -> dict:
    enriched = {**row, **compute_scores(row)}
    enriched["__priority_breakdown"] = compute_priority_breakdown(row)
    return enriched


def main() -> None:
    df = pd.read_csv(SOURCE_PATH)
    rows = [enrich_row(row) for row in df.to_dict(orient="records")]
    payload = {
        "dataset": SOURCE_PATH.name,
        "exportedAt": datetime.now(timezone.utc).isoformat(),
        "rowCount": len(df),
        "columnCount": len(df.columns),
        "scoreMetadata": SCORE_METADATA,
        "columns": [
            {"id": column, "type": str(dtype)}
            for column, dtype in zip(df.columns, df.dtypes, strict=False)
        ],
        "rows": clean_json_value(rows),
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )

    print(
        f"Exported {payload['rowCount']} rows and {payload['columnCount']} columns to "
        f"{OUTPUT_PATH.relative_to(ROOT)}"
    )


if __name__ == "__main__":
    main()
