"""
Migration ponctuelle : réécrit les anciens fichiers Bronze au format du contrat
(src/bronze_schema.py). Idempotent : un fichier déjà au bon format est sauté.

Pour chaque fichier, avant d'écrire, on vérifie que les valeurs sont identiques
à celles que la couche Silver lisait jusqu'ici (flatten_columns) : la migration
change les noms et l'ordre des colonnes, jamais les données.

    python scripts/migrate_bronze_schema.py --dry-run    # ce qui serait fait
    python scripts/migrate_bronze_schema.py              # migration
"""

from __future__ import annotations

import argparse
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))
from src.bronze_schema import BRONZE_COLUMNS, to_bronze_schema  # noqa: E402
from src.transform import flatten_columns  # noqa: E402

BRONZE_DIR = Path(__file__).resolve().parent.parent / "data" / "bronze"


def check_same_values(old: pd.DataFrame, new: pd.DataFrame) -> None:
    """Compare avec la lecture historique de Silver. Lève une erreur au moindre écart."""
    reference = flatten_columns(old.copy())
    assert len(reference) == len(new), f"{len(reference)} lignes avant, {len(new)} après"
    for col in ["open", "high", "low", "close", "volume"]:
        pd.testing.assert_series_equal(
            reference[col].astype("float64").reset_index(drop=True),
            new[col].reset_index(drop=True), check_names=False,
        )
    assert (pd.to_datetime(reference["date"]).values.astype("datetime64[ms]") == new["date"].values).all()
    assert (reference["ticker"].astype(str).values == new["ticker"].astype(str).values).all()


def write_atomically(df: pd.DataFrame, path: Path) -> None:
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    try:
        df.to_parquet(tmp_path, index=False)
        shutil.copyfile(tmp_path, path)
    finally:
        tmp_path.unlink(missing_ok=True)


def main(data_dir: Path, dry_run: bool) -> None:
    files = sorted(data_dir.glob("*/*.parquet"))
    migrated = skipped = 0
    for path in files:
        old = pd.read_parquet(path)
        if list(old.columns) == BRONZE_COLUMNS:
            skipped += 1
            continue
        new = to_bronze_schema(old)
        check_same_values(old, new)
        if not dry_run:
            write_atomically(new, path)
            check_same_values(old, pd.read_parquet(path))   # relu depuis le disque
        migrated += 1
    action = "à migrer" if dry_run else "migrés"
    print(f"{len(files)} fichiers : {migrated} {action}, {skipped} déjà au bon format")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--data-dir", type=Path, default=BRONZE_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    main(args.data_dir, args.dry_run)
