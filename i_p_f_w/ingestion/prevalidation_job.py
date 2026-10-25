import csv
import logging
import shutil
import time
from pathlib import Path

import pandas as pd
import pyogrio
from datetime import datetime, timezone

log = logging.getLogger(__name__)

TABULAR_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".parquet"}
GEOSPATIAL_EXTENSIONS = {".geojson", ".shp", ".gpkg"}
SUPPORTED_EXTENSIONS = TABULAR_EXTENSIONS | GEOSPATIAL_EXTENSIONS

TARGET_DATABASE = "humanitarian_db"
TARGET_SCHEMA = "raw_data"

# Prevalidation only answers "is this safe to load into the raw layer?".
# Missing-value profiling, duplicate rows/features (WKB), statistics and
# cleaning are deliberately left to dbt staging / downstream DQ.

def validate_general(df):
    errors, warnings = [], []

    if df.shape[1] == 0:
        errors.append("Dataset has no columns")
    if df.shape[0] == 0:
        errors.append("Dataset has no rows")

    # a raw table cannot be created with duplicate column names
    duplicated = df.columns[df.columns.duplicated()].unique().tolist()
    if duplicated:
        warnings.append(f"Duplicate column names: {duplicated}")

    return errors, warnings


def validate_geospatial(gdf):
    errors = []
    warnings = []

    if gdf.empty:
        errors.append("Dataset has no features")

    elif "geometry" not in gdf.columns:
        warnings.append("No geometry column found")

    else:
        geometry_types = gdf.geometry.geom_type.dropna().unique()

        if len(geometry_types) == 0:
            errors.append("No geometry type found")

    if gdf.crs is None:
        warnings.append(
            "No CRS defined; verify the source CRS before spatial operations"
        )

    return errors, warnings

class Prevalidationjob:
    def __init__(self, project_root=None):
        self.root = Path(project_root or Path.cwd()).resolve()
        layer = self.root / "i_p_f_w"
        self.landing = layer / "data" / "landing"
        self.rejected = layer / "data" / "rejected"
        self.achieved = layer / "data" / "achieved"
        self.metadata_dir = layer / "metadata_manager" / "metadata"

    def discover_datasets(self):
        return sorted(
            p for p in self.landing.rglob("*")
            if p.is_file() and p.suffix.lower() in SUPPORTED_EXTENSIONS
        )

    @staticmethod
    def list_spatial_layers(path):
        layers = pyogrio.list_layers(path)
        names = [str(row[0]) for row in layers if row[0] is not None]

        if not names:
            raise ValueError(
                f"No readable spatial layers found in '{path.name}'."
            )

        if len(names) > 5:
            raise ValueError(
                f"Cannot ingest '{path.name}': found {len(names)} spatial "
                f"layers, but the maximum allowed is 5. "
                f"Layers found: {', '.join(names)}."
            )

        return names

    @staticmethod
    def extract_dataset(path):
        ext = path.suffix.lower()

        if ext in GEOSPATIAL_EXTENSIONS:
            return pyogrio.read_dataframe(path)
        if ext == ".csv":
            with open(path, newline="", encoding="utf-8-sig") as f:
                header = next(csv.reader(f), [])

            if not header:
                return pd.DataFrame()

            return pd.read_csv(
                path,
                header=0,
                names=header,
                encoding="utf-8-sig",
            )

        if ext in {".xlsx", ".xls"}:
            raw = pd.read_excel(path, header=None)

            if raw.empty:
                return pd.DataFrame()

            df = raw.iloc[1:]
            df.columns = raw.iloc[0].tolist()

            return df

        if ext == ".json":
            return pd.read_json(path)

        return pd.read_parquet(path)

    @staticmethod
    def prevalidate(data, path):
        if path.suffix.lower() in GEOSPATIAL_EXTENSIONS:
            return validate_geospatial(data)

        return validate_general(data)

    def reject_dataset(self, path):
        dest_dir = (self.rejected / path.relative_to(self.landing)).parent
        dest_dir.mkdir(parents=True, exist_ok=True)
        # a shapefile is a set of sidecar files sharing one stem
        if path.suffix.lower() == ".shp":
            files = [f for f in path.parent.iterdir() if f.is_file() and f.stem == path.stem]
        else:
            files = [path]
        for f in files:
            shutil.move(str(f), str(dest_dir / f.name))



    def metadata_exists(self, path):
        return (self.metadata_dir / f"{path.stem}.yml").exists()

    def build_metadata(self, path):
        return {
            "dataset": {
                "name": path.stem, 
                "registered_at": datetime.now(timezone.utc).isoformat(),
            },
            "source": {
                "type": "local_file",
                "path": path.relative_to(self.root).as_posix(),
                "file_type": path.suffix.lower(),
            },
            "target": {
                "database": TARGET_DATABASE,
                "schema": TARGET_SCHEMA,
                "table": path.stem,
            },
        }


    def run(self):
        job_start = time.perf_counter()


        # -------------------------
        # Dataset discovery timing
        # -------------------------
        discovery_start = time.perf_counter()
        datasets = self.discover_datasets()
        discovery_time = time.perf_counter() - discovery_start


        log.info("Startup | discovery: %.4fs | datasets: %d", discovery_time, len(datasets))

        results = []

        for path in datasets:
            dataset_start = time.perf_counter()

            rel_path = path.relative_to(self.root).as_posix()

            # -------------------------
            # Skip datasets already in achieved/
            # -------------------------
            if (self.achieved / path.relative_to(self.landing)).exists():
                log.info(
                    "%s | already exists in achieved - skipping",
                    rel_path,
                )

                results.append({
                    "path": rel_path,
                    "status": "skipped",
                    "reason": "already_achieved",
                    "data": None,
                    "metadata": None,
                })

                continue

            result = {
                "path": rel_path,
                "crs": None,
                "data": None,
                "metadata": None,
            }

            extraction_time = 0.0
            validation_time = 0.0

            # -------------------------
            # Extraction
            # -------------------------
            extraction_start = time.perf_counter()

            try:
                if path.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                    self.list_spatial_layers(path)

                data = self.extract_dataset(path)
            except Exception as exc:
                extraction_time = time.perf_counter() - extraction_start

                errors = [f"Extraction failed: {exc}"]
                warnings = []

            else:
                extraction_time = time.perf_counter() - extraction_start

                validation_start = time.perf_counter()

                errors, warnings = self.prevalidate(data, path)

                validation_time = time.perf_counter() - validation_start

                if (
                    path.suffix.lower() in GEOSPATIAL_EXTENSIONS
                    and hasattr(data, "crs")
                    and data.crs is not None
                ):
                    result["crs"] = data.crs.to_string()

            # -------------------------
            # Validation results
            # -------------------------
            result["errors"] = errors
            result["warnings"] = warnings

            for warning in warnings:
                log.warning("%s: %s", rel_path, warning)

            # -------------------------
            # Action
            # -------------------------
            action_start = time.perf_counter()

            if errors:
                for error in errors:
                    log.error("%s: %s", rel_path, error)

                self.reject_dataset(path)
                result["status"] = "rejected"

            else:
                result["metadata"] = self.build_metadata(path)
                result["status"] = "valid"
                result["data"] = data

            action_time = time.perf_counter() - action_start

            # -------------------------
            # Total dataset time
            # -------------------------
            dataset_time = time.perf_counter() - dataset_start

            result["timing"] = {
                "extraction_seconds": round(extraction_time, 4),
                "validation_seconds": round(validation_time, 4),
                "action_seconds": round(action_time, 4),
                "total_seconds": round(dataset_time, 4),
            }

            log.info(
                "%s | extraction: %.4fs | validation: %.4fs | "
                "action: %.4fs | total: %.4fs",
                rel_path,
                extraction_time,
                validation_time,
                action_time,
                dataset_time,
            )

            results.append(result)

        # -------------------------
        # Total job time
        # -------------------------
        total_time = time.perf_counter() - job_start

        log.info(
            "Prevalidation job completed in %.4fs",
            total_time,
        )

        return results

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    Prevalidationjob().run()