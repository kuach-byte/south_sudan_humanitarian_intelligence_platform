"""Read the metadata YAML that sits next to each data file in data/landing.

A sidecar shares the data file's stem (``jiaf_south_sudan_2025.csv`` + ``jiaf_south_sudan_2025.yml``) and holds
only descriptive source metadata, which maps 1:1 to columns of metadata_manager.source:

    source: "Humanitarian partners"
    source_id: "bfdf0ecb-0ee1-4813-bfa2-0cf3b5b8f9c5"
    contributor: "OCHA South Sudan"
    time_period_begin: 01/01/2025        # DD/MM/YYYY (ISO YYYY-MM-DD also accepted)
    time_period_end: 31/12/2025

Dataset name, source.type/path/file_type and the target are NOT in the YAML; they come from the data file through
Prevalidationjob.build_metadata(), so there is a single definition of the metadata dict that MetadataRegistry.run() takes.
This module only parses and validates; it never touches the database.

Run directly (parse only, nothing is registered):  python -m i_p_f_w.metadata_manager.yaml_metadata_parser
"""
import logging
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

log = logging.getLogger(__name__)

YAML_SUFFIXES = (".yml", ".yaml")
REQUIRED_FIELDS = ("source", "source_id", "contributor", "time_period_begin", "time_period_end")
TEXT_FIELDS = ("source", "source_id", "contributor")
DATE_FIELDS = ("time_period_begin", "time_period_end")
DATE_FORMAT = "%d/%m/%Y"


class YamlMetadataError(Exception):
    pass


def _text(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("must be a non-empty string (quote it if it looks like a number)")
    return value.strip()


def _timestamp(value):
    """DD/MM/YYYY text, or a YAML date/datetime, as a timezone-aware UTC datetime (source.time_period_* is timestamptz)."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            return datetime.strptime(value.strip(), DATE_FORMAT).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"must be a valid date written DD/MM/YYYY, got {value!r}")


def parse_sidecar(yaml_path):
    """Return the validated source fields from one YAML file. Raises YamlMetadataError naming the file and every problem."""
    yaml_path = Path(yaml_path)
    where = yaml_path.as_posix()
    try:
        doc = yaml.safe_load(yaml_path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError) as exc:
        raise YamlMetadataError(f"{where}: cannot read file: {exc}") from exc
    except yaml.YAMLError as exc:
        raise YamlMetadataError(f"{where}: malformed YAML: {' '.join(str(exc).split())}") from exc
    if not isinstance(doc, dict):
        raise YamlMetadataError(f"{where}: expected 'key: value' pairs at the top level")

    problems = []
    unknown = sorted(str(k) for k in doc if k not in REQUIRED_FIELDS)
    if unknown:
        problems.append(f"unknown field(s) {unknown} (allowed: {list(REQUIRED_FIELDS)})")
    fields = {}
    for name in REQUIRED_FIELDS:
        if doc.get(name) is None:
            problems.append(f"missing required field '{name}'")
            continue
        try:
            fields[name] = _text(doc[name]) if name in TEXT_FIELDS else _timestamp(doc[name])
        except ValueError as exc:
            problems.append(f"'{name}' {exc}")
    begin, end = fields.get("time_period_begin"), fields.get("time_period_end")
    if begin and end and begin > end:
        problems.append("time_period_begin is later than time_period_end")
    if problems:
        raise YamlMetadataError(f"{where}: " + "; ".join(problems))
    return fields


def sidecars_for(data_path):
    """Every YAML file next to a data file (same folder, same stem). Used to move them together with the data file."""
    data_path = Path(data_path)
    return sorted(p for p in data_path.parent.iterdir()
                  if p.is_file() and p.stem == data_path.stem and p.suffix.lower() in YAML_SUFFIXES)


def sidecar_for(data_path):
    """The YAML file next to a data file (same folder, same stem), or None. Two candidates is an error, not a guess."""
    data_path = Path(data_path)
    found = sidecars_for(data_path)
    if len(found) > 1:
        raise YamlMetadataError(f"{data_path.as_posix()}: several metadata files found: {[p.name for p in found]}")
    return found[0] if found else None


class YamlMetadataParser:
    def __init__(self, project_root=None, job=None):
        """`job` is anything with .landing, .discover_datasets() and .build_metadata(path) (a Prevalidationjob);
        it is created lazily so importing this module stays light."""
        self.root = Path(project_root or Path(__file__).resolve().parents[2]).resolve()
        self._job = job

    @property
    def job(self):
        if self._job is None:
            from i_p_f_w.ingestion.prevalidation_job import Prevalidationjob
            self._job = Prevalidationjob(self.root)
        return self._job

    def collect(self):
        """Walk data/landing for YAML files. Returns (metadata, errors): metadata is a list of dicts ready for
        MetadataRegistry.run(); errors is a list of messages, one per rejected file. Nothing is chosen by file
        timestamps: any ambiguity (same dataset name twice, two YAMLs for one dataset) rejects every file involved."""
        job = self.job
        errors, metadata = [], []
        data_files = job.discover_datasets()
        by_folder_stem = defaultdict(list)
        for p in data_files:
            by_folder_stem[(p.parent, p.stem)].append(p)
        name_count = Counter(p.stem for p in data_files)

        # same order as the data files (chronological within a folder), so registration follows it too
        order = {(p.parent, p.stem): i for i, p in enumerate(data_files)}
        yml_files = sorted((p for p in job.landing.rglob("*") if p.is_file() and p.suffix.lower() in YAML_SUFFIXES),
                           key=lambda p: (order.get((p.parent, p.stem), len(order)), p))
        yml_count = Counter((p.parent, p.stem) for p in yml_files)

        for yml in yml_files:
            label = yml.as_posix()
            data = by_folder_stem.get((yml.parent, yml.stem), [])
            if not data:
                log.warning("%s: no data file next to it in landing - skipped", label)
                continue
            if yml_count[(yml.parent, yml.stem)] > 1:
                errors.append(f"{label}: more than one metadata file for dataset '{yml.stem}'")
                continue
            if len(data) > 1:
                errors.append(f"{label}: several data files share the name '{yml.stem}': {[p.name for p in data]}")
                continue
            if name_count[yml.stem] > 1:
                others = [p.as_posix() for p in data_files if p.stem == yml.stem and p != data[0]]
                errors.append(f"{label}: dataset name '{yml.stem}' is also used by {others}; names must be unique")
                continue
            try:
                fields = parse_sidecar(yml)
            except YamlMetadataError as exc:
                errors.append(str(exc))
                continue
            m = job.build_metadata(data[0])
            m["source"].update(fields)
            metadata.append(m)
        log.info("YAML metadata: %d valid, %d rejected", len(metadata), len(errors))
        return metadata, errors

    def enrich(self, metadata):
        """Merge each dataset's sidecar (if any) into its metadata dict in place. Returns a list of error messages.
        A dataset without a sidecar is left as is (warning); a present but invalid sidecar is an error."""
        errors = []
        for m in metadata:
            data_path = self.root / m["source"]["path"]
            try:
                yml = sidecar_for(data_path)
                if yml is None:
                    log.warning("%s: no metadata YAML next to the data file; descriptive source fields left unchanged",
                                m["source"]["path"])
                    continue
                m["source"].update(parse_sidecar(yml))
            except YamlMetadataError as exc:
                errors.append(str(exc))
        return errors


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    found, problems = YamlMetadataParser().collect()
    for p in problems:
        log.error("%s", p)
    sys.exit(1 if problems else 0)