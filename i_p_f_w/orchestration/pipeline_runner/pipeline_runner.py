import logging
from pathlib import Path

from i_p_f_w.ingestion.loader_job import Loader
from i_p_f_w.ingestion.prevalidation_job import Prevalidationjob
from i_p_f_w.metadata_manager.metadata_registry import MetadataRegistry
from i_p_f_w.data_profiling.data_profiling import run_profiling

log = logging.getLogger(__name__)


class PipelineRunner:
    def __init__(self, project_root=None):
        # <root>/data_engineering_layer/orchestration/dagster/pipeline_runner/pipeline_runner.py
        self.root = Path(project_root or Path(__file__).resolve().parents[3]).resolve()

    def run_metadata_registration(self, validation_results):
        log.info("Metadata registration started")
        metadata = [r["metadata"] for r in validation_results if r["status"] == "valid"]
        try:
            exit_code = MetadataRegistry().run(metadata)
        except Exception as exc:
            raise RuntimeError(f"Metadata registration failed: {exc}") from exc
        if exit_code != 0:
            raise RuntimeError("Metadata registration failed (MetadataRegistry returned a non-zero exit code)")
        log.info("Metadata registration completed")

    def run_prevalidation(self):
        log.info("Prevalidation started")
        try:
            results = Prevalidationjob(self.root).run()
        except Exception as exc:
            raise RuntimeError(f"Prevalidation failed: {exc}") from exc

        counts = {s: sum(r["status"] == s for r in results) for s in ("valid", "rejected", "skipped")}
        log.info("Prevalidation completed")
        log.info("%d datasets valid", counts["valid"])
        log.info("%d datasets rejected", counts["rejected"])
        log.info("%d datasets skipped (already achieved)", counts["skipped"])
        return results

    def run_loading(self, validation_results):
        valid = [r for r in validation_results if r["status"] == "valid"]
        log.info("Loading started (%d datasets)", len(valid))

        loader = Loader(self.root)
        failed = []
        for r in valid:
            try:
                rows = loader.run(r["metadata"], r["data"])
                log.info("Dataset %s loaded (%d rows)", r["path"], rows)
            except Exception as exc:
                failed.append(r["path"])
                log.error("Dataset %s failed to load: %s", r["path"], exc)

        log.info("Loading completed: %d loaded, %d failed", len(valid) - len(failed), len(failed))
        if failed:
            raise RuntimeError(f"Loading failed for {len(failed)} of {len(valid)} dataset(s): {failed}")
        return len(valid)

    def run_data_profiling(self):
        log.info("Data profiling started")

        try:
            summary = run_profiling()
        except Exception as exc:
            raise RuntimeError(f"Data profiling failed: {exc}") from exc

        completed = summary["completed"]
        failed = summary["failed"]
        missing = summary["missing_tables"]

        log.info("Data profiling completed: %d datasets", len(completed))

        if missing:
            log.warning("Datasets with missing raw_data tables: %s", missing)

        if failed:
            raise RuntimeError(
                f"Data profiling failed for {len(failed)} dataset(s): {failed}"
            )

        return summary


    def run(self):
        validation_results = self.run_prevalidation()
        self.run_metadata_registration(validation_results)
        self.run_loading(validation_results)
        return self.run_data_profiling()