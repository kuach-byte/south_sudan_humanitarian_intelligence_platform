from pathlib import Path
from typing import Any

import yaml

import logging

logger = logging.getLogger(__name__)

class YamlParser:
    """
    Generic YAML parser.

    Responsible only for reading YAML files and converting
    their contents into Python dictionaries.
    """

    def parse_file(
        self,
        file_path: str | Path,
    ) -> dict[str, Any]:

        path = Path(file_path)

        with path.open("r", encoding="utf-8") as file:
            parsed = yaml.safe_load(file)

        if not isinstance(parsed, dict):
            raise ValueError(
                f"YAML file '{path}' must contain a mapping."
            )

        return parsed

    def parse_directory(
        self,
        directory: str | Path,
    ) -> list[tuple[Path, dict[str, Any]]]:

        root = Path(directory)

        if not root.is_dir():
            raise ValueError(
                f"Metadata directory not found: '{root}'"
            )

        results = []

        for file_path in sorted(root.rglob("*.yml")):
            try:
                parsed = self.parse_file(file_path)
                results.append((file_path, parsed))

            except (yaml.YAMLError, ValueError, OSError) as exc:
                logger.error(
                    "Failed to parse YAML file '%s': %s",
                    file_path,
                    exc,
                )
                continue

        return results