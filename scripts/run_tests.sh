#!/usr/bin/env sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_dir"
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 scripts/validate_project.py

