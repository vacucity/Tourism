# Databricks notebook source
# MAGIC %md
# MAGIC # 00 Setup: schema, Volume, and landing files
# MAGIC
# MAGIC Run this notebook from the Git folder. It creates `workspace.nps_tourism` and its `landing` Volume,
# MAGIC then copies every file under the repository's `data/landing/` into the Volume, keeping the
# MAGIC `<source>/pull_date=YYYY-MM-DD/` layout that `01_bronze` expects. Safe to rerun: files are overwritten.

# COMMAND ----------

import shutil
from collections import Counter
from pathlib import Path

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.nps_tourism")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.nps_tourism.landing")

# The working directory of a notebook is the folder that contains it: <repo>/databricks/notebooks
REPO_LANDING = Path.cwd().parents[1] / "data" / "landing"
VOLUME = Path("/Volumes/workspace/nps_tourism/landing")
assert REPO_LANDING.is_dir(), f"Not found: {REPO_LANDING}. Open this notebook from the Git folder."

# COMMAND ----------

per_source = Counter()
for src in sorted(REPO_LANDING.rglob("*")):
    if src.is_file():
        rel = src.relative_to(REPO_LANDING)
        dst = VOLUME / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dst)
        per_source[rel.parts[0]] += 1

for source, n in sorted(per_source.items()):
    print(f"{source}: {n} files")
print(sum(per_source.values()), "files copied to", VOLUME)
