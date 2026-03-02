# Next Phase Runbook (Tier A Pilot)

## What you have
- `generation_config_tierA.yaml` — exported from your reviewed spreadsheet spec
- `schema_tierA.json` — machine-readable table schemas
- `prompt_bundle_tierA.json` — distilabel / DeepEval / QA prompts and rules
- `starter_generator_tierA.py` — starter pilot data generator (CSV + Parquet)

## Step 1 — Install basics
```bash
pip install pandas pyyaml openpyxl
# optional for parquet
pip install pyarrow
```

## Step 2 — Generate a quick pilot (10k sessions)
```bash
python starter_generator_tierA.py --config generation_config_tierA.yaml --outdir pilot_v1 --pilot-sessions 10000
```

## Step 3 — Review outputs
Check:
- `pilot_v1/row_counts_summary.csv`
- `pilot_v1/quick_summary.txt`
- `pilot_v1/samples/*_sample.csv`

## Step 4 — Paste sample rows into workbook `20_SAMPLES`
Copy 100–500 rows/table (not full data) for visual QA.

## Step 5 — Iterate realism before scaling
Tune weights in the spreadsheet (`01_CONFIG`, `02_DISTRIBUTIONS`) → re-export → regenerate.

## Step 6 — Full Tier A (when pilot looks good)
```bash
python starter_generator_tierA.py --config generation_config_tierA.yaml --outdir tierA_full --pilot-sessions 80000
```

## Next after this
- Distilabel pipeline on `menu_items` (`P1/P2/P3`)
- DeepEval semantic gates (`J1/J2`)
- Label Studio review sample (from `50_QA_SAMPLING_PLAN`)
