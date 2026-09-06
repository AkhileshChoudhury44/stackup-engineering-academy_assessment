# Python Environment Setup

Detailed Python setup for the assessment.

---

## Why a virtual environment?

A virtual environment isolates the assessment's Python packages from your system Python. Without it:
- Versions can conflict with other projects on your machine
- You may need admin rights to install packages
- Uninstalling becomes messy

**Always use a virtual environment for this assessment.**

---

## Step 1 — Verify Python version

```bash
python --version
```

You need Python **3.9 or higher**. If your version is lower:

| Platform | How to upgrade |
|---|---|
| Windows | Download latest from https://python.org/downloads — tick "Add to PATH" |
| Mac | `brew install python@3.11` |
| Linux | `sudo apt install python3.11 python3.11-venv` |

If `python --version` returns nothing or "command not found":
- Windows: Python isn't in your PATH — reinstall and tick the PATH option
- Mac/Linux: try `python3 --version` instead — use `python3` everywhere below

---

## Step 2 — Create a virtual environment

In the assessment repo folder:

```bash
python -m venv venv
```

This creates a `venv/` directory containing an isolated Python installation.

---

## Step 3 — Activate the environment

### Mac / Linux
```bash
source venv/bin/activate
```

### Windows (Command Prompt)
```cmd
venv\Scripts\activate.bat
```

### Windows (PowerShell)
```powershell
venv\Scripts\Activate.ps1
```

If PowerShell blocks the script with "execution policy" error:
```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Confirm activation

Your prompt should now show `(venv)`:
```
(venv) $
```

`which python` (Mac/Linux) or `where python` (Windows) should point to inside the `venv/` folder.

---

## Step 4 — Upgrade pip

```bash
pip install --upgrade pip setuptools wheel
```

---

## Step 5 — Install requirements

```bash
pip install -r requirements.txt
```

This installs:

| Package | Purpose |
|---|---|
| pandas, numpy | Data manipulation |
| pyspark | Big data processing |
| kafka-python | Streaming pipeline |
| apache-airflow | Pipeline orchestration |
| duckdb, sqlalchemy | SQL engine |
| pyarrow | Parquet read/write |
| great-expectations | Optional DQ library |
| pytest | Testing framework |

Installation takes 3–5 minutes on first run.

---

## Step 6 — Verify installation

```bash
python -c "
import pandas as pd
import pyspark
import kafka
import airflow
import duckdb
import pyarrow

print('All packages installed successfully')
print(f'Pandas:   {pd.__version__}')
print(f'PySpark:  {pyspark.__version__}')
print(f'Airflow:  {airflow.__version__}')
print(f'DuckDB:   {duckdb.__version__}')
"
```

---

