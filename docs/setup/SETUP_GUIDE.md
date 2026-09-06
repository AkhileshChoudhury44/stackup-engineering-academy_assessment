# Complete Setup Guide

> **Read this before starting the assessment.** This guide covers every tool you need across all four pillars.

---

## ⏱️ Time required

| Component | Setup time |
|---|---|
| Python environment | 10 min |
| Git | 5 min |
| Docker Desktop | 20 min |
| Database (DuckDB / PostgreSQL) | 5 min |
| Power BI Desktop | 15 min |
| IDE (VS Code) | 10 min |
| Verifying everything works | 10 min |
| **Total** | **~75 min** |

---

## 📋 Setup order

Follow this order — each step builds on the previous:

1. [Install Python 3.9+](#1-python-39)
2. [Install Git](#2-git)
3. [Install Docker Desktop](#3-docker-desktop)
4. [Install a database engine](#4-database-engine)
5. [Install Power BI Desktop](#5-power-bi-desktop) (or alternative)
6. [Install VS Code or your preferred IDE](#6-ide-vs-code-recommended)
7. [Clone the assessment repo](#7-clone-the-repo)
8. [Install Python dependencies](#8-python-dependencies)
9. [Start Docker services](#9-start-docker-services)
10. [Verify everything works](#10-verify-everything)

---

## 1. Python 3.9+

Python is the primary language used in this assessment.

### Install

**Windows:**
- Download from https://www.python.org/downloads/windows/
- ⚠️ **Important:** Tick **"Add Python to PATH"** during installation
- Choose **Customize installation** → tick all optional features → **Install for all users**

**Mac:**
```bash
# Using Homebrew (recommended)
brew install python@3.11
```
Or download from https://www.python.org/downloads/macos/

**Linux (Ubuntu/Debian):**
```bash
sudo apt update
sudo apt install python3.11 python3.11-venv python3-pip
```

### Verify
```bash
python --version    # Should show 3.9 or higher
pip --version
```

---

## 2. Git

Required for cloning the assessment repo and submitting your work.

### Install

**Windows:**
- Download from https://git-scm.com/download/win
- Run installer with defaults
- ✅ Tick **"Git from the command line and 3rd-party software"**

**Mac:**
```bash
brew install git
# OR install Xcode Command Line Tools:
xcode-select --install
```

**Linux:**
```bash
sudo apt install git
```

### Configure
```bash
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

### Verify
```bash
git --version
```

---

## 3. Docker Desktop

Required for Kafka (Task 3.2), Airflow (Task 3.3), and the Docker tasks (Task 4.1).

### System requirements

| Resource | Minimum |
|---|---|
| RAM | 8 GB (4 GB+ available to Docker) |
| Disk | 15 GB free |
| OS | Windows 10/11 Pro/Home, macOS 10.15+, Ubuntu 20.04+ |

### Install

**Windows / Mac:**
1. Download Docker Desktop: https://www.docker.com/products/docker-desktop/
2. Run installer with defaults
3. Restart your machine
4. Launch Docker Desktop and wait for the whale icon to turn green

**Linux:**
```bash
sudo apt install docker.io docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```

### Allocate resources

1. Open Docker Desktop → **Settings → Resources**
2. **Memory:** at least 6 GB (recommended for Airflow + Kafka)
3. **CPUs:** at least 2
4. **Disk image size:** at least 20 GB
5. Click **Apply & Restart**

### Verify
Run below command on Docker terminal:
```bash
docker --version
docker compose version
docker run hello-world    # Downloads a test image
```

Output of above  would be like:
Hello from Docker!
This message shows that your installation appears to be working correctly.
### Troubleshooting

| Problem | Fix |
|---|---|
| "Docker Desktop starting…" stuck on Windows | Open PowerShell as Admin → `wsl --shutdown` → restart Docker |
| "Cannot connect to Docker daemon" | Make sure Docker Desktop is running, not just installed |
| Slow on Windows | Switch to WSL 2 backend in Settings → General |

---

## 4. Database engine

You need a SQL engine to complete Tasks 1.2, 2.1, and 2.3.

**Choose one** (DuckDB recommended — easiest):

### Option A — DuckDB (recommended for assessment)

Lightweight, embedded SQL engine. No server setup.

DuckDB generally requires Python 3.8+

```bash
pip install duckdb
```

or

```bash
python3 -m pip install duckdb
```

Test it by running below command on terminal:
```python
python -c "import duckdb; print(duckdb.query('SELECT 42').to_df())"
```

DuckDB also has a CLI:
- Download from https://duckdb.org/docs/installation/

### Option B — PostgreSQL

Already running inside Docker (via `docker-compose.yml`). Connect using any SQL client.

**Connection details:**
- Host: `localhost`
- Port: `5432`
- Database: `airflow` (or create your own)
- User: `presight`
- Password: `presight123`

### Option C — SQLite

Pre-installed on Mac and Linux. For Windows, download from https://www.sqlite.org/download.html

```bash
sqlite3 --version
```

---

## 5. Power BI Desktop

Required for Task 2.4 — Dashboard Design.

### Install (Windows only)

1. Download from https://powerbi.microsoft.com/desktop/
2. Run installer with defaults
3. Launch Power BI Desktop — no Microsoft account needed for local reports

### Alternatives for Mac / Linux users

Power BI Desktop is Windows-only. If you're on Mac or Linux, use one of these:

| Alternative | Output format | Setup |
|---|---|---|
| **Power BI Service** (web) | PDF export | Sign up at https://app.powerbi.com |


Any of the above are accepted for grading. Place output in `outputs/` and reference it in your PR.

---

## 6. IDE — VS Code (recommended)

Any editor works, but VS Code is widely supported and has excellent Python / SQL / Docker extensions.

### Install
Download from https://code.visualstudio.com/

### Recommended extensions

Install via the Extensions panel (`Ctrl+Shift+X`):

| Extension | Purpose |
|---|---|
| Python (Microsoft) | Python syntax, debugging, linting |
| Pylance (Microsoft) | Python type checking |
| Jupyter | Notebook support if needed |
| Docker (Microsoft) | Manage Docker containers from VS Code |
| GitLens | Git history and blame |
| SQLTools | Run SQL queries against any DB |
| SQLTools DuckDB / PostgreSQL Driver | DB drivers |
| Better TOML | YAML / config syntax |
| Apache Airflow (Astronomer) | Airflow DAG support |

### Alternative IDEs

| IDE | Best for |
|---|---|
| PyCharm Community (free) | Pure Python development |
| DataGrip (paid) | Heavy SQL work |
| Jupyter Notebook | Data exploration |

---

## 7. Clone the repo

Before cloning the repo, set your Git hub token to clone the repo at your local using below steps:
Go to GitHub → Settings
Developer Settings → Personal access tokens
Generate new token
Select scope: repo (important for private repos)

<IMPORTANT:Save this generated token as it is one time visible in your GIt account>

When prompted after git clone:

Username → your GitHub username
Password → Paste TOKEN (not your GitHub password)

```bash
# Navigate to where you want to keep the project
cd ~/projects     # or wherever you keep code

# Clone
git clone https://github.com/Presight-AI/stackup-engineering-academy_assessment.git
cd stackup-engineering-academy_assessment

# Create your personal branch
git checkout -b candidate/your-name
```

### Verify
```bash
ls
```

You should see: `README.md`, `datasets/`, `starter_files/`, `tasks/`, `docker-compose.yml`, etc.

---



## 8. Python dependencies

Install the pyspark independently as this package have 500MB + size by following below stepes:

Step 1: Download Apache Spark
	1. Open browser
	2. Go to: https://spark.apache.org/downloads.html
	3. Select: 
Latest version
Package: Pre-built for Hadoop 3
	Download .tgz file
	
Step 2: Extract Spark
  

cd ~/Downloads
tar -xvzf spark-*.tgz

Step 3: Move Spark to a permanent location

mkdir ~/spark
mv spark-*/* ~/spark

Final structure should look like:
~/spark/ 
      ├── bin/ 
                        ├── conf/ 
                                       ├── jars/


Step 4: Set environment variables

echo 'export SPARK_HOME=~/spark' >> ~/.zshrc
echo 'export PATH=$SPARK_HOME/bin:$PATH' >> ~/.zshrc
source ~/.zshrc


Step 5: Validate Spark installation

spark-shell

If Spark starts → installation successful


### Create a virtual environment (recommended)

A virtual environment isolates project dependencies from your system Python.This shoudl be created once you moved to your local project directlry created while GIT clone.

```bash
# Create
python -m venv venv

# Activate
source venv/bin/activate            # Mac / Linux
venv\Scripts\activate               # Windows

# Verify activation — your prompt should now show (venv)
```

### Install requirements

```bash
pip install --upgrade pip setuptools wheel
pip install -r requirements.txt --no-cache-dir --default-timeout=300

```

### Install Pyspark inside the Virtual env 
pip install pyspark --no-deps
pip install py4j


### Final validation  
python -c "import pandas, numpy, duckdb, pyspark; print('All setup working ')"

### Final checklist (YOU ARE DONE)

Run below commands in your venv, if all executes without error your are good with setup.

spark-shell
python -c "import pyspark"
python -c "import pandas"
python -c "import duckdb"

⚠️ **PySpark requires Java** — see [Java setup](#java-setup-for-pyspark) below if PySpark fails to run.

### Verify
```bash
python -c "import pandas, pyspark, kafka, airflow; print('All imports OK')"
```

---

### Java setup (for PySpark)

PySpark requires Java 8, 11, or 17.

**Windows:**
- Download OpenJDK 17 from https://adoptium.net/
- Set `JAVA_HOME` environment variable to the install directory

**Mac:**
```bash
brew install openjdk@17
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 17)' >> ~/.zshrc
source ~/.zshrc
```

**Linux:**
```bash
sudo apt install openjdk-17-jdk
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
source ~/.bashrc
```

Verify:
```bash
java -version    # Should show 8, 11, or 17
echo $JAVA_HOME  # Should show the install path
```

---

## 9. Start Docker services

From the assessment repo root:

```bash
docker compose up -d
```

First start takes **5–10 minutes** to download images.

### Wait for services to be ready

```bash
docker compose ps
```

You should see six services all in `Up` or `healthy` state:

| Service | Port | URL |
|---|---|---|
| presight-zookeeper | 2181 | (internal only) |
| presight-kafka | 9092 | `localhost:9092` |
| presight-kafka-ui | 8080 | http://localhost:8080 |
| presight-postgres | 5432 | `localhost:5432` |
| presight-airflow-webserver | 8081 | http://localhost:8081 |
| presight-airflow-scheduler | — | (internal only) |

### Login credentials

| Service | URL | Username | Password |
|---|---|---|---|
| Airflow | http://localhost:8081 | `admin` | `admin` |
| Kafka UI | http://localhost:8080 | — | — |
| PostgreSQL | `localhost:5432` | `presight` | `presight123` |

---

## 10. Verify everything

Run through this checklist to confirm your environment is working:

### Python
```bash
python -c "
import pandas as pd
import pyspark
from kafka import KafkaProducer
from airflow.models import DAG
print('Python OK')
print('Pandas:', pd.__version__)
print('PySpark:', pyspark.__version__)
"
```

### Database
```bash
# DuckDB
python -c "import duckdb; print(duckdb.query(\"SELECT 'DuckDB works' as msg\").to_df())"

# OR PostgreSQL (if using Docker)
docker exec presight-postgres psql -U presight -d airflow -c "SELECT 'PostgreSQL works' AS msg;"
```

### Docker
```bash
docker compose ps        # All services Up
curl -s http://localhost:8081/health    # Airflow returns healthy
curl -s http://localhost:8080           # Kafka UI loads
```

### Kafka topic test
```bash
docker exec presight-kafka kafka-topics --bootstrap-server localhost:9092 --create --topic test.setup --partitions 1 --replication-factor 1
docker exec presight-kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec presight-kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic test.setup
```

### Spark test
```bash
python -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.createDataFrame([(1, 'a'), (2, 'b')], ['id', 'val'])
df.show()
spark.stop()
print('Spark OK')
"
```

### Datasets
```bash
ls -la datasets/
head datasets/projects.csv
head datasets/employees.csv
python -c "import json; print(len(json.load(open('datasets/transactions.json'))), 'transactions')"
```

---

## ✅ Setup complete

If all the above checks pass, you're ready to start the assessment.

Start with **Pillar 1 — Foundations**: `tasks/01_foundations/INSTRUCTIONS.md`

---



