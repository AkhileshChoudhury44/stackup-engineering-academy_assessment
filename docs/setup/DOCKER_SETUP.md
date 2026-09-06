# Docker Setup

Docker runs the Kafka cluster, Airflow scheduler, and PostgreSQL database used by Tasks 3.2, 3.3, and 4.1.

---

## Why Docker?

Without Docker, you would need to manually install and configure:
- Apache Kafka + Zookeeper
- Apache Airflow + scheduler
- PostgreSQL server

That's hours of setup per machine. Docker spins all of them up with one command.

---

## Step 1 — System requirements

| Resource | Minimum | Recommended |
|---|---|---|
| RAM | 8 GB total | 16 GB total |
| Available RAM for Docker | 4 GB | 6+ GB |
| Disk space | 15 GB free | 25 GB free |
| CPU cores | 2 | 4+ |
| OS | Win 10 Pro/Home (64-bit), macOS 10.15+, Ubuntu 20.04+ | latest |

Check your free disk space:

| Platform | Command |
|---|---|
| Windows | `wmic logicaldisk get size,freespace,caption` |
| Mac | `df -h` |
| Linux | `df -h` |

---

## Step 2 — Install Docker Desktop

### Windows

1. Download from https://www.docker.com/products/docker-desktop/
2. Run **Docker Desktop Installer.exe**
3. When prompted, ensure these options are ticked:
   - ✅ **Use WSL 2 instead of Hyper-V** (recommended)
   - ✅ **Add shortcut to desktop**
4. Click **Install**
5. Restart your machine
6. Launch Docker Desktop from the Start menu
7. Accept the service agreement
8. Wait for the whale icon in your taskbar to stop animating — Docker is ready when it's solid

### Mac

1. Download the right installer for your Mac:
   - **Apple Silicon (M1/M2/M3):** Docker Desktop for Mac (Apple Silicon)
   - **Intel:** Docker Desktop for Mac (Intel Chip)
2. Open the `.dmg` file and drag Docker to Applications
3. Launch Docker from Applications
4. Authenticate when prompted
5. Wait for the whale icon in the menu bar to stop animating

### Linux (Ubuntu / Debian)

```bash
# Update package index
sudo apt update

# Install Docker Engine and Compose plugin
sudo apt install -y docker.io docker-compose-plugin

# Add yourself to the docker group (so you don't need sudo)
sudo usermod -aG docker $USER

# Apply group change immediately (or log out and back in)
newgrp docker

# Start Docker service
sudo systemctl enable --now docker
```

---

## Step 3 — Verify install

```bash
docker --version
# Docker version 24.0.x or higher

docker compose version
# Docker Compose version v2.x.x

docker run hello-world
# Should download and run a tiny test container
```

If `hello-world` runs and prints a welcome message, Docker is correctly installed.

---

## Step 4 — Allocate resources

Docker uses default resource limits that are often too low for this assessment.

### Windows / Mac

1. Open Docker Desktop
2. Click the gear icon (Settings)
3. Go to **Resources**
4. Set:
   - **CPUs:** at least 2 (4 recommended)
   - **Memory:** at least 6 GB
   - **Swap:** 1 GB
   - **Disk image size:** at least 30 GB
5. Click **Apply & Restart**

### Linux

No resource limits by default — Docker uses what's available on the host.

---

## Step 5 — Start assessment services

From the assessment repo root:

```bash
cd stackup-engineering-academy_assessment
docker compose up -d
```

The `-d` flag runs containers in the background ("detached" mode).

### First-run timing

Initial start takes **5–10 minutes** because Docker downloads container images. Subsequent starts take ~30 seconds.

### Watch progress

```bash
docker compose logs -f
```

Press `Ctrl+C` to stop watching (containers keep running).

Look for these "ready" messages:
- Kafka: `[KafkaServer id=1] started`
- Postgres: `database system is ready to accept connections`
- Airflow: `Listening at: http://0.0.0.0:8080`

---

## Step 6 — Verify all services are running

```bash
docker compose ps
```

Expected output (state should be `Up` or `healthy` for all):

```
NAME                              STATUS              PORTS
presight-airflow-scheduler        Up X seconds
presight-airflow-webserver        Up X seconds        0.0.0.0:8081->8080/tcp
presight-kafka                    Up X seconds        0.0.0.0:9092->9092/tcp
presight-kafka-ui                 Up X seconds        0.0.0.0:8080->8080/tcp
presight-postgres                 Up X seconds        0.0.0.0:5432->5432/tcp
presight-zookeeper                Up X seconds        2181/tcp
```

### Test the UIs

Open in your browser:

| URL | Service | Login |
|---|---|---|
| http://localhost:8081 | Airflow | admin / admin |
| http://localhost:8080 | Kafka UI | — |

---

## Step 7 — Common commands cheat-sheet

| Command | Purpose |
|---|---|
| `docker compose up -d` | Start all services in background |
| `docker compose stop` | Stop all services (keep data) |
| `docker compose start` | Resume previously stopped services |
| `docker compose restart <service>` | Restart one service |
| `docker compose down` | Stop & remove containers (keep data volumes) |
| `docker compose down -v` | Stop & remove everything including data |
| `docker compose logs -f <service>` | Tail logs for a service |
| `docker compose ps` | List all running services |
| `docker exec -it <name> bash` | Open a shell inside a container |
| `docker stats` | Live CPU/memory usage per container |

---

