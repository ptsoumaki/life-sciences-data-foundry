# Local Development & Environment Setup Guide 🛠️

This document provides step-by-step instructions for establishing a local development station, configuring required runtimes (Python 3.11 and Java 17), installing dependencies, and bootstrapping environment variables.

---

## 📋 Prerequisites & Toolchain Requirements

Ensure the following core tools are installed on your host system:

| Tool | Required Version | Purpose |
| :--- | :--- | :--- |
| **Python** | `3.11` (`>=3.10, <3.13`) | PySpark transformations, Great Expectations data contracts, MLflow tracking |
| **Java / OpenJDK** | `17` (`>=11` required) | JVM runtime required by Apache Spark / PySpark and Nextflow |
| **Git** | `>=2.30` | Source control and repository versioning |
| **Docker Engine** | Latest Stable | Containerized Nextflow process execution context |
| **Terraform** | `>=1.5.0` | Declarative IaC toolchain for cloud infrastructure |
| **AWS CLI** | `v2` | Cloud authentication and resource verification |

---

## 🐍 1. Python Virtual Environment (`.venv`) Setup

### Step A: Create and Activate Virtual Environment

**On Windows (PowerShell):**
```powershell
# 1. Create virtual environment using Python 3.11
py -3.11 -m venv .venv

# 2. Activate virtual environment
.\.venv\Scripts\Activate.ps1
```

**On Linux / macOS:**
```bash
# 1. Create virtual environment using Python 3.11
python3.11 -m venv .venv

# 2. Activate virtual environment
source .venv/bin/activate
```

---

### Step B: Install Project Dependencies

With `.venv` activated, install the package in editable mode with development dependencies:

```bash
# Upgrade pip to latest version
pip install --upgrade pip

# Install package and dev dependencies from pyproject.toml
pip install -e ".[dev]"
```

To install optional agentic AI dependencies (`langgraph`, `mcp`):
```bash
pip install -e ".[dev,agentic]"
```

---

## ☕ 2. Java Runtime (`JAVA_HOME`) Configuration

Apache Spark 3.5+ and PySpark require a Java Virtual Machine (**Java 17 recommended**).

### Windows (PowerShell)
```powershell
# Set JAVA_HOME to your OpenJDK 17 installation directory
$env:JAVA_HOME="C:\Program Files\Microsoft\jdk-17.0.20.8-hotspot"
$env:Path="$env:JAVA_HOME\bin;$env:Path"

# Verify Java runtime
java -version
```

### Linux / macOS
```bash
# Export JAVA_HOME (adjust path to match your system JDK location)
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Verify Java runtime
java -version
```

---

## ⚙️ 3. Environment Variable Bootstrapping

The platform provides automated bootstrap scripts to set runtime parameters (`ENVIRONMENT`, `AWS_REGION`, `TF_VAR_*`):

1. **Create Local `.env` File:**
   ```bash
   cp .env.example .env
   ```

2. **Run the Bootstrap Initializer:**

   - **Windows (PowerShell):**
     ```powershell
     .\scripts\bootstrap.ps1
     ```

   - **POSIX (Linux / macOS / WSL):**
     ```bash
     source scripts/bootstrap.sh
     ```

---

## 🧪 4. Environment Verification

To verify that all components are configured correctly:

```bash
# 1. Verify PySpark & Delta Lake execution
python -c "import pyspark, delta; print(f'PySpark: {pyspark.__version__}, Delta: {delta.__version__}')"

# 2. Run unit test suite
pytest tests/unit/ -v
```
