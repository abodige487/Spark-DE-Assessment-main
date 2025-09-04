**Spark-DE-Assessment**

A small, self-contained data-engineering exercise using PySpark (Spark Connect).
It reads JSONL input, applies transformations defined in assessment/, and writes a JSONL output for verification.

Spark-DE-Assessment-main/

├─ assessment/
|
│  ├─ __init__.py
│  ├─ __main__.py        # Entry point: python -m assessment
│  ├─ helpers.py         # Reusable utilities
│  └─ transform.py       # Core transformations
|
├─ data/
│  ├─ edges.jsonl        # Input data
│  └─ solution.jsonl     # Expected solution (for local check)
|
├─ artifacts/            # (ignored) 
├─ docker-compose.yaml   # Spark Connect server (apache/spark:4.0.0)
├─ requirements.txt      # Python dependencies
└─ README.md

**Prerequisites**

Python 3.13

Docker & Docker Compose (for Spark Connect server)

Git (optional, for version control)

The project uses Spark Connect so you don't need local Hadoop/winutils.exe.

**Create and activate a virtual environment**

python -m venv myenv
myenv/Scripts/activate

**Install dependencies**

pip install -r requirements.txt

**Start Spark Connect (Docker)**

docker compose up -d

**Run the assessment**

python -m assessment

