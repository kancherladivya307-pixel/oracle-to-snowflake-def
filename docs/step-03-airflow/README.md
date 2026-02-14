Step 03 — Airflow Setup on EC2
Objective:
Deploy Apache Airflow on AWS EC2 and securely access the UI using SSH port forwarding.

**1. Environment Setup**

EC2 instance launched in public subnet

Security Group configured:

SSH (22) from personal IP

No public exposure of Airflow UI (port 8080 not publicly required)

Python virtual environment created

AIRFLOW_HOME set to:~/def-platform/airflow

**2. Airflow Initialization**
export AIRFLOW_HOME=~/def-platform/airflow
airflow db init


Disabled example DAGs in airflow.cfg:

load_examples = False

**3. Create Admin User**
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

**4. Start Services**
airflow webserver --host 0.0.0.0 --port 8080 &
airflow scheduler &


**Verified:**

curl -I http://127.0.0.1:8080

**5. Secure UI Access via SSH Port Forwarding **

Instead of exposing port 8080 publicly, SSH tunneling was used.

PuTTY Configuration

Connection → SSH → Tunnels:

Source port: 8080

Destination: 127.0.0.1:8080

Type: Local

Access Airflow UI via:

http://localhost:8080


No public exposure of Airflow UI

No dependency on public IP changes

Secure encrypted access

**6. Verification**

Webserver running (Gunicorn)

Scheduler running

Ports 8080 and 8793 active

Airflow UI accessible

Architecture (Current State):
Laptop Browser (localhost:8080)
        ↓
SSH Tunnel (Port 22)
        ↓
EC2 Instance (Amazon Linux)
        ↓
Airflow Webserver (Gunicorn)
        ↓
Airflow Scheduler
        ↓
SQLite Metadata DB

**Step 03.04 – DAG Execution & Validation**
Objective

Validate Airflow orchestration by creating and executing a custom DAG with multiple sequential Bash tasks.

DAG Created

hello_def.py

Located in:

~/def-platform/airflow/dags/

Browser (localhost:8080)
        ↓
SSH Tunnel (Port 22)
        ↓
EC2 Instance
        ↓
Airflow Webserver (Gunicorn)
        ↓
Scheduler
        ↓
SequentialExecutor
        ↓
BashOperator (subprocess)



