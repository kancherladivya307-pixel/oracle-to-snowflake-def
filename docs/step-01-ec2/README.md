# Step 01 — EC2 Application Server

This step sets up the AWS EC2 instance that will run the data platform components:
- Apache Airflow
- Python ingestion/processing code
- Connectivity to AWS S3 and Snowflake

## Sub-steps
- 01. Create SSH key pair
- 02. Create IAM role for EC2
- 03. Create Security Group
- 04. Launch EC2 instance
- 05. SSH into EC2 and install base packages


01. Create SSH key pair — def-keypair.pem downloaded and stored securely.
02. Created IAM role `def-ec2-role` with S3 and CloudWatch permissions.
03. Created security group `def-ec2-sg` allowing SSH (port 22) from my IP only.
04. Launched EC2 instance
05.Update Operating System-System packages verified and up to date using dnf.
06. Install Git and basic utilities-Git installed and verified using git --version.
07.  Python virtual environment
On Amazon Linux 2023, Python virtual environment support is included by default.
  Created and activated a Python virtual environment using python3 -m venv.
08.Apache Airflow installation- Installed Apache Airflow in a Python virtual environment and verified using `airflow version`.
09.# Step 03 — Apache Airflow Setup
 Initialize Airflow
 Initialized Airflow metadata database and generated required directories.
10.create Airflow admin user
  Created an admin user for accessing the Airflow UI.







