# D2TH-big-data-etl-gg-maps

## Tech Stack
- **Docker**: Manages services like Airflow, Minio, Postgres, and MongoDB.
- **Airflow**: Orchestrates the ETL pipeline.
- **Minio**: Object storage for raw and processed data.
- **Postgres**: Relational database to store processed data.
- **MongoDB**: NoSQL database for testing queries and learning.
- **PowerBI**: Visualizes insights from the data.
- **Python**: Language used for scripting.

---

## Setup Guide

### 1. Install Docker
- Follow the instructions to install Docker from the [Docker website](https://www.docker.com/).

---

### 2. Running the Application

#### 2.1. Windows:
- Run the `docker_compose.bat` file.

#### 2.2. MacOS/Linux:
- Run the `docker_compose.sh` file.

---

### 3. Access Minio
- Open Minio in your browser at `localhost:9001`.
- **Login**: 
  - Username: `admin12345`
  - Password: `admin12345`
- **Create Access Keys**:
  - Navigate to "Access Keys" in the left menu and generate new access and secret keys.
  - Make sure to store these keys securely.

---

### 4. Configure `keys.json`
- Create a `keys.json` file in the `plugins` directory with the following content:

```json
{
  "access_key": "replace your access_key",
  "secret_key": "replace your secret_key",
  "mongodb_user": "admin",
  "mongodb_password": "admin",
  "postgres_user": "airflow",
  "postgres_password": "airflow"
}
```

### 5. Access Airflow
- Open Airflow in your browser at `localhost:8080`
- `Run the Pipeline`: Click the `Run` button on the DAG to execute the workflow.