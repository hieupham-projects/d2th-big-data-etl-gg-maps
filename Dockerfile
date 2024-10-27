FROM apache/airflow:2.6.0-python3.10

# Switch back to airflow user
USER airflow

COPY plugins plugins

# Install additional packages
RUN pip install -r plugins/requirements.txt