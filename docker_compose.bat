echo "Starting Airflow and MongoDB services..."

docker-compose -f docker-compose-airflow.yaml -p airflow-etl-ggmaps up -d

docker-compose -f docker-compose-mongodb.yaml -p mongodb up -d

echo "Airflow and MongoDB services have been started."