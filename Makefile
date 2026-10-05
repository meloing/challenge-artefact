.PHONY: help test run-gen up-level1-2 up-all down clean lint

help:
	@echo "======================================================================="
	@echo "🏦 WABA Group - Modern Data Lakehouse Platform Commands"
	@echo "======================================================================="
	@echo "  make test          : Execute all unit and integration tests"
	@echo "  make run-gen       : Run local data generator (CLI mode)"
	@echo "  make up-level1-2   : Deploy Levels 1 & 2 stack (MinIO, Spark, Trino, Airflow)"
	@echo "  make up-all        : Deploy full stack (including Kafka, NiFi, Observability)"
	@echo "  make down          : Stop all docker containers and networks"
	@echo "  make clean         : Remove local storage and test artifacts"
	@echo "======================================================================="

test:
	@echo "🧪 Running WABA Data Lakehouse Test Suite..."
	python3 -m unittest discover tests -v

run-gen:
	@echo "🚀 Génération des flux de test pour les 8 pays..."
	python3 data_generator/app.py --cli --customers 5000 --accounts 10000 --branches 200 --bank-txns 1000 --insurance-ops 500 --mobile-money 2000 --loan-repayments 500

up-level1-2:
	@echo "🐳 Starting Docker Compose for Levels 1 & 2..."
	docker compose -f docker-compose.level1-2.yml up -d

up-all:
	@echo "🐳 Starting Full Production Docker Compose Stack..."
	docker compose up -d

down:
	@echo "🛑 Stopping all containers..."
	docker compose down

clean:
	@echo "🧹 Cleaning temporary files..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	rm -rf .pytest_cache /tmp/spark-checkpoints
