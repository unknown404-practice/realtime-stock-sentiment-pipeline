.PHONY: help install test lint run docker-up docker-down clean

PYTHON ?= python
PIP ?= pip
STREAMLIT ?= streamlit

help:
	@echo "Available commands:"
	@echo "  make install     Install production and development dependencies"
	@echo "  make test        Execute automated pytest test suite"
	@echo "  make lint        Run ruff linter checks across codebase"
	@echo "  make run         Launch the Streamlit presentation dashboard"
	@echo "  make pipeline    Run Kafka producer and FinBERT consumer pipeline"
	@echo "  make docker-up   Start full-stack container orchestration via Docker Compose"
	@echo "  make docker-down Stop container cluster and remove orphan containers"
	@echo "  make clean       Remove temporary Python cache and build artifacts"

install:
	$(PIP) install --upgrade pip setuptools wheel
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-pipeline.txt
	$(PIP) install pytest pytest-cov mongomock ruff

test:
	$(PYTHON) -m pytest -v tests/

lint:
	$(PYTHON) -m ruff check .

run:
	$(STREAMLIT) run app.py

pipeline:
	$(PYTHON) run_pipeline.py

docker-up:
	docker compose up --build -d

docker-down:
	docker compose down --remove-orphans

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage 2>/dev/null || true
