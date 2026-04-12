
# Makefile for the inventory management system

AIRFLOW_HOME ?= $(CURDIR)/.airflow
export AIRFLOW_HOME

.PHONY: install fake_data
setup:
	@echo "Setting up the project..."
	$(MAKE) install
	$(MAKE) fake_data
	.venv/bin/python -c "from contracts.catalogue_contract import CatalogueRecordV1; print('Contrats OK')"
	.venv/bin/pytest tests/ -v
	
	@echo "Setup complete."


install:
	@echo "Installing the dependencies..."
	mkdir -p $(AIRFLOW_HOME)
	uv venv .venv
	uv sync --extra test --extra airflow
	docker compose up -d # PostgreSQL + Airflow
	.venv/bin/python scripts/load_to_postgres.py --init	
	@echo "Installation complete."

fake_data:
	@echo "Generating fake data..."
	.venv/bin/python scripts/generate_flows.py --palier small --orphan-ratio 0.20  # 200 produits, 10 000 mouvements, 20% de mouvements où le produit n'existe pas dans le catalogue
#	.venv/bin/python scripts/generate_flows.py --palier small # 200 produits, 10 000 mouvements
#	.venv/bin/python scripts/generate_flows.py --palier medium # 500 produits, 100 000 mouvements
#	.venv/bin/python scripts/generate_flows.py --palier large # 1 000 produits, 1 000 000 mouvements
	@echo "Fake data generation complete."
	
reset_docker:
	@echo "Resetting Docker containers..."
	docker compose down -v
	docker compose up -d --build
	@echo "Docker containers reset complete."