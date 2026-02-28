ROOT_DIR  := $(realpath $(dir $(lastword $(MAKEFILE_LIST)))/..)
COMPOSE   := $(ROOT_DIR)/examples/docker-compose.yml

S3_ENDPOINT           ?= http://localhost:9000
CATALOG_URI           ?= http://localhost:8181
AWS_ACCESS_KEY_ID     ?= minioadmin
AWS_SECRET_ACCESS_KEY ?= minioadmin
LIMIT ?= 500

.PHONY: generate-local-data generate-s3-data generate-catalog-data \
        local s3 s3-with-catalog clean setup-minio wait-for-catalog

setup-minio:
	docker compose -f $(COMPOSE) up -d minio
	@echo "Waiting for Minio at $(S3_ENDPOINT) ..."
	@for i in $$(seq 1 30); do \
		if curl -sf "$(S3_ENDPOINT)/minio/health/live" > /dev/null 2>&1; then \
			echo "Minio is ready."; break; \
		fi; \
		if [ "$$i" -eq 30 ]; then echo "ERROR: Minio did not become ready in time."; exit 1; fi; \
		sleep 1; \
	done
	@STATUS=$$(curl -sf -o /dev/null -w "%{http_code}" \
		-X PUT "$(S3_ENDPOINT)/warehouse" \
		-u "$(AWS_ACCESS_KEY_ID):$(AWS_SECRET_ACCESS_KEY)" 2>/dev/null || echo "000"); \
	if [ "$$STATUS" = "200" ]; then echo "Bucket 'warehouse' created."; \
	elif [ "$$STATUS" = "409" ]; then echo "Bucket 'warehouse' already exists."; \
	else \
		echo "S3 API returned $$STATUS, trying mc CLI..."; \
		docker compose -f $(COMPOSE) exec -T minio \
			mc alias set local "$(S3_ENDPOINT)" "$(AWS_ACCESS_KEY_ID)" "$(AWS_SECRET_ACCESS_KEY)" 2>/dev/null; \
		docker compose -f $(COMPOSE) exec -T minio \
			mc mb --ignore-existing "local/warehouse"; \
		echo "Bucket 'warehouse' created via mc."; \
	fi

wait-for-catalog: setup-minio
	docker compose -f $(COMPOSE) up -d rest
	@echo "Waiting for REST catalog at $(CATALOG_URI) ..."
	@for i in $$(seq 1 30); do \
		if curl -sf "$(CATALOG_URI)/v1/config" > /dev/null 2>&1; then \
			echo "REST catalog is ready."; break; \
		fi; \
		if [ "$$i" -eq 30 ]; then echo "ERROR: REST catalog not ready"; exit 1; fi; \
		sleep 1; \
	done

generate-local-data:
	cd $(ROOT_DIR) && cargo run --example create_sample_data -- local

generate-s3-data: setup-minio
	cd $(ROOT_DIR) && \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	S3_ENDPOINT=$(S3_ENDPOINT) \
	cargo run --example create_sample_data -- s3

generate-catalog-data: wait-for-catalog
	cd $(ROOT_DIR) && \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	S3_ENDPOINT=$(S3_ENDPOINT) \
	CATALOG_URI=$(CATALOG_URI) \
	cargo run --example create_sample_data -- catalog

local: generate-local-data
	cd $(ROOT_DIR) && cargo run -- open examples/sample_table --limit $(LIMIT)

s3: generate-s3-data
	cd $(ROOT_DIR) && \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	cargo run -- open s3://warehouse/sample_table --s3-endpoint $(S3_ENDPOINT) --limit $(LIMIT)

s3-with-catalog: generate-catalog-data
	cd $(ROOT_DIR) && \
	AWS_ACCESS_KEY_ID=$(AWS_ACCESS_KEY_ID) \
	AWS_SECRET_ACCESS_KEY=$(AWS_SECRET_ACCESS_KEY) \
	S3_ENDPOINT=$(S3_ENDPOINT) \
	cargo run -- catalog --uri $(CATALOG_URI) --table demo.sample_data --limit $(LIMIT)

clean:
	docker compose -f $(COMPOSE) down -v
	rm -rf $(ROOT_DIR)/examples/sample_table
