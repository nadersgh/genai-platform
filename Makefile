LOCAL=infra/envs/local
# Use the active docker context's socket (works for Colima, Docker Desktop, etc.)
export TF_VAR_docker_host ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)
AWS=infra/envs/aws

.PHONY: local-up local-down local-plan aws-plan fmt validate
local-up:
	tofu -chdir=$(LOCAL) init && tofu -chdir=$(LOCAL) apply -auto-approve
local-down:
	tofu -chdir=$(LOCAL) destroy -auto-approve
local-plan:
	tofu -chdir=$(LOCAL) init && tofu -chdir=$(LOCAL) plan
aws-plan:
	tofu -chdir=$(AWS) init && tofu -chdir=$(AWS) plan
fmt:
	tofu fmt -recursive infra
validate:
	tofu -chdir=$(LOCAL) init -backend=false >/dev/null && tofu -chdir=$(LOCAL) validate
	tofu -chdir=$(AWS) init -backend=false >/dev/null && tofu -chdir=$(AWS) validate

# ---- pipelines (week 2+) ----
PIPE=cd pipelines && PYTHONPATH=. uv run python -m
.PHONY: bronze-init produce-telemetry produce-docs sink verify test
bronze-init:
	$(PIPE) ingest.catalog
produce-telemetry:
	$(PIPE) ingest.producer_telemetry --events $${EVENTS:-5000} --rate 0
produce-docs:
	$(PIPE) ingest.producer_docs
sink:
	$(PIPE) ingest.sink_bronze --idle-exit 10 --max-wait 3
verify:
	$(PIPE) ingest.verify
test:
	cd pipelines && PYTHONPATH=. uv run pytest -q

# ---- real data (downloads are opt-in: make download SRC="cars-en cars-fr cmapss") ----
.PHONY: sources download produce-adsb produce-cmapss
sources:
	$(PIPE) ingest.download --list
download:
	$(PIPE) ingest.download $(SRC)
produce-adsb:
	$(PIPE) ingest.producer_opensky --max-polls $${POLLS:-3}
produce-cmapss:
	$(PIPE) ingest.producer_cmapss $(ARGS)
