PROJECT_NAME ?= pins

# Docker settings
DOCKER_COMPOSE ?= docker compose
DOCKER_CONNECT_IMAGE ?= rstudio/rstudio-connect
DOCKER_PROJECT_IMAGE_TAG ?= $(PROJECT_NAME):latest

# Connect settings
CONNECT_BOOTSTRAP_SECRETKEY ?= $(shell head -c 32 /dev/random | base64)

# pytest settings
PYTEST_ARGS ?= "-s"

.DEFAULT_GOAL := latest

.PHONY: $(CONNECT_VERSIONS) \
	all \
	build \
	down \
	down-% \
	latest \
	test \
	up \
	up-% \
	help

# Versions
CONNECT_VERSIONS := \
	2025.07.0 \
	2025.06.0 \
	2025.05.0 \
	2025.04.0 \
	2025.03.0 \
	2025.02.0 \
	2025.01.0 \
	2024.12.0 \
	2024.11.0 \
	2024.09.0 \
	2024.08.0 \
	2024.06.0 \
	2024.05.0 \
	2024.04.1 \
	2024.04.0 \
	2024.03.0 \
	2024.02.0 \
	2024.01.0 \
	2023.12.0 \
	2023.10.0 \
	2023.09.0 \
	2023.07.0 \
	2023.06.0 \
	2023.05.0 \
	2023.01.1 \
	2023.01.0 \
	2022.12.0 \
	2022.11.0

clean:
	rm -rf logs reports
	find . -type d -empty -delete

# Run test suite for a specific Connect version.
$(CONNECT_VERSIONS): %: down-% up-%

# Run test suite against all Connect versions.
all: $(CONNECT_VERSIONS:%=%) preview

# Run test suite against latest Connect version.
latest:
	$(MAKE) $(firstword $(CONNECT_VERSIONS))

# Run test suite against preview Connect version.
preview:
	$(MAKE) \
	DOCKER_CONNECT_IMAGE=rstudio/rstudio-connect-preview \
	DOCKER_CONNECT_IMAGE_TAG=dev-jammy-daily \
	down-preview up-preview

# Build Dockerfile
build:
	make -C .. $(UV_LOCK)
	docker build -t $(DOCKER_PROJECT_IMAGE_TAG) ..

# Tear down resources.
down: $(CONNECT_VERSIONS:%=down-%)
down-%: DOCKER_CONNECT_IMAGE_TAG=jammy-$*
down-%: CONNECT_VERSION=$*
down-%:
	CONNECT_BOOTSTRAP_SECRETKEY=$(CONNECT_BOOTSTRAP_SECRETKEY) \
	CONNECT_VERSION=$* \
	DOCKER_CONNECT_IMAGE_TAG=$(DOCKER_CONNECT_IMAGE_TAG) \
	DOCKER_CONNECT_IMAGE=$(DOCKER_CONNECT_IMAGE) \
	DOCKER_PROJECT_IMAGE_TAG=$(DOCKER_PROJECT_IMAGE_TAG) \
	PYTEST_ARGS="$(PYTEST_ARGS)" \
	$(DOCKER_COMPOSE) -p $(PROJECT_NAME)-$(subst .,-,$(CONNECT_VERSION)) down -v

# Create, start, and run Docker Compose.
up: $(CONNECT_VERSIONS:%=up-%)
up-%: CONNECT_VERSION=$*
up-%: DOCKER_CONNECT_IMAGE_TAG=jammy-$*
up-%: build
	CONNECT_BOOTSTRAP_SECRETKEY=$(CONNECT_BOOTSTRAP_SECRETKEY) \
	CONNECT_VERSION=$* \
	DOCKER_CONNECT_IMAGE_TAG=$(DOCKER_CONNECT_IMAGE_TAG) \
	DOCKER_CONNECT_IMAGE=$(DOCKER_CONNECT_IMAGE) \
	DOCKER_PROJECT_IMAGE_TAG=$(DOCKER_PROJECT_IMAGE_TAG) \
	PYTEST_ARGS="$(PYTEST_ARGS)" \
	$(DOCKER_COMPOSE) -p $(PROJECT_NAME)-$(subst .,-,$(CONNECT_VERSION)) up -V --abort-on-container-exit --no-build

# Show available versions
print-versions:
	@printf "%s\n" $(strip $(CONNECT_VERSIONS))

# Show help message.
help:
	@echo "Makefile Targets:"
	@echo "  all (default)    Run test suite for all Connect versions."
	@echo "  latest           Run test suite for latest Connect version."
	@echo "  preview          Run test suite for preview Connect version."
	@echo "  <version>        Run test suite for the specified Connect version. (e.g., make 2024.05.0)"
	@echo "  up               Start Docker Compose for all Connect versions."
	@echo "  down             Tear down Docker resources for all Connect versions."
	@echo "  clean            Clean up the project directory."
	@echo "  print-versions   Show the available Connect versions."
	@echo "  help             Show this help message."
	@echo
	@echo "Common Usage:"
	@echo "  make -j 4        Run test suite in parallel for all Connect versions."
	@echo "  make latest      Run test suite for latest Connect version."
	@echo "  make preview     Run test suite for preview Connect version."
	@echo "  make 2024.05.0   Run test suite for specific Connect version."
	@echo
	@echo "Environment Variables:"
	@echo "  DOCKER_COMPOSE           Command to invoke Docker Compose. Default: docker compose"
	@echo "  DOCKER_CONNECT_IMAGE     Docker image name for Connect. Default: rstudio/rstudio-connect"
	@echo "  DOCKER_PROJECT_IMAGE_TAG Docker image name and tag for the project image. Default: $(PROJECT_NAME):latest"
	@echo "  PYTEST_ARGS              Arguments to pass to pytest. Default: \"-s\""

# Run tests.
test:
	mkdir -p logs
	set -o pipefail; \
	CONNECT_VERSION=${CONNECT_VERSION} \
		CONNECT_API_KEY="$(shell $(UV) run rsconnect bootstrap -i -s http://connect:3939 --raw)" \
		echo "Hello, World!" | \
		tee ./integration/logs/$(CONNECT_VERSION).log;
