SPHINX_BUILDARGS=

CONNECT_VERSION ?= latest
CONNECT_LICENSE ?= ./posit-connect.lic

README.md:
	quarto render README.qmd

test: test-most test-connect

test-most:
	pytest pins -m "not fs_rsc and not fs_s3" --workers 4 --tests-per-worker 1 -vv

test-connect: install-connect-test-deps
	with-connect --version $(CONNECT_VERSION) --license $(CONNECT_LICENSE) --config script/setup-rsconnect/rstudio-connect.gcfg -- $(MAKE) _test-connect

_test-connect:
	python script/setup-rsconnect/dump_api_keys.py pins/tests/rsconnect_api_keys.json
	PINS_ALLOW_RSC_SHORT_NAME=1 pytest pins -m "fs_rsc"

install-connect-test-deps:
	@if ! command -v docker >/dev/null 2>&1; then \
		echo "Please install Docker"; \
		exit 1; \
	fi
	@if ! command -v uv >/dev/null 2>&1; then \
		echo "Please install uv"; \
		exit 1; \
	fi
	@if ! command -v with-connect >/dev/null 2>&1; then \
		echo "Installing with-connect..."; \
		uv tool install git+https://github.com/posit-dev/with-connect.git; \
	fi
	@if ! docker desktop status >/dev/null 2>&1; then \
		echo "💬 Docker Desktop is not running. Trying to start it."; \
		docker desktop start; \
	fi

docs-build:
	cd docs && python -m quartodoc build --verbose
	cd docs && quarto render

docs-clean:
	rm -rf docs/_build docs/api/api_card

requirements/dev.txt: pyproject.toml
	@# allows you to do this...
	@# make requirements | tee > requirements/some_file.txt
	@uv pip compile pyproject.toml --extra doc --extra test --extra check --output-file $@

binder/requirements.txt: requirements/dev.txt
	cp $< $@

ci-compat-check:
	# TODO: mark as dummy
	$(MAKE) -C script/$@
