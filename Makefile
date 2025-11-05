SPHINX_BUILDARGS=

README.md:
	quarto render README.qmd

test: test-most test-rsc

test-most:
	pytest pins -m "not fs_rsc and not fs_s3" --workers 4 --tests-per-worker 1 -vv

test-rsc:
	pytest pins -m "fs_rsc"

docs-build:
	cd docs && python -m quartodoc build --verbose
	cd docs && quarto render

docs-clean:
	rm -rf docs/_build docs/api/api_card

requirements/dev.txt: pyproject.toml
	@# allows you to do this...
	@# make requirements | tee > requirements/some_file.txt
	@pip-compile pyproject.toml --rebuild --extra doc --extra test --extra check --output-file=- > $@

binder/requirements.txt: requirements/dev.txt
	cp $< $@

ci-compat-check:
	# TODO: mark as dummy
	$(MAKE) -C script/$@
