SPHINX_BUILDARGS=

test:
	pytest

docs-build:
	cd docs && sphinx-build . ./_build/html $(SPHINX_BUILDARGS)

docs-watch:
	cd docs && sphinx-autobuild . ./_build/html $(SPHINX_BUILDARGS)
