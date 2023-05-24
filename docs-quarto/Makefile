# Minimal makefile for quartodoc documentation

.PHONY: help Makefile

# Catch-all target:
%: Makefile
	python -m quartodoc build --verbose
	quarto render
