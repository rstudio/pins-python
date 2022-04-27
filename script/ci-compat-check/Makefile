BOARD_BASE_DIR=tmp
BOARD_PY=$(BOARD_BASE_DIR)/board-py
BOARD_R=$(BOARD_BASE_DIR)/board-r

all: validate

clean:
	rm -r $(BOARD_PY) $(BOARD_R)

validate: $(BOARD_PY) $(BOARD_R)
	@echo "\n\nRUNNING R PINS ---\n"
	Rscript validate_py_to_r.R $(BOARD_PY) $(BOARD_R)
	@echo "\n\nRUNNING PYTHON PINS ---\n"
	python validate_r_to_py.py $(BOARD_PY) $(BOARD_R)

$(BOARD_PY): dump_py_pins.py
	python dump_py_pins.py $@

$(BOARD_R): dump_r_pins.R
	Rscript dump_r_pins.R $@
