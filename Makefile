PYTHON ?= python3


all:
	:


flake8:
	$(PYTHON) -m flake8 --max-line-len=125 pg/*.py
