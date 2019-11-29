SHELL := /bin/bash
.PHONY: docs clean

VIRTUALENV_DIR=.env
PYTHON=${VIRTUALENV_DIR}/bin/python
PIP=${VIRTUALENV_DIR}/bin/pip
INTEGRATION_PATH=tests/test_integration.py
stage=dev
env=local

all:
	pip3 install virtualenv
	virtualenv -p python3 $(VIRTUALENV_DIR) --no-site-packages
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

run:
	pip install lookerapi
	python3 -B main.py $(stage) $(env)

test:
	py.test --verbose -s --disable-pytest-warnings --color=yes $(INTEGRATION_PATH)
