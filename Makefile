.PHONY: clean data lint requirements

#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))
PROFILE = default
PROJECT_NAME = bot_detector
PYTHON_INTERPRETER = python3

#################################################################################
# COMMANDS                                                                      #
#################################################################################
## Test python environment is setup correctly
test_environment:
	$(PYTHON_INTERPRETER) test_environment.py
	
## Install Python Dependencies
requirements: test_environment
	./requirements/requirements.sh "temp_venv/bin/pip"
	rm -rf temp_venv/

## Make Dataset
data:
	$(PYTHON_INTERPRETER) src/data/make_dataset.py --data_from $(data_from) --data_to $(data_to)

interim_data:
	make data data_from="raw" data_to="interim"

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using flake8
lint:
	flake8 src
