#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = 
PYTHON_VERSION = 3.11.9
PYTHON_INTERPRETER = python

#################################################################################
# COMMANDS                                                                      #
#################################################################################


## Initialize git repo
.PHONY: git-init
git-init:
	git init
	git add .
	git commit -m "Initial commit"

## Install Python dependencies, setup hooks
.PHONY: init
init:
	pyenv install -s $(PYTHON_VERSION)
	pyenv local $(PYTHON_VERSION)
	poetry env use $$(pyenv which python)
	poetry install --with viz
	@if [ -d .git ]; then \
		poetry run pre-commit install; \
	else \
		echo "⚠️  Git repo not initialized, skipping pre-commit install"; \
    fi

.PHONY: shell
shell:
	poetry shell

## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Delete the virtual environment
.PHONY: clean-env
clean-env:
	poetry env remove --all || true

## Lint using ruff (use `make format` to do formatting)
.PHONY: lint
lint:
	poetry run ruff format --check
	poetry run ruff check

## Format source code with ruff
.PHONY: format
format:
	poetry run ruff check --fix
	poetry run ruff format



## Run tests
.PHONY: test
test:
	poetry run pytest tests




#################################################################################
# PROJECT RULES                                                                 #
#################################################################################



#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys; \
lines = '\n'.join([line for line in sys.stdin]); \
matches = re.findall(r'\n## (.*)\n[\s\S]+?\n([a-zA-Z_-]+):', lines); \
print('Available rules:\n'); \
print('\n'.join(['{:25}{}'.format(*reversed(match)) for match in matches]))
endef
export PRINT_HELP_PYSCRIPT

help:
	@$(PYTHON_INTERPRETER) -c "${PRINT_HELP_PYSCRIPT}" < $(MAKEFILE_LIST)
