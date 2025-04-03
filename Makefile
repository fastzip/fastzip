SOURCES=fastzip setup.py

ifeq ($(OS),Windows_NT)
    ACTIVATE:=.venv/Scripts/activate
else
    ACTIVATE:=.venv/bin/activate
endif

UV:=$(shell uv --version)
ifdef UV
	VENV:=uv venv
	PIP:=uv pip
else
	VENV:=python -m venv
	PIP:=python -m pip
endif

.venv:
	$(VENV) .venv

.PHONY: setup
setup: .venv
	source $(ACTIVATE) && $(PIP) install -Ue .[dev,test,all]

# The rest of these are intended to be run within the venv, where python points
# to whatever was used to set up the venv.

.PHONY: test
test:
	python -m coverage run -m fastzip.tests $(TESTOPTS)
	python -m coverage report

.PHONY: format
format:
	python -m ufmt format $(SOURCES)

.PHONY: lint
lint:
	python -m ufmt check $(SOURCES)
	python -m flake8 $(SOURCES)
	python -m checkdeps --allow-names fastzip fastzip
	mypy --strict fastzip

.PHONY: release
release:
	rm -rf dist
	python setup.py sdist bdist_wheel
	twine upload dist/*
