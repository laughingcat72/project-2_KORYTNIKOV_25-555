.PHONY: install project lint build publish package-install clean

install:
	poetry install

project:
	poetry run project

lint:
	poetry run ruff check database/
	poetry run ruff format --check database/

format:
	poetry run ruff format database/
	poetry run ruff check --fix database/

build:
	poetry build

publish:
	poetry publish

package-install:
	pip install dist/*.whl

clean:
	rm -rf dist/ build/ *.egg-info .venv data/ db_meta.json __pycache__ */__pycache__