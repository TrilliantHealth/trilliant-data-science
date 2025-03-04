# Trilliant Health Machine Learning Open Source Monorepo

A subset of our work is published here in the hopes that it will be useful. New projects may be added
over time.

## Structure

Projects are directories with their own `pyproject.toml` and `src` subdirectory. They represent a single,
publishable Python package.

Projects can depend on other projects, and within the monorepo they express that as an
editable/from-source dependency. When built using our internal (not source-available) tooling, they
define their dependencies using the conventional by-name approach.

## Usage

All projects here are installable via PyPI: `pip install thds.<pkg-name>` should do the trick.

The code is available for use under the terms of its license.

We use `uv` to manage virtual environments for each project for development purposes.

## Testing

All of this code is tested during continuous integration at Trilliant Health. You can run the tests
yourself once you have the virtual environment set up, with `uv run pytest tests` in the project
directory.
