# Trilliant Health Data Science Open Source Monorepo

A subset of our work is published here in the hopes that it will be
useful. New projects may be added over time.

## Structure

Projects are directories with their own `pyproject.toml` and `src`
subdirectory. They represent a single, publishable Python package.

Projects can depend on other projects, and within the monorepo they
express that as an editable/from-source dependency.

## Usage

We use `poetry` to manage virtual environments for each project.

Currently we have not set up publishing to PyPi. The code is available
for use under the terms of its license.

In order to make use of one project depending on another project
(e.g. `atacama` depending on `core`), you will need to bundle the
source code together. The simplest way of doing this is to copy all
the source code of the upstream projects into the project you want to
use, then modify the `pyproject.toml` to include all of those
projects, and finally run `python -m build sdist` or some form of
wheel build.

## Testing

All of this code is tested during continuous integration at Trilliant
Health. You can run the tests yourself once you have the virtual
environment set up, with `poetry run pytest tests` in the project
directory.
