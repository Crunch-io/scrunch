# Pythonic scripting library for cleaning data in `Crunch <http://crunch.io/>`__

To learn more, including how to install the library, see the
[Overview](https://github.com/Crunch-io/scrunch/wiki/Overview) wiki
page.

Once you have it installed, to get started using `scrunch` to work with data in
Crunch, see the [User Guide](https://github.com/Crunch-io/scrunch/wiki/User-Reference).

## Development

### Running tests locally

The easiest way to run the test suite is with Docker Compose:

```bash
docker compose run --rm test
```

This builds a container with Python 3.11 and runs `tox -e py311`, matching the CI environment.

To run a specific test file or test:

```bash
docker compose run --rm test tox -e py311 -- scrunch/tests/test_datasets.py -x
docker compose run --rm test tox -e py311 -- scrunch/tests/test_projects.py::TestProjectNesting -x
```

To rebuild the container after dependency changes:

```bash
docker compose build --no-cache test
```

### Running integration tests

Integration tests run against a live Crunch API. Copy `.env` and fill in credentials, then:

```bash
docker compose run --rm test tox -e py311 -- integration/ -x
```

