# Development

## Set up

```bash
git clone https://github.com/GeiserX/media-download.git && cd media-download
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt -r requirements-test.txt
```

The scripts run on Python 3.11 (the version in both Dockerfiles, pinned because `lxml` did not build on 3.12 when they were written) and 3.12 (the version CI tests with).

## Test

```bash
pytest tests/ --cov=src --cov-report=term -v
```

`tests/test_media_download.py` loads both scripts as modules and mocks every HTTP call, so the suite needs no catalog and no network. The `Tests` workflow (`.github/workflows/tests.yml`) runs the same tests on every push to `main` and every pull request, on Python 3.12, and uploads the coverage to [Codecov](https://codecov.io/gh/GeiserX/media-download). `codecov.yml` sets a 90% target for the project and for each patch.

## Build the images

```bash
docker compose build
```

One image per script, from `Dockerfile-media` and `Dockerfile-epubs`. Keep them separate. No image is published; both build from the checkout.

## Dependencies

Dependabot opens a weekly grouped pull request for minor and patch updates of `requirements.txt` and `requirements-test.txt`, and another for `docs/requirements-docs.txt`. Major versions of those two files are left for a person to do.

## Docs

The pages live in `docs/` and are published to [geiserx.github.io/media-download](https://geiserx.github.io/media-download/) by `.github/workflows/docs.yml` on every push to `main` that touches them. To preview locally:

```bash
pip install -r docs/requirements-docs.txt
mkdocs serve
```

`mkdocs build --strict` fails on a broken link, a missing page or a page that is in no nav entry; pull requests run it as the `build (strict)` check.

## Contributing

Open an [issue](https://github.com/GeiserX/media-download/issues) before a large change. Security problems go to the [security policy](https://github.com/GeiserX/media-download/blob/main/SECURITY.md).
