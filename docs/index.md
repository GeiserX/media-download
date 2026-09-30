---
hide:
  - navigation
---

# media-download { .mdl-visually-hidden }

<p align="center">
  <img src="images/banner.svg" alt="media-download" width="100%">
</p>

<p align="center">
  <a href="https://github.com/GeiserX/media-download/actions/workflows/tests.yml"><img alt="Tests" src="https://img.shields.io/github/actions/workflow/status/GeiserX/media-download/tests.yml?style=flat-square&label=tests"></a>
  <a href="https://github.com/GeiserX/media-download/stargazers"><img alt="GitHub Stars" src="https://img.shields.io/github/stars/GeiserX/media-download?style=flat-square&logo=github"></a>
  <a href="https://github.com/GeiserX/media-download/releases"><img alt="Release" src="https://img.shields.io/github/v/release/GeiserX/media-download?style=flat-square"></a>
  <a href="https://github.com/GeiserX/media-download/blob/main/LICENSE"><img alt="License: GPL-3.0-or-later" src="https://img.shields.io/github/license/GeiserX/media-download?style=flat-square"></a>
</p>

---

**media-download** is two Python scripts that mirror a publication catalog into a folder: `media-vtt.py` saves the VTT subtitles of every media item, and `publications-epub.py` saves the EPUB of every publication. Each keeps a SQLite file of what it already fetched, so you can run it again whenever you like and it only downloads what is new. The catalog endpoints in the source are placeholders (`https://place.holder/...`): nothing runs until you point them at a catalog that answers in the shape the scripts expect. Start with [Getting started](getting-started.md), then [Usage](usage.md).

<div class="grid cards" markdown>

-   :material-docker: **[Getting started](getting-started.md)**

    ---

    Set the endpoints, add `unit.db` for the EPUBs, and run both scripts with Docker Compose or plain Python.

-   :material-console: **[Usage](usage.md)**

    ---

    Run one script or both, run them again, and read what the log and the output folder tell you.

-   :material-tune: **[Configuration](configuration.md)**

    ---

    The four environment variables, the endpoints in the source and the compose file's folders.

-   :material-sitemap-outline: **[How it works](how-it-works.md)**

    ---

    What each script asks the catalog for, the answers it expects, and which items it tries again.

</div>

## What a run looks like

![media-vtt.py run twice against a stand-in catalog of three media items: the first run saves two subtitle files and finds none for the third; the second run skips the two it has and asks again for the third; the output folder then holds the two VTT files, the extracted catalog and media.db](images/screenshots/media-vtt-run.png){ .mdl-terminal }

`media-vtt.py`, run twice against a stand-in for the catalog with three invented media items. The first run saves two VTT files and records that the third item has no subtitles. The second run skips the two it already has, asks again about the third, and downloads nothing new. [Usage](usage.md#reading-the-log) explains each line.

## What it does

- Saves the subtitles of every media item in the catalog as VTT files, each under the file name in its URL.
- Saves the EPUB of every publication in one language, including each issue of a periodical, under the name the server sends or `<symbol>_<issue>.epub`.
- Records each item's result in SQLite (`media.db`, `pubs.db`), skips what it already has, and tries each download up to three times, waiting longer after each failure.
- `LANG` picks the catalog language. One container per script, both in the shipped `docker-compose.yml`.

## How it runs

- Each script runs once, from start to end, and exits. Nothing in the repository schedules it; run it again, by hand or from your own scheduler, to pick up what is new.
- Docker Compose builds both images from the repository (`Dockerfile-media`, `Dockerfile-epubs`, Python 3.11). No image is published to a registry.
- Without Docker, `python src/media-vtt.py` works with Python 3.11 or 3.12 and the packages in `requirements.txt`. See [Getting started](getting-started.md#without-docker).

## What it does not do

- It does not download from an arbitrary web page. It reads one catalog in one fixed shape. To save a live page, see [web-mirror](https://github.com/GeiserX/web-mirror) on [Related projects](related.md).
- It does not ship working endpoints. You set them in the source.
- It does not download the videos or audio themselves, only their subtitles.
- `media-vtt.py` does not retry an item it marked `failed`; [Troubleshooting](troubleshooting.md#an-item-marked-failed-is-never-tried-again) says how to clear it.

## Privacy

- Both scripts talk only to the endpoints you set. There is no telemetry and no account.
- Everything they fetch stays in the output folders you mount.

## Getting help

- Something broken: read [Troubleshooting](troubleshooting.md), then open an [issue](https://github.com/GeiserX/media-download/issues) with the details it lists.
- A security problem: follow the [security policy](https://github.com/GeiserX/media-download/blob/main/SECURITY.md), never a public issue.
- Running the tests or changing the code: [Development](development.md).

## License

media-download is released under the [GPL-3.0-or-later](https://github.com/GeiserX/media-download/blob/main/LICENSE) license.
