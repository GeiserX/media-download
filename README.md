<p align="center">
  <img src="docs/images/banner.svg" alt="media-download" width="900">
</p>

<h1 align="center">media-download</h1>

<p align="center">
  <a href="https://github.com/GeiserX/media-download/actions/workflows/tests.yml"><img src="https://img.shields.io/github/actions/workflow/status/GeiserX/media-download/tests.yml?style=flat-square&label=tests" alt="Tests"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/GeiserX/media-download?style=flat-square" alt="License"></a>
  <a href="https://codecov.io/gh/GeiserX/media-download"><img src="https://img.shields.io/codecov/c/github/GeiserX/media-download?style=flat-square" alt="Coverage"></a>
</p>

Two Python scripts that mirror a publication catalog into a folder: `media-vtt.py` downloads the VTT subtitles of each media item, and `publications-epub.py` downloads the EPUB of each publication. Both keep a SQLite file of what they already fetched, so a rerun only gets what is new. The catalog endpoints in the source are placeholders (`https://place.holder/...`), so neither script runs until you set them.

## Quick start

Set the endpoints first (`grep -n place.holder src/*.py`); `publications-epub.py` also needs the catalog's `unit.db` at `UNIT_DB_PATH` (default `/app/db/unit.db`).

```bash
git clone https://github.com/GeiserX/media-download.git && cd media-download
docker compose up --build
```

Subtitles land in `./vtts` and EPUBs in `./epubs`, each next to its SQLite index. `LANG` in `docker-compose.yml` picks the catalog language.

## Related projects

| Project | Description |
|---------|-------------|
| [Wayback-Archive](https://github.com/GeiserX/Wayback-Archive) | Download complete websites from the Wayback Machine with full asset preservation |
| [Wayback-Diff](https://github.com/GeiserX/Wayback-Diff) | Web page comparison tool with Wayback Machine support |
| [Way-CMS](https://github.com/GeiserX/Way-CMS) | Simple web CMS for editing HTML/CSS files downloaded from Wayback Archive |
| [web-mirror](https://github.com/GeiserX/web-mirror) | Mirror a web page to a local server for offline access |
| [n8n-nodes-way-cms](https://github.com/GeiserX/n8n-nodes-way-cms) (archived) | n8n community node for Way-CMS archived web content management |

## License

[GPL-3.0-or-later](LICENSE)
