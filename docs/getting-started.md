# Getting started

media-download is two scripts. `media-vtt.py` saves subtitles, `publications-epub.py` saves EPUBs. Both read one publication catalog, and the address of that catalog is not in the repository: every endpoint in the source is a placeholder, `https://place.holder/...`. So the first step is always the same: tell the scripts where the catalog is.

## What you need

- Docker with Compose, or Python 3.11 or 3.12.
- A catalog that answers in the shape [How it works](how-it-works.md) describes.
- For `publications-epub.py` only: the catalog's `unit.db`, a SQLite file with a `Language` table that maps a language symbol such as `S` to its numeric id.

## Set the endpoints

List the placeholders:

```bash
git clone https://github.com/GeiserX/media-download.git && cd media-download
grep -n place.holder src/*.py
```

There are six, two in `src/media-vtt.py` and four in `src/publications-epub.py`. Replace `https://place.holder` in each with your catalog's address. [Configuration](configuration.md#the-endpoints) lists what each one fetches.

## With Docker Compose

Put `unit.db` in `./db/`, then build and run both scripts:

```bash
mkdir -p db && cp /path/to/unit.db db/
docker compose up --build
```

Compose builds one image per script and starts both. Each runs to the end and exits. Subtitles land in `./vtts` with `media.db` beside them; EPUBs land in `./epubs` with `pubs.db`. `LANG=S` in `docker-compose.yml` picks the catalog language; see [Configuration](configuration.md).

To run only the subtitles, which do not need `unit.db`:

```bash
docker compose up --build media-download-media
```

## Without Docker

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
LANG=S OUTPUT_PATH=vtts DB_PATH=vtts/media.db python src/media-vtt.py
```

Set all three variables on the command line. Without them the script writes to `/vtts/`, and `LANG` is read from your shell, where it is usually a locale such as `en_US.UTF-8`, not a catalog language. For the EPUBs, set `OUTPUT_PATH`, `DB_PATH` and `UNIT_DB_PATH` the same way (see [Configuration](configuration.md)).

## What "it works" looks like

For the subtitles, the log says `Total media items to process:` with a number above zero, then one `Downloaded: vtts/<name>.vtt` line per file, and `vtts/media.db` lists one row per item. For the EPUBs, the log says `Total publications found:` with a number above zero, then `Downloaded file to ...` lines, and ends with `Download complete.` Run the same command again and each item it already has shows `Already successfully processed ..., skipping.` (subtitles) or `Skipping already processed entry` (EPUBs). [Usage](usage.md) shows a full run.

Read the log, not the exit code: both scripts exit 0 even when they fail. With the placeholder endpoints still in place, `media-vtt.py` logs `Error in downloading or extracting JSON` and then, all the same, `Finished processing all media items.` See [Troubleshooting](troubleshooting.md).
