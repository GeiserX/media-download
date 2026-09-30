# Configuration

Settings come from environment variables. The catalog's address is not a setting: it is written in the source, as a placeholder, and you edit it there.

## Environment variables

| Variable | Script | Default | What it sets |
|---|---|---|---|
| `LANG` | both | `S` | The catalog language symbol, used in every request and in the catalog file name. |
| `OUTPUT_PATH` | both | `/vtts/` (subtitles), `/epubs/` (EPUBs) | The folder the files are saved to. Created if missing. |
| `DB_PATH` | both | `/vtts/media.db`, `/epubs/pubs.db` | The SQLite file that records what was fetched. Its folder is created if missing. |
| `UNIT_DB_PATH` | `publications-epub.py` | `/app/db/unit.db` | The catalog's `unit.db`, read to turn `LANG` into a numeric language id. The script only reads it; the compose file mounts it read-only. |

`LANG` is also the variable your shell uses for its locale. Inside the containers the compose file sets it to `S`. Outside Docker, always set it on the command line, or the script asks the catalog for a language called `en_US.UTF-8`.

`DB_PATH` does not follow `OUTPUT_PATH`: if you change the output folder outside Docker, set `DB_PATH` too, or the state file still goes to `/vtts/` or `/epubs/`.

## The endpoints

Every catalog address in the source is `https://place.holder`. Replace it in each of these lines (`grep -n place.holder src/*.py` finds them):

| File | What it fetches |
|---|---|
| `src/media-vtt.py`, `catalog_url` in `__main__` | The catalog index, `<base>/<LANG>.json.gz`: gzipped JSON, one object per line. |
| `src/media-vtt.py`, `base_url` in `get_pub_media_links` | The files of one media item, asked with `langwritten`, `track`, and `pub` or `docid`. |
| `src/publications-epub.py`, `manifest_url` in `fetch_log_db` | A JSON object whose `current` field names the latest catalog manifest. |
| `src/publications-epub.py`, `log_url` in `fetch_log_db` | The gzipped SQLite catalog for that manifest, with a `Publication` table. |
| `src/publications-epub.py`, the two `url` lines in `download_epubs` | The files of one publication, for a periodical issue (`pub`, `issue`) or a single publication (`pub`), with `fileformat=epub`. |

[How it works](how-it-works.md) describes the answers each request must return.

## The compose file

The shipped `docker-compose.yml`, word for word:

```yaml
--8<-- "docker-compose.yml"
```

- `./vtts` and `./epubs` on the host receive the files and the state databases. Both are created by Docker if missing.
- `./db` holds `unit.db` and is mounted read-only. The subtitles service does not need it.
- Both services build from the repository. There is no published image to pull.
