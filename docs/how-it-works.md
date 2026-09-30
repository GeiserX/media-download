# How it works

Two independent scripts, one catalog. Neither talks to the other; they share only the `LANG` setting and the compose file.

## media-vtt.py: subtitles

1. Creates `media.db` with a `downloaded_vtts` table if it does not exist.
2. Downloads `<base>/<LANG>.json.gz`, unpacks it to `<LANG>.json` in the output folder and deletes the `.gz`.
3. Reads the file line by line and keeps each object whose `type` is `media-item` and whose `o.keyParts` has an identifier (`pubS`, or else `docID`), a `track` and a `formatCode`.
4. For each item, looks up `(identifier, track, formatCode)` in `media.db`. `success` and `failed` are skipped.
5. Otherwise asks `<base>?langwritten=<LANG>&track=<track>&pub=<pubS>` (or `docid=<docID>`) and expects JSON with `files.<LANG>.<format>`: lists of file entries. The first entry, in any format, with a `subtitles.url` wins.
6. Downloads that URL and saves it in the output folder under the last part of the URL's path, decoded. A network error is retried after 2 and 4 seconds; the third failure records `failed`.
7. Records `success`, `no_subtitles` (no entry had subtitles) or `failed` (no answer, or no `files` in it).

## publications-epub.py: EPUBs

1. Creates `pubs.db` with a `PublicationState` table if it does not exist.
2. Opens `unit.db` and reads the `LanguageId` whose `Symbol` is `LANG`. No match stops the run.
3. Fetches the manifest JSON, takes its `current` field, downloads that manifest's gzipped catalog and unpacks it to `log` in the output folder: a SQLite database.
4. Lists `TagNumber, Symbol, sym` from its `Publication` table for that language. A `TagNumber` other than 0 is an issue of a periodical, fetched with `pub=<sym>&issue=<TagNumber>`; 0 is a single publication, fetched with `pub=<Symbol>`.
5. Skips each publication `pubs.db` already marks `processed`. For the rest, asks for its files with `fileformat=epub` and expects JSON with `files.<LANG>.EPUB[0].file.url`.
6. Downloads that URL and saves it under the name in the server's `Content-Disposition` header, or `<Symbol>_<TagNumber>.epub` without one. A network error is retried after 2 and 4 seconds.
7. Records `processed`, `no_epub` (no EPUB listed) or `failed`. Only `processed` is skipped next time.

## State

The two SQLite files are the only memory between runs. Delete a row and the next run fetches that item again; delete the file and the next run starts from nothing (files already in the folder are overwritten, not duplicated, because the names come from the catalog).

## What talks to what

Each script makes HTTPS requests to the addresses you set in the source, and to the file URLs the catalog returns. Nothing else leaves the machine. Both run as root inside their containers. The containers need no extra privileges, and nothing in them listens on a port (the `EXPOSE 80` in the Dockerfiles is unused, and the compose file publishes nothing).
