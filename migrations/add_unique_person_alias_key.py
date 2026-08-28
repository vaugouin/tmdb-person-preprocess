"""
Migration: add a UNIQUE key on (ID_PERSON, PERSON_NAME) to

    T_WC_TMDB_PERSON_ALSO_KNOWN_AS

Why
---
The table is fully derived from T_WC_TMDB_PERSON (NAME + ALSO_KNOWN_AS) and the
preprocess treats (ID_PERSON, PERSON_NAME) as its logical key, but nothing in the
schema enforces it: the only key is PRIMARY KEY (ID_ROW). Two consequences.

1. Duplicates accumulate. The previous preprocess loop built its in-memory index
   with "first ID_ROW wins" and never revisited the copies, so a row inserted
   twice stayed forever, invisible to both the update and the stale-alias delete.
   The current loop deletes the copies it meets, but only enforcement stops them
   from coming back.

2. f_sqlbulkupsert cannot do a true upsert on the logical key. Without a UNIQUE
   key covering the conflict columns, INSERT ... ON DUPLICATE KEY UPDATE degrades
   to a plain multi-row INSERT. The preprocess works around this by diffing in
   Python first (which it wants to do anyway, to avoid rewriting rows that have
   not moved), but the guard rail is missing.

NULL handling
-------------
A UNIQUE index treats NULLs as distinct, so rows with a NULL PERSON_NAME never
conflict and are left untouched. They are junk in a derived table, but removing
them is a separate decision, not this migration's.

Which row survives
------------------
The lowest ID_ROW of each duplicate group. That is the row the preprocess has
been maintaining all along (its index is built in ID_ROW ASC order, first wins),
so it carries the oldest DAT_CREAT and the values the pipeline believes in.

Safety / operations
-------------------
- DRY RUN by default: only reports. Pass --apply to dedupe then add the index.
- Idempotent: re-running after success is a no-op (index present, no duplicates).
- Deletes are chunked and committed as they go, no single giant transaction.
- HEAVY: the duplicate scan and the ALTER each walk a multi-million row table.
  Run in a low-traffic window, and on a live primary prefer pt-online-schema-change
  or gh-ost over the blocking ALTER this script issues (see --skip-alter).

Usage
-----
    python migrations/add_unique_person_alias_key.py             # dry run
    python migrations/add_unique_person_alias_key.py --apply     # dedupe + ALTER
    python migrations/add_unique_person_alias_key.py --apply --skip-alter
                                                                 # dedupe only
"""
import argparse
import os
import sys
import time

# citizenphil.py lives in the repo root (one level up); make it importable no
# matter where this script is launched from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import citizenphil as cp

INDEX_NAME = "UQ_TMDB_PERSON_ALSO_KNOWN_AS_PERSON_NAME"
KEY_COLUMNS = ["ID_PERSON", "PERSON_NAME"]
TABLE = "T_WC_TMDB_PERSON_ALSO_KNOWN_AS"
# Rows with a NULL PERSON_NAME are excluded from the dedup: a UNIQUE index allows
# repeated NULLs, so they neither block the ALTER nor need removing.
NON_NULL_FILTER = "ID_PERSON IS NOT NULL AND PERSON_NAME IS NOT NULL"
DELETE_CHUNK = 5000


def f_scalar(row, strkey, lngindex):
    """Read one column from a row that may be a dict (DictCursor) or a tuple."""
    if not row:
        return 0
    return row[strkey] if isinstance(row, dict) else row[lngindex]


def f_index_exists(conn):
    """Return True if an index named INDEX_NAME already exists on the table."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) AS n
        FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND INDEX_NAME = %s
        """,
        (TABLE, INDEX_NAME),
    )
    return int(f_scalar(cursor.fetchone(), "n", 0) or 0) > 0


def f_count_duplicates(conn):
    """Return (duplicate_group_count, extra_row_count) for the key tuple.

    extra_row_count is how many rows would be deleted (each group keeps one).
    """
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT COUNT(*) AS groups, COALESCE(SUM(c - 1), 0) AS extras
        FROM (
            SELECT COUNT(*) AS c
            FROM {TABLE}
            WHERE {NON_NULL_FILTER}
            GROUP BY ID_PERSON, PERSON_NAME
            HAVING COUNT(*) > 1
        ) d
        """
    )
    row = cursor.fetchone()
    return int(f_scalar(row, "groups", 0) or 0), int(f_scalar(row, "extras", 1) or 0)


def f_collect_doomed_ids(conn):
    """Return the ID_ROW values to delete: all but the lowest per key tuple."""
    cursor = conn.cursor()
    cursor.execute(
        f"""
        SELECT t.ID_ROW AS doomed
        FROM {TABLE} t
        JOIN (
            SELECT ID_PERSON, PERSON_NAME, MIN(ID_ROW) AS keep_id
            FROM {TABLE}
            WHERE {NON_NULL_FILTER}
            GROUP BY ID_PERSON, PERSON_NAME
            HAVING COUNT(*) > 1
        ) keep
          ON t.ID_PERSON = keep.ID_PERSON
         AND t.PERSON_NAME = keep.PERSON_NAME
         AND t.ID_ROW <> keep.keep_id
        """
    )
    return [f_scalar(r, "doomed", 0) for r in cursor.fetchall()]


def f_delete_ids(conn, arrids):
    """Delete the given ID_ROW values in committed chunks. Returns rows deleted."""
    lngdeleted = 0
    for lngstart in range(0, len(arrids), DELETE_CHUNK):
        arrchunk = arrids[lngstart:lngstart + DELETE_CHUNK]
        strplaceholders = ", ".join(["%s"] * len(arrchunk))
        cursor = conn.cursor()
        cursor.execute(
            f"DELETE FROM {TABLE} WHERE ID_ROW IN ({strplaceholders})",
            arrchunk,
        )
        conn.commit()
        lngdeleted += cursor.rowcount
        print(f"    deleted {lngdeleted}/{len(arrids)} extra rows", flush=True)
    return lngdeleted


def f_add_unique_index(conn):
    """Add the composite UNIQUE index (blocking ALTER)."""
    strcols = ", ".join(KEY_COLUMNS)
    print(f"    ALTER TABLE {TABLE} ADD UNIQUE KEY {INDEX_NAME} ({strcols}) ...", flush=True)
    cursor = conn.cursor()
    cursor.execute(f"ALTER TABLE {TABLE} ADD UNIQUE KEY {INDEX_NAME} ({strcols})")
    conn.commit()
    print(f"    index {INDEX_NAME} added on {TABLE}", flush=True)


def f_process_table(conn, intapply, intskipalter):
    print(f"\n=== {TABLE} ===", flush=True)

    if f_index_exists(conn):
        print(f"  index {INDEX_NAME} already present -> nothing to do", flush=True)
        return

    print("  scanning for duplicate (ID_PERSON, PERSON_NAME) tuples ...", flush=True)
    lnggroups, lngextras = f_count_duplicates(conn)
    print(f"  duplicate groups: {lnggroups}   extra rows to remove: {lngextras}", flush=True)

    if not intapply:
        print("  DRY RUN -> no changes made. Re-run with --apply to dedupe + add index.", flush=True)
        return

    if lngextras > 0:
        print("  collecting ID_ROW values to delete (keeping lowest per key) ...", flush=True)
        arrids = f_collect_doomed_ids(conn)
        print(f"  {len(arrids)} rows flagged for deletion", flush=True)
        if len(arrids) != lngextras:
            print(
                f"  WARNING: flagged ({len(arrids)}) != counted extras ({lngextras}); "
                "aborting to stay safe.",
                flush=True,
            )
            return
        f_delete_ids(conn, arrids)
        # Re-verify there are no duplicates left before the ALTER.
        lnggroups2, lngextras2 = f_count_duplicates(conn)
        if lngextras2 > 0:
            print(f"  ERROR: {lngextras2} duplicates remain after dedupe; skipping ALTER.", flush=True)
            return

    if intskipalter:
        print("  --skip-alter set -> dedupe done, add the index yourself "
              "(e.g. via pt-online-schema-change).", flush=True)
        return

    f_add_unique_index(conn)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="actually dedupe and add the index (default: dry run)")
    parser.add_argument("--skip-alter", action="store_true",
                        help="dedupe only; do not run the blocking ALTER")
    args = parser.parse_args()

    strmode = "APPLY" if args.apply else "DRY RUN"
    print(f"Migration add_unique_person_alias_key -- mode: {strmode}", flush=True)
    dblstart = time.time()

    conn = cp.f_getconnection()
    try:
        f_process_table(conn, args.apply, args.skip_alter)
    except Exception as err:  # noqa: BLE001 - surface, roll back, continue
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"  FAILED on {TABLE}: {err}", flush=True)

    print(f"\nDone in {time.time() - dblstart:.1f}s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
