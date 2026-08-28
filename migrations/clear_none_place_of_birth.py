"""
Migration: turn the literal text "None" back into SQL NULL in

    T_WC_TMDB_PERSON.PLACE_OF_BIRTH
    T_WC_TMDB_PERSON.COUNTRY_OF_BIRTH_LONG

Why
---
tmdb-crawler used to read the person payload with

    strpersonplaceofbirth = str(data['place_of_birth'])

and TMDb sends "place_of_birth": null for most persons. str(None) is the
4-character string "None", so the column was filled with that text instead of
NULL, on roughly 4.5M rows out of 5M.

The consequence was not cosmetic. Every filter of the form

    WHERE PLACE_OF_BIRTH IS NOT NULL AND PLACE_OF_BIRTH <> ''

matched those rows, so the country-of-birth pass of tmdb-person-preprocess read,
cleaned and compared 4.5M rows per run for nothing. The pass then derived
COUNTRY_OF_BIRTH_LONG = 'none' from that text and stored it, propagating the junk
into a second column.

The crawler now stores NULL (tmdb_functions.py, person payload block). This script
clears the backlog it left behind. Run it AFTER the crawler fix is deployed,
otherwise the crawler writes "None" again on the persons it refreshes.

What it does NOT touch
----------------------
TIM_UPDATED. The column has no ON UPDATE clause and the UPDATE below names only
the two target columns, so the timestamp keeps its meaning. This matters: tmdb-crawler
builds its person refresh queue from "TIM_UPDATED < J-30", and stamping 4.5M rows
here would starve that queue for a month. The script refuses to run if the column
has picked up an auto-update clause since.

COUNTRY_OF_BIRTH is left alone as well. It already holds '' on these rows, which is
what the pass stores for any place it cannot resolve. Turning only this subset into
NULL would make "unknown country" mean two different things depending on why it is
unknown.

How it walks the table
----------------------
By ranges of the primary key, not by repeated "WHERE PLACE_OF_BIRTH = 'None' LIMIT n".
Both forms are correct, but the range walk costs exactly one pass over the table and
its progress is legible (it reports the ID it has reached), whereas the LIMIT form
dives into an index whose contents it is concurrently emptying.

Safety / operations
-------------------
- DRY RUN by default: only counts. Pass --apply to write.
- Idempotent: once cleared, a second run finds nothing and does nothing.
- Each range is committed as it goes, no single giant transaction. Interrupting it
  is safe: the ranges already done stay done, re-running finishes the job.
- Run it with the preprocess stopped (bash off.sh, see README).

Usage
-----
    python migrations/clear_none_place_of_birth.py               # dry run
    python migrations/clear_none_place_of_birth.py --apply       # clear
    python migrations/clear_none_place_of_birth.py --apply --chunk-size 20000
"""
import argparse
import os
import sys
import time

# citizenphil.py lives in the repo root (one level up); make it importable no
# matter where this script is launched from.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import citizenphil as cp

TABLE = "T_WC_TMDB_PERSON"
CHUNK_SIZE = 50000
# The column collation is utf8mb4_unicode_ci, so these comparisons are already
# case-insensitive: 'None', 'none' and 'NONE' all match.
PLACE_JUNK = "None"
LONG_JUNK = "none"


def f_scalar(row, strkey, lngindex=0):
    """Read one column from a row that may be a dict (DictCursor) or a tuple."""
    if not row:
        return None
    return row[strkey] if isinstance(row, dict) else row[lngindex]


def f_check_tim_updated_is_manual(conn):
    """Abort unless TIM_UPDATED is still a plain column we control.

    If someone adds ON UPDATE CURRENT_TIMESTAMP to it, every UPDATE below would
    silently restamp 4.5M persons and starve the crawler's refresh queue.
    """
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT EXTRA
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND COLUMN_NAME = 'TIM_UPDATED'
        """,
        (TABLE,),
    )
    row = cursor.fetchone()
    if row is None:
        print(f"  ERROR: {TABLE}.TIM_UPDATED not found; aborting.", flush=True)
        return False
    strextra = (f_scalar(row, "EXTRA") or "").lower()
    if "on update" in strextra:
        print(
            f"  ERROR: {TABLE}.TIM_UPDATED carries '{strextra}'. The UPDATE would "
            "restamp every row it touches and starve tmdb-crawler's refresh queue. "
            "Aborting.",
            flush=True,
        )
        return False
    return True


def f_count_junk(conn):
    """Return (rows with PLACE_OF_BIRTH junk, rows with COUNTRY_OF_BIRTH_LONG junk).

    Both columns are indexed, so each count is an index-only scan of one value.
    """
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COUNT(*) AS n FROM {TABLE} WHERE PLACE_OF_BIRTH = %s", (PLACE_JUNK,)
    )
    lngplace = int(f_scalar(cursor.fetchone(), "n") or 0)
    cursor.execute(
        f"SELECT COUNT(*) AS n FROM {TABLE} WHERE COUNTRY_OF_BIRTH_LONG = %s", (LONG_JUNK,)
    )
    lnglong = int(f_scalar(cursor.fetchone(), "n") or 0)
    return lngplace, lnglong


def f_id_bounds(conn):
    """Return (min, max) ID_PERSON, or (None, None) on an empty table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT MIN(ID_PERSON) AS lo, MAX(ID_PERSON) AS hi FROM {TABLE}")
    row = cursor.fetchone()
    return f_scalar(row, "lo", 0), f_scalar(row, "hi", 1)


def f_clear_junk(conn, lngchunksize):
    """Walk the primary key and NULL out the junk. Returns rows updated."""
    lnglo, lnghi = f_id_bounds(conn)
    if lnglo is None:
        print("  table is empty, nothing to do", flush=True)
        return 0

    # Each column is cleared only where it is junk, so a row whose PLACE_OF_BIRTH is
    # genuine keeps it even if COUNTRY_OF_BIRTH_LONG next to it is not, and vice versa.
    strsql = (
        f"UPDATE {TABLE} SET "
        "PLACE_OF_BIRTH = CASE WHEN PLACE_OF_BIRTH = %s THEN NULL ELSE PLACE_OF_BIRTH END, "
        "COUNTRY_OF_BIRTH_LONG = CASE WHEN COUNTRY_OF_BIRTH_LONG = %s THEN NULL ELSE COUNTRY_OF_BIRTH_LONG END "
        "WHERE ID_PERSON > %s AND ID_PERSON <= %s "
        "AND (PLACE_OF_BIRTH = %s OR COUNTRY_OF_BIRTH_LONG = %s)"
    )

    lngupdated = 0
    lngstart = lnglo - 1
    lngranges = 0
    while lngstart < lnghi:
        lngend = min(lngstart + lngchunksize, lnghi)
        cursor = conn.cursor()
        cursor.execute(
            strsql,
            (PLACE_JUNK, LONG_JUNK, lngstart, lngend, PLACE_JUNK, LONG_JUNK),
        )
        conn.commit()
        lngupdated += cursor.rowcount
        lngstart = lngend
        lngranges += 1
        if lngranges % 20 == 0 or lngstart >= lnghi:
            print(
                f"    up to ID_PERSON {lngstart}/{lnghi}, {lngupdated} rows cleared",
                flush=True,
            )
    return lngupdated


def f_process(conn, intapply, lngchunksize):
    print(f"\n=== {TABLE} ===", flush=True)

    print("  counting rows holding the literal text ...", flush=True)
    lngplace, lnglong = f_count_junk(conn)
    print(f"  PLACE_OF_BIRTH = '{PLACE_JUNK}':        {lngplace}", flush=True)
    print(f"  COUNTRY_OF_BIRTH_LONG = '{LONG_JUNK}':  {lnglong}", flush=True)

    if lngplace == 0 and lnglong == 0:
        print("  nothing to clear -> already done", flush=True)
        return

    if not intapply:
        print("  DRY RUN -> no changes made. Re-run with --apply to clear.", flush=True)
        return

    if not f_check_tim_updated_is_manual(conn):
        return

    print(f"  clearing, by ranges of {lngchunksize} ID_PERSON ...", flush=True)
    lngupdated = f_clear_junk(conn, lngchunksize)
    print(f"  {lngupdated} rows cleared", flush=True)

    lngplace2, lnglong2 = f_count_junk(conn)
    if lngplace2 or lnglong2:
        print(
            f"  WARNING: {lngplace2} + {lnglong2} rows still hold the text. Rows created "
            "while this ran are expected; anything larger means the crawler fix is not "
            "deployed yet.",
            flush=True,
        )
    else:
        print("  verified: no row holds the literal text any more", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="actually clear the values (default: dry run)")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE,
                        help=f"ID_PERSON range width per UPDATE (default {CHUNK_SIZE})")
    args = parser.parse_args()

    strmode = "APPLY" if args.apply else "DRY RUN"
    print(f"Migration clear_none_place_of_birth -- mode: {strmode}", flush=True)
    dblstart = time.time()

    conn = cp.f_getconnection()
    try:
        f_process(conn, args.apply, max(1, args.chunk_size))
    except Exception as err:  # noqa: BLE001 - surface, roll back, continue
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"  FAILED on {TABLE}: {err}", flush=True)

    print(f"\nDone in {time.time() - dblstart:.1f}s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
