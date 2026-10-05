"""
Migration: keep the combining marks in the generated columns PERSON_NAME_NORM and
PERSON_NAME_KEY of

    T_WC_TMDB_PERSON_ALSO_KNOWN_AS

Why
---
PERSON_NAME_NORM kept letters and digits only, [^\\p{L}\\p{N} ]+ turned into a
space. In Devanagari, Thai, Kannada and the other abugidas the vowels are written
as combining marks (Unicode category M), which are neither letters nor digits, so
each vowel became a space and the word fell apart: "हयाओ मियाज़ाकी" (Hayao
Miyazaki) was stored as "हय ओ म य ज क". TMDB-PERSON-PREPROCESS-008 adds \\p{M}
to the class. Chinese, Japanese, Korean, Cyrillic and precomposed Latin names are
unchanged.

The same expression must stay identical in three places, changed together:
- here, the source table (this migration);
- T_WC_T2S_PERSON_ALSO_KNOWN_AS, rebuilt by tmdb-movie-preprocess Process 51 from
  the DDL written in its code (picked up at its next run, no manual drop);
- normalize_name() in fastapi-text2sql/rapidfuzz_query.py (restart the API so the
  in-memory index is rebuilt).

What it does
------------
- Default (DRY RUN): prints the current generation expression, then counts, per
  LANGUAGE_FAMILY, the rows whose PERSON_NAME_NORM would change, with samples.
  One full scan, no write.
- --rehearse: copies the table into T_WC_TMDB_PERSON_ALSO_KNOWN_AS_REHEARSAL,
  times the ALTER on the copy, then drops the copy. Measures the rebuild without
  touching the live table. Needs disk for a full copy.
- --apply: ALTER TABLE ... MODIFY both generated columns. A STORED generated
  column changes only through a full table rebuild (ALGORITHM=COPY): about 6.1
  million rows, writes to the table are blocked meanwhile, and the rebuild needs
  disk for a full copy. Then checks PERSON_NAME_KEY against PERSON_NAME_NORM.

Safety / operations
-------------------
- Idempotent: if the stored expression already carries p{M}, nothing is done.
- Run outside the nightly window: switch off this preprocess and
  tmdb-movie-preprocess (Process 51 reads this table) with their off.sh first,
  and back on with on.sh afterwards.
- PERSON_NAME is never written. The UNIQUE key is (ID_PERSON, PERSON_NAME), so the
  rebuild cannot create a duplicate. The fold used by the nightly pass to compare
  aliases (person_names.f_aliaskey) is computed in Python and does not read
  these columns.
"""
import argparse
import os
import sys
import time

# The migration is run as `python ./migrations/<name>.py` from the app root, so the
# package modules sit one directory up. Same prologue as the other migrations here.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import citizenphil as cp  # noqa: E402

TABLE = "T_WC_TMDB_PERSON_ALSO_KNOWN_AS"
REHEARSAL = TABLE + "_REHEARSAL"

# Raw strings: the regex must reach MariaDB as a backslash-p class. Keep these two
# expressions identical to Process 51 of tmdb-movie-preprocess (T2S copy).
NORM_EXPR = r"lcase(regexp_replace(regexp_replace(`PERSON_NAME`,'[^\\p{L}\\p{M}\\p{N} ]+',' '),' +',' '))"
KEY_EXPR = r"replace(`PERSON_NAME_NORM`,' ','')"

ALTER_TEMPLATE = (
    "ALTER TABLE `__TABLE__` "
    "MODIFY `PERSON_NAME_NORM` varchar(255) GENERATED ALWAYS AS (" + NORM_EXPR + ") STORED, "
    "MODIFY `PERSON_NAME_KEY` varchar(255) GENERATED ALWAYS AS (" + KEY_EXPR + ") STORED"
)


def f_rows(cur):
    arrrows = cur.fetchall()
    return [r if isinstance(r, dict) else dict(zip([d[0] for d in cur.description], r)) for r in arrrows]


def f_current_expression(conn, strtable):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT GENERATION_EXPRESSION FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = 'PERSON_NAME_NORM'",
            (strtable,),
        )
        arrrows = f_rows(cur)
    return arrrows[0]["GENERATION_EXPRESSION"] if arrrows else None


def f_survey(conn):
    """Count, per family, the rows whose NORM would change, and show a few."""
    strwhere = f"PERSON_NAME IS NOT NULL AND NOT (PERSON_NAME_NORM <=> {NORM_EXPR})"
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT LANGUAGE_FAMILY, COUNT(*) AS n FROM `{TABLE}` WHERE {strwhere} "
            f"GROUP BY LANGUAGE_FAMILY ORDER BY n DESC"
        )
        arrcounts = f_rows(cur)
        cur.execute(
            f"SELECT LANGUAGE_FAMILY, PERSON_NAME, PERSON_NAME_NORM, {NORM_EXPR} AS NEW_NORM "
            f"FROM `{TABLE}` WHERE {strwhere} AND ID_PERSON IN (608, 5026) LIMIT 20"
        )
        arrsamples = f_rows(cur)
    return arrcounts, arrsamples


def f_alter(conn, strtable):
    dblstart = time.time()
    with conn.cursor() as cur:
        cur.execute(ALTER_TEMPLATE.replace("__TABLE__", strtable))
    conn.commit()
    return time.time() - dblstart


def f_rehearse(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s "
            "AND (GENERATION_EXPRESSION IS NULL OR GENERATION_EXPRESSION = '') "
            "ORDER BY ORDINAL_POSITION",
            (TABLE,),
        )
        strcols = ", ".join(f"`{r['COLUMN_NAME']}`" for r in f_rows(cur))
        cur.execute(f"DROP TABLE IF EXISTS `{REHEARSAL}`")
        cur.execute(f"CREATE TABLE `{REHEARSAL}` LIKE `{TABLE}`")
        print(f"  copying {TABLE} into {REHEARSAL} ...", flush=True)
        dblstart = time.time()
        cur.execute(f"INSERT INTO `{REHEARSAL}` ({strcols}) SELECT {strcols} FROM `{TABLE}`")
    conn.commit()
    print(f"  copy done in {time.time() - dblstart:.1f}s", flush=True)
    try:
        print(f"  ALTER on {REHEARSAL} ...", flush=True)
        dblalter = f_alter(conn, REHEARSAL)
        print(f"  ALTER on the copy took {dblalter:.1f}s", flush=True)
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{REHEARSAL}`")
        conn.commit()


def f_verify(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT COUNT(*) AS n FROM `{TABLE}` "
            f"WHERE NOT (PERSON_NAME_KEY <=> replace(PERSON_NAME_NORM,' ',''))"
        )
        lngbadkey = f_rows(cur)[0]["n"]
        cur.execute(
            f"SELECT PERSON_NAME, PERSON_NAME_NORM, PERSON_NAME_KEY FROM `{TABLE}` "
            f"WHERE ID_PERSON = 608 AND LANGUAGE_FAMILY IN ('Devanagari', 'Kannada', 'Thai') LIMIT 10"
        )
        arrsamples = f_rows(cur)
    print(f"\n  rows whose KEY is not NORM without spaces (must be 0): {lngbadkey}")
    for row in arrsamples:
        print(f"    {row['PERSON_NAME']}  ->  {row['PERSON_NAME_NORM']}  /  {row['PERSON_NAME_KEY']}")
    return lngbadkey == 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--apply", action="store_true", help="rebuild the live table (default: dry run)")
    group.add_argument("--rehearse", action="store_true", help="time the rebuild on a throw-away copy")
    args = parser.parse_args()

    strmode = "APPLY" if args.apply else ("REHEARSE" if args.rehearse else "DRY RUN")
    print(f"Migration keep_marks_in_alias_norm -- mode: {strmode}", flush=True)
    dblstart = time.time()

    conn = cp.f_getconnection()
    try:
        strexpr = f_current_expression(conn, TABLE)
        print(f"\n  current PERSON_NAME_NORM expression: {strexpr}")
        if strexpr is None:
            print(f"  FAILED: {TABLE}.PERSON_NAME_NORM not found.", flush=True)
            return 1
        if "p{M}" in strexpr:
            print("\n  The expression already keeps the marks. Nothing to do.", flush=True)
            return 0

        if args.rehearse:
            f_rehearse(conn)
        else:
            arrcounts, arrsamples = f_survey(conn)
            lngtotal = sum(r["n"] for r in arrcounts)
            print(f"\n  rows whose PERSON_NAME_NORM will change: {lngtotal}")
            for row in arrcounts:
                print(f"    {row['LANGUAGE_FAMILY']}: {row['n']}")
            print("\n  samples (Hayao Miyazaki 608, Akira Kurosawa 5026):")
            for row in arrsamples:
                print(f"    [{row['LANGUAGE_FAMILY']}] {row['PERSON_NAME']}")
                print(f"        now: {row['PERSON_NAME_NORM']}")
                print(f"        new: {row['NEW_NORM']}")
            if args.apply:
                print(f"\n  ALTER on {TABLE} (table rebuild) ...", flush=True)
                dblalter = f_alter(conn, TABLE)
                print(f"  ALTER took {dblalter:.1f}s", flush=True)
                print(f"  new expression: {f_current_expression(conn, TABLE)}")
                if not f_verify(conn):
                    return 1
            else:
                print("\n  Dry run: nothing written. Time it with --rehearse, run it with --apply.", flush=True)
    except Exception as err:  # noqa: BLE001 - surface, roll back, continue
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"  FAILED on {TABLE}: {err}", flush=True)
        return 1

    print(f"\nDone in {time.time() - dblstart:.1f}s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
