"""
Migration: re-run the language-family guess on every alias currently filed as
"Arabic" in

    T_WC_TMDB_PERSON_ALSO_KNOWN_AS

and move to "Persian" or "Urdu" the ones that carry a letter specific to those
languages.

Why
---
guess_language_family recognises scripts by Unicode block. Arabic, Persian and
Urdu share the same blocks, so until 2026-09-06 all three answered "Arabic".
TMDB-PERSON-PREPROCESS-007 added two code-point sets tested inside the Arabic
branch, Urdu first (it uses the Persian letters and adds its own), then Persian.

The nightly preprocess already writes LANGUAGE_FAMILY drift back in place, so
this migration is not strictly required. It exists because the nightly pass only
revisits the persons it sweeps: without it, a person whose aliases never change
again keeps a label the code no longer agrees with, possibly forever.

Scope
-----
Only rows whose stored family is exactly "Arabic" can move: the code change
touches nothing outside that branch. Every other family is left alone, which is
what makes this migration cheap (a few thousand rows) rather than a full pass
over six million.

Soft deletes are skipped (DELETED = 0). A deleted row is not served to anyone,
and rewriting it would only add noise to the counters.

What cannot be fixed here, and it is not a bug
----------------------------------------------
Only the PRESENCE of a specific letter proves a language. A Persian name spelled
without any of the six Persian letters is indistinguishable from Arabic and stays
"Arabic". Pashto, Sindhi and Shahmukhi Punjabi are not modelled at all.

Safety / operations
-------------------
- DRY RUN by default: counts and samples only. Pass --apply to write.
- Idempotent: a second run after success reports zero rows to move.
- Updates are chunked and committed as they go, no single giant transaction.
- Read-only on PERSON_NAME: only LANGUAGE_FAMILY is written, so the folded index
  the nightly pass relies on cannot be disturbed.
"""
import argparse
import os
import sys
import time

# The migration is run as `python ./migrations/<name>.py` from the app root, so the
# package modules sit one directory up. Same prologue as the other migrations here.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import citizenphil as cp  # noqa: E402
from language_family import guess_language_family  # noqa: E402

TABLE = "T_WC_TMDB_PERSON_ALSO_KNOWN_AS"
SOURCE_FAMILY = "Arabic"
CHUNK = 1000


def f_collect(conn):
    """Return {target_family: [ID_ROW, ...]} for rows whose guess has moved."""
    arrmoves = {}
    arrsamples = {}
    lngseen = 0
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT ID_ROW, PERSON_NAME FROM `{TABLE}` "
            f"WHERE LANGUAGE_FAMILY = %s AND DELETED = 0 AND PERSON_NAME IS NOT NULL",
            (SOURCE_FAMILY,),
        )
        while True:
            arrrows = cur.fetchmany(10000)
            if not arrrows:
                break
            for row in arrrows:
                lngseen += 1
                strname = row["PERSON_NAME"] if isinstance(row, dict) else row[1]
                lngidrow = row["ID_ROW"] if isinstance(row, dict) else row[0]
                strguess = guess_language_family(strname)
                if strguess and strguess != SOURCE_FAMILY:
                    arrmoves.setdefault(strguess, []).append(lngidrow)
                    arrsamples.setdefault(strguess, [])
                    if len(arrsamples[strguess]) < 5:
                        arrsamples[strguess].append(strname)
    return lngseen, arrmoves, arrsamples


def f_apply(conn, arrmoves):
    """Write the new family, chunk by chunk, committing as we go."""
    lngwritten = 0
    for strfamily, arrids in sorted(arrmoves.items()):
        for lngstart in range(0, len(arrids), CHUNK):
            arrchunk = arrids[lngstart:lngstart + CHUNK]
            strplaceholders = ",".join(["%s"] * len(arrchunk))
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE `{TABLE}` SET LANGUAGE_FAMILY = %s WHERE ID_ROW IN ({strplaceholders})",
                    [strfamily] + arrchunk,
                )
            conn.commit()
            lngwritten += len(arrchunk)
            print(f"  {strfamily}: {lngwritten} rows written", flush=True)
    return lngwritten


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true",
                        help="actually write the new families (default: dry run)")
    args = parser.parse_args()

    strmode = "APPLY" if args.apply else "DRY RUN"
    print(f"Migration reclassify_arabic_script_families -- mode: {strmode}", flush=True)
    dblstart = time.time()

    conn = cp.f_getconnection()
    try:
        lngseen, arrmoves, arrsamples = f_collect(conn)
        lngtomove = sum(len(v) for v in arrmoves.values())
        print(f"\n  rows filed as '{SOURCE_FAMILY}' and live: {lngseen}")
        print(f"  rows the current code would move:        {lngtomove}")
        for strfamily in sorted(arrmoves):
            print(f"    -> {strfamily}: {len(arrmoves[strfamily])}")
            for strname in arrsamples[strfamily]:
                print(f"         {strname}")
        if not lngtomove:
            print("\n  Nothing to do.", flush=True)
        elif args.apply:
            print("", flush=True)
            lngwritten = f_apply(conn, arrmoves)
            print(f"\n  {lngwritten} rows updated.", flush=True)
        else:
            print("\n  Dry run: nothing written. Re-run with --apply.", flush=True)
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
