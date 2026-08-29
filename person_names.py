"""Building and normalizing the list of names a person is known by.

Shared by the pipeline and by the diagnostics under migrations/, so both agree on
what the alias set of a person is supposed to be.
"""
import unicodedata
from typing import List, Optional, Set

# Combining marks are dropped only when they sit on a Latin base character. On a
# Latin letter a diacritic is decoration the collation ignores, so "Beyonce" and
# "Beyoncé" are one row for the server. Elsewhere it carries meaning: U+3099 turns
# か into が, and folding those together would make the pass believe one of two
# genuinely distinct aliases is a duplicate.
LNGLATINMAX = 0x0250


def split_also_known_as(value: Optional[str]) -> List[str]:
    """Split the pipe-separated ALSO_KNOWN_AS blob into a deduplicated list."""
    if not value:
        return []
    parts = [p.strip() for p in str(value).split('|')]
    parts = [p for p in parts if p]

    # Deduplicate while preserving order
    seen: Set[str] = set()
    out: List[str] = []
    for p in parts:
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def f_aliaskey(person_name: str) -> str:
    """Return the key under which the database considers two aliases to be one.

    T_WC_TMDB_PERSON_ALSO_KNOWN_AS.PERSON_NAME is utf8mb4_unicode_ci and now carries
    a UNIQUE key on (ID_PERSON, PERSON_NAME), so "Jean Reno" and "JEAN RENO" are the
    same row as far as the server is concerned. Matching aliases with Python's exact
    string equality therefore made the pass believe one of the two was missing: it
    inserted it, the ON DUPLICATE KEY UPDATE landed on the existing row and imposed
    its DISPLAY_ORDER, and the next run put the other one back. Around 26k writes per
    run, forever, with no deletion to show for it.

    Comparing on this key instead makes the Python side agree with the server about
    what counts as one alias: it folds case, and diacritics on Latin letters, which is
    what utf8mb4_unicode_ci does.

    It does not claim to reproduce that collation exactly, and it does not have to.
    The caller looks an alias up by its exact spelling first and only then by this key,
    so a key that folds too much can at worst skip an insert, never delete an alias the
    server was willing to keep. A key that folds too little just leaves a little churn,
    visible in the alsoknownasinserted / alsoknownasupdated server variables.
    """
    strdecomposed = unicodedata.normalize("NFD", person_name)
    arrkept = []
    lngbase = 0
    for chr_ in strdecomposed:
        if unicodedata.combining(chr_):
            if lngbase < LNGLATINMAX:
                continue
            arrkept.append(chr_)
            continue
        lngbase = ord(chr_)
        arrkept.append(chr_)
    return unicodedata.normalize("NFC", "".join(arrkept)).casefold()


def build_person_names(name: Optional[str], also_known_as: Optional[str]) -> List[str]:
    """Return the ordered alias list of a person: NAME first, then ALSO_KNOWN_AS.

    Aliases that collide under f_aliaskey are collapsed, keeping the first spelling,
    so the returned list can never ask the database to hold two rows where its unique
    key allows one.
    """
    names: List[str] = []

    if name:
        primary_name = str(name).strip()
        if primary_name:
            names.append(primary_name)

    names.extend(split_also_known_as(also_known_as))

    seen: Set[str] = set()
    out: List[str] = []
    for person_name in names:
        strkey = f_aliaskey(person_name)
        if strkey not in seen:
            seen.add(strkey)
            out.append(person_name)
    return out
