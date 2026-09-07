from typing import List


def _contains_any_in_ranges(s: str, ranges: List[range]) -> bool:
    """Return whether any character in ``s`` falls inside the provided Unicode ranges."""
    for ch in s:
        cp_ = ord(ch)
        for r in ranges:
            if cp_ in r:
                return True
    return False


# Letters that separate three languages sharing the Arabic script. They all live inside
# the Arabic blocks, so a range test cannot tell them apart: only the presence of one of
# these code points can. Absence proves nothing, a Persian name spelled without any of
# them is indistinguishable from Arabic and stays "Arabic".
PERSIAN_LETTERS = frozenset({
    0x067E,  # پ pe
    0x0686,  # چ che
    0x0698,  # ژ zhe
    0x06A9,  # ک keheh, the Persian kaf, against the Arabic ك U+0643
    0x06AF,  # گ gaf
    0x06CC,  # ی farsi yeh, against the Arabic ي U+064A
})

# Urdu uses the Persian letters above and adds its own. Tested first, otherwise every
# Urdu name would answer "Persian" and the split would trade one wrong label for another.
URDU_LETTERS = frozenset({
    0x0679,  # ٹ tteh
    0x0688,  # ڈ ddal
    0x0691,  # ڑ rreh
    0x06BA,  # ں noon ghunna
    0x06BE,  # ھ heh doachashmee
    0x06C1,  # ہ heh goal, absent from Arabic and Persian, which write ه
    0x06C2,  # ۂ heh goal with hamza above
    0x06D2,  # ے yeh barree
    0x06D3,  # ۓ yeh barree with hamza above
})


def _contains_any_in_set(s: str, codepoints: frozenset) -> bool:
    """Return whether any character in ``s`` is one of the given code points."""
    return any(ord(ch) in codepoints for ch in s)


def guess_language_family(person_name: str) -> str:
    """Guess a broad script or language family from the Unicode characters in a name.

    Scripts are recognised by Unicode block, which is enough everywhere except the Arabic
    block, where Arabic, Persian and Urdu share the same ranges and are separated by the
    letters each language adds. See ``PERSIAN_LETTERS`` and ``URDU_LETTERS``.
    """
    if not person_name:
        return ""

    s = person_name.strip()
    if not s:
        return ""

    # Unicode blocks (approx.)
    hangul_ranges = [range(0xAC00, 0xD7B0), range(0x1100, 0x1200), range(0x3130, 0x3190)]
    hiragana_katakana_ranges = [range(0x3040, 0x30A0), range(0x30A0, 0x3100), range(0x31F0, 0x3200)]
    han_ranges = [range(0x4E00, 0xA000), range(0x3400, 0x4DC0)]
    cyrillic_ranges = [range(0x0400, 0x0530), range(0x2DE0, 0x2E00), range(0xA640, 0xA6A0)]
    arabic_ranges = [range(0x0600, 0x0700), range(0x0750, 0x0780), range(0x08A0, 0x0900)]
    hebrew_ranges = [range(0x0590, 0x0600)]

    devanagari_ranges = [range(0x0900, 0x0980)]
    greek_ranges = [range(0x0370, 0x0400)]
    thai_ranges = [range(0x0E00, 0x0E80)]
    armenian_ranges = [range(0x0530, 0x0590)]
    georgian_ranges = [range(0x10A0, 0x1100), range(0x2D00, 0x2D30)]
    bengali_ranges = [range(0x0980, 0x0A00)]
    tamil_ranges = [range(0x0B80, 0x0C00)]
    telugu_ranges = [range(0x0C00, 0x0C80)]
    kannada_ranges = [range(0x0C80, 0x0D00)]
    malayalam_ranges = [range(0x0D00, 0x0D80)]
    ethiopic_ranges = [range(0x1200, 0x1380), range(0x1380, 0x13A0)]
    khmer_ranges = [range(0x1780, 0x1800)]
    sinhala_ranges = [range(0x0D80, 0x0E00)]

    has_hangul = _contains_any_in_ranges(s, hangul_ranges)
    has_kana = _contains_any_in_ranges(s, hiragana_katakana_ranges)
    has_han = _contains_any_in_ranges(s, han_ranges)
    has_cyrillic = _contains_any_in_ranges(s, cyrillic_ranges)
    has_arabic = _contains_any_in_ranges(s, arabic_ranges)
    has_hebrew = _contains_any_in_ranges(s, hebrew_ranges)

    has_devanagari = _contains_any_in_ranges(s, devanagari_ranges)
    has_greek = _contains_any_in_ranges(s, greek_ranges)
    has_thai = _contains_any_in_ranges(s, thai_ranges)
    has_armenian = _contains_any_in_ranges(s, armenian_ranges)
    has_georgian = _contains_any_in_ranges(s, georgian_ranges)
    has_bengali = _contains_any_in_ranges(s, bengali_ranges)
    has_tamil = _contains_any_in_ranges(s, tamil_ranges)
    has_telugu = _contains_any_in_ranges(s, telugu_ranges)
    has_kannada = _contains_any_in_ranges(s, kannada_ranges)
    has_malayalam = _contains_any_in_ranges(s, malayalam_ranges)
    has_ethiopic = _contains_any_in_ranges(s, ethiopic_ranges)
    has_khmer = _contains_any_in_ranges(s, khmer_ranges)
    has_sinhala = _contains_any_in_ranges(s, sinhala_ranges)

    if has_hangul:
        return "Hangul"
    if has_kana:
        return "Japanese"
    if has_han:
        return "Chinese"
    if has_cyrillic:
        return "Cyrillic"
    if has_greek:
        return "Greek"
    if has_armenian:
        return "Armenian"
    if has_georgian:
        return "Georgian"
    if has_arabic:
        # Same script, three languages. Most specific first.
        if _contains_any_in_set(s, URDU_LETTERS):
            return "Urdu"
        if _contains_any_in_set(s, PERSIAN_LETTERS):
            return "Persian"
        return "Arabic"
    if has_hebrew:
        return "Hebrew"
    if has_devanagari:
        return "Devanagari"
    if has_bengali:
        return "Bengali"
    if has_tamil:
        return "Tamil"
    if has_telugu:
        return "Telugu"
    if has_kannada:
        return "Kannada"
    if has_malayalam:
        return "Malayalam"
    if has_sinhala:
        return "Sinhala"
    if has_thai:
        return "Thai"
    if has_khmer:
        return "Khmer"
    if has_ethiopic:
        return "Ethiopic"
    return "Latin"
