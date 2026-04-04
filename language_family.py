from typing import List


def _contains_any_in_ranges(s: str, ranges: List[range]) -> bool:
    """Return whether any character in ``s`` falls inside the provided Unicode ranges."""
    for ch in s:
        cp_ = ord(ch)
        for r in ranges:
            if cp_ in r:
                return True
    return False


def guess_language_family(person_name: str) -> str:
    """Guess a broad script or language family from the Unicode characters in a name."""
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
