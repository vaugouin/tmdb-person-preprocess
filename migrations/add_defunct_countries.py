"""
Migration: defunct countries and country succession, shared by the reader and the writer
of TMDb places of birth.

    T_WC_COUNTRY                + ID_WIKIDATA, IS_DEFUNCT, DAT_START, DAT_END, PLACE_NAME_EN
                                + rows for the defunct countries (DELETED = 1)
    T_WC_COUNTRY_SUCCESSION     new: which country a place of today's country was in, by date
    T_WC_TMDB_PERSON            + COUNTRY_OF_BIRTH_HISTORIC

Why
---
TMDb's rule for Place of Birth (https://www.themoviedb.org/bible/people, 2026-11-14):
"The place of birth should reflect the names and political divisions used at the time of
birth", the current name added in brackets: "Brno, Czechoslovakia [now Czech Republic]".
Two processes handle that text and must agree on the defunct countries:

- tmdb-person-preprocess READS it (PLACE_OF_BIRTH -> COUNTRY_OF_BIRTH, today's country).
  It also needs to keep the country of the time (COUNTRY_OF_BIRTH_HISTORIC).
- selenium-tmdb WRITES it on TMDb from Wikidata, which only knows today's country of a
  place. From 2026-11-11 to 11-13 it wrote "Brno, Czech Republic" for people born in
  Czechoslovakia; a TMDb contributor reported it. The writer needs, for today's country
  and a birth date, the country of the time.

TMDb's own production countries already use the codes of the defunct countries (SU, XC,
YU, XG, CS, AN, BU, ZR, TP): 28,000 rows of T_WC_TMDB_MOVIE_PRODUCTION_COUNTRY carried a
code T_WC_COUNTRY did not have, so tmdb-front printed the raw code. The defunct countries
are therefore rows of T_WC_COUNTRY, with DELETED = 1 like YU, AN and TP already were: the
front translates them (f_countrycodetranslate reads every row) but does not offer them as a
watch country (f_watchcountryoptions reads DELETED = 0), and the country-of-birth lookup of
tmdb-person-preprocess, which reads DELETED = 0, does not map a place to them.

Codes: TMDb's when TMDb has one (SU, XC, YU, XG, CS, AN, BU, ZR, TP). The others take the
ISO 3166 user-assigned range QM-QZ, which TMDb does not use (its own are X*): QM
Austria-Hungary, QO Russian Empire, QP West Germany, QS Protectorate of Bohemia and Moravia.

How the writer reads the succession table
-----------------------------------------
For a place whose country today is C, born on day D:

1. the rows of C covering D, region-specific rows first (REGION_QIDS lists Wikidata
   regions, P131 of the place, one to three levels up), in DISPLAY_ORDER: the first one
   that applies gives COUNTRY_CODE_AT_BIRTH. NULL there means "not known from the country
   alone": write nothing. A region-specific row covering D when the place's region is
   unknown: write nothing.
2. no row: C itself when DAT_START of C is NULL or <= D, otherwise nothing (C did not
   exist yet, and the country of the time is not in the table).

The table covers what the contributor reported, Czechoslovakia, the USSR, Yugoslavia and
both Germanies, plus Austria-Hungary and the Russian Empire, with the borders that moved
inside them. Every gap is on the side of writing nothing: a country whose borders changed
in ways the table does not follow (Poland before 1945, western Ukraine and Belarus before
1945, Lithuania between the wars) has no row for those years and a DAT_START after them.
DAT_START of the other countries is their Wikidata inception (P571) from 1850 on, so a
place in a country that became independent after the birth is not written either; GB and
CN are set to NULL (their inception is a change of regime, not of territory). Known
limits, in NOTE: the WWII occupations, Istria and the Slovene Littoral (Italian 1920-1947).

Safety / operations
-------------------
- DRY RUN by default: prints what it would do. --apply to write.
- Idempotent: ADD COLUMN IF NOT EXISTS (ALGORITHM=INSTANT, no table rebuild, even on the
  5M rows of T_WC_TMDB_PERSON), the defunct rows upserted on COUNTRY_CODE, the reference
  columns of the live rows set again, T_WC_COUNTRY_SUCCESSION emptied and filled again
  (it is reference data this script owns: edit SUCCESSION below, not the table).
- T_WC_TMDB_PERSON.TIM_UPDATED is not touched (AGENTS.md): only a column is added.
- After it: bump lngderivationversion in tmdb-person-preprocess.py, so the next run fills
  COUNTRY_OF_BIRTH_HISTORIC on every person.

Usage
-----
    python migrations/add_defunct_countries.py            # dry run
    python migrations/add_defunct_countries.py --apply
"""
import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import citizenphil as cp

# --- Defunct countries -----------------------------------------------------------------
# code, name EN, name FR, name used in TMDb places, Wikidata QID, start, end, aliases.
# The aliases are matched by the reader, lower case, after normalisation; the place name
# comes from the most frequent spelling in TMDb's places (2026-11-14, 443,542 persons).
DEFUNCT = [
    ('SU', 'Soviet Union', 'URSS', 'USSR', 'Q15180', '1922-12-30', '1991-12-26',
     'ussr|u.s.s.r.|ussr.|soviet union|cccp|ссср|urss|zsrr|sssr|union of soviet socialist republics'),
    ('XC', 'Czechoslovakia', 'Tchécoslovaquie', 'Czechoslovakia', 'Q33946', '1918-10-28', '1992-12-31',
     'československo|ceskoslovensko|čssr|cssr|czechoslovak socialist republic|tchécoslovaquie|tschechoslowakei'),
    ('YU', 'Yugoslavia', 'Yougoslavie', 'Yugoslavia', 'Q36704', '1918-12-01', '2003-02-04',
     'jugoslávie|jugoslavija|југославија|jugosławia|sfr yugoslavia|sfry|fr yugoslavia|federal republic of yugoslavia|'
     'kingdom of yugoslavia|kingdom of serbs, croats and slovenes|socialist federal republic of yugoslavia'),
    ('XG', 'East Germany', "Allemagne de l'Est (RDA)", 'East Germany', 'Q16957', '1949-10-07', '1990-10-03',
     'gdr|german democratic republic|ddr|deutsche demokratische republik|rda'),
    ('QP', 'West Germany', "Allemagne de l'Ouest (RFA)", 'West Germany', 'Q713750', '1949-05-23', '1990-10-03',
     'frg|brd|rfa'),
    ('CS', 'Serbia and Montenegro', 'Serbie-et-Monténégro', 'Serbia and Montenegro', 'Q37024', '2003-02-04', '2006-06-05',
     'state union of serbia and montenegro|srbija i crna gora'),
    ('QM', 'Austria-Hungary', 'Autriche-Hongrie', 'Austria-Hungary', 'Q28513', '1867-03-30', '1918-10-31',
     'austria hungary|austriahungary|austro-hungarian empire|austro-hungary|rakousko-uhersko|österreich-ungarn|'
     'austro-węgry|osztrák–magyar monarchia'),
    ('QO', 'Russian Empire', 'Empire russe', 'Russian Empire', 'Q34266', '1721-11-02', '1917-09-14',
     'rossiyskaya imperiya|российская империя|imperium rosyjskie|empire of russia'),
    ('QS', 'Protectorate of Bohemia and Moravia', 'Protectorat de Bohême-Moravie',
     'Protectorate of Bohemia and Moravia', 'Q152750', '1939-03-16', '1945-05-08',
     'protectorate bohemia and moravia|protektorát čechy a morava|protektorat böhmen und mähren'),
    ('BU', 'Burma', 'Birmanie', 'Burma', None, '1948-01-04', '1989-06-18', 'burma.'),
    ('ZR', 'Zaire', 'Zaïre', 'Zaire', None, '1971-10-27', '1997-05-17', 'zaïre|republic of zaire'),
    ('AN', 'Netherlands Antilles', 'Antilles néerlandaises', 'Netherlands Antilles', 'Q25227', '1954-12-15', '2010-10-10',
     'nederlandse antillen'),
    ('TP', 'East Timor', 'Timor oriental', 'East Timor', None, '1975-11-28', '2002-05-20', ''),
]

# --- Live countries --------------------------------------------------------------------
# Wikidata QID of each ISO 3166-1 alpha-2 code (WDQS, P297, 2026-11-14).
QIDS = {
    'AD': 'Q228', 'AE': 'Q878', 'AF': 'Q889', 'AG': 'Q781', 'AL': 'Q222', 'AM': 'Q399',
    'AO': 'Q916', 'AR': 'Q414', 'AT': 'Q40', 'AU': 'Q408', 'AW': 'Q21203', 'AZ': 'Q227',
    'BA': 'Q225', 'BB': 'Q244', 'BD': 'Q902', 'BE': 'Q31', 'BF': 'Q965', 'BG': 'Q219',
    'BH': 'Q398', 'BI': 'Q967', 'BJ': 'Q962', 'BN': 'Q921', 'BO': 'Q750', 'BR': 'Q155',
    'BS': 'Q778', 'BT': 'Q917', 'BW': 'Q963', 'BY': 'Q184', 'BZ': 'Q242', 'CA': 'Q16',
    'CD': 'Q974', 'CF': 'Q929', 'CG': 'Q971', 'CH': 'Q39', 'CI': 'Q1008', 'CK': 'Q26988',
    'CL': 'Q298', 'CM': 'Q1009', 'CN': 'Q148', 'CO': 'Q739', 'CR': 'Q800', 'CU': 'Q241',
    'CV': 'Q1011', 'CW': 'Q25279', 'CY': 'Q229', 'CZ': 'Q213', 'DE': 'Q183', 'DJ': 'Q977',
    'DK': 'Q35', 'DM': 'Q784', 'DO': 'Q786', 'DZ': 'Q262', 'EC': 'Q736', 'EE': 'Q191',
    'EG': 'Q79', 'ER': 'Q986', 'ES': 'Q29', 'ET': 'Q115', 'FI': 'Q33', 'FJ': 'Q712',
    'FM': 'Q702', 'FO': 'Q4628', 'FR': 'Q142', 'GA': 'Q1000', 'GB': 'Q145', 'GD': 'Q769',
    'GE': 'Q230', 'GH': 'Q117', 'GI': 'Q1410', 'GL': 'Q223', 'GM': 'Q1005', 'GN': 'Q1006',
    'GQ': 'Q983', 'GR': 'Q41', 'GT': 'Q774', 'GW': 'Q1007', 'GY': 'Q734', 'HN': 'Q783',
    'HR': 'Q224', 'HT': 'Q790', 'HU': 'Q28', 'ID': 'Q252', 'IE': 'Q27', 'IL': 'Q801',
    'IN': 'Q668', 'IQ': 'Q796', 'IR': 'Q794', 'IS': 'Q189', 'IT': 'Q38', 'JM': 'Q766',
    'JO': 'Q810', 'JP': 'Q17', 'KE': 'Q114', 'KG': 'Q813', 'KH': 'Q424', 'KI': 'Q710',
    'KM': 'Q970', 'KN': 'Q763', 'KP': 'Q423', 'KR': 'Q884', 'KW': 'Q817', 'KZ': 'Q232',
    'LA': 'Q819', 'LB': 'Q822', 'LC': 'Q760', 'LI': 'Q347', 'LK': 'Q854', 'LR': 'Q1014',
    'LS': 'Q1013', 'LT': 'Q37', 'LU': 'Q32', 'LV': 'Q211', 'LY': 'Q1016', 'MA': 'Q1028',
    'MC': 'Q235', 'MD': 'Q217', 'ME': 'Q236', 'MG': 'Q1019', 'MH': 'Q709', 'MK': 'Q221',
    'ML': 'Q912', 'MM': 'Q836', 'MN': 'Q711', 'MR': 'Q1025', 'MT': 'Q233', 'MU': 'Q1027',
    'MV': 'Q826', 'MW': 'Q1020', 'MX': 'Q96', 'MY': 'Q833', 'MZ': 'Q1029', 'NA': 'Q1030',
    'NE': 'Q1032', 'NG': 'Q1033', 'NI': 'Q811', 'NL': 'Q29999', 'NO': 'Q20', 'NP': 'Q837',
    'NR': 'Q697', 'NU': 'Q34020', 'NZ': 'Q664', 'OM': 'Q842', 'PA': 'Q804', 'PE': 'Q419',
    'PG': 'Q691', 'PH': 'Q928', 'PK': 'Q843', 'PL': 'Q36', 'PS': 'Q219060', 'PT': 'Q45',
    'PW': 'Q695', 'PY': 'Q733', 'QA': 'Q846', 'RO': 'Q218', 'RS': 'Q403', 'RU': 'Q159',
    'RW': 'Q1037', 'SA': 'Q851', 'SB': 'Q685', 'SC': 'Q1042', 'SD': 'Q1049', 'SE': 'Q34',
    'SG': 'Q334', 'SI': 'Q215', 'SK': 'Q214', 'SL': 'Q1044', 'SM': 'Q238', 'SN': 'Q1041',
    'SO': 'Q1045', 'SR': 'Q730', 'SS': 'Q958', 'ST': 'Q1039', 'SV': 'Q792', 'SX': 'Q26273',
    'SY': 'Q858', 'SZ': 'Q1050', 'TD': 'Q657', 'TG': 'Q945', 'TH': 'Q869', 'TJ': 'Q863',
    'TL': 'Q574', 'TM': 'Q874', 'TN': 'Q948', 'TO': 'Q678', 'TR': 'Q43', 'TT': 'Q754',
    'TV': 'Q672', 'TW': 'Q865', 'TZ': 'Q924', 'UA': 'Q212', 'UG': 'Q1036', 'US': 'Q30',
    'UY': 'Q77', 'UZ': 'Q265', 'VA': 'Q237', 'VC': 'Q757', 'VE': 'Q717', 'VN': 'Q881',
    'VU': 'Q686', 'WS': 'Q683', 'XK': 'Q1246', 'YE': 'Q805', 'ZA': 'Q258', 'ZM': 'Q953',
    'ZW': 'Q954',
}

# DAT_START: Wikidata inception (P571, earliest, from 1850 on) unless overridden below.
INCEPTIONS = {
    'AE': '1971-01-01', 'AG': '1981-11-01', 'AL': '1912-11-28', 'AM': '1991-09-23',
    'AO': '1975-11-11', 'AU': '1901-01-01', 'AW': '1986-01-01', 'AZ': '1991-01-01',
    'BA': '1992-03-06', 'BD': '1971-03-26', 'BF': '1960-01-01', 'BG': '1878-07-13',
    'BH': '1971-08-14', 'BI': '1962-07-01', 'BJ': '1960-08-01', 'BS': '1973-07-10',
    'BT': '1907-12-17', 'BW': '1966-09-30', 'BZ': '1981-09-21', 'CA': '1867-07-01',
    'CD': '1960-06-30', 'CF': '1960-01-01', 'CG': '1960-01-01', 'CI': '1960-01-01',
    'CK': '1965-08-04', 'CM': '1960-01-01', 'CN': '1949-10-01', 'CU': '1898-12-10',
    'CV': '1975-07-05', 'CW': '1954-01-01', 'CY': '1960-08-16', 'DJ': '1977-06-23',
    'DM': '1978-01-01', 'DZ': '1962-07-03', 'EE': '1918-02-24', 'EG': '1922-02-28',
    'ER': '1993-05-24', 'ET': '1974-09-12', 'FI': '1917-12-06', 'FJ': '1970-01-01',
    'FM': '1947-01-01', 'FO': '1948-04-01', 'GA': '1960-01-01', 'GB': '1921-12-06',
    'GD': '1974-01-01', 'GH': '1957-01-01', 'GM': '1965-01-01', 'GN': '1958-10-02',
    'GQ': '1968-10-12', 'GW': '1974-01-01', 'GY': '1966-01-01', 'HR': '1991-06-25',
    'ID': '1945-08-17', 'IL': '1948-05-14', 'IN': '1947-08-15', 'IQ': '1932-01-01',
    'IS': '1918-12-01', 'JM': '1962-01-01', 'JO': '1946-01-01', 'KE': '1963-01-01',
    'KG': '1991-01-01', 'KH': '1953-11-09', 'KI': '1979-07-12', 'KM': '1975-07-06',
    'KN': '1967-02-27', 'KP': '1948-09-09', 'KR': '1948-08-15', 'KW': '1991-02-26',
    'KZ': '1936-01-01', 'LA': '1949-01-01', 'LB': '1943-11-22', 'LC': '1967-03-01',
    'LK': '1972-05-22', 'LS': '1966-01-01', 'LV': '1918-11-18', 'MD': '1991-08-27',
    'ME': '2006-06-03', 'MG': '1960-01-01', 'MH': '1979-05-01', 'MK': '1991-01-01',
    'ML': '1960-09-22', 'MM': '1948-01-04', 'MN': '1911-01-01', 'MR': '1960-11-28',
    'MT': '1964-09-21', 'MU': '1968-03-12', 'MV': '1965-07-26', 'MW': '1964-07-06',
    'MY': '1963-09-16', 'MZ': '1975-06-25', 'NA': '1990-01-01', 'NE': '1960-01-01',
    'NG': '1963-10-01', 'NR': '1968-01-31', 'NU': '1974-01-01', 'OM': '1970-07-23',
    'PA': '1903-01-01', 'PG': '1975-01-01', 'PK': '1947-08-14', 'PS': '1988-11-15',
    'PW': '1981-01-01', 'QA': '1870-01-01', 'RW': '1962-01-01', 'SB': '1978-01-01',
    'SC': '1976-06-29', 'SD': '1956-01-01', 'SK': '1939-03-14', 'SL': '1971-04-19',
    'SN': '1960-01-01', 'SO': '1960-01-01', 'SR': '1975-11-25', 'SS': '2011-07-09',
    'ST': '1975-07-12', 'SX': '2010-01-01', 'SY': '1920-03-08', 'SZ': '1968-09-06',
    'TD': '1960-08-11', 'TG': '1960-04-27', 'TJ': '1924-10-27', 'TL': '1975-11-28',
    'TM': '1991-10-27', 'TN': '1956-03-20', 'TO': '1970-01-01', 'TR': '1923-10-29',
    'TT': '1889-01-01', 'TV': '1978-10-01', 'TW': '1912-01-01', 'TZ': '1964-04-26',
    'UG': '1962-01-01', 'UZ': '1991-01-01', 'VA': '1929-02-11', 'VC': '1979-01-01',
    'VU': '1980-07-30', 'WS': '1962-01-01', 'XK': '2008-02-17', 'YE': '1990-05-22',
    'ZA': '1910-05-31', 'ZM': '1964-10-24', 'ZW': '1980-04-18',
}
DAT_START_OVERRIDES = {
    # a change of regime, not of territory: the country is written the same before
    'GB': None, 'CN': None,
    # end of the defunct country they were part of (first day on their own)
    'RU': '1991-12-26', 'UA': '1991-08-24', 'BY': '1991-08-25', 'MD': '1991-08-27', 'GE': '1991-04-09',
    'AM': '1991-09-21', 'AZ': '1991-10-18', 'KZ': '1991-12-16', 'UZ': '1991-08-31', 'TM': '1991-10-27',
    'KG': '1991-08-31', 'TJ': '1991-09-09', 'EE': '1991-08-20', 'LV': '1991-08-21', 'LT': '1991-09-06',
    'CZ': '1993-01-01', 'SK': '1993-01-01', 'SI': '1991-06-25', 'HR': '1991-06-25', 'MK': '1991-09-08',
    'BA': '1992-03-03', 'RS': '2006-06-05', 'ME': '2006-06-03',
    # renamed: the old name is in DEFUNCT
    'MM': '1989-06-18', 'CW': '2010-10-10', 'SX': '2010-10-10', 'TL': '2002-05-20',
    # Wikidata's inception is not the one that matters for a birthplace
    'TW': '1945-10-25',   # Japanese rule until 1945
    'SG': '1965-08-09',   # independence
    'VN': '1976-07-02',   # North and South Vietnam before
    'FI': '1917-12-06',
    # borders inside a covered period: the succession rows decide, NULL elsewhere
    'DE': None, 'AT': None, 'HU': None, 'PL': None, 'RO': None, 'IT': None,
}

# Name of the country as written in TMDb places, for the countries the writer composes
# around a defunct one ("[now Czech Republic]"). Others: NULL, the writer keeps its rule.
PLACE_NAMES = {
    'RU': 'Russia', 'UA': 'Ukraine', 'BY': 'Belarus', 'MD': 'Moldova', 'GE': 'Georgia', 'AM': 'Armenia',
    'AZ': 'Azerbaijan', 'KZ': 'Kazakhstan', 'UZ': 'Uzbekistan', 'TM': 'Turkmenistan', 'KG': 'Kyrgyzstan',
    'TJ': 'Tajikistan', 'EE': 'Estonia', 'LV': 'Latvia', 'LT': 'Lithuania', 'FI': 'Finland',
    'CZ': 'Czech Republic', 'SK': 'Slovakia', 'SI': 'Slovenia', 'HR': 'Croatia', 'BA': 'Bosnia and Herzegovina',
    'RS': 'Serbia', 'ME': 'Montenegro', 'MK': 'North Macedonia', 'DE': 'Germany', 'AT': 'Austria',
    'HU': 'Hungary', 'PL': 'Poland', 'RO': 'Romania', 'IT': 'Italy', 'MM': 'Myanmar',
    'CD': 'Democratic Republic of the Congo', 'CW': 'Curaçao', 'SX': 'Sint Maarten', 'TL': 'Timor-Leste',
    'US': 'USA', 'GB': 'UK', 'CN': 'China', 'NL': 'Netherlands',
}

# --- Succession -------------------------------------------------------------------------
# (country today, country at birth or None = unknown, start, end, region QIDs or None, note)
# Rows of one country are tried in this order; region rows first.
EAST_LANDER = 'Q1202,Q1205,Q1208,Q1206,Q1196'   # Saxony, Thuringia, Brandenburg, Saxony-Anhalt, M-V
WWII_YU = 'Axis occupation and partition of Yugoslavia, 1941-04-06 to 1945-05-08: not written'
ISTRIA = 'Istria, Rijeka, Zadar and the Slovene Littoral were Italian 1920-1947: not separated yet'
SUCCESSION = [
    # Czech Republic, Slovakia
    ('CZ', 'QM', '1867-03-30', '1918-10-27', None, None),
    ('CZ', 'XC', '1918-10-28', '1938-09-30', None, 'Sudetenland annexed 1938-10-01: not written until 1939-03-16'),
    ('CZ', 'QS', '1939-03-16', '1945-05-08', None, None),
    ('CZ', 'XC', '1945-05-09', '1992-12-31', None, None),
    ('SK', 'QM', '1867-03-30', '1918-10-27', None, None),
    ('SK', 'XC', '1918-10-28', '1938-11-01', None, 'Slovak State and Hungarian annexations 1938-11-02 to 1945-05-08: not written'),
    ('SK', 'XC', '1945-05-09', '1992-12-31', None, None),
    # Yugoslavia and its successors
    ('SI', 'QM', '1867-03-30', '1918-10-28', None, None),
    ('SI', 'YU', '1918-12-01', '1941-04-05', None, ISTRIA),
    ('SI', 'YU', '1945-05-09', '1991-06-24', None, WWII_YU),
    ('HR', 'QM', '1867-03-30', '1918-10-28', None, None),
    ('HR', 'YU', '1918-12-01', '1941-04-05', None, ISTRIA),
    ('HR', 'YU', '1945-05-09', '1991-06-24', None, WWII_YU),
    ('BA', 'QM', '1908-10-06', '1918-10-28', None, 'Ottoman, then occupied by Austria-Hungary, before 1908: not written'),
    ('BA', 'YU', '1918-12-01', '1941-04-05', None, None),
    ('BA', 'YU', '1945-05-09', '1992-03-02', None, WWII_YU),
    ('MK', 'YU', '1918-12-01', '1941-04-05', None, None),
    ('MK', 'YU', '1945-05-09', '1991-09-07', None, WWII_YU),
    ('RS', 'YU', '1918-12-01', '1941-04-05', None, 'Kingdom of Serbia, and Vojvodina in Austria-Hungary, before 1918: not written'),
    ('RS', 'YU', '1945-05-09', '2003-02-03', None, WWII_YU),
    ('RS', 'CS', '2003-02-04', '2006-06-04', None, None),
    ('ME', 'YU', '1918-12-01', '1941-04-05', None, 'Kingdom of Montenegro before 1918: not written'),
    ('ME', 'YU', '1945-05-09', '2003-02-03', None, WWII_YU),
    ('ME', 'CS', '2003-02-04', '2006-06-02', None, None),
    # Russian Empire, USSR and their successors
    ('RU', None, '1000-01-01', '1945-04-09', 'Q1749', 'Kaliningrad Oblast: German (Konigsberg) until 1945'),
    ('RU', 'QO', '1721-11-02', '1917-09-14', None, None),
    ('RU', 'SU', '1922-12-30', '1991-12-25', None, '1917-09-15 to 1922-12-29 (Russian Republic, RSFSR): not written'),
    ('UA', 'SU', '1945-06-29', '1991-08-23', None,
     'Western Ukraine was Austro-Hungarian, Polish, Czechoslovak or Romanian before 1939-1945: earlier births not written'),
    ('BY', 'SU', '1945-08-16', '1991-08-24', None, 'Western Belarus was Polish 1921-1939: earlier births not written'),
    ('MD', 'QO', '1812-05-28', '1917-09-14', None, None),
    ('MD', 'SU', '1944-08-24', '1991-08-26', None, 'Bessarabia was Romanian 1918-1940 and 1941-1944: not written'),
    ('GE', 'QO', '1878-03-03', '1917-09-14', None, None),
    ('GE', 'SU', '1922-12-30', '1991-04-08', None, None),
    ('AM', 'QO', '1828-02-22', '1917-09-14', None, None),
    ('AM', 'SU', '1922-12-30', '1991-09-20', None, None),
    ('AZ', 'QO', '1828-02-22', '1917-09-14', None, None),
    ('AZ', 'SU', '1922-12-30', '1991-10-17', None, None),
    ('KZ', 'SU', '1922-12-30', '1991-12-15', None, None),
    ('UZ', 'SU', '1924-10-27', '1991-08-30', None, 'Bukhara and Khiva were protectorates before 1920: not written'),
    ('TM', 'SU', '1924-10-27', '1991-10-26', None, None),
    ('TJ', 'SU', '1924-10-14', '1991-09-08', None, None),
    ('KG', 'SU', '1924-10-14', '1991-08-30', None, None),
    ('EE', 'QO', '1721-09-10', '1917-09-14', None, None),
    ('EE', 'EE', '1918-02-24', '1940-08-05', None, 'Independent Estonia between the wars'),
    ('EE', 'SU', '1940-08-06', '1991-08-19', None, None),
    ('LV', 'QO', '1795-04-15', '1917-09-14', None, None),
    ('LV', 'LV', '1918-11-18', '1940-08-04', None, 'Independent Latvia between the wars'),
    ('LV', 'SU', '1940-08-05', '1991-08-20', None, None),
    ('LT', 'SU', '1945-01-28', '1991-09-05', None,
     'Klaipeda was German and Vilnius Polish between the wars: earlier births not written'),
    ('FI', 'QO', '1809-09-17', '1917-12-05', None, 'Grand Duchy of Finland'),
    # Germany
    ('DE', None, '1945-05-08', '1990-10-02', 'Q64', 'Berlin: East or West not known from the Land'),
    ('DE', None, '1947-01-01', '1956-12-31', 'Q1201', 'Saar Protectorate'),
    ('DE', None, '1945-05-08', '1949-10-06', EAST_LANDER, 'Soviet occupation zone'),
    ('DE', 'XG', '1949-10-07', '1990-10-02', EAST_LANDER, None),
    ('DE', 'QP', '1949-05-23', '1990-10-02', None, None),
    # Austria, Hungary, Poland, Romania, Italy: borders of 1918-1920 and 1938-1945
    ('AT', 'QM', '1867-03-30', '1918-10-30', None, None),
    ('AT', None, '1938-03-13', '1945-04-26', None, 'Anschluss'),
    ('AT', None, '1000-01-01', '1867-03-29', None, 'Austrian Empire'),
    ('HU', 'QM', '1867-03-30', '1918-10-31', None, None),
    ('HU', None, '1000-01-01', '1867-03-29', None, 'Austrian Empire'),
    ('PL', None, '1000-01-01', '1945-08-01', None, 'Partitions, then the borders of 1918-1945: not written'),
    ('RO', None, '1000-01-01', '1920-06-04', None, 'Transylvania and Bukovina were Austro-Hungarian'),
    ('RO', None, '1940-08-30', '1944-10-25', None, 'Northern Transylvania was Hungarian'),
    ('IT', None, '1000-01-01', '1920-11-12', 'Q1237,Q1250', 'Trentino-South Tyrol, Friuli-Venezia Giulia: Austro-Hungarian'),
    # Renamed
    ('MM', 'BU', '1948-01-04', '1989-06-17', None, None),
    ('CD', 'ZR', '1971-10-27', '1997-05-16', None, None),
    ('CW', 'AN', '1954-12-15', '2010-10-09', None, None),
    ('SX', 'AN', '1954-12-15', '2010-10-09', None, None),
]

SUCCESSION_DDL = """
CREATE TABLE IF NOT EXISTS T_WC_COUNTRY_SUCCESSION (
  ID_ROW int(11) NOT NULL AUTO_INCREMENT,
  COUNTRY_CODE char(2) NOT NULL,
  COUNTRY_CODE_AT_BIRTH char(2) DEFAULT NULL,
  DAT_START date NOT NULL,
  DAT_END date NOT NULL,
  REGION_QIDS varchar(200) DEFAULT NULL,
  DISPLAY_ORDER int(5) NOT NULL,
  NOTE varchar(300) DEFAULT NULL,
  DELETED int(5) DEFAULT 0,
  DAT_CREAT date DEFAULT NULL,
  TIM_UPDATED datetime DEFAULT NULL,
  PRIMARY KEY (ID_ROW),
  KEY COUNTRY_CODE (COUNTRY_CODE, DISPLAY_ORDER),
  KEY COUNTRY_CODE_AT_BIRTH (COUNTRY_CODE_AT_BIRTH)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Country a place of today''s country was in, by date of birth (tmdb-person-preprocess migrations/add_defunct_countries.py)'
"""

COUNTRY_COLUMNS = [
    ("ID_WIKIDATA", "varchar(20) DEFAULT NULL"),
    ("IS_DEFUNCT", "tinyint(1) NOT NULL DEFAULT 0"),
    ("DAT_START", "date DEFAULT NULL"),
    ("DAT_END", "date DEFAULT NULL"),
    ("PLACE_NAME_EN", "varchar(100) DEFAULT NULL"),
]


def f_check_data():
    """Refuse inconsistent reference data before touching the database."""
    arrdefunct = {row[0] for row in DEFUNCT}
    for strcode, stratbirth, strstart, strend, _, _ in SUCCESSION:
        if strstart > strend:
            raise ValueError(f"succession row {strcode} {stratbirth}: start {strstart} after end {strend}")
        if stratbirth is not None and stratbirth not in arrdefunct and stratbirth != strcode:
            raise ValueError(f"succession row {strcode}: unknown country at birth {stratbirth}")
    for strcode in DAT_START_OVERRIDES:
        if strcode in arrdefunct:
            raise ValueError(f"{strcode} is defunct, it takes no DAT_START override")


def f_execute(conn, intapply, strsql, arrparams=None):
    print("   ", " ".join(strsql.split())[:160], ("| " + str(arrparams)[:80]) if arrparams else "", flush=True)
    if intapply:
        cursor = conn.cursor()
        cursor.execute(strsql, arrparams)
        return cursor.rowcount
    return 0


def f_live_dat_start(strcode):
    if strcode in DAT_START_OVERRIDES:
        return DAT_START_OVERRIDES[strcode]
    return INCEPTIONS.get(strcode)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true", help="write (default: dry run)")
    args = parser.parse_args()
    intapply = 1 if args.apply else 0
    print(f"Migration add_defunct_countries -- mode: {'APPLY' if intapply else 'DRY RUN'}", flush=True)
    dblstart = time.time()
    f_check_data()

    conn = cp.f_getconnection()
    cursor = conn.cursor()

    print("\n1. T_WC_COUNTRY columns", flush=True)
    for strcol, strtype in COUNTRY_COLUMNS:
        f_execute(conn, intapply, f"ALTER TABLE T_WC_COUNTRY ADD COLUMN IF NOT EXISTS {strcol} {strtype}, ALGORITHM=INSTANT")
    cursor.execute("SELECT COUNT(*) AS n FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() "
                   "AND TABLE_NAME = 'T_WC_COUNTRY' AND INDEX_NAME = 'ID_WIKIDATA'")
    if not cursor.fetchone()["n"]:
        f_execute(conn, intapply, "ALTER TABLE T_WC_COUNTRY ADD KEY ID_WIKIDATA (ID_WIKIDATA)")

    print("\n2. T_WC_TMDB_PERSON.COUNTRY_OF_BIRTH_HISTORIC (instant, TIM_UPDATED untouched)", flush=True)
    f_execute(conn, intapply, "ALTER TABLE T_WC_TMDB_PERSON ADD COLUMN IF NOT EXISTS COUNTRY_OF_BIRTH_HISTORIC "
                              "varchar(10) DEFAULT NULL, ALGORITHM=INSTANT")

    print("\n3. Defunct countries (DELETED = 1)", flush=True)
    cursor.execute("SELECT ID_COUNTRY, COUNTRY_CODE, DELETED FROM T_WC_COUNTRY WHERE COUNTRY_CODE IN ("
                   + ", ".join(["%s"] * len(DEFUNCT)) + ")", [row[0] for row in DEFUNCT])
    arrexisting = {row["COUNTRY_CODE"].upper(): row for row in cursor.fetchall()}
    for strcode, stren, strfr, strplace, strqid, strstart, strend, straliases in DEFUNCT:
        arrvalues = [stren, strfr, strplace, strqid, strstart, strend, straliases]
        if strcode in arrexisting:
            if not arrexisting[strcode]["DELETED"]:
                raise ValueError(f"{strcode} is a live row of T_WC_COUNTRY: refusing to mark it defunct")
            f_execute(conn, intapply,
                      "UPDATE T_WC_COUNTRY SET COUNTRY_NAME_EN = %s, COUNTRY_NAME_FR = %s, PLACE_NAME_EN = %s, "
                      "ID_WIKIDATA = %s, DAT_START = %s, DAT_END = %s, COUNTRY_ALIASES = %s, IS_DEFUNCT = 1, "
                      "DELETED = 1, TIM_UPDATED = NOW() WHERE ID_COUNTRY = %s",
                      arrvalues + [arrexisting[strcode]["ID_COUNTRY"]])
        else:
            f_execute(conn, intapply,
                      "INSERT INTO T_WC_COUNTRY (COUNTRY_CODE, COUNTRY_NAME_EN, COUNTRY_NAME_FR, PLACE_NAME_EN, "
                      "ID_WIKIDATA, DAT_START, DAT_END, COUNTRY_ALIASES, IS_DEFUNCT, DELETED, DAT_CREAT, TIM_UPDATED) "
                      "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1, 1, CURDATE(), NOW())",
                      [strcode] + arrvalues)

    print("\n4. Live countries: ID_WIKIDATA, DAT_START, PLACE_NAME_EN", flush=True)
    cursor.execute("SELECT ID_COUNTRY, COUNTRY_CODE FROM T_WC_COUNTRY WHERE COALESCE(DELETED, 0) = 0 AND COUNTRY_CODE IS NOT NULL")
    arrlive = cursor.fetchall()
    lngchanged = 0
    for row in arrlive:
        strcode = row["COUNTRY_CODE"].upper()
        arrvalues = [QIDS.get(strcode), f_live_dat_start(strcode), PLACE_NAMES.get(strcode)]
        if not intapply:
            lngchanged += 1
            continue
        lngchanged += cursor.execute(
            "UPDATE T_WC_COUNTRY SET ID_WIKIDATA = %s, DAT_START = %s, PLACE_NAME_EN = %s, IS_DEFUNCT = 0 "
            "WHERE ID_COUNTRY = %s AND NOT (ID_WIKIDATA <=> %s AND DAT_START <=> %s AND PLACE_NAME_EN <=> %s AND IS_DEFUNCT = 0)",
            arrvalues + [row["ID_COUNTRY"]] + arrvalues)
    print(f"    {len(arrlive)} live rows, {lngchanged} {'updated' if intapply else 'to examine'}; "
          f"{sum(1 for r in arrlive if r['COUNTRY_CODE'].upper() not in QIDS)} without a Wikidata QID", flush=True)
    arrmissing = sorted({r[0] for r in SUCCESSION} - {r["COUNTRY_CODE"].upper() for r in arrlive})
    if arrmissing:
        print(f"    WARNING: succession rows for countries absent from T_WC_COUNTRY: {arrmissing}", flush=True)

    print("\n5. T_WC_COUNTRY_SUCCESSION", flush=True)
    f_execute(conn, intapply, SUCCESSION_DDL)
    f_execute(conn, intapply, "DELETE FROM T_WC_COUNTRY_SUCCESSION")
    arrrows = [(strcode, stratbirth, strstart, strend, strregions, lngorder, strnote)
               for lngorder, (strcode, stratbirth, strstart, strend, strregions, strnote) in enumerate(SUCCESSION, 1)]
    print(f"    {len(arrrows)} rows to insert", flush=True)
    if intapply:
        cursor.executemany(
            "INSERT INTO T_WC_COUNTRY_SUCCESSION (COUNTRY_CODE, COUNTRY_CODE_AT_BIRTH, DAT_START, DAT_END, "
            "REGION_QIDS, DISPLAY_ORDER, NOTE, DELETED, DAT_CREAT, TIM_UPDATED) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, 0, CURDATE(), NOW())", arrrows)
        conn.commit()
    else:
        print("    DRY RUN -> nothing written. Re-run with --apply.", flush=True)

    print(f"\nDone in {time.time() - dblstart:.1f}s", flush=True)


if __name__ == "__main__":
    sys.exit(main())
