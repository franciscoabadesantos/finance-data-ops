"""Conservative legal/listing name normalization for identity measurement."""

from __future__ import annotations

import re
from typing import Any

_CORPORATE_SUFFIXES = {
    "INC",
    "INCORPORATED",
    "CORP",
    "CORPORATION",
    "PLC",
    "SE",
    "AG",
    "NV",
    "N V",
    "AS",
    "A S",
    "LTD",
    "LIMITED",
}

_LISTING_PHRASES = [
    "SPONSORED ADR",
    "SPONS ADR",
    "SPON ADR",
    "DEPOSITARY RECEIPT",
    "NY REG SHS",
    "REG SHS",
    "COMMON STOCK",
    "CLASS A",
    "CLASS B",
    "CLASS C",
    "CL A",
    "CL B",
    "CL C",
    "ADR",
    "ADS",
    "SHS",
]


def normalize_legal_name_conservative(value: Any) -> str:
    text = _clean_name_text(value)
    if not text:
        return ""
    for phrase in _LISTING_PHRASES:
        text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text)
    text = re.sub(r"\bNY\b", " ", text)
    text = re.sub(r"\bREG\b", " ", text)
    text = re.sub(r"\bCL\b\s+\b[A-Z]\b", " ", text)
    text = re.sub(r"\bCLASS\b\s+\b[A-Z]\b", " ", text)
    tokens = [token for token in text.split() if token]
    while tokens and tokens[-1] in {"A", "B", "C"}:
        tokens.pop()
    while tokens and _suffix_token(tokens):
        tokens = tokens[: -_suffix_token(tokens)]
    return " ".join(tokens)


def legal_name_query_from_listing(value: Any) -> str:
    text = _clean_name_text(value)
    if not text:
        return ""
    for phrase in _LISTING_PHRASES:
        text = re.sub(rf"\b{re.escape(phrase)}\b", " ", text)
    text = re.sub(r"\bNY\b", " ", text)
    text = re.sub(r"\bREG\b", " ", text)
    text = re.sub(r"\bCL\b\s+\b[A-Z]\b", " ", text)
    text = re.sub(r"\bCLASS\b\s+\b[A-Z]\b", " ", text)
    tokens = [token for token in text.split() if token]
    while tokens and tokens[-1] in {"A", "B", "C"}:
        tokens.pop()
    aliases = {"CORP": "CORPORATION", "LTD": "LIMITED", "N V": "NV", "A S": "AS"}
    text = " ".join(tokens)
    for old, new in aliases.items():
        text = re.sub(rf"\b{old}\b", new, text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.title() if text else ""


def _clean_name_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().upper()
    if not text or text in {"NAN", "NONE", "NULL", "N/A"}:
        return ""
    text = text.replace("&", " AND ")
    text = text.replace("N.V.", " NV ")
    text = text.replace("A/S", " AS ")
    text = re.sub(r"[-_/.,()]+", " ", text)
    text = re.sub(r"[^A-Z0-9 ]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _suffix_token(tokens: list[str]) -> int:
    for size in (2, 1):
        if len(tokens) >= size and " ".join(tokens[-size:]) in _CORPORATE_SUFFIXES:
            return size
    return 0
