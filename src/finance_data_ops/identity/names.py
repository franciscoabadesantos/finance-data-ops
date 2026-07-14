"""Conservative legal/listing name normalization for identity measurement."""

from __future__ import annotations

import re
from typing import Any

_CORPORATE_SUFFIXES = {
    "CO",
    "COMPANY",
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

_QUERY_ALIASES = {
    "CO": ["COMPANY", "CO"],
    "COMPANY": ["COMPANY", "CO"],
    "CORP": ["CORPORATION", "CORP"],
    "CORPORATION": ["CORPORATION", "CORP"],
    "LTD": ["LIMITED", "LTD"],
    "LIMITED": ["LIMITED", "LTD"],
    "INC": ["INC", "INCORPORATED"],
    "INCORPORATED": ["INCORPORATED", "INC"],
    "NV": ["NV", "N.V."],
    "N V": ["NV"],
    "A S": ["AS"],
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
    text = _listing_name_base_text(value)
    if not text:
        return ""
    tokens = [token for token in text.split() if token]
    tokens = _strip_edge_article(tokens)
    while tokens and tokens[-1] in {"A", "B", "C"}:
        tokens.pop()
    tokens = _strip_edge_article(tokens)
    while tokens and _suffix_token(tokens):
        tokens = tokens[: -_suffix_token(tokens)]
        tokens = _strip_edge_article(tokens)
    return " ".join(tokens)


def legal_name_query_from_listing(value: Any) -> str:
    variants = legal_name_query_variants_from_listing(value)
    return variants[0] if variants else ""


def legal_name_query_variants_from_listing(*values: Any) -> list[str]:
    variants: list[str] = []
    for value in values:
        text = _listing_name_base_text(value)
        if not text:
            continue
        token_variants = _query_token_variants([token for token in text.split() if token])
        for tokens in token_variants:
            for candidate_tokens in _query_suffix_variants(tokens):
                for article_variant in _article_variants(candidate_tokens):
                    query = _title(" ".join(article_variant))
                    if query and query not in variants:
                        variants.append(query)
    return variants


def _listing_name_base_text(value: Any) -> str:
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
    return re.sub(r"\s+", " ", " ".join(tokens)).strip()


def _query_token_variants(tokens: list[str]) -> list[list[str]]:
    if not tokens:
        return []
    variants = [tokens]
    for index, token in enumerate(tokens):
        aliases = _QUERY_ALIASES.get(token, [])
        if not aliases:
            continue
        next_variants = []
        for variant in variants:
            for alias in aliases:
                replaced = list(variant)
                replaced[index] = alias
                next_variants.append(replaced)
        variants = next_variants
    out: list[list[str]] = []
    seen: set[str] = set()
    for variant in variants:
        key = " ".join(variant)
        if key not in seen:
            seen.add(key)
            out.append(variant)
    return out


def _query_suffix_variants(tokens: list[str]) -> list[list[str]]:
    variants = [tokens]
    stripped = _strip_trailing_corporate_suffix(tokens)
    if stripped != tokens:
        variants.append(stripped)
    out: list[list[str]] = []
    seen: set[str] = set()
    for variant in variants:
        key = " ".join(variant)
        if key and key not in seen:
            seen.add(key)
            out.append(variant)
    return out


def _strip_trailing_corporate_suffix(tokens: list[str]) -> list[str]:
    out = _strip_edge_article(tokens)
    while out and _suffix_token(out):
        out = out[: -_suffix_token(out)]
        out = _strip_edge_article(out)
    return out


def _article_variants(tokens: list[str]) -> list[list[str]]:
    if not tokens:
        return []
    variants = [tokens]
    stripped = _strip_edge_article(tokens)
    if stripped != tokens:
        variants.append(stripped)
    if tokens and tokens[-1] == "THE":
        moved = ["THE"] + tokens[:-1]
        variants.append(moved)
        stripped_moved = _strip_edge_article(moved)
        if stripped_moved != moved:
            variants.append(stripped_moved)
    out: list[list[str]] = []
    seen: set[str] = set()
    for variant in variants:
        key = " ".join(variant)
        if key and key not in seen:
            seen.add(key)
            out.append(variant)
    return out


def _strip_edge_article(tokens: list[str]) -> list[str]:
    out = list(tokens)
    while out and out[0] == "THE":
        out = out[1:]
    while out and out[-1] == "THE":
        out = out[:-1]
    return out


def _title(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().title()


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
