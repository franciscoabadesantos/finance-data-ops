#!/usr/bin/env python3
"""Fill the sector and industry that earlier onboarding never asked for.

Both onboarding paths now read these fields from the provider, but the entities
already in the registry were written before that. This backfills them once.

It is deliberately narrow: it only ever writes a value where the column is
currently NULL. It never overwrites an existing classification, so re-running it
is safe and it cannot silently rewrite history if the provider changes its mind
about a company.

    python scripts/backfill_entity_classification.py --dry-run
    python scripts/backfill_entity_classification.py --only-atlas
    python scripts/backfill_entity_classification.py

Every symbol the provider has nothing for is reported by name at the end, so a
run that fills less than expected says which ones and why rather than looking
like a success.
"""
from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from finance_data_ops.providers.entity_metadata import (  # noqa: E402
    default_metadata_lookup,
    descriptive_fields,
    safe_metadata_lookup,
)

LOGGER = logging.getLogger("backfill_entity_classification")

SELECT_TARGETS = """
    SELECT e.entity_id
    FROM feature_store.entity_attributes_static AS e
    WHERE (e.sector IS NULL OR e.industry IS NULL)
    ORDER BY e.entity_id
"""

SELECT_TARGETS_IN_ATLAS = """
    SELECT DISTINCT e.entity_id
    FROM feature_store.entity_attributes_static AS e
    JOIN feature_store.relationship_atlas_nodes AS n
      ON UPPER(n.symbol) = UPPER(e.entity_id)
    WHERE (e.sector IS NULL OR e.industry IS NULL)
      AND n.as_of_date = (SELECT MAX(as_of_date) FROM feature_store.relationship_atlas_nodes)
    ORDER BY e.entity_id
"""

# COALESCE keeps whatever is already recorded: this fills blanks, it does not
# relabel companies someone or something else already classified.
UPDATE_ENTITY = """
    UPDATE feature_store.entity_attributes_static
    SET sector = COALESCE(sector, :sector),
        industry = COALESCE(industry, :industry),
        updated_at = NOW()
    WHERE entity_id = :entity_id
      AND (sector IS NULL OR industry IS NULL)
"""


def unresolvable_form(symbol: str) -> str | None:
    """Why the provider cannot match this symbol as written, if it cannot.

    A syntactic check, not a guess about the company. The registry took some
    symbols straight from ETF holdings files, which use conventions no quote
    provider accepts: Bloomberg codes carry the country after a space
    (`AMMN IJ`), mainland listings arrive as bare exchange numbers with no
    suffix (`601012`), and one row is a literal dash.

    Reporting these as "re-run to retry" is false -- rerunning cannot fix a
    symbol that is not a symbol. They need normalisation at onboarding, which
    is a different job from asking the provider again.
    """

    text = symbol.strip()
    if not text or text == "-":
        return "empty or placeholder"
    if " " in text:
        return "Bloomberg-style code (exchange after a space)"
    if text.startswith("."):
        return "index-style prefix"
    if re.fullmatch(r"\d+", text):
        return "bare exchange number, no market suffix"
    return None


def _engine():
    from sqlalchemy import create_engine

    dsn = (
        os.environ.get("DATA_OPS_DATABASE_URL")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("POSTGRES_DSN")
        or os.environ.get("PG_DSN")
        or ""
    ).strip()
    if not dsn:
        raise SystemExit("Set DATA_OPS_DATABASE_URL (or DATABASE_URL) to the feature-store database.")
    return create_engine(dsn, pool_pre_ping=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Look everything up, write nothing.")
    parser.add_argument(
        "--only-atlas",
        action="store_true",
        help="Restrict to symbols in the latest relationship-map snapshot.",
    )
    parser.add_argument("--limit", type=int, default=0, help="Stop after this many symbols (0 = all).")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    from sqlalchemy import text

    engine = _engine()
    query = SELECT_TARGETS_IN_ATLAS if args.only_atlas else SELECT_TARGETS
    with engine.connect() as connection:
        symbols = [str(row[0]) for row in connection.execute(text(query))]
    if args.limit > 0:
        symbols = symbols[: args.limit]

    LOGGER.info("%d entities missing a sector or an industry.", len(symbols))

    outcomes: Counter[str] = Counter()
    unresolved: list[str] = []
    updated = 0

    unreachable: list[str] = []
    malformed: list[tuple[str, str]] = []

    for index, entity_id in enumerate(symbols, start=1):
        if (reason := unresolvable_form(entity_id)) is not None:
            # Not worth a network call, and more importantly not worth
            # reporting as a retryable failure.
            outcomes["malformed"] += 1
            malformed.append((entity_id, reason))
            continue
        metadata = safe_metadata_lookup(default_metadata_lookup, entity_id)
        fields = descriptive_fields(metadata)
        sector, industry = fields["sector"], fields["industry"]
        if not sector and not industry:
            # An empty payload means the call failed or the symbol is unknown to
            # the provider. A populated payload with no sector means the provider
            # genuinely has no classification -- which is the right answer for an
            # ETF. Collapsing the two is how a rate-limited run reports itself as
            # a success: the first full run here hit YFRateLimitError and filed
            # every throttled symbol under "provider had nothing".
            if metadata:
                outcomes["provider_had_nothing"] += 1
                unresolved.append(entity_id)
            else:
                outcomes["unreachable"] += 1
                unreachable.append(entity_id)
            continue
        outcomes["resolved"] += 1
        if args.dry_run:
            continue
        with engine.begin() as connection:
            result = connection.execute(
                text(UPDATE_ENTITY),
                {"entity_id": entity_id, "sector": sector, "industry": industry},
            )
            updated += int(result.rowcount or 0)
        if index % 50 == 0:
            LOGGER.info("%d/%d processed.", index, len(symbols))

    LOGGER.info(
        "Done. resolved=%d provider_had_nothing=%d unreachable=%d malformed=%d rows_updated=%d%s",
        outcomes["resolved"],
        outcomes["provider_had_nothing"],
        outcomes["unreachable"],
        outcomes["malformed"],
        updated,
        " (dry run, nothing written)" if args.dry_run else "",
    )
    if unresolved:
        # Named, not counted. A backfill that quietly fills 80% and reports
        # success is the failure this whole audit keeps finding.
        LOGGER.warning(
            "Provider has no classification for %d symbols (expected for ETFs): %s",
            len(unresolved),
            ", ".join(unresolved),
        )
    if malformed:
        # Separated from `unreachable` on purpose. Telling someone to re-run
        # over a symbol that is not a symbol sends them back to the provider
        # forever; these need normalising where they are onboarded.
        grouped: dict[str, list[str]] = {}
        for entity_id, reason in malformed:
            grouped.setdefault(reason, []).append(entity_id)
        LOGGER.error(
            "%d symbols cannot be looked up as written and re-running will not change that. "
            "They need normalising at onboarding, not another provider call:",
            len(malformed),
        )
        for reason, entity_ids in sorted(grouped.items()):
            LOGGER.error("  %s (%d): %s", reason, len(entity_ids), ", ".join(sorted(entity_ids)))
    if unreachable:
        LOGGER.error(
            "Provider returned nothing for %d well-formed symbols -- failed call, rate limit, or "
            "a ticker it does not know. These are NOT settled; re-run to retry them, and treat "
            "any that survive several runs as delisted rather than throttled: %s",
            len(unreachable),
            ", ".join(unreachable),
        )
    return 2 if (unreachable or malformed) else 0


if __name__ == "__main__":
    raise SystemExit(main())
