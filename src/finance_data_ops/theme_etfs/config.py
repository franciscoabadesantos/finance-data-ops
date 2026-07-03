"""Curated thematic ETF catalog for relationship-map theme ingestion."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ThemeETF:
    theme: str
    etf_ticker: str
    wave: int
    source_type: str
    source_ref: str
    issuer: str
    notes: str = ""


def _global_x(theme: str, ticker: str, wave: int) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="global_x_csv",
        source_ref=ticker.lower(),
        issuer="Global X",
    )


def _ark(theme: str, ticker: str, wave: int, slug: str) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="ark_csv",
        source_ref=f"https://assets.ark-funds.com/fund-documents/funds-etf-csv/{slug}",
        issuer="ARK",
    )


def _vaneck(theme: str, ticker: str, wave: int, slug: str) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="vaneck_xlsx",
        source_ref=f"https://www.vaneck.com/us/en/investments/{slug}/downloads/holdings/",
        issuer="VanEck",
    )


def _ssga(theme: str, ticker: str, wave: int) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="state_street_xlsx",
        source_ref=(
            "https://www.ssga.com/us/en/intermediary/etfs/library-content/products/"
            f"fund-data/etfs/us/holdings-daily-us-en-{ticker.lower()}.xlsx"
        ),
        issuer="State Street",
    )


def _yfinance(theme: str, ticker: str, wave: int, issuer: str, notes: str) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="yfinance_funds_data",
        source_ref=ticker,
        issuer=issuer,
        notes=notes,
    )


# One narrow ETF per theme. Holdings ingestion runs for both waves; price-universe
# expansion is gated to wave 1 by the universe expansion module.
THEME_ETFS: tuple[ThemeETF, ...] = (
    _vaneck("ai_semis", "SMH", 1, "semiconductor-etf-smh"),
    _yfinance("software", "IGV", 1, "iShares", "iShares CSV blocked in some non-browser runtimes; yfinance fallback."),
    _global_x("ai", "AIQ", 1),
    _global_x("fintech", "FINX", 1),
    _global_x("cyber", "BUG", 1),
    _yfinance("internet_ecommerce", "FDN", 1, "First Trust", "First Trust download is not stable; yfinance fallback."),
    _yfinance("defense", "ITA", 1, "iShares", "iShares CSV blocked in some non-browser runtimes; yfinance fallback."),
    _ark("space", "ARKX", 1, "ARK_SPACE_EXPLORATION_&_INNOVATION_ETF_ARKX_HOLDINGS.csv"),
    _ssga("biotech", "XBI", 1),
    _yfinance("glp1_obesity", "THNR", 1, "Amplify", "Issuer holdings download is not stable; yfinance fallback."),
    _ark("genomics", "ARKG", 1, "ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv"),
    _ssga("oil_gas", "XOP", 1),
    _vaneck("oil_services", "OIH", 1, "oil-services-etf-oih"),
    _yfinance("clean_energy", "ICLN", 1, "iShares", "iShares CSV blocked in some non-browser runtimes; yfinance fallback."),
    _yfinance("solar", "TAN", 1, "Invesco", "Invesco download rejected automated requests; yfinance fallback."),
    _global_x("robotics", "BOTZ", 2),
    _global_x("ev_battery", "LIT", 2),
    _global_x("infra_construction", "PAVE", 2),
    _yfinance("homebuilders", "ITB", 2, "iShares", "iShares CSV blocked in some non-browser runtimes; yfinance fallback."),
    _global_x("nuclear_uranium", "URA", 2),
    _yfinance("water", "PHO", 2, "Invesco", "Invesco download rejected automated requests; yfinance fallback."),
    _vaneck("critical_minerals", "REMX", 2, "rare-earth-strategic-metals-etf-remx"),
    _vaneck("gold_miners", "GDX", 2, "gold-miners-etf-gdx"),
    _vaneck("agriculture", "MOO", 2, "agribusiness-etf-moo"),
    _yfinance("timber_materials", "WOOD", 2, "iShares", "iShares CSV blocked in some non-browser runtimes; yfinance fallback."),
    _yfinance("medical_devices", "IHI", 2, "iShares", "iShares CSV blocked in some non-browser runtimes; yfinance fallback."),
    _yfinance("cannabis", "MSOS", 2, "AdvisorShares", "Issuer holdings download is not stable; yfinance fallback."),
    _vaneck("gaming_esports", "ESPO", 2, "video-gaming-esports-etf-espo"),
    _yfinance("sports_betting", "BETZ", 2, "Roundhill", "Issuer holdings download is not stable; yfinance fallback."),
    _ssga("regional_banks", "KRE", 2),
    _yfinance("airlines_travel", "JETS", 2, "U.S. Global", "Issuer holdings download is not stable; yfinance fallback."),
)
