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
    fallback_source_type: str | None = None
    fallback_source_ref: str | None = None
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


def _ishares(theme: str, ticker: str, wave: int, product_id: str, slug: str) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="ishares_csv",
        source_ref=f"{product_id}/{slug}",
        issuer="iShares",
        fallback_source_type="yfinance_funds_data",
        fallback_source_ref=ticker,
    )


def _first_trust(theme: str, ticker: str, wave: int) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="first_trust_html",
        source_ref=f"https://www.ftportfolios.com/retail/etf/ETFholdings.aspx?Ticker={ticker.upper()}",
        issuer="First Trust",
    )


def _advisorshares_csv(theme: str, ticker: str, wave: int, source_ref: str) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="advisorshares_csv",
        source_ref=source_ref,
        issuer="AdvisorShares",
    )


def _roundhill(theme: str, ticker: str, wave: int) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="roundhill_csv",
        source_ref=f"https://www.roundhillinvestments.com/etf/{ticker.lower()}/",
        issuer="Roundhill",
    )


def _issuer_csv(theme: str, ticker: str, wave: int, issuer: str, source_ref: str) -> ThemeETF:
    return ThemeETF(
        theme=theme,
        etf_ticker=ticker,
        wave=wave,
        source_type="issuer_csv",
        source_ref=source_ref,
        issuer=issuer,
        fallback_source_type="yfinance_funds_data",
        fallback_source_ref=ticker,
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


# Curated narrow ETFs per theme. Holdings ingestion runs for both waves; price-universe
# expansion is gated to wave 1 by the universe expansion module. A theme can have
# corroborating ETFs when the holdings improve relationship-map signal quality.
THEME_ETFS: tuple[ThemeETF, ...] = (
    _vaneck("ai_semis", "SMH", 1, "semiconductor-etf-smh"),
    _ishares("software", "IGV", 1, "239771", "ishares-north-american-techsoftware-etf"),
    _global_x("ai", "AIQ", 1),
    _global_x("fintech", "FINX", 1),
    _global_x("cyber", "BUG", 1),
    _first_trust("internet_ecommerce", "FDN", 1),
    _ishares("defense", "ITA", 1, "239502", "ishares-us-aerospace-defense-etf"),
    _roundhill("space", "MARS", 1),
    _ssga("biotech", "XBI", 1),
    _roundhill("glp1_obesity", "OZEM", 1),
    _ark("genomics", "ARKG", 1, "ARK_GENOMIC_REVOLUTION_ETF_ARKG_HOLDINGS.csv"),
    _ssga("oil_gas", "XOP", 1),
    _vaneck("oil_services", "OIH", 1, "oil-services-etf-oih"),
    _ishares("clean_energy", "ICLN", 1, "239738", "ishares-global-clean-energy-etf"),
    _global_x("robotics", "BOTZ", 2),
    _roundhill("humanoid_robotics", "HUMN", 2),
    _global_x("ev_battery", "LIT", 2),
    _global_x("infra_construction", "PAVE", 2),
    _ishares("homebuilders", "ITB", 2, "239512", "ishares-us-home-construction-etf"),
    _global_x("nuclear_uranium", "URA", 2),
    _global_x("water", "AQWA", 2),
    _vaneck("critical_minerals", "REMX", 2, "rare-earth-strategic-metals-etf-remx"),
    _vaneck("gold_miners", "GDX", 2, "gold-miners-etf-gdx"),
    _vaneck("agriculture", "MOO", 2, "agribusiness-etf-moo"),
    _ishares("timber_materials", "WOOD", 2, "239752", "ishares-global-timber-forestry-etf"),
    _ishares("medical_devices", "IHI", 2, "239516", "ishares-us-medical-devices-etf"),
    _advisorshares_csv(
        "cannabis",
        "YOLO",
        2,
        "https://advisorshares.com/wp-content/uploads/csv/holdings/AdvisorShares_YOLO_Holdings_File.csv",
    ),
    _vaneck("gaming_esports", "ESPO", 2, "video-gaming-esports-etf-espo"),
    _roundhill("sports_betting", "BETZ", 2),
    _ssga("regional_banks", "KRE", 2),
    _issuer_csv("airlines_travel", "JETS", 2, "U.S. Global", "JETS"),
    _ishares("reits", "USRT", 2, "239544", "ishares-real-estate-50-etf"),
)
