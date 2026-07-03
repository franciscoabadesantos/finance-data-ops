from __future__ import annotations

from datetime import date

import pandas as pd

from finance_data_ops.publish.ticker_registry import build_entity_attributes_static_payload
from finance_data_ops.refresh.storage import read_parquet_table
from finance_data_ops.theme_etfs.config import THEME_ETFS, ThemeETF
from finance_data_ops.theme_etfs import holdings as holdings_mod
from finance_data_ops.theme_etfs.holdings import fetch_theme_etf_holdings, write_theme_etf_outputs
from finance_data_ops.theme_etfs.universe import build_wave_universe_additions


def test_theme_etf_catalog_has_expected_waves_and_no_broad_sector_spdrs() -> None:
    themes = {spec.theme: spec for spec in THEME_ETFS}
    tickers = {spec.etf_ticker for spec in THEME_ETFS}

    assert len(themes) == 31
    assert themes["ai_semis"].wave == 1
    assert themes["internet_ecommerce"].etf_ticker == "FDN"
    assert themes["internet_ecommerce"].source_type == "first_trust_html"
    assert themes["water"].etf_ticker == "AQWA"
    assert themes["cannabis"].etf_ticker == "YOLO"
    assert themes["reits"].etf_ticker == "USRT"
    assert themes["robotics"].wave == 2
    assert themes["airlines_travel"].wave == 2
    assert len(tickers) == len(THEME_ETFS)
    assert not {"MSOS", "PHO", "TAN", "XLK", "XLE", "XLV"}.intersection(tickers)
    assert "solar" not in themes
    assert not any(spec.source_type == "yfinance_funds_data" for spec in THEME_ETFS)


def test_fetch_theme_etf_holdings_adds_theme_reference_and_normalizes_csv(monkeypatch) -> None:
    spec = ThemeETF(
        theme="fintech",
        etf_ticker="FINX",
        wave=1,
        source_type="global_x_csv",
        source_ref="finx",
        issuer="Global X",
    )
    monkeypatch.setattr(
        holdings_mod,
        "_global_x_candidate_dates",
        lambda _start: [date(2026, 7, 3), date(2026, 7, 2)],
    )
    fetched_urls: list[str] = []

    def fake_fetch(url: str) -> bytes:
        fetched_urls.append(url)
        if url.endswith("finx_full-holdings_20260703.csv"):
            raise RuntimeError("not published yet")
        assert url.endswith("finx_full-holdings_20260702.csv")
        return (
            "Global X FinTech ETF | Fund Holdings Data as of 07/02/2026\n"
            "% of Net Assets,Ticker,Name,SEDOL,Market Price ($),Shares Held,Market Value ($)\n"
            '8.39,HOOD,ROBINHOOD MARKETS INC - A US,BP0TQN6,112.73,"4,792","540,202.16"\n'
            '0.16,,CASH,,1.0,"287,254.82","287,254.82"\n'
            "2.50,IRE AU,IRESS LTD,6297497,4.39,59805,262911.93\n"
            "2.40,000598 C2,CHENGDU XINGRONG ENVIRONMENT CO LTD,B00G0S5,1.00,100,100.00\n"
            "2.30,371 HK,BEIJING ENTERPRISES WATER GROUP LTD,B01JJF8,1.00,100,100.00\n"
            "2.20,6254 JP,NOMURA MICRO SCIENCE CO LTD,6631506,1.00,100,100.00\n"
            "2.10,AWPT AB,AQUAPORIN A/S,BKPG1S8,1.00,100,100.00\n"
        ).encode()

    holdings, themes, failures = fetch_theme_etf_holdings(theme_etfs=[spec], fetch_bytes=fake_fetch)

    assert failures == []
    assert themes[["theme", "etf_ticker", "wave"]].to_dict(orient="records") == [
        {"theme": "fintech", "etf_ticker": "FINX", "wave": 1}
    ]
    assert holdings[["etf_ticker", "holding_symbol", "holding_name", "weight", "as_of"]].to_dict(
        orient="records"
    ) == [
        {
            "etf_ticker": "FINX",
            "holding_symbol": "HOOD",
            "holding_name": "ROBINHOOD MARKETS INC - A US",
            "weight": 0.0839,
            "as_of": date(2026, 7, 2),
        },
        {
            "etf_ticker": "FINX",
            "holding_symbol": "IRE.AX",
            "holding_name": "IRESS LTD",
            "weight": 0.025,
            "as_of": date(2026, 7, 2),
        },
        {
            "etf_ticker": "FINX",
            "holding_symbol": "000598.SZ",
            "holding_name": "CHENGDU XINGRONG ENVIRONMENT CO LTD",
            "weight": 0.024,
            "as_of": date(2026, 7, 2),
        },
        {
            "etf_ticker": "FINX",
            "holding_symbol": "371.HK",
            "holding_name": "BEIJING ENTERPRISES WATER GROUP LTD",
            "weight": 0.023,
            "as_of": date(2026, 7, 2),
        },
        {
            "etf_ticker": "FINX",
            "holding_symbol": "6254.T",
            "holding_name": "NOMURA MICRO SCIENCE CO LTD",
            "weight": 0.022,
            "as_of": date(2026, 7, 2),
        },
        {
            "etf_ticker": "FINX",
            "holding_symbol": "AWPT.ST",
            "holding_name": "AQUAPORIN A/S",
            "weight": 0.021,
            "as_of": date(2026, 7, 2),
        },
    ]
    assert themes.iloc[0]["holdings_count"] == 6
    assert themes.iloc[0]["holdings_as_of"] == date(2026, 7, 2)
    assert themes.iloc[0]["holdings_source_depth"] == "full"
    assert bool(themes.iloc[0]["holdings_shallow"]) is False
    assert fetched_urls == [
        "https://assets.globalxetfs.com/funds/holdings/finx_full-holdings_20260703.csv",
        "https://assets.globalxetfs.com/funds/holdings/finx_full-holdings_20260702.csv",
        "https://assets.globalxetfs.com/funds/holdings/finx_full-holdings_20260702.csv",
    ]


def test_global_x_candidate_dates_skip_weekends_for_last_trading_day_fallback() -> None:
    assert holdings_mod._global_x_candidate_dates(date(2026, 7, 5), lookback_days=4) == [
        date(2026, 7, 3),
        date(2026, 7, 2),
        date(2026, 7, 1),
    ]


def test_ishares_issuer_holdings_preferred_over_yfinance_top_ten() -> None:
    spec = ThemeETF(
        theme="software",
        etf_ticker="IGV",
        wave=1,
        source_type="ishares_csv",
        source_ref="239771/ishares-north-american-techsoftware-etf",
        issuer="iShares",
        fallback_source_type="yfinance_funds_data",
        fallback_source_ref="IGV",
    )

    def fake_fetch(url: str) -> bytes:
        assert "get-fund-document" in url
        assert "portfolioId=239771" in url
        rows = [
            "iShares Expanded Tech-Software Sector ETF Holdings as of 07/02/2026",
            "Ticker,Name,Weight (%)",
        ]
        rows.extend(f"SOFT{i},Software Company {i},{1.0 + i / 100}" for i in range(12))
        return "\n".join(rows).encode()

    holdings, themes, failures = fetch_theme_etf_holdings(theme_etfs=[spec], fetch_bytes=fake_fetch)

    assert failures == []
    assert len(holdings.index) == 12
    assert themes.iloc[0]["source_type"] == "ishares_csv"
    assert themes.iloc[0]["holdings_count"] == 12
    assert themes.iloc[0]["holdings_source_depth"] == "full"
    assert set(holdings["source"]) == {"theme_etf:ishares_csv"}


def test_first_trust_html_parser_keeps_full_holdings_and_filters_cash() -> None:
    spec = ThemeETF(
        theme="internet_ecommerce",
        etf_ticker="FDN",
        wave=1,
        source_type="first_trust_html",
        source_ref="https://www.ftportfolios.com/retail/etf/ETFholdings.aspx?Ticker=FDN",
        issuer="First Trust",
    )

    def fake_fetch(_url: str) -> bytes:
        return (
            "<html><body>"
            '<span id="ContentPlaceHolder1_HoldingsListing_lblHoldingsTitle">'
            "Holdings of the Fund as of 7/2/2026</span>"
            '<table class="fundSilverGrid">'
            "<tr><td>Security Name</td><td>Identifier</td><td>CUSIP</td>"
            "<td>Classification</td><td>Shares / Quantity</td><td>Market Value</td><td>Weighting</td></tr>"
            "<tr><td>Meta Platforms, Inc. (Class A)</td><td>META</td><td>30303M102</td>"
            "<td>Communication Services</td><td>870764</td><td>$507,568,335.60</td><td>10.07%</td></tr>"
            "<tr><td>Amazon.com, Inc.</td><td>AMZN</td><td>023135106</td>"
            "<td>Consumer Discretionary</td><td>2089029</td><td>$506,944,667.43</td><td>10.06%</td></tr>"
            "<tr><td>US Dollar</td><td>$USD</td><td></td>"
            "<td>Other</td><td>6816907</td><td>$6,816,907.43</td><td>0.14%</td></tr>"
            "</table></body></html>"
        ).encode()

    holdings, themes, failures = fetch_theme_etf_holdings(theme_etfs=[spec], fetch_bytes=fake_fetch)

    assert failures == []
    assert holdings[["holding_symbol", "holding_name", "weight", "as_of"]].to_dict(orient="records") == [
        {
            "holding_symbol": "META",
            "holding_name": "Meta Platforms, Inc. (Class A)",
            "weight": 0.1007,
            "as_of": date(2026, 7, 2),
        },
        {
            "holding_symbol": "AMZN",
            "holding_name": "Amazon.com, Inc.",
            "weight": 0.1006,
            "as_of": date(2026, 7, 2),
        },
    ]
    assert themes.iloc[0]["source_type"] == "first_trust_html"
    assert themes.iloc[0]["holdings_count"] == 2


def test_advisorshares_parser_filters_etf_money_market_and_cash_rows() -> None:
    spec = ThemeETF(
        theme="cannabis",
        etf_ticker="YOLO",
        wave=2,
        source_type="advisorshares_csv",
        source_ref="https://advisorshares.com/wp-content/uploads/csv/holdings/AdvisorShares_YOLO_Holdings_File.csv",
        issuer="AdvisorShares",
    )

    def fake_fetch(_url: str) -> bytes:
        return (
            "Date,Account Symbol,Stock Ticker,Security Number,Security Description,Shares/Par (Full),"
            "Price (Base),Traded Market Value (Base),Portfolio Weight %,Asset Group\n"
            '7/2/2026,YOLO,MSOS,00768Y453,ADVISORSHARES PURE US CANN,"1,548,325",'
            '4.89,"7,571,309.25",22.52%,MF\n'
            ',,TLRY,88688T209,TILRAY BRANDS INC,"96,263",4.62,"444,735.06",1.32%,S\n'
            ',,CURLD,23126M300,CURALEAF HOLDINGS INC,"500,000",10.24,"5,120,000.00",15.23%,FS\n'
            ',,,X9USDBLYT,BLACKROCK TREASURY TRUST INSTL 62,"3,113,086.2",1,"3,113,086.20",9.26%,MM\n'
            ',,,,CASH,"-1,579,420.01",100,"-1,579,420.01",-4.70%,CA\n'
        ).encode()

    holdings, themes, failures = fetch_theme_etf_holdings(theme_etfs=[spec], fetch_bytes=fake_fetch)

    assert failures == []
    assert holdings[["holding_symbol", "holding_name", "weight", "as_of"]].to_dict(orient="records") == [
        {
            "holding_symbol": "TLRY",
            "holding_name": "TILRAY BRANDS INC",
            "weight": 0.0132,
            "as_of": date(2026, 7, 2),
        },
        {
            "holding_symbol": "CURLD",
            "holding_name": "CURALEAF HOLDINGS INC",
            "weight": 0.1523,
            "as_of": date(2026, 7, 2),
        },
    ]
    assert not {"MSOS", "X9USDBLYT", "CASH"}.intersection(set(holdings["holding_symbol"]))
    assert themes.iloc[0]["source_type"] == "advisorshares_csv"
    assert themes.iloc[0]["holdings_count"] == 2


def test_issuer_fetch_falls_back_to_shallow_yfinance_and_flags_theme(monkeypatch) -> None:
    spec = ThemeETF(
        theme="software",
        etf_ticker="IGV",
        wave=1,
        source_type="ishares_csv",
        source_ref="239771/ishares-north-american-techsoftware-etf",
        issuer="iShares",
        fallback_source_type="yfinance_funds_data",
        fallback_source_ref="IGV",
    )

    def fake_fetch(_url: str) -> bytes:
        raise RuntimeError("issuer unavailable")

    monkeypatch.setattr(
        holdings_mod,
        "_fetch_yfinance_funds_data",
        lambda _ticker: pd.DataFrame(
            [
                {"Ticker": "MSFT", "Name": "Microsoft", "Weight (%)": 8.0},
                {"Ticker": "ORCL", "Name": "Oracle", "Weight (%)": 7.0},
            ]
        ),
    )

    holdings, themes, failures = fetch_theme_etf_holdings(theme_etfs=[spec], fetch_bytes=fake_fetch)

    assert failures == []
    assert holdings["holding_symbol"].tolist() == ["MSFT", "ORCL"]
    assert set(holdings["source"]) == {"theme_etf:yfinance_funds_data:shallow"}
    assert themes.iloc[0]["source_type"] == "yfinance_funds_data"
    assert themes.iloc[0]["holdings_source_depth"] == "shallow"
    assert bool(themes.iloc[0]["holdings_shallow"]) is True


def test_full_holdings_filter_non_equity_cash_and_placeholder_rows() -> None:
    spec = ThemeETF(
        theme="solar",
        etf_ticker="TAN",
        wave=1,
        source_type="issuer_csv",
        source_ref="https://example.test/tan.csv",
        issuer="Invesco",
        fallback_source_type="yfinance_funds_data",
        fallback_source_ref="TAN",
    )

    def fake_fetch(_url: str) -> bytes:
        return (
            "Holdings as of 2026-07-02\n"
            "Ticker,Name,Weight (%)\n"
            "FSLR,First Solar Inc,9.5\n"
            "-AUD CASH-,Cash,0.4\n"
            "USD,US Dollar,0.2\n"
            "ESU6,Equity Index Future,0.1\n"
            "ENPH,Enphase Energy Inc,4.5\n"
            '"Holdings subject to change. See issuer website.",,\n'
        ).encode()

    holdings, themes, failures = fetch_theme_etf_holdings(theme_etfs=[spec], fetch_bytes=fake_fetch)

    assert failures == []
    assert holdings["holding_symbol"].tolist() == ["FSLR", "ENPH"]
    assert themes.iloc[0]["holdings_count"] == 2


def test_theme_etf_refresh_replaces_older_cached_snapshot(tmp_path) -> None:
    old_holdings = pd.DataFrame(
        [
            {
                "etf_ticker": "ARKX",
                "holding_symbol": "OLD",
                "holding_name": "Old Space Co",
                "weight": 0.10,
                "as_of": "2026-01-02",
                "source": "theme_etf:ark_csv",
                "fetched_at": "2026-01-03T00:00:00Z",
                "updated_at": "2026-01-03T00:00:00Z",
            },
            {
                "etf_ticker": "SMH",
                "holding_symbol": "NVDA",
                "holding_name": "NVIDIA Corp",
                "weight": 0.20,
                "as_of": "2026-01-02",
                "source": "theme_etf:vaneck_xlsx",
                "fetched_at": "2026-01-03T00:00:00Z",
                "updated_at": "2026-01-03T00:00:00Z",
            },
        ]
    )
    new_holdings = pd.DataFrame(
        [
            {
                "etf_ticker": "ARKX",
                "holding_symbol": "RKLB",
                "holding_name": "Rocket Lab Corp",
                "weight": 0.12,
                "as_of": "2026-07-02",
                "source": "theme_etf:ark_csv",
                "fetched_at": "2026-07-03T00:00:00Z",
                "updated_at": "2026-07-03T00:00:00Z",
            }
        ]
    )

    write_theme_etf_outputs(holdings=old_holdings, themes=pd.DataFrame(), cache_root=str(tmp_path))
    write_theme_etf_outputs(holdings=new_holdings, themes=pd.DataFrame(), cache_root=str(tmp_path))

    cached = read_parquet_table("etf_holdings", cache_root=tmp_path, required=True)
    by_etf = {
        etf: frame[["holding_symbol", "as_of"]].to_dict(orient="records")
        for etf, frame in cached.sort_values("holding_symbol").groupby("etf_ticker")
    }
    assert by_etf["ARKX"] == [{"holding_symbol": "RKLB", "as_of": "2026-07-02"}]
    assert by_etf["SMH"] == [{"holding_symbol": "NVDA", "as_of": "2026-01-02"}]


def test_wave_universe_merge_dedupes_existing_filters_cash_and_builds_entity_attributes() -> None:
    holdings = pd.DataFrame(
        [
            {"etf_ticker": "FINX", "holding_symbol": "HOOD", "holding_name": "Robinhood", "weight": 0.08},
            {"etf_ticker": "FINX", "holding_symbol": "SQ", "holding_name": "Block", "weight": 0.07},
            {"etf_ticker": "AIQ", "holding_symbol": "SQ", "holding_name": "Block", "weight": 0.02},
            {"etf_ticker": "AIQ", "holding_symbol": "USD", "holding_name": "Cash", "weight": 0.01},
            {"etf_ticker": "BOTZ", "holding_symbol": "ROK", "holding_name": "Rockwell", "weight": 0.05},
        ]
    )
    themes = pd.DataFrame(
        [
            {"etf_ticker": "FINX", "theme": "fintech", "wave": 1},
            {"etf_ticker": "AIQ", "theme": "ai", "wave": 1},
            {"etf_ticker": "BOTZ", "theme": "robotics", "wave": 2},
        ]
    )
    existing = pd.DataFrame(
        [
            {
                "normalized_symbol": "HOOD",
                "status": "active",
                "promotion_status": "validated_full",
            }
        ]
    )

    def metadata(symbol: str) -> dict[str, str]:
        return {"sector": "Technology", "country": "US", "exchange": "NMS", "currency": "USD"} if symbol == "SQ" else {}

    registry_rows, entity_rows, summary = build_wave_universe_additions(
        holdings=holdings,
        etf_themes=themes,
        existing_registry=existing,
        wave=1,
        max_new_tickers=10,
        batch_size=2,
        metadata_lookup=metadata,
    )

    assert summary["candidate_tickers"] == 2
    assert summary["skipped_existing"] == 1
    assert registry_rows["normalized_symbol"].tolist() == ["SQ"]
    assert registry_rows.iloc[0]["theme_ramp_batch"] == 1
    assert "themes=ai,fintech" in registry_rows.iloc[0]["notes"]
    assert entity_rows[["entity_id", "country", "region", "exchange", "currency", "sector"]].to_dict(
        orient="records"
    ) == [
        {
            "entity_id": "SQ",
            "country": "US",
            "region": "US",
            "exchange": "NMS",
            "currency": "USD",
            "sector": "Technology",
        }
    ]


def test_wave_universe_merge_resumes_after_prior_theme_batch() -> None:
    holdings = pd.DataFrame(
        [
            {"etf_ticker": "AIQ", "holding_symbol": "AAA", "holding_name": "A", "weight": 0.05},
            {"etf_ticker": "AIQ", "holding_symbol": "BBB", "holding_name": "B", "weight": 0.04},
            {"etf_ticker": "AIQ", "holding_symbol": "CCC", "holding_name": "C", "weight": 0.03},
            {"etf_ticker": "AIQ", "holding_symbol": "DDD", "holding_name": "D", "weight": 0.02},
        ]
    )
    themes = pd.DataFrame([{"etf_ticker": "AIQ", "theme": "ai", "wave": 1}])

    first_rows, _, first_summary = build_wave_universe_additions(
        holdings=holdings,
        etf_themes=themes,
        existing_registry=pd.DataFrame(),
        wave=1,
        max_new_tickers=2,
        batch_size=2,
        metadata_lookup=lambda _symbol: {"country": "US"},
    )
    second_rows, _, second_summary = build_wave_universe_additions(
        holdings=holdings,
        etf_themes=themes,
        existing_registry=first_rows.drop(columns=["theme_ramp_batch"]),
        wave=1,
        max_new_tickers=2,
        batch_size=2,
        batch_offset=int(first_rows["theme_ramp_batch"].max()),
        metadata_lookup=lambda _symbol: {"country": "US"},
    )
    third_rows, _, third_summary = build_wave_universe_additions(
        holdings=holdings,
        etf_themes=themes,
        existing_registry=pd.concat(
            [
                first_rows.drop(columns=["theme_ramp_batch"]),
                second_rows.drop(columns=["theme_ramp_batch"]),
            ],
            ignore_index=True,
        ),
        wave=1,
        max_new_tickers=2,
        batch_size=2,
        batch_offset=int(second_rows["theme_ramp_batch"].max()),
        metadata_lookup=lambda _symbol: {"country": "US"},
    )

    assert first_rows["normalized_symbol"].tolist() == ["AAA", "BBB"]
    assert first_rows["theme_ramp_batch"].tolist() == [1, 1]
    assert first_summary["pending_after_selection"] == 2
    assert second_rows["normalized_symbol"].tolist() == ["CCC", "DDD"]
    assert second_rows["theme_ramp_batch"].tolist() == [2, 2]
    assert second_summary["skipped_existing"] == 2
    assert second_summary["pending_after_selection"] == 0
    assert third_rows.empty
    assert third_summary["pending_after_selection"] == 0


def test_entity_attributes_payload_preserves_resolved_country_and_sector() -> None:
    payload = build_entity_attributes_static_payload(
        [
            {
                "input_symbol": "NESN.SW",
                "normalized_symbol": "NESN.SW",
                "region": "eu",
                "exchange": "SWX",
                "exchange_mic": "XSWX",
                "currency": "CHF",
                "country": "CH",
                "sector": "Consumer Defensive",
            }
        ]
    )

    assert payload[0]["country"] == "CH"
    assert payload[0]["sector"] == "Consumer Defensive"
