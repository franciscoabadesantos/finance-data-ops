# Thematic ETF Sources & the Relationship Map — Lessons, State, Roadmap

> Knowledge doc. Captures what we learned building the **theme layer** of the relationship map: how to source ETF
> holdings, the traps, what's done, and what's left. Written 2026-07 after a long calibration/hygiene cycle.

## 1. What the relationship map is (context)
A multi-layer map of "what orbits a company" for **investment insight, not prediction**. Layers, each kept separate and
combined only at query/display with a per-layer toggle:
- **Price layers** (mechanical, whole-universe, free): `raw_price` (correlation), `residual_price` (partial correlation
  after removing market+FX factors — kills spurious regime edges), `lead_lag` (directional).
- **Theme layer** (`theme_etf`): "these belong to the same basket", from **narrow thematic ETF co-holdings**.
- Future feeders (not built): news/GDELT, explicit relations (filings/LLM-on-anomaly), TNIC/VTNIC, ownership, global identity.

Philosophy: **"Market first, explanation second, LLM last."** The market is the cheap radar; news/LLM are the
anomaly-triggered magnifying glass. Insight not prediction → the bar is *stable + interpretable*, not statistically optimal.

**Where we are:** price + theme are built and calibrated. The rest of the vision (other feeders, global) is ahead.

---

## 2. Lessons on SOURCES (the hard-won part)

### 2.1 The theme signal = narrow thematic ETF co-membership
Two companies co-held in a **narrow** thematic ETF is evidence they're the same theme. Value scales with the ETF's
**narrowness (specificity)** and the holdings' weights. Broad ETFs (AIQ 83, IGV 107) give weak, noisy signal; narrow ones
(SMH 25, THNR/OZEM, ARKX/MARS) give strong signal.

### 2.2 Get FULL holdings — NOT yfinance top-10
yfinance `funds_data` returns only the **top-10** holdings → a theme can't be represented (IGV came in at 10 instead of
107). **Always source the issuer's full holdings file.** Per-issuer access learned:
| Issuer | Access | Format notes |
|---|---|---|
| **VanEck** | xlsx | e.g. SMH, OIH, REMX |
| **iShares / BlackRock** | fund-document CSV (ajax endpoint w/ a fixed constant, not a date) | IGV, ICLN, ITA, USRT, XBI… ; 403s browser fetch but the pipeline path works |
| **Global X** | **dated** CSV `..._full-holdings_YYYYMMDD.csv` | AIQ, AQWA, BOTZ, BUG, FINX, LIT, PAVE, URA |
| **ARK** | CSV `..._<FUND_NAME>_<TICKER>_HOLDINGS.csv` | ARKG (ARKX dropped) — **filename embeds the fund NAME** (rename risk, see 2.4) |
| **State Street / SSGA** | holdings file | KRE, XBI, XOP |
| **First Trust** | holdings page + "Export to Excel" | FDN, FIW |
| **Roundhill** | ONE **combined** CSV (~50 funds) filtered by ticker, **dated** URL | OZEM, BETZ (+ many themed funds unused) |
| **AdvisorShares** | stable CSV | YOLO |
| **Tema** | stable CSV (per-ticker; THNR's own was 404 → we used OZEM instead) | — |

### 2.3 Weight formats differ per issuer — normalize to sum=1 (BIG lesson)
Weights arrive in inconsistent forms and caused repeated silent corruption:
- **Percent strings** `"0.93%"` (VanEck, Roundhill) — must divide by 100 → `0.0093`. Bug: read as `0.93` → a holding
  looked like 93%.
- **Bare percent numbers** `9.30` (Global X, SSGA) — no `%` sign, so a string-`%` fix misses them → stay ×100 → the
  ETF's weights sum to ~100 (not ~1).
- **Decimals** — already fractions.
**Fix that finally worked: normalize each ETF's weights to sum ~1.0 at ingestion (scale-agnostic), then sanity-check.**
Sanity gate: reject a single holding > ~50% or a fund sum > ~135% (flag the ETF/row). Sums < 1.0 are FINE and expected
(YOLO ~0.73 after filtering non-equity; JETS ~0.68 shallow subset).

### 2.4 Durability traps (sources go stale silently)
- **Dated URLs** (Global X, Roundhill): must build the current date dynamically + **fall back to the last trading day**
  (skip weekends/holidays). Never hardcode a date.
- **Fund renames break frozen URLs:** ARK renamed "Space **Exploration** & Innovation" → "Space **& Defense** Innovation";
  the old CSV froze at its last snapshot (2026-01-02) and served stale data for months. Only the freshness gate caught it,
  and only by eye.
- **Safety nets:** a **freshness gate** (holdings older than ~180d drop out visibly) + a **refresh cadence** (re-fetch
  periodically) + **tombstoning** (an ETF not refreshed is marked inactive with 0 holdings, not left active-but-empty).
  These fail *safe and visible* rather than serving silent stale/partial data.

### 2.5 What is NOT a usable source
- **Swap-based / derivative ETFs** (MSOS, MAGS, WEED): hold total-return swaps + treasuries, not an equity basket → no
  co-membership signal. (MSOS→YOLO for cannabis; ARKX parser was also corrupted → dropped.)
- **Single-stock income ETFs** (AAPW/NVDW/TSLW…): one underlying + covered calls. Useful *products*, useless as *data*.
- **Broad sector SPDRs** (XLK/XLE/XLV): low specificity → down-weighted or excluded; they add noise, not signal.

### 2.6 Identity / universe traps
- **Symbology:** HK wants zero-padded `0700.HK` not `700.HK`; ADR vs local line (Sony `SONY` vs `6758.T`); cash/FX/
  placeholder rows (`-AUD CASH-`) must be filtered. ~56% of themed-holdings symbols didn't match graph nodes at one point.
- **In-universe vs TRUE breadth:** specificity must use the ETF's **true `holdings_count`**, not the count of its holdings
  that happen to be in our universe. Using in-universe breadth made tiny-coverage ETFs (IHI 2-in-universe) look ultra-narrow
  and dominate spuriously.
- **Theme edges only form between graph NODES.** A theme ETF's constituent that isn't in the price universe contributes
  nothing (e.g. MARS holds ASTS, but ASTS isn't a node → no ASTS edges). Enriching the map needs universe expansion, which
  is the deferred global work — "the map is only as rich as the universe."

---

## 3. What we DID
- Built the **price layers** (raw/residual/lead_lag) + the **theme layer** (`theme_etf`), served via `/relationships` and
  the per-ticker orbit + theme-set (Venn) UI.
- Curated a **~32-theme catalog** across the issuers in 2.2 (ai_semis, software, ai, fintech, cyber, internet, defense,
  space [MARS], biotech, glp1_obesity [OZEM], genomics, oil_gas, oil_services, clean_energy, robotics, humanoid_robotics
  [HUMN], ev_battery, infra_construction, homebuilders, nuclear_uranium, water [AQWA], critical_minerals, gold_miners,
  agriculture, timber, medical_devices, cannabis [YOLO], gaming_esports, sports_betting [BETZ], regional_banks,
  airlines_travel [JETS, shallow], reits [USRT]). Full issuer holdings, not yfinance top-10.
- **Calibrated strength & confidence** (config-driven, tuned by inspection): specificity on true breadth, tempered weight
  term, corroboration by n_etfs, per-edge confidence distinct from strength (confidence = reliability/corroboration,
  strength = magnitude). Widened confidence to ~0.9 for strong pairs.
- **Weight hygiene:** the `%`/scale fixes, sum-to-1 normalization, sanity validation, tombstoning, and a feature-store
  guardrail that clamps/renormalizes bad weights instead of silently dropping a whole ETF (SMH once vanished to 0 edges).
- **Fixed the ARK staleness** (fund rename → new URL) and standardized dated-URL / trading-day fallback handling.

---

## 4. Key design principles (so future changes stay consistent)
- **Specificity** ← `1/holdings_count^α` (true breadth; α steep so broad ETFs contribute little). Shallow ETFs use a floor.
- **Strength** = Σ_etf specificity · f(weight) · freshness · corroboration(n_etfs), normalized 0..1. Weight is *tempered*
  (top-weight pairs must not dominate on size alone).
- **Confidence** is a *separate* signal (reliability), from corroboration + specificity + freshness — never a re-encoding
  of strength. Price-layer confidence uses sample size + t-stat + stability; lead_lag uses its t-stat/persistence.
- **Layers stay separate**, combined only at query/display with toggles. `theme_etf` strength is 0..1; price layers are
  signed (negative = rotates-against).
- **Everything free/candidate first, paid/ground-truth later.** Calibrate by inspection (configurable knobs), not
  black-box optimization — the product is *insight*.

---

## 5. What's LEFT
### Immediate
- **Backoffice source-health monitoring** — surface + alert on: stale sources (holdings `as_of` age), themes with 0
  holdings, degraded/partial ingestion, shallow flags, weight-hygiene warnings. Every one of these we've so far caught **by
  hand** (SMH=0 edges, partial 516-holding publish, ARKX stale, 3 weight-parser bugs). Do this **before** global expansion.

### Parked (by decision, next step known)
- **Theme-peer strength threshold** — names with only a broad-ETF membership (e.g. ACN, only in AIQ) show many weak,
  identical peers; hide sub-threshold theme peers so "no strong theme basket" reads honestly. (Display first, then builder.)
- **Corroboration-overlap** — overlapping tech ETFs (FDN/IGV/AIQ/BUG share cloud names) can over-reward the same cluster
  via n_etfs. Refine to count *independent* themes, not overlapping ETFs — only if it actually dominates the ranking.

### The big roadmap (the rest of the vision)
- **Global expansion** — identity (OpenFIGI + GLEIF) to reconcile ADR/ISIN/local lines (Sony/Sonae gap), then bring theme
  constituents + non-US names into the **price universe as nodes** (the map is universe-gated). Largest single effort.
- **Other feeders** — news/GDELT (anomaly-triggered, short half-life, low confidence), explicit relations (SEC/ESEF/EDINET
  filings or LLM-on-anomaly), TNIC/VTNIC US priors, 13F/N-PORT ownership, and paid supply-chain (FactSet/S&P) last.

---

## 6. Operational guardrails (keep these)
- Freshness gate + refresh cadence + tombstoning → sources fail **safe and visible**.
- Weight normalization to sum=1 + sanity gate + no-silent-drop → parser bugs can't silently kill a theme.
- Dated URLs built dynamically with trading-day fallback.
- (Coming) backoffice monitoring closes the loop so these guardrails **alert** instead of waiting to be noticed by eye.
