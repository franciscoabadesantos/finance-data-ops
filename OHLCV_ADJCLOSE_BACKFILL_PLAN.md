# Plano — backfill de OHLCV (pré-2020) + adicionar/popular `adj_close`

> **Para o agente:** `finance-data-ops`. Inclui uma **migração SQL** (o dono corre na Supabase) e uma **mudança de código**. O dono depois corre o **rebuild** no Prefect. Corre `pytest` no fim.

## Contexto

`market_price_daily` tem dois problemas:
1. **OHLCV só desde 2020** — as linhas pré-2020 (ex. AMZN 2000–2019) têm `open/high/low/volume` a NULL (o backfill antigo era close-only, anterior à migração 008). O publish **já envia** OHLCV, por isso um **rebuild full-history** preenche estes nulos — sem mudança de código.
2. **`adj_close` não existe** como coluna, **e o publish nem o envia** (`build_market_price_daily_payload` em `publish/prices.py:13` só manda open/high/low/close/volume). Para guardar adj_close é preciso: (a) coluna nova, (b) incluir adj_close no publish, (c) rebuild.

Tanto o daily como o rebuild publicam via `publish_prices_surfaces` → `build_market_price_daily_payload`, por isso **uma única mudança** cobre os dois caminhos. O frame normalizado já tem a coluna `adj_close` (`providers/market.py`), portanto o `frame.get("adj_close")` funciona.

## Mudança 1 — Migração SQL (adicionar coluna)

Criar `sql/010_market_price_daily_adj_close.sql`:
```sql
alter table if exists public.market_price_daily
  add column if not exists adj_close double precision;
```

> **Ordem crítica (avisar o dono):** esta migração tem de ser aplicada na Supabase **antes** de o código da Mudança 2 entrar em produção. Caso contrário o publish tenta escrever `adj_close` numa coluna inexistente e os runs diários falham.

## Mudança 2 — Incluir `adj_close` no publish

Em `src/finance_data_ops/publish/prices.py`, função `build_market_price_daily_payload`:

(a) No dict do `payload` (a seguir à linha do `close`), acrescentar:
```python
            "close": pd.to_numeric(frame.get("close"), errors="coerce"),
            "adj_close": pd.to_numeric(frame.get("adj_close"), errors="coerce"),
            "volume": pd.to_numeric(frame.get("volume"), errors="coerce"),
```

(b) Na lista final de colunas do `return payload[[...]]`, acrescentar `"adj_close"` a seguir a `"close"`:
```python
    return payload[
        [
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
            "source",
            "fetched_at",
            "created_at",
        ]
    ].to_dict(orient="records")
```
> Não tocar no `dropna(subset=["ticker", "date", "close"])` — `adj_close` pode ser NULL nalgumas linhas e não deve fazer dropar a linha.

(c) Se houver um teste que valide as colunas do payload de market prices (ex. em `tests/`), atualizá-lo para incluir `adj_close`.

## Verificação (agente)
- `pytest -q` passa.
- `build_market_price_daily_payload` num frame de teste com `adj_close` produz a chave `adj_close` nos registos.

## Sequência para o dono (depois de o agente entregar)
1. **Aplicar a migração `010`** na Supabase (adiciona a coluna). ← primeiro, sempre.
2. **Commit + push + deploy** do código (Mudança 2).
3. **Correr o rebuild full-history do market** — `mode=historical_backfill` (upsert, **não** `wipe_rebuild`), com `start_date` antigo (ex. `2000-01-01`) e **todos os símbolos** (não só os novos — os antigos como AMZN é que têm os nulos pré-2020 a preencher). Este rebuild preenche o **OHLCV pré-2020 E o adj_close** de uma vez.
4. (Opcional, vigiar a quota) confirmar que a DB da Supabase fica < 0.5 GB depois do backfill (preencher nulos + adj_close ≈ poucos MB; o grosso é o histórico dos novos tickers internacionais).

## Follow-up frontend (separado, depois do rebuild confirmado)
No `spy-signal-site/app/(app)/stocks/[ticker]/page.tsx`, trocar `getOhlcData(ticker, 1825)` → `getOhlcData(ticker, 3650)` para o **Monthly** da análise técnica ganhar histórico longo real (deixa de mostrar `—` nos indicadores de período longo). O OHLCV pré-2020 já não é o limite depois do backfill.
> Nota: a TA usa `close` (que já é split-adjusted, correto para indicadores). O `adj_close` (ajustado a dividendos) fica guardado para uso futuro (retorno total) — **não** é preciso expô-lo no endpoint `/ohlc` nem na TA agora.

## NÃO fazer
- Não usar `mode=wipe_rebuild` (apagaria a tabela antes de re-fetch).
- Não mexer no normalizer (`providers/market.py`) — já produz adj_close.
- Não expor adj_close no backend `/ohlc` nesta ronda (opcional/futuro).
