# Plano — mover o refresh do trading-calendar de market-daily para fundamentals-daily

> **Para o agente:** Só `finance-data-ops`. Sem deploy/reinício (o dono trata). Corre `pytest` no fim. É uma mudança pequena de **localização** de um passo já existente.

## Contexto

O `exchange_trading_calendar` é populado por um passo "best-effort" que faz `run_dataops_trading_calendar_daily(...)` dentro de um flow diário (em vez de um deployment Prefect próprio — o free tier só permite 5 deployments). Atualmente esse passo está no **`dataops_market_daily_flow`**, que corre **3×/dia** — exagero, porque o calendário muda raramente.

Queremos que corra **1×/dia**, movendo o passo para o **`dataops_fundamentals_daily_flow`** (que tem schedule único `0 3 * * 1-5`). Assim qualquer mudança no calendário (atualização da lib `exchange_calendars`, nova bolsa) fica registada dentro de um dia, sem o triplo de escritas.

**Não mexer** no `prefect.yaml` (continua com 5 deployments; o deployment `trading-calendar` já foi removido) nem no flow standalone `flows/dataops_trading_calendar_daily.py` (o core `run_dataops_trading_calendar_daily` é reutilizado).

## Mudança 1 — Remover o passo do `dataops_market_daily_flow`

Em `flows/prefect_dataops_daily.py`, no `dataops_market_daily_flow`, **reverter** o bloco do calendário para o original:

Atual:
```python
    summary["execution"] = execution_plan.as_dict()

    # Exchange trading calendars ride along with the daily market refresh instead of
    # owning a separate Prefect deployment (free-tier 5-deployment limit). Cheap,
    # idempotent upsert across all supported exchanges; never fail the market flow on it.
    try:
        summary["trading_calendar"] = run_dataops_trading_calendar_daily(
            cache_root=cache_root,
            publish_enabled=bool(publish_enabled),
            raise_on_failed_hard=False,
        )
    except Exception as exc:  # noqa: BLE001 - calendar refresh is best-effort
        logger.warning("Trading-calendar refresh failed (non-fatal): %s", exc)
        summary["trading_calendar"] = {"status": "error", "error": str(exc)}

    return summary
```
Novo (volta ao original do market flow):
```python
    summary["execution"] = execution_plan.as_dict()
    return summary
```

## Mudança 2 — Adicionar o passo ao `dataops_fundamentals_daily_flow`

No mesmo ficheiro, no `dataops_fundamentals_daily_flow`, **adicionar** o passo a seguir ao `summary["execution"] = execution_plan.as_dict()`:

Atual:
```python
    summary["execution"] = execution_plan.as_dict()
    return summary
```
Novo:
```python
    summary["execution"] = execution_plan.as_dict()

    # Exchange trading calendars ride along with fundamentals-daily (runs once per day)
    # instead of owning a separate Prefect deployment (free-tier 5-deployment limit).
    # Daily cadence captures any calendar change (exchange_calendars update, new exchange)
    # within a day. Idempotent upsert; never fail the fundamentals flow on it.
    try:
        summary["trading_calendar"] = run_dataops_trading_calendar_daily(
            cache_root=cache_root,
            publish_enabled=bool(publish_enabled),
            raise_on_failed_hard=False,
        )
    except Exception as exc:  # noqa: BLE001 - calendar refresh is best-effort
        logger.warning("Trading-calendar refresh failed (non-fatal): %s", exc)
        summary["trading_calendar"] = {"status": "error", "error": str(exc)}

    return summary
```

> O import `from flows.dataops_trading_calendar_daily import run_dataops_trading_calendar_daily` (topo do ficheiro) **mantém-se** — agora é usado pelo fundamentals flow. `logger`, `cache_root` e `publish_enabled` já existem no escopo do fundamentals flow.

## Verificação
- `python -c "import ast; ast.parse(open('flows/prefect_dataops_daily.py').read())"` ok.
- `grep -c "run_dataops_trading_calendar_daily" flows/prefect_dataops_daily.py` → 2 (o import + 1 uso no fundamentals flow; **0 usos no market flow**).
- `grep -cE "^  - name:" prefect.yaml` → 5 (inalterado).
- `pytest -q` passa.

## NÃO fazer
- Não adicionar/remover deployments no `prefect.yaml` (fica em 5).
- Não tocar no core `run_dataops_trading_calendar_daily` nem no provider/publisher do calendário.
- Não pôr o passo no market-daily nem em mais nenhum flow além do fundamentals.
