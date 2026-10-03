# Risk Rules (v1)

- Max leverage hard cap from `config/risk.json`
- Max position notional (USDT)
- Max daily loss (USDT)
- Reduce-only default enabled
- Kill-switch blocks all new exposure when triggered
- All order paths must validate quantity, precision, and symbol allowlist
