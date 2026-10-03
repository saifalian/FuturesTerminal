# Signal Definitions (Draft)

1. Liquidity heatmap: persistent size concentrations in depth ladder
2. Spread pressure: spread compression + quote skew
3. Tape speed: burst rate and aggressor imbalance
4. CVD delta: cumulative buy-sell pressure
5. Absorption: high aggressor volume with low displacement
6. Spoofing: large displayed levels rapidly canceled
7. Liquidation pressure: force-order clustering
8. Positioning context: OI + ratio + taker context

Combined output (`bias_engine.py`):
- `long_score`
- `short_score`
- `confidence`
- `danger_score`
- `reason_tags`
