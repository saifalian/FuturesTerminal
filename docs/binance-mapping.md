# Binance Mapping (USD-M Futures)

Public streams (BTCUSDT):
- diff depth: `<symbol>@depth`
- trades/tape: `<symbol>@aggTrade`
- best bid/ask: `<symbol>@bookTicker`
- kline: `<symbol>@kline_1m`
- mark/funding context: `<symbol>@markPrice@1s`
- liquidation: `<symbol>@forceOrder`

Order book sync model:
- Buffer diff updates
- Pull REST depth snapshot
- Apply only updates with matching update IDs

Private stream:
- user data listen key lifecycle (keepalive every <= 60 min)
- consume `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`
