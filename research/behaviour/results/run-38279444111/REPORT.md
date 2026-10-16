# Behaviour discovery — automatic report

Instruments evaluated: 102 / streamed 102. Tests: 498252.
Discovery passes: stock-FDR 1, global-FDR 0. Candidates: 3.
Tiers: {'VALIDATED_EDGE': 0, 'PROMISING': 0, 'FAILED_HOLDOUT': 0, 'DISCOVERY_ONLY': 3}
Recurring behaviours (descriptive): 34, persisting in holdout: 23
Walk-forward: {"trades": 2410, "weeks": 104, "mean_pct": {"gross": 0.18, "net_low": 0.128, "net_base": 0.088, "net_high": 0.006}, "t_week_clustered": {"gross": 5.14, "net_low": 3.65, "net_base": 2.51, "net_high": 0.17}, "win_pct_net_base": 51.5}

## Profit tiers (top 40)

| tier | stock | hypothesis | h | side | n_D | net_D | n_V | net_V | n_H | net_base_H | CI90_H | net_high_H |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| DISCOVERY_ONLY | MAXHEALTH | pdl_break | tod=09:15-09:45 | EOD | long | 106 | +0.682 | 65 | +0.024 | 62 | -0.029 | -0.263..+0.271 | -0.117 |
| DISCOVERY_ONLY | ANGELONE | trend down, 30m momentum weak, below VWAP, rvol high | 09:45-11:00 | EOD | short | 166 | +0.593 | 75 | +0.182 | 85 | -0.068 | -0.393..+0.192 | -0.166 |
| DISCOVERY_ONLY | SONACOMS | gap+0.3..1%, first 5m bigDn | EOD | short | 69 | +0.714 | 29 | +0.406 | 33 | -0.599 | -1.091..-0.224 | -0.689 |

## Recurring behaviours (top 60 by reliability; descriptive, not trade signals)

| stock | behaviour | h | dir | /week | excess % | t | years | regimes | holdout excess % | net after base cost (pre) |
|---|---|---|---|---|---|---|---|---|---|---|
| TIINDIA | enter 13:15 daily | 60m | down | 4.513 | +0.128 | 6.6 | 4/4 | 4/4 | -0.003 | -0.046 |
| TIINDIA | 5m candle flat | 13:00-14:30 | 60m | down | 4.513 | +0.118 | 5.3 | 4/4 | 3/4 | +0.032 | -0.056 |
| TMPV | pdl_break | nifty=flat | EOD | down | 0.846 | +0.314 | 5.0 | 3/3 | 4/4 | +0.243 | +0.250 |
| TITAN | 5m candles dn-dn-up | 11:00-13:00 | 60m | down | 0.519 | +0.147 | 4.8 | 3/3 | 4/4 | -0.117 | +0.070 |
| BSE | or_break_dn | vwap=above | 15m | down | 0.417 | +0.249 | 4.7 | 3/3 | 4/4 | +0.015 | +0.071 |
| ASHOKLEY | or_trap_dn | rvol=low | 30m | down | 1.308 | +0.105 | 4.7 | 4/4 | 4/4 | +0.030 | -0.023 |
| ANANDRATHI | deep_pullback_up | trend=up | 15m | down | 3.506 | +0.078 | 4.6 | 3/4 | 4/4 | +0.026 | -0.010 |
| TRENT | ema20_pullback_up | rvol=low | 15m | down | 2.647 | +0.062 | 4.6 | 4/4 | 3/4 | -0.010 | -0.033 |
| DLF | squeeze_break_dn | tod=09:45-11:00 | 30m | up | 0.808 | +0.118 | 4.5 | 3/3 | 4/4 | -0.026 | +0.009 |
| PFC | breakout_dn | vwap=above | 15m | up | 1.115 | +0.102 | 4.5 | 4/4 | 4/4 | +0.057 | -0.002 |
| SUZLON | or_break_dn | dayvol=expanded | 15m | up | 0.859 | +0.386 | 4.5 | 3/4 | 4/4 | +0.004 | +0.325 |
| DELHIVERY | vwap_reclaim_up | rvol=low | EOD | down | 1.301 | +0.312 | 4.4 | 4/4 | 4/4 | +0.125 | +0.203 |
| SBIN | trend mixed, 30m momentum neutral, below VWAP, rvol normal | 14:30-15:05 | 15m | up | 0.641 | +0.059 | 4.4 | 3/3 | 4/4 | -0.000 | -0.033 |
| TIINDIA | squeeze_break_dn | nifty=flat | EOD | down | 1.833 | +0.258 | 4.4 | 4/4 | 4/4 | +0.073 | -0.085 |
| TIINDIA | trend down, 30m momentum neutral, below VWAP, rvol normal | 13:00-14:30 | 60m | down | 2.814 | +0.136 | 4.4 | 4/4 | 4/4 | +0.038 | -0.038 |
| TIINDIA | enter 13:30 daily | 60m | down | 4.513 | +0.090 | 4.3 | 4/4 | 4/4 | -0.004 | -0.082 |
| BSE | trend down, 30m momentum neutral, below VWAP, rvol high | 09:45-11:00 | EOD | down | 0.885 | +0.641 | 4.3 | 4/4 | 4/4 | -0.043 | +0.386 |
| TIINDIA | enter 13:15 daily | 30m | down | 4.513 | +0.057 | 4.3 | 4/4 | 3/4 | +0.023 | -0.044 |
| TIINDIA | enter 13:15 on Thu | 60m | down | 0.91 | +0.173 | 4.3 | 3/3 | 4/4 | +0.107 | +0.019 |
| ANANDRATHI | 5m candles flat-dn-dn | 11:00-13:00 | 30m | up | 1.154 | +0.099 | 4.3 | 4/4 | 4/4 | -0.012 | +0.016 |
| AUROPHARMA | rvol_thrust_up | all | 15m | up | 2.692 | +0.107 | 4.3 | 4/4 | 4/4 | -0.012 | +0.021 |
| AUROPHARMA | rvol_thrust_up | rvol=high | 15m | up | 2.692 | +0.107 | 4.3 | 4/4 | 4/4 | -0.012 | +0.021 |
| BSE | trend down, 30m momentum neutral, below VWAP, rvol normal | 14:30-15:05 | 60m | down | 2.282 | +0.123 | 4.2 | 4/4 | 4/4 | +0.081 | -0.027 |
| BSE | trend down, 30m momentum neutral, below VWAP, rvol normal | 14:30-15:05 | EOD | down | 2.282 | +0.123 | 4.2 | 4/4 | 4/4 | +0.081 | -0.027 |
| BSE | ema20_pullback_dn | rvol=low | 60m | down | 2.769 | +0.138 | 4.1 | 4/4 | 3/4 | +0.029 | +0.011 |
| BSE | pdh_break | nifty=flat | 60m | up | 0.968 | +0.449 | 4.1 | 3/3 | 4/4 | +0.053 | +0.372 |
| BSE | failed_breakout_dn | vsNifty=weak | EOD | down | 1.699 | +0.324 | 4.0 | 3/3 | 4/4 | +0.128 | +0.117 |
| BSE | squeeze_break_dn | dayvol=compressed | 30m | down | 1.224 | +0.144 | 4.0 | 3/3 | 4/4 | +0.150 | +0.042 |
| BSE | failed_breakout_dn | vsSector=weak | EOD | down | 1.724 | +0.292 | 4.0 | 4/4 | 4/4 | -0.030 | +0.180 |
| TIINDIA | enter 14:30 daily | 30m | up | 4.513 | +0.079 | 4.0 | 4/4 | 4/4 | +0.019 | +0.003 |
| BSE | squeeze_break_dn | dayvol=compressed | 15m | down | 1.224 | +0.102 | 3.9 | 3/3 | 4/4 | +0.080 | -0.006 |
| BSE | trend down, 30m momentum neutral, below VWAP, rvol normal | 14:30-15:05 | 30m | down | 2.282 | +0.091 | 3.9 | 4/4 | 4/4 | +0.059 | -0.040 |
| BSE | squeeze_break_up | rvol=low | 30m | down | 0.756 | +0.110 | 3.8 | 3/3 | 4/4 | +0.096 | +0.010 |
| BSE | squeeze_break_dn | dayvol=compressed | 60m | down | 1.224 | +0.167 | 3.8 | 3/3 | 4/4 | +0.246 | +0.067 |

## Fingerprint headline per instrument

- **NIFTY** recurring 0 (persisting 0); strongest: enter 11:15 on Mon (15m, up, t 3.4)
- **BANKNIFTY** recurring 0 (persisting 0); strongest: breakout_up | trend=down (15m, down, t 4.0)
- **SENSEX** recurring 0 (persisting 0); strongest: 5m candle bigUp | 14:30-15:05 (15m, up, t 3.8)
- **ADANIPORTS** recurring 0 (persisting 0); strongest: rvol_thrust_up | tod=11:00-13:00 (30m, up, t 3.2)
- **RELIANCE** recurring 0 (persisting 0); strongest: failed_breakout_dn | rvol=normal (15m, down, t 3.6)
- **HDFCBANK** recurring 0 (persisting 0); strongest: vwap_loss_dn | rvol=low (60m, up, t 3.6)
- **ICICIBANK** recurring 0 (persisting 0); strongest: or_trap_dn | vsNifty=strong (30m, down, t 3.5)
- **BSE** recurring 13 (persisting 11); strongest: or_break_dn | vwap=above (15m, down, t 4.7)
- **DMART** recurring 0 (persisting 0); strongest: deep_pullback_up | tod=09:45-11:00 (60m, down, t 4.1)
- **SBIN** recurring 1 (persisting 0); strongest: trend mixed, 30m momentum neutral, below VWAP, rvol normal | 14:30-15:05 (15m, up, t 4.4)
- **INFY** recurring 0 (persisting 0); strongest: pdh_break | vsNifty=strong (60m, up, t 4.1)
- **BHARTIARTL** recurring 0 (persisting 0); strongest: trend up, 30m momentum strong, above VWAP, rvol normal | 11:00-13:00 (30m, up, t 3.5)
- **APOLLOHOSP** recurring 0 (persisting 0); strongest: enter 13:00 on Fri (15m, up, t 4.3)
- **KOTAKBANK** recurring 0 (persisting 0); strongest: 5m candles flat-flat-bigDn | 09:45-11:00 (60m, up, t 3.7)
- **TCS** recurring 0 (persisting 0); strongest: pdl_break | vwap=above (30m, up, t 4.1)
- **AXISBANK** recurring 0 (persisting 0); strongest: failed_breakout_dn | rvol=high (30m, down, t 3.3)
- **TECHM** recurring 0 (persisting 0); strongest: or_trap_dn | tod=13:00-14:30 (30m, down, t 3.7)
- **MCX** recurring 0 (persisting 0); strongest: trend down, 30m momentum weak, below VWAP, rvol normal | 11:00-13:00 (60m, down, t 3.9)
- **SUZLON** recurring 1 (persisting 1); strongest: or_break_dn | dayvol=expanded (15m, up, t 4.5)
- **M&M** recurring 0 (persisting 0); strongest: failed_breakout_dn | vsSector=inline (15m, down, t 3.7)
- **ATHERENERG** recurring 0 (persisting 0); strongest: breakout_dn | all (30m, down, t 3.2)
- **BAJFINANCE** recurring 0 (persisting 0); strongest: 5m candles dn-dn-flat | 11:00-13:00 (15m, up, t 3.5)
- **SAIL** recurring 0 (persisting 0); strongest: rvol_thrust_dn | nifty=down (60m, up, t 3.9)
- **TATASTEEL** recurring 0 (persisting 0); strongest: 5m candles up-flat-up | 11:00-13:00 (15m, up, t 3.7)
- **DELHIVERY** recurring 1 (persisting 1); strongest: vwap_reclaim_up | rvol=low (EOD, down, t 4.4)
- **INDUSINDBK** recurring 0 (persisting 0); strongest: enter 10:15 on Mon (15m, up, t 3.5)
- **LT** recurring 0 (persisting 0); strongest: 5m candles up-up-dn | 13:00-14:30 (EOD, up, t 4.0)
- **IDEA** recurring 0 (persisting 0); strongest: enter 10:15 on Tue (15m, down, t 3.6)
- **COALINDIA** recurring 0 (persisting 0); strongest: squeeze_break_up | trend=down (60m, down, t 3.8)
- **NTPC** recurring 0 (persisting 0); strongest: squeeze_break_dn | vsSector=strong (60m, down, t 3.5)
- **ETERNAL** recurring 0 (persisting 0); strongest: rvol_thrust_up | dayvol=normal (60m, up, t 3.7)
- **ITC** recurring 0 (persisting 0); strongest: squeeze_break_dn | vsNifty=strong (60m, up, t 3.5)
- **POLYCAB** recurring 0 (persisting 0); strongest: 5m candles flat-flat-bigDn | 13:00-14:30 (EOD, down, t 3.9)
- **INDIGO** recurring 0 (persisting 0); strongest: failed_breakout_up | rvol=normal (60m, up, t 3.1)
- **PAYTM** recurring 0 (persisting 0); strongest: trend down, 30m momentum strong, below VWAP, rvol high | 13:00-14:30 (EOD, down, t 3.7)
- **MARUTI** recurring 0 (persisting 0); strongest: failed_breakout_up | tod=11:00-13:00 (15m, up, t 3.9)
- **BHEL** recurring 0 (persisting 0); strongest: vwap_reclaim_up | vsSector=inline (15m, up, t 3.9)
- **DIVISLAB** recurring 0 (persisting 0); strongest: vwap_reclaim_up | vsNifty=inline (30m, down, t 4.0)
- **TITAN** recurring 1 (persisting 0); strongest: 5m candles dn-dn-up | 11:00-13:00 (60m, down, t 4.8)
- **BEL** recurring 0 (persisting 0); strongest: trend up, 30m momentum strong, below VWAP, rvol normal | 13:00-14:30 (15m, down, t 3.7)
- **POLICYBZR** recurring 0 (persisting 0); strongest: trend mixed, 30m momentum neutral, above VWAP, rvol high | 13:00-14:30 (15m, down, t 3.7)
- **HCLTECH** recurring 0 (persisting 0); strongest: pdl_break | tod=13:00-14:30 (15m, up, t 3.3)
- **ADANIENT** recurring 0 (persisting 0); strongest: ema20_pullback_up | nifty=up (15m, down, t 4.2)
- **LAURUSLABS** recurring 0 (persisting 0); strongest: 5m candles bigUp-flat-flat | 09:45-11:00 (30m, down, t 4.1)
- **KALYANKJIL** recurring 0 (persisting 0); strongest: 5m candles flat-dn-dn | 11:00-13:00 (15m, up, t 4.2)
- **OFSS** recurring 0 (persisting 0); strongest: or_trap_dn | tod=11:00-13:00 (60m, down, t 4.2)
- **VEDL** recurring 0 (persisting 0); strongest: trend_fail_dn | vsNifty=inline (15m, down, t 3.5)
- **TVSMOTOR** recurring 0 (persisting 0); strongest: enter 13:00 on Tue (30m, up, t 3.8)
- **HINDUNILVR** recurring 0 (persisting 0); strongest: 5m candle flat | 14:30-15:05 (15m, down, t 3.4)
- **EICHERMOT** recurring 0 (persisting 0); strongest: trend up, 30m momentum strong, above VWAP, rvol high | 09:15-09:45 (60m, down, t 3.9)
- **TMPV** recurring 1 (persisting 1); strongest: pdl_break | nifty=flat (EOD, down, t 5.0)
- **TATAELXSI** recurring 0 (persisting 0); strongest: failed_breakout_up | vsNifty=weak (EOD, up, t 3.7)
- **SOLARINDS** recurring 0 (persisting 0); strongest: breakout_up | dayvol=compressed (30m, up, t 4.1)
- **HAL** recurring 0 (persisting 0); strongest: ema20_pullback_up | vsNifty=strong (15m, up, t 3.6)
- **ADANIPOWER** recurring 0 (persisting 0); strongest: trend_fail_dn | rvol=low (60m, down, t 4.1)
- **ULTRACEMCO** recurring 0 (persisting 0); strongest: trend up, 30m momentum neutral, below VWAP, rvol normal | 14:30-15:05 (15m, down, t 3.9)
- **COFORGE** recurring 0 (persisting 0); strongest: squeeze_break_up | trend=down (60m, down, t 3.8)
- **HINDALCO** recurring 0 (persisting 0); strongest: 5m candles dn-flat-dn | 13:00-14:30 (30m, up, t 4.0)
- **PFC** recurring 1 (persisting 1); strongest: breakout_dn | vwap=above (15m, up, t 4.5)
- **DIXON** recurring 0 (persisting 0); strongest: squeeze_break_up | vwap=below (EOD, down, t 4.3)
- **INDUSTOWER** recurring 0 (persisting 0); strongest: enter 12:00 on Mon (30m, up, t 3.6)
- **PNBHOUSING** recurring 0 (persisting 0); strongest: pdl_reclaim | vsNifty=weak (60m, down, t 3.5)
- **BHARATFORG** recurring 0 (persisting 0); strongest: pdh_reject | vwap=below (EOD, down, t 3.7)
- **DRREDDY** recurring 0 (persisting 0); strongest: rvol_thrust_up | nifty=down (EOD, down, t 3.8)
- **KAYNES** recurring 0 (persisting 0); strongest: squeeze_break_dn | vsNifty=inline (60m, down, t 4.0)
- **RECLTD** recurring 0 (persisting 0); strongest: ema20_pullback_up | nifty=flat (30m, up, t 3.5)
- **ONGC** recurring 0 (persisting 0); strongest: ema20_pullback_up | tod=09:45-11:00 (15m, down, t 3.8)
- **AUROPHARMA** recurring 2 (persisting 0); strongest: rvol_thrust_up | all (15m, up, t 4.3)
- **SUNPHARMA** recurring 0 (persisting 0); strongest: squeeze_break_up | tod=13:00-14:30 (EOD, up, t 4.1)
- **POWERINDIA** recurring 0 (persisting 0); strongest: ema20_pullback_dn | nifty=flat (30m, down, t 4.3)
- **LICHSGFIN** recurring 0 (persisting 0); strongest: squeeze_break_up | vwap=below (30m, down, t 3.9)
- **ANGELONE** recurring 0 (persisting 0); strongest: trend up, 30m momentum strong, above VWAP, rvol high | 14:30-15:05 (30m, up, t 4.1)
- **TORNTPHARM** recurring 0 (persisting 0); strongest: or_trap_dn | vsSector=weak (60m, up, t 4.1)
- **ADANIGREEN** recurring 0 (persisting 0); strongest: trend mixed, 30m momentum weak, below VWAP, rvol normal | 09:45-11:00 (60m, down, t 3.8)
- **ANANDRATHI** recurring 2 (persisting 1); strongest: deep_pullback_up | trend=up (15m, down, t 4.6)
- **PERSISTENT** recurring 0 (persisting 0); strongest: sweep_high_reject | rvol=high (EOD, up, t 3.8)
- **TRENT** recurring 1 (persisting 0); strongest: ema20_pullback_up | rvol=low (15m, down, t 4.6)
- **CHOLAFIN** recurring 0 (persisting 0); strongest: pdh_reject | tod=09:15-09:45 (30m, down, t 3.5)
- **SHRIRAMFIN** recurring 0 (persisting 0); strongest: 5m candles dn-dn-up | 13:00-14:30 (15m, down, t 3.5)
- **ASHOKLEY** recurring 1 (persisting 1); strongest: or_trap_dn | rvol=low (30m, down, t 4.7)
- **NESTLEIND** recurring 0 (persisting 0); strongest: or_trap_up | trend=up (30m, up, t 3.6)
- **MAXHEALTH** recurring 0 (persisting 0); strongest: 5m candles up-flat-up | 11:00-13:00 (15m, down, t 3.6)
- **VBL** recurring 0 (persisting 0); strongest: rvol_thrust_up | tod=13:00-14:30 (30m, down, t 4.0)
- **HDFCAMC** recurring 0 (persisting 0); strongest: rvol_thrust_up | vsSector=weak (60m, up, t 3.5)
- **HINDPETRO** recurring 0 (persisting 0); strongest: enter 09:30 on Thu (15m, up, t 3.7)
- **FEDERALBNK** recurring 0 (persisting 0); strongest: sweep_high_reject | vsNifty=inline (30m, down, t 4.2)
- **JIOFIN** recurring 0 (persisting 0); strongest: 5m candles flat-flat-up | 11:00-13:00 (15m, down, t 4.2)
- **SONACOMS** recurring 0 (persisting 0); strongest: enter 13:45 on Fri (60m, down, t 3.7)
- **GMRAIRPORT** recurring 0 (persisting 0); strongest: trend_fail_up | nifty=down (30m, up, t 3.5)
- **MANAPPURAM** recurring 0 (persisting 0); strongest: deep_pullback_up | tod=13:00-14:30 (15m, down, t 3.9)
- **GLENMARK** recurring 0 (persisting 0); strongest: or_trap_dn | trend=up (15m, down, t 3.9)
- **HEROMOTOCO** recurring 0 (persisting 0); strongest: failed_breakout_up | dayvol=expanded (60m, down, t 3.4)
- **UPL** recurring 0 (persisting 0); strongest: or_trap_dn | nifty=flat (15m, down, t 3.5)
- **POWERGRID** recurring 0 (persisting 0); strongest: trend_fail_up | vsNifty=inline (15m, up, t 4.2)
- **INDHOTEL** recurring 0 (persisting 0); strongest: 5m candles flat-bigUp-flat | 13:00-14:30 (30m, up, t 3.8)
- **TIINDIA** recurring 8 (persisting 6); strongest: enter 13:15 daily (60m, down, t 6.6)
- **GAIL** recurring 0 (persisting 0); strongest: enter 10:15 on Mon (15m, up, t 4.0)
- **DLF** recurring 1 (persisting 0); strongest: squeeze_break_dn | tod=09:45-11:00 (30m, up, t 4.5)
- **RBLBANK** recurring 0 (persisting 0); strongest: enter 11:15 on Thu (15m, down, t 4.0)
- **ENRIN** recurring 0 (persisting 0); strongest: none
- **ABB** recurring 0 (persisting 0); strongest: enter 13:30 on Fri (15m, down, t 3.6)
- **LICI** recurring 0 (persisting 0); strongest: or_trap_dn | nifty=flat (15m, down, t 3.8)
