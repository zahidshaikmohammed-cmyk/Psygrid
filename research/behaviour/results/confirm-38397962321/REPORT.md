# Pre-registered confirmation run

- Status: **COMPLETE**
- Pre-registration lock: **intact**
- Stream: {'interval': 1, 'years': 4, 'start': '2021-10-01', 'end': '2026-10-12', 'complete': True}

> 2021-10-01 .. 2022-10-12 is a HISTORICAL CONFIRMATION PERIOD THAT PREDATES THE DISCOVERY DATA (2022-10-13 onwards). It is untouched by the earlier search, but it is a backward out-of-sample check, not a forward chronological test.

## Verdicts (confirmation window, base cost, Holm across six)

| | Verdict | Trades | Weeks | Stocks | Mean net base % | 90% CI | Holm p | Net zero / low / high % | Failed checks |
|---|---|---|---|---|---|---|---|---|---|
| H1 | REJECTED | 23578 | 50 | 93 | -0.1227 | -0.1291 .. -0.1146 | 1.0000 | -0.0041 / -0.0411 / -0.2587 | significant, ci_above_zero, high_cost_positive, not_concentrated, both_halves_positive  |
| H2 | REJECTED | 950 | 50 | 93 | -0.0681 | -0.0970 .. -0.0362 | 1.0000 | 0.0538 / 0.0147 / -0.2068 | significant, ci_above_zero, high_cost_positive, not_concentrated, both_halves_positive  |
| H3 | REJECTED | 8097 | 50 | 69 | -0.1269 | -0.1342 .. -0.1193 | 1.0000 | -0.0084 / -0.0453 / -0.2629 | significant, ci_above_zero, high_cost_positive, not_concentrated, both_halves_positive, in_sample_direction  |
| H4 | REJECTED | 1987 | 31 | 93 | -0.1472 | -0.1730 .. -0.1274 | 1.0000 | -0.0233 / -0.0633 / -0.2880 | significant, ci_above_zero, high_cost_positive, not_concentrated, both_halves_positive, in_sample_direction  |
| H5 | REJECTED | 355 | 6 | 35 | -0.1764 | -0.3728 .. 0.2697 | 1.0000 | -0.0515 / -0.0889 / -0.3212 | sample, significant, ci_above_zero, high_cost_positive, not_concentrated, both_halves_positive, in_sample_direction  |
| H6 | REJECTED | 4530 | 50 | 93 | -0.1125 | -0.1271 .. -0.1002 | 1.0000 | 0.0156 / -0.0246 / -0.2595 | significant, ci_above_zero, high_cost_positive, not_concentrated, both_halves_positive  |

A hypothesis that is not CONFIRMED means no edge was established under the pre-registered test. It does not prove that no edge exists: the test can only detect effects above its minimum detectable effect.

## Known biases that the verdicts cannot remove

- **Survivorship:** the universe is today's top-99 F&O names (chosen 2026-10-12). In 2021-22 it over-represents stocks that later did well. H5 (drift) is capped at INCONCLUSIVE for this reason; H1-H4 and H6 trade both sides, so the bias is smaller but not zero.
- **Corporate actions:** Dhan's 1-minute history is not known to be split/bonus adjusted. Sessions with an index-adjusted overnight gap of 8% or more are flagged and +-5 sessions around them are excluded (integrity.py). A stock with flags inside a window has per-stock results marked CA_UNCERTAIN; per-stock results are descriptive only in any case.
- Instruments with fewer than 150 complete sessions in a window are NOT_APPLICABLE there, never failures.

## Data integrity

| Instrument | Sessions with rows | Kept (complete / filled) | Excluded | Missing vs NIFTY calendar | Missing 09:15 | Rows from 15:30 | Dup. identical / conflicting |
|---|---|---|---|---|---|---|---|
| NIFTY | 1133 | 1113 / 11 | NO_REGULAR_SESSION 3, NO_0915 2, INCOMPLETE 4 | - | 5 | 134 | 0 / 0 |
| BANKNIFTY | 838 | 825 / 5 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 1 | 411 | 7 | 166 | 0 / 0 |
| SENSEX | 1254 | 1026 / 214 | NO_REGULAR_SESSION 4, NO_0915 7, INCOMPLETE 3 | 0 | 11 | 240 | 0 / 0 |
| ADANIPORTS | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| RELIANCE | 1250 | 1188 / 51 | NO_REGULAR_SESSION 3, NO_0915 6, INCOMPLETE 2 | 0 | 9 | 222 | 0 / 0 |
| HDFCBANK | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| ICICIBANK | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| BSE | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| DMART | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 221 | 0 / 0 |
| SBIN | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| INFY | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| BHARTIARTL | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| APOLLOHOSP | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 220 | 0 / 0 |
| KOTAKBANK | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| TCS | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| AXISBANK | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| TECHM | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| MCX | 1249 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 221 | 0 / 0 |
| SUZLON | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| M&M | 1249 | 1189 / 51 | NO_REGULAR_SESSION 2, NO_0915 5, INCOMPLETE 2 | 0 | 7 | 221 | 0 / 0 |
| ATHERENERG | 358 | 324 / 32 | NO_0915 2 | 0 | 2 | 3 | 0 / 0 |
| BAJFINANCE | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| SAIL | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| TATASTEEL | 1192 | 1132 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 164 | 0 / 0 |
| DELHIVERY | 1089 | 1032 / 50 | NO_REGULAR_SESSION 3, NO_0915 2, INCOMPLETE 2 | 0 | 5 | 65 | 0 / 0 |
| INDUSINDBK | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| LT | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| IDEA | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| COALINDIA | 1191 | 1132 / 50 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 58 | 7 | 165 | 0 / 0 |
| NTPC | 1192 | 1131 / 52 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 164 | 0 / 0 |
| ETERNAL | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| ITC | 1250 | 1188 / 51 | NO_REGULAR_SESSION 3, NO_0915 6, INCOMPLETE 2 | 0 | 9 | 222 | 0 / 0 |
| POLYCAB | 1249 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 220 | 0 / 0 |
| INDIGO | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 216 | 0 / 0 |
| PAYTM | 1157 | 1098 / 51 | NO_REGULAR_SESSION 3, NO_0915 3, INCOMPLETE 2 | 0 | 6 | 144 | 0 / 0 |
| MARUTI | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 221 | 0 / 0 |
| BHEL | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| DIVISLAB | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| TITAN | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| BEL | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| POLICYBZR | 1220 | 1158 / 53 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 192 | 0 / 0 |
| HCLTECH | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| ADANIENT | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| LAURUSLABS | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 221 | 0 / 0 |
| KALYANKJIL | 1249 | 1166 / 74 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 216 | 0 / 0 |
| OFSS | 1249 | 1173 / 67 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 212 | 0 / 0 |
| VEDL | 1250 | 1188 / 51 | NO_REGULAR_SESSION 3, NO_0915 6, INCOMPLETE 2 | 0 | 9 | 222 | 0 / 0 |
| TVSMOTOR | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 220 | 0 / 0 |
| HINDUNILVR | 1250 | 1188 / 51 | NO_REGULAR_SESSION 3, NO_0915 6, INCOMPLETE 2 | 0 | 9 | 222 | 0 / 0 |
| EICHERMOT | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 221 | 0 / 0 |
| TMPV | 1250 | 1188 / 51 | NO_REGULAR_SESSION 3, NO_0915 6, INCOMPLETE 2 | 0 | 9 | 222 | 0 / 0 |
| TATAELXSI | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| SOLARINDS | 1249 | 1108 / 132 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 164 | 0 / 0 |
| HAL | 1249 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 221 | 0 / 0 |
| ADANIPOWER | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| ULTRACEMCO | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 220 | 0 / 0 |
| COFORGE | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| HINDALCO | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| PFC | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| DIXON | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| INDUSTOWER | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| PNBHOUSING | 1249 | 1141 / 99 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 209 | 0 / 0 |
| BHARATFORG | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 217 | 0 / 0 |
| DRREDDY | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 220 | 0 / 0 |
| KAYNES | 965 | 909 / 50 | NO_REGULAR_SESSION 2, NO_0915 2, INCOMPLETE 2 | 0 | 4 | 60 | 0 / 0 |
| RECLTD | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| ONGC | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| AUROPHARMA | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| SUNPHARMA | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| POWERINDIA | 1249 | 1115 / 125 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 1 | 0 | 8 | 201 | 0 / 0 |
| LICHSGFIN | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| ANGELONE | 1248 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 3, INCOMPLETE 2 | 0 | 6 | 219 | 0 / 0 |
| TORNTPHARM | 1249 | 1157 / 83 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 178 | 0 / 0 |
| ADANIGREEN | 1249 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 221 | 0 / 0 |
| ANANDRATHI | 1198 | 1097 / 85 | NO_REGULAR_SESSION 3, NO_0915 7, INCOMPLETE 6 | 0 | 10 | 150 | 0 / 0 |
| PERSISTENT | 1249 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 219 | 0 / 0 |
| TRENT | 1249 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 218 | 0 / 0 |
| CHOLAFIN | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 221 | 0 / 0 |
| SHRIRAMFIN | 1249 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 220 | 0 / 0 |
| ASHOKLEY | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| NESTLEIND | 1250 | 1185 / 55 | NO_REGULAR_SESSION 4, NO_0915 4, INCOMPLETE 2 | 0 | 8 | 200 | 0 / 0 |
| MAXHEALTH | 1249 | 1184 / 56 | NO_REGULAR_SESSION 4, NO_0915 3, INCOMPLETE 2 | 0 | 7 | 215 | 0 / 0 |
| VBL | 1249 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 206 | 0 / 0 |
| HDFCAMC | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 220 | 0 / 0 |
| HINDPETRO | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 221 | 0 / 0 |
| FEDERALBNK | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| JIOFIN | 780 | 725 / 49 | NO_REGULAR_SESSION 2, NO_0915 2, INCOMPLETE 2 | 0 | 4 | 60 | 0 / 0 |
| SONACOMS | 1249 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 222 | 0 / 0 |
| GMRAIRPORT | 1249 | 1188 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| MANAPPURAM | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| GLENMARK | 1249 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 222 | 0 / 0 |
| HEROMOTOCO | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 221 | 0 / 0 |
| UPL | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| POWERGRID | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| INDHOTEL | 1249 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 4, INCOMPLETE 2 | 0 | 7 | 222 | 0 / 0 |
| TIINDIA | 1249 | 1146 / 94 | NO_REGULAR_SESSION 4, NO_0915 3, INCOMPLETE 2 | 0 | 7 | 194 | 0 / 0 |
| GAIL | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| DLF | 1250 | 1189 / 51 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| RBLBANK | 1250 | 1188 / 52 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 222 | 0 / 0 |
| ENRIN | 326 | 316 / 8 | NO_0915 2 | 0 | 2 | 3 | 0 / 0 |
| ABB | 1250 | 1168 / 72 | NO_REGULAR_SESSION 3, NO_0915 5, INCOMPLETE 2 | 0 | 8 | 214 | 0 / 0 |
| LICI | 1094 | 1037 / 50 | NO_REGULAR_SESSION 3, NO_0915 2, INCOMPLETE 2 | 0 | 5 | 70 | 0 / 0 |
