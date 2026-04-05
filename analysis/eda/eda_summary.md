# EDA Summary — Spend per Arrival (Q2)
## Data Quality & Coverage

### Missingness

| Table | Column | Null % | Cause |
|---|---|---|---|
| international_tourist_spending | all columns | 0% | Clean after sub-region exclusion |
| joined | quarterly_arrivals | 44.6% | StatCan suppression of low-volume origin-province combinations |
| joined | spend_per_arrival | 51.7% | 44.6% join failure + 7.1% zero-arrival rows (NULLIF) |

44.6% of joined rows have no matching visitor count. This is expected since StatCan suppresses counts for low-volume combinations to protect respondent privacy. The remaining 7.1% gap splits into 887 zero-arrival rows and 25 zero-spend rows, all correctly dropped by the existing filters.

### Row-level Coverage

| Stage | Rows | % of total |
|---|---|---|
| Province-level spending rows | 12,365 | 100% |
| After excluding Overseas / Other countries | 9,656 | 78.1% |
| After excluding NULL or zero arrivals | 5,968 | 48.3% |
| After excluding < 20 quarterly arrivals | 5434 | 43.9% |

The 50-arrival threshold was added after observing a $433,093 spend_per_arrival outlier caused by low-volume origin-province pairs within otherwise reliable provinces. Province-name flagging alone does not catch these.

## Province Reliability

| Province | Pre-COVID spend/arrival | Reason flagged |
|---|---|---|
| Prince Edward Island | $7,831 | Very low international volumes |
| Nunavut | $1,004 | Very low international volumes |
| Northwest Territories | suppressed | Majority of counts suppressed by StatCan |
| Yukon | $100 | Very low international volumes |

Included in outputs, flagged via `low_reliability` column.

## Summary Statistics (post-filtering, all years)

| | amount_spent | spend_per_arrival | quarterly_arrivals |
|---|---|---|---|
| Mean | $11.0M | $2,241 | 110,437 |
| Median | $981.5K | $328 | 4,025 |
| Max | $625.1M | $433,093 | 4,855,171 |

Large mean/median gap across all three metrics confirms heavy right skew driven by US arrivals into Ontario and BC.

## Key Findings

**Volume/value inversion.** The US is the highest-volume market but has the lowest spend per arrival ($81 pre-COVID). China has the highest spend per arrival ($424) with comparatively low volumes. High-value and high-volume markets are not the same.

**Seasonal variation.** Australia peaks Q1, China peaks Q4, Germany and Japan peak Q3. The US shows the least variation, consistent with drive-tourism patterns.

**COVID impact by expenditure type.** Accommodation was the largest category pre-COVID and was hit hardest. No category returned to 2019 levels by 2023. Transportation and recreation lag accommodation and food in their recovery.

**Post-COVID data availability.** StatCan suppressed provincial visitor counts for all non-US origin countries from 2021 onward. Post-COVID recovery analysis is therefore limited to US arrivals. This is a dataset limitation, not a pipeline issue.

## Influence on Downstream Analysis

| Finding | Effect |
|---|---|
| Volume/value inversion | `recovery()` tracks spend and arrival indexes separately |
| Seasonal variation | `recovery()` matches by quarter against 2019 baseline |
| Low-reliability provinces | `low_reliability` flag carried through all outputs |
| Sparse pairs outlier | `MIN_QUARTERLY_ARRIVALS = 50` added to EDA and q2.py |
| Post-COVID suppression | `shift()` produces US-only results by data availability |