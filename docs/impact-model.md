# Impact model — assumptions

| # | Assumption | Value used | Source (fill in before publishing) |
|---|---|---|---|
| 1 | Orders/month for a mid-size delivery platform | TODO | TODO |
| 2 | % of orders affected by stale-ETA-driven late delivery | TODO | TODO |
| 3 | Refund/support cost per late-order incident | TODO | TODO |
| 4 | Reduction in late-order rate attributable to improved ETA MAE | TODO | TODO |

## Calculation

```
incidents_avoided_per_month = orders_per_month * affected_rate * reduction_pct
value_per_month               = incidents_avoided_per_month * cost_per_incident
value_per_year                 = value_per_month * 12
```

## Rule for this file

Never change the README's "Modeled business impact" number without updating this file in the same commit.
