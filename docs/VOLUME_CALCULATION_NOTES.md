# Volume Calculation Notes

## Important: Avoiding Double-Counting

Each trade on Hyperliquid creates **TWO fill records** - one for each party (buyer and seller, sides A and B). This means:

- 1 trade = 2 fills
- If we sum volume across all fills, we **double-count** every trade
- Example: Alice buys 100 ETH from Bob at $2000 = $200K volume
  - Fill 1 (Alice, side A): $200K
  - Fill 2 (Bob, side B): $200K
  - Naive sum: $400K ❌ (should be $200K)

## Solution: Filter to One Side

For **market-level metrics** (exchange volume, not user activity), we filter to `WHERE side = 'A'` to count each trade only once:

- Total exchange volume
- Daily/monthly volume
- Volume per coin/asset

## User-Level Metrics

For **user-specific metrics**, we count all their fills (both sides) because:
- A user's trading activity includes both buying and selling
- We want to measure their total participation

User metrics that count all fills:
- Volume per user (for bucketing/cohorts)
- Top users by volume
- Individual user stats

## Verification

July 15, 2025:
- ❌ All fills: $36.1B (double-counted)
- ✅ Side A only: $18.05B (correct)
- ✅ Side B only: $18.05B (matches)
- External sources: ~$17B ✓

The small difference (~6%) may be due to:
- Time zone differences
- Inclusion/exclusion of certain trade types
- Data source variations
