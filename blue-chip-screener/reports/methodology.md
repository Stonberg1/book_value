# Blue Chip Value Screener Methodology

## What this screener does
This screener looks through S&P 500 companies and finds the ones trading below their book value per share. It then scores each company on business quality and valuation discount, combines those scores into a single ranking, and displays the results in a visual chart and table so you can quickly see which names look most attractive.

## What "book value" means
Book value is the accounting value of a company's assets minus its liabilities. Book value per share takes that net value and divides it by the number of shares outstanding. If a stock trades below this number, you are theoretically buying the company for less than what its net assets are worth on paper.

## The Blue Chip Score
The Blue Chip Score is designed to measure quality and durability.

### 1) Market Cap
Larger companies usually have more resources, more analyst coverage, and stronger institutional support. We rank companies by market cap on a log scale so size comparisons are fair across the spectrum. In plain terms, the jump from $10B to $20B counts similarly to the jump from $200B to $400B. Without this adjustment, the very largest companies would overpower the score.

### 2) Dividend Streak
Consistent dividend payers often show steady profitability and shareholder focus. We count consecutive years with regular dividend activity, and a gap resets the streak. This rewards reliability over one-time payouts.

### 3) Financial Stability
This combines three balance-sheet and cash-flow signals:
- Interest coverage: how easily operating cash flow covers interest expense.
- Debt-to-equity: how much debt the company carries relative to equity.
- FCF consistency: how often the company has produced positive free cash flow in recent years.

Together, these indicate whether a company's financial foundation looks sturdy or stretched.

### 4) S&P 500 Tenure
A company that has remained in the S&P 500 for decades has already survived recessions, rate cycles, and market shocks. Newer index additions may still be proving themselves. Longer tenure gets a higher score.

## The Value Score
The Value Score measures how far below book value a stock is trading, on a 0 to 100 scale. A score of 100 means the stock is at least 50% below book value. Deeper discounts score higher.

## The P/B Anomaly Score

Not all stocks trading below book value are equally unusual. Some companies have always traded near or below book value, making the current reading unremarkable. The P/B Anomaly Score identifies the stocks that are trading **anomalously** low relative to their own long-run history.

For each stock we build a daily price-to-book (P/B) series going back up to five years, aligning quarterly book value data from financial statements with the daily closing price. We then compute the **median P/B** over that period as a baseline. The anomaly score formula is:

$$\text{Anomaly Score} = \min\!\left(\frac{\text{Median P/B} - \text{Current P/B}}{\text{Median P/B}} \times 100,\ 100\right)$$

A score of 0 means the current P/B is at or above the historical median — no anomaly. A score near 100 means the stock is trading at a dramatically lower P/B ratio than its own five-year history would suggest.

Stocks with an anomaly score of 50 or above are flagged with the ⚡ symbol in the table and the hover tooltip.

**Note:** To calculate a meaningful baseline, we require at least 4 quarters of book value history. Stocks with fewer than 4 quarters — such as recent index additions — receive no anomaly score. We also skip the anomaly calculation entirely if the historical median P/B is below 1.0, since companies that have always traded at or near book value are not experiencing an anomaly — that is simply their normal valuation.

## Why We Exclude Financials and Real Estate

Banks and insurance companies (Financials sector) use different accounting rules than industrial or technology companies. Their book value is more akin to a regulatory capital measure than a conventional asset-minus-liability calculation. A bank trading at 0.8x book may simply reflect its rate environment and return on equity, not a hidden discount. For this reason, comparing Financials companies to their book value on the same scale as other sectors produces misleading signals.

Real Estate Investment Trusts (REITs) face a similar issue. REIT accounting requires distributing most income as dividends, which depresses retained earnings and keeps book value artificially low. REITs are often better evaluated using funds from operations (FFO) or net asset value (NAV) than price-to-book.

Excluding both sectors from the screener keeps the results focused on companies where price-to-book comparisons are most meaningful.

## The Attractiveness Score
The Attractiveness Score combines Blue Chip and Value scores (default is a 50/50 blend), then adjusts the result downward if the stock has stayed below book value for a long time.

## The Prolonged Discount Penalty
Not every stock below book value is a hidden gem. Some companies stay below book value for over a year, and when that happens it can be a warning sign that the market believes the stated book value is too optimistic. Assets may be deteriorating, the business model may be under pressure, or goodwill may no longer reflect true value. This screener applies a penalty for longer below-book periods, reducing attractiveness by up to 50% for stocks below book for a full year or more. These stocks are still shown, but they are ranked lower so you can investigate with caution.

## The Eroding Book Value Flag
If a company's book value per share has dropped by more than 10% over the last year, the screener adds a warning flag. This helps you see when a discount may be caused by shrinking equity rather than an improving price opportunity.

## How to read the chart
The chart shows percent below book value over time.
- Higher on the Y axis means a deeper discount to book value.
- The 0% line is the book value threshold.
- A stock line appears only when it is below book value.
- If a stock recovers above book, the line disappears until it drops below again.
- Colors represent sectors.

## Limitations
Book value is an accounting metric, not a perfect measure of true economic value. It can be less useful for asset-light businesses like many software firms, and it behaves differently in sectors like banking and insurance. Use this screener as a research starting point, not as a buy signal.
