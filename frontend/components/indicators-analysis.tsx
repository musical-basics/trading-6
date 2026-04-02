"use client"

import { useEffect, useState } from "react"
import {
  fetchIndicatorTickers,
  fetchIndicators,
  FundamentalIndicators,
  IndicatorsResult,
  StatisticalIndicators,
  TechnicalIndicators,
} from "@/lib/api"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  AlertCircle,
  BarChart3,
  Check,
  DollarSign,
  Loader2,
  Plus,
  Search,
  TrendingDown,
  TrendingUp,
  X,
} from "lucide-react"

export function IndicatorsAnalysis() {
  const [selectedTickers, setSelectedTickers] = useState(["AAPL"])
  const [availableTickers, setAvailableTickers] = useState<string[]>([])
  const [pickerOpen, setPickerOpen] = useState(false)
  const [rfrSource, setRfrSource] = useState("irx")
  const [loading, setLoading] = useState(false)
  const [dataByTicker, setDataByTicker] = useState<Record<string, IndicatorsResult>>({})
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    fetchIndicatorTickers()
      .then((tickers) => {
        if (!cancelled) setAvailableTickers(tickers)
      })
      .catch(() => {
        if (!cancelled) setAvailableTickers([])
      })

    return () => {
      cancelled = true
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)

    Promise.all(
      selectedTickers.map(async (ticker) => ({
        ticker,
        result: await fetchIndicators(ticker, rfrSource),
      })),
    )
      .then((results) => {
        if (cancelled) return

        const nextData: Record<string, IndicatorsResult> = {}
        const failed: string[] = []

        for (const { ticker, result } of results) {
          if (result) nextData[ticker] = result
          else failed.push(ticker)
        }

        setDataByTicker(nextData)

        if (failed.length === selectedTickers.length) {
          setError(`Could not load data for ${failed.join(", ")}`)
          return
        }

        if (failed.length > 0) {
          setError(`Some tickers failed to load: ${failed.join(", ")}`)
        }
      })
      .catch(() => {
        if (!cancelled) setError("Failed to fetch indicators data")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [selectedTickers, rfrSource])

  const comparisonResults = selectedTickers
    .map((ticker) => dataByTicker[ticker])
    .filter((result): result is IndicatorsResult => Boolean(result))

  const addTicker = (ticker: string) => {
    setSelectedTickers((current) => (current.includes(ticker) ? current : [...current, ticker]))
    setPickerOpen(false)
  }

  const removeTicker = (ticker: string) => {
    setSelectedTickers((current) => (current.length === 1 ? current : current.filter((value) => value !== ticker)))
  }

  return (
    <div className="flex h-full flex-col space-y-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">Indicators Analysis</h2>
          <p className="text-muted-foreground">Multi-factor metrics and technicals across the execution timeline.</p>
        </div>

        <div className="flex flex-col gap-3 lg:items-end">
          <Select value={rfrSource} onValueChange={setRfrSource}>
            <SelectTrigger className="w-[180px] bg-background">
              <SelectValue placeholder="Risk-Free Rate" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="irx">13-Week T-Bill (^IRX)</SelectItem>
              <SelectItem value="tnx">10-Year Yield (^TNX)</SelectItem>
              <SelectItem value="fixed_4.3">Fixed 4.3% (Static)</SelectItem>
            </SelectContent>
          </Select>

          <Popover open={pickerOpen} onOpenChange={setPickerOpen}>
            <PopoverTrigger asChild>
              <Button variant="secondary" className="gap-2">
                <Plus className="h-4 w-4" />
                Ticker
              </Button>
            </PopoverTrigger>
            <PopoverContent className="w-[280px] p-0" align="end">
              <Command>
                <CommandInput placeholder="Search ticker..." />
                <CommandList>
                  <CommandEmpty>No tickers found.</CommandEmpty>
                  <CommandGroup heading="Available tickers">
                    {availableTickers.map((ticker) => {
                      const selected = selectedTickers.includes(ticker)
                      return (
                        <CommandItem
                          key={ticker}
                          value={ticker}
                          onSelect={() => addTicker(ticker)}
                          className="justify-between"
                        >
                          <span>{ticker}</span>
                          {selected ? (
                            <Check className="h-4 w-4 text-emerald-400" />
                          ) : (
                            <Search className="h-4 w-4 text-muted-foreground" />
                          )}
                        </CommandItem>
                      )
                    })}
                  </CommandGroup>
                </CommandList>
              </Command>
            </PopoverContent>
          </Popover>
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {selectedTickers.map((ticker) => {
          const removable = selectedTickers.length > 1
          return (
            <Badge key={ticker} variant="outline" className="gap-2 px-3 py-1 text-sm">
              <span>{ticker}</span>
              <button
                type="button"
                onClick={() => removeTicker(ticker)}
                disabled={!removable}
                className="rounded-sm text-muted-foreground transition hover:text-foreground disabled:cursor-not-allowed disabled:opacity-40"
                aria-label={`Remove ${ticker}`}
              >
                <X className="h-3.5 w-3.5" />
              </button>
            </Badge>
          )
        })}
      </div>

      {error && comparisonResults.length === 0 ? (
        <Card className="border-red-900/50 bg-red-950/10">
          <CardContent className="flex items-center gap-2 py-6 text-red-400">
            <AlertCircle className="h-5 w-5" />
            <p>{error}</p>
          </CardContent>
        </Card>
      ) : loading && comparisonResults.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <Tabs defaultValue="technical" className="flex flex-1 flex-col">
          {error ? (
            <Card className="mb-4 border-amber-900/50 bg-amber-950/10">
              <CardContent className="flex items-center gap-2 py-4 text-amber-300">
                <AlertCircle className="h-5 w-5" />
                <p>{error}</p>
              </CardContent>
            </Card>
          ) : null}

          <div className="mb-4 grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_auto] lg:items-start">
            <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
              {selectedTickers.map((ticker) => {
                const result = dataByTicker[ticker]
                return (
                  <Card key={ticker} className="border-border/50 bg-card/30">
                    <CardHeader className="pb-2">
                      <CardTitle className="flex items-center justify-between text-sm font-medium text-muted-foreground">
                        <span>{ticker}</span>
                        {loading && !result ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-1">
                      <div className="text-2xl font-bold text-emerald-400">{formatCurrency(result?.technical?.latest_price)}</div>
                      <div className="text-xs text-muted-foreground">
                        {result?.technical?.sma_trend === "above"
                          ? "Bullish above 200-day trend"
                          : result?.technical?.sma_trend === "below"
                            ? "Trading below 200-day trend"
                            : "Awaiting market context"}
                      </div>
                    </CardContent>
                  </Card>
                )
              })}
            </div>

            <TabsList className="border border-border bg-card/50">
              <TabsTrigger value="technical" className="data-[state=active]:bg-emerald-950/50 data-[state=active]:text-emerald-400">
                <BarChart3 className="mr-2 h-4 w-4" />
                Technical
              </TabsTrigger>
              <TabsTrigger value="fundamental" className="data-[state=active]:bg-blue-950/50 data-[state=active]:text-blue-400">
                <DollarSign className="mr-2 h-4 w-4" />
                Fundamental
              </TabsTrigger>
              <TabsTrigger value="statistical" className="data-[state=active]:bg-purple-950/50 data-[state=active]:text-purple-400">
                <BarChart3 className="mr-2 h-4 w-4" />
                Statistical
              </TabsTrigger>
            </TabsList>
          </div>

          <ScrollArea className="-mx-4 flex-1 px-4">
            <TabsContent value="technical" className="mt-0 pb-6 focus-visible:outline-none">
              <TickerAnalyticsPanels
                results={comparisonResults}
                render={(result) => <TechnicalTab data={result.technical} />}
              />
            </TabsContent>
            <TabsContent value="fundamental" className="mt-0 pb-6 focus-visible:outline-none">
              <TickerAnalyticsPanels
                results={comparisonResults}
                render={(result) => <FundamentalTab data={result.fundamental} />}
              />
            </TabsContent>
            <TabsContent value="statistical" className="mt-0 pb-6 focus-visible:outline-none">
              <TickerAnalyticsPanels
                results={comparisonResults}
                render={(result) => <StatisticalTab data={result.statistical} />}
              />
            </TabsContent>
          </ScrollArea>
        </Tabs>
      )}
    </div>
  )
}

function TickerAnalyticsPanels({
  results,
  render,
}: {
  results: IndicatorsResult[]
  render: (result: IndicatorsResult) => React.ReactNode
}) {
  if (results.length === 0) return <EmptyState />

  return (
    <div className="space-y-6">
      {results.map((result) => (
        <section key={result.ticker} className="space-y-3">
          <div className="flex items-center gap-3">
            <h4 className="text-lg font-semibold text-emerald-400">{result.ticker}</h4>
            <span className="text-sm text-muted-foreground">{formatCurrency(result.technical?.latest_price)}</span>
          </div>
          {render(result)}
        </section>
      ))}
    </div>
  )
}

function TechnicalTab({ data }: { data: TechnicalIndicators | null }) {
  if (!data) return <EmptyState />

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-sm font-medium text-muted-foreground">
            Moving Averages
            {data.sma_trend === "above" ? <TrendingUp className="h-4 w-4 text-green-400" /> : <TrendingDown className="h-4 w-4 text-red-400" />}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="SMA 20 (Short)" value={data.sma_20} prefix="$" />
          <MetricRow label="SMA 50 (Med)" value={data.sma_50} prefix="$" />
          <MetricRow label="SMA 200 (Long)" value={data.sma_200} prefix="$" />

          <div className="flex flex-col gap-1 pt-2">
            <span className="text-xs text-muted-foreground">Current vs SMA 200:</span>
            <div
              className={`inline-flex w-fit rounded px-2 py-1 text-sm font-medium ${
                data.sma_trend === "above" ? "bg-green-950/40 text-green-400" : "bg-red-950/40 text-red-400"
              }`}
            >
              {data.sma_trend === "above" ? "Bullish (Above)" : "Bearish (Below)"}
            </div>
          </div>
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Mean Reversion</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-1 pb-2">
            <span className="flex items-baseline gap-1 text-3xl font-bold tracking-tight">
              {data.mean_reversion_zscore?.toFixed(2) ?? "---"}
              <span className="text-sm font-normal text-muted-foreground">Z-Score</span>
            </span>
            <span className="text-xs text-muted-foreground">
              {data.mean_reversion_zscore !== null && Math.abs(data.mean_reversion_zscore) > 2
                ? "Extreme deviation (Reversion likely)"
                : "Within normal variance"}
            </span>
          </div>

          <MetricRow label="Bollinger Band Position" value={data.bollinger_position} suffix="σ" />
          <MetricRow
            label="Relative Strength (RSI)"
            value={data.rsi_14}
            valueClass={data.rsi_14 && data.rsi_14 > 70 ? "text-red-400" : data.rsi_14 && data.rsi_14 < 30 ? "text-green-400" : ""}
          />
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Momentum & Flow</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="10-Day Momentum" value={data.momentum_10d} suffix="%" colorize />
          <MetricRow label="20-Day Momentum" value={data.momentum_20d} suffix="%" colorize />
          <MetricRow label="60-Day Momentum" value={data.momentum_60d} suffix="%" colorize />

          <div className="border-t border-border/50 pt-2">
            <MetricRow
              label="Volume Ratio (20d/60d)"
              value={data.volume_ratio_20_60}
              suffix="x"
              valueClass={data.volume_ratio_20_60 && data.volume_ratio_20_60 > 1.2 ? "text-blue-400" : ""}
            />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function FundamentalTab({ data }: { data: FundamentalIndicators | null }) {
  if (!data) return <EmptyState />

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Valuation</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-1 pb-2">
            <span className="text-3xl font-bold tracking-tight text-blue-400">{data.market_cap_label ?? "---"}</span>
            <span className="text-xs text-muted-foreground">Market Capitalization</span>
          </div>

          <MetricRow label="Price / Sales" value={data.price_to_sales} suffix="x" />
          <MetricRow label="EV / Sales" value={data.ev_to_sales} suffix="x" />
          <MetricRow label="EV/Sales Z-Score" value={data.ev_sales_zscore} colorize invertedColor />
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Discounted Cash Flow</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-1 pb-2">
            <span className="flex items-baseline gap-1 text-3xl font-bold tracking-tight">
              {data.dcf_npv_gap !== null && data.dcf_npv_gap !== undefined ? (data.dcf_npv_gap * 100).toFixed(1) : "---"}%
            </span>
            <span className="text-xs text-muted-foreground">DCF Intrinsic Value Gap</span>
          </div>

          <MetricRow
            label="Dynamic Discount Rate"
            value={data.dynamic_discount_rate !== null && data.dynamic_discount_rate !== undefined ? data.dynamic_discount_rate * 100 : null}
            suffix="%"
          />
          <MetricRow label="Latest Filing Date" stringValue={data.filing_date} />
          <MetricRow label="Features Updated" stringValue={data.feature_date} />
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Balance Sheet Health</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="Revenue (TTM Proxy)" stringValue={data.revenue_label} />
          <MetricRow label="Net Debt" stringValue={data.net_debt_label} />

          <div className="border-t border-border/50 pt-2">
            <MetricRow label="Cash to Revenue Ratio" value={data.cash_to_revenue} suffix="x" />
          </div>
        </CardContent>
      </Card>

      {data.dcf_breakdown && data.dcf_breakdown.length > 0 && (
        <Card className="col-span-1 border-border/50 bg-card/30 md:col-span-2 lg:col-span-3">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">DCF Monthly Projection (5-Year Explicit Forecast)</CardTitle>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[300px] rounded-md border border-border/50">
              <Table>
                <TableHeader className="sticky top-0 z-10 bg-muted/50">
                  <TableRow>
                    <TableHead className="w-[80px]">Month</TableHead>
                    <TableHead className="text-right">Cash Flow</TableHead>
                    <TableHead className="text-right">Discount Factor</TableHead>
                    <TableHead className="text-right">Present Value</TableHead>
                    <TableHead className="text-right">Cumulative NPV</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {data.dcf_breakdown.map((row) => (
                    <TableRow key={row.month}>
                      <TableCell className="font-medium text-muted-foreground">{row.month}</TableCell>
                      <TableCell className="text-right font-mono">${(row.cash_flow / 1e6).toFixed(1)}M</TableCell>
                      <TableCell className="text-right font-mono">{row.discount_factor.toFixed(4)}x</TableCell>
                      <TableCell className="text-right font-mono font-medium">${(row.present_value / 1e6).toFixed(1)}M</TableCell>
                      <TableCell className="text-right font-mono font-medium text-emerald-400">${(row.cumulative_npv / 1e6).toFixed(1)}M</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </ScrollArea>
            <p className="mt-4 text-center text-xs text-muted-foreground">
              Note: Month 60 incorporates Terminal Value capitalization. Cash flows represent annualized run-rates distributed linearly equivalent per month.
            </p>
          </CardContent>
        </Card>
      )}
    </div>
  )
}

function StatisticalTab({ data }: { data: StatisticalIndicators | null }) {
  if (!data) return <EmptyState />

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Risk Metrics</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="Risk-Free Rate (Selected)" value={data.risk_free_rate} suffix="%" colorize />
          <MetricRow label="Volatility (30d)" value={data.volatility_30d} suffix="%" />
          <MetricRow label="Volatility (1y)" value={data.volatility_1y} suffix="%" />
          <MetricRow label="Max Drawdown (All-Time)" value={data.max_drawdown} suffix="%" colorize />

          <div className="border-t border-border/50 pt-2">
            <MetricRow label="95% Value at Risk (1d)" value={data.var_95} suffix="%" />
            <MetricRow label="99% Value at Risk (1d)" value={data.var_99} suffix="%" />
          </div>
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Return Profile</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="1-Year Return" value={data.return_1y} suffix="%" colorize />
          <MetricRow label="3-Month Return" value={data.return_3m} suffix="%" colorize />
          <MetricRow label="Sharpe Ratio (1y)" value={data.sharpe_1y} />

          <div className="border-t border-border/50 pt-2">
            <MetricRow label="Skewness" value={data.skewness} />
            <MetricRow label="Kurtosis (Fat Tails)" value={data.kurtosis} />
          </div>
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/30">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Factor Betas & CAPM</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="Expected Return (CAPM)" value={data.capm_expected_return} suffix="%" />

          <div className="border-t border-border/50 pt-2">
            <MetricRow label="Beta (SPY)" value={data.beta_spy} />
            <MetricRow label="Beta (10Y Yield)" value={data.beta_tnx} />
            <MetricRow label="Beta (VIX)" value={data.beta_vix} />
          </div>

          <div className="border-t border-border/50 pt-2">
            <MetricRow label="Correlation vs SPY (90d)" value={data.correlation_spy_90d} />
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function EmptyState() {
  return (
    <div className="mt-4 flex items-center justify-center rounded-lg border border-dashed border-border/50 p-12 text-muted-foreground">
      No data available for this category.
    </div>
  )
}

interface MetricRowProps {
  label: string
  value?: number | null
  stringValue?: string | null
  prefix?: string
  suffix?: string
  colorize?: boolean
  invertedColor?: boolean
  valueClass?: string
}

function MetricRow({
  label,
  value,
  stringValue,
  prefix = "",
  suffix = "",
  colorize = false,
  invertedColor = false,
  valueClass = "",
}: MetricRowProps) {
  let colorStr = ""
  if (colorize && value !== undefined && value !== null) {
    if (value > 0) colorStr = invertedColor ? "text-red-400" : "text-green-400"
    if (value < 0) colorStr = invertedColor ? "text-green-400" : "text-red-400"
  }

  return (
    <div className="flex items-center justify-between">
      <span className="text-sm text-muted-foreground">{label}</span>
      <span className={`font-mono font-medium ${colorStr} ${valueClass}`}>
        {stringValue !== undefined && stringValue !== null ? stringValue : value !== undefined && value !== null ? `${prefix}${value}${suffix}` : "---"}
      </span>
    </div>
  )
}

function formatCurrency(value?: number | null, digits: number = 2) {
  return value !== undefined && value !== null ? `$${value.toFixed(digits)}` : "---"
}
