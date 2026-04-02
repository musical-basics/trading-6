"use client"

import { useState, useEffect } from "react"
import { 
  fetchIndicatorTickers,
  fetchIndicators, 
  IndicatorsResult,
  TechnicalIndicators,
  FundamentalIndicators,
  StatisticalIndicators
} from "@/lib/api"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { AlertCircle, BarChart3, Check, DollarSign, Loader2, Plus, Search, TrendingDown, TrendingUp, X } from "lucide-react"

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
    setSelectedTickers((current) => {
      if (current.includes(ticker)) return current
      return [...current, ticker]
    })
    setPickerOpen(false)
  }

  const removeTicker = (ticker: string) => {
    setSelectedTickers((current) => {
      if (current.length === 1) return current
      return current.filter((value) => value !== ticker)
    })
  }

  return (
    <div className="flex flex-col h-full space-y-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">Indicators Analysis</h2>
          <p className="text-muted-foreground">
            Multi-factor metrics and technicals across the execution timeline.
          </p>
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
                          {selected ? <Check className="h-4 w-4 text-emerald-400" /> : <Search className="h-4 w-4 text-muted-foreground" />}
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
          <CardContent className="flex items-center gap-2 text-red-400 py-6">
            <AlertCircle className="w-5 h-5" />
            <p>{error}</p>
          </CardContent>
        </Card>
      ) : loading && comparisonResults.length === 0 ? (
        <div className="flex-1 flex items-center justify-center">
          <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <Tabs defaultValue="technical" className="flex-1 flex flex-col">
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
                      <div className="text-2xl font-bold text-emerald-400">
                        {formatCurrency(result?.technical?.latest_price)}
                      </div>
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
            
            <TabsList className="bg-card/50 border border-border">
              <TabsTrigger value="technical" className="data-[state=active]:bg-emerald-950/50 data-[state=active]:text-emerald-400">
                <BarChart3 className="w-4 h-4 mr-2" />
                Technical
              </TabsTrigger>
              <TabsTrigger value="fundamental" className="data-[state=active]:bg-blue-950/50 data-[state=active]:text-blue-400">
                <DollarSign className="w-4 h-4 mr-2" />
                Fundamental
              </TabsTrigger>
              <TabsTrigger value="statistical" className="data-[state=active]:bg-purple-950/50 data-[state=active]:text-purple-400">
                <BarChart3 className="w-4 h-4 mr-2" />
                Statistical
              </TabsTrigger>
            </TabsList>
          </div>

          <ScrollArea className="flex-1 -mx-4 px-4">
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
              <span className="text-sm text-muted-foreground">
                {formatCurrency(result.technical?.latest_price)}
              </span>
            </div>
            {render(result)}
          </section>
        ))}
      </div>
    )
  }

  // ── Helpers ─────────────────────────────────────────────────────────────────-
            <MetricRow label="Kurtosis (Fat Tails)" value={data.kurtosis} />
          </div>
        </CardContent>
      </Card>

      {/* Factor Betas & CAPM */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Factor Betas & CAPM</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="Expected Return (CAPM)" value={data.capm_expected_return} suffix="%" />
          
          <div className="pt-2 border-t border-border/50">
            <MetricRow label="Beta (SPY)" value={data.beta_spy} />
            <MetricRow label="Beta (10Y Yield)" value={data.beta_tnx} />
            <MetricRow label="Beta (VIX)" value={data.beta_vix} />
          </div>

          <div className="pt-2 border-t border-border/50">
            <MetricRow label="Correlation vs SPY (90d)" value={data.correlation_spy_90d} />
          </div>
        </CardContent>
      </Card>

    </div>
  )
}

function TechnicalCompareTab({ results }: { results: IndicatorsResult[] }) {
  return (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
      <ComparisonCard title="Trend & Price">
        <ComparisonTable
          results={results}
          rows={[
            { label: "Last Price", values: results.map((result) => formatCurrency(result.technical?.latest_price)) },
            { label: "SMA 20", values: results.map((result) => formatCurrency(result.technical?.sma_20)) },
            { label: "SMA 50", values: results.map((result) => formatCurrency(result.technical?.sma_50)) },
            { label: "SMA 200", values: results.map((result) => formatCurrency(result.technical?.sma_200)) },
            {
              label: "Trend vs 200d",
              values: results.map((result) => ({
                label: result.technical?.sma_trend === "above" ? "Bullish" : result.technical?.sma_trend === "below" ? "Bearish" : "---",
                className: result.technical?.sma_trend === "above" ? "text-green-400" : result.technical?.sma_trend === "below" ? "text-red-400" : "",
              })),
            },
          ]}
        />
      </ComparisonCard>

      <ComparisonCard title="Mean Reversion">
        <ComparisonTable
          results={results}
          rows={[
            {
              label: "Z-Score",
              values: results.map((result) => formatSignedNumber(result.technical?.mean_reversion_zscore, 2)),
            },
            {
              label: "Bollinger Position",
              values: results.map((result) => formatNumber(result.technical?.bollinger_position, 3, "", "σ")),
            },
            {
              label: "RSI 14",
              values: results.map((result) => ({
                label: formatNumberLabel(result.technical?.rsi_14),
                className: result.technical?.rsi_14 !== undefined && result.technical?.rsi_14 !== null
                  ? result.technical.rsi_14 > 70
                    ? "text-red-400"
                    : result.technical.rsi_14 < 30
                      ? "text-green-400"
                      : ""
                  : "",
              })),
            },
          ]}
        />
      </ComparisonCard>

      <ComparisonCard title="Momentum & Flow">
        <ComparisonTable
          results={results}
          rows={[
            {
              label: "10D Momentum",
              values: results.map((result) => formatPercent(result.technical?.momentum_10d, 2, true)),
            },
            {
              label: "20D Momentum",
              values: results.map((result) => formatPercent(result.technical?.momentum_20d, 2, true)),
            },
            {
              label: "60D Momentum",
              values: results.map((result) => formatPercent(result.technical?.momentum_60d, 2, true)),
            },
            {
              label: "Volume Ratio",
              values: results.map((result) => ({
                label: formatNumberLabel(result.technical?.volume_ratio_20_60, 3, "", "x"),
                className: result.technical?.volume_ratio_20_60 !== undefined && result.technical?.volume_ratio_20_60 !== null && result.technical.volume_ratio_20_60 > 1.2 ? "text-blue-400" : "",
              })),
            },
          ]}
        />
      </ComparisonCard>
    </div>
  )
}

function FundamentalCompareTab({ results }: { results: IndicatorsResult[] }) {
  return (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
      <ComparisonCard title="Valuation">
        <ComparisonTable
          results={results}
          rows={[
            { label: "Market Cap", values: results.map((result) => result.fundamental?.market_cap_label ?? "---") },
            { label: "Price / Sales", values: results.map((result) => formatNumber(result.fundamental?.price_to_sales, 2, "", "x")) },
            { label: "EV / Sales", values: results.map((result) => formatNumber(result.fundamental?.ev_to_sales, 2, "", "x")) },
            { label: "EV/Sales Z", values: results.map((result) => formatPercentless(result.fundamental?.ev_sales_zscore, true)) },
          ]}
        />
      </ComparisonCard>

      <ComparisonCard title="Discounted Cash Flow">
        <ComparisonTable
          results={results}
          rows={[
            { label: "DCF Gap", values: results.map((result) => formatScaledPercent(result.fundamental?.dcf_npv_gap, 1, true)) },
            { label: "Discount Rate", values: results.map((result) => formatScaledPercent(result.fundamental?.dynamic_discount_rate, 2)) },
            { label: "Latest Filing", values: results.map((result) => result.fundamental?.filing_date ?? "---") },
            { label: "Features Updated", values: results.map((result) => result.fundamental?.feature_date ?? "---") },
          ]}
        />
      </ComparisonCard>

      <ComparisonCard title="Balance Sheet Health">
        <ComparisonTable
          results={results}
          rows={[
            { label: "Revenue", values: results.map((result) => result.fundamental?.revenue_label ?? "---") },
            { label: "Net Debt", values: results.map((result) => result.fundamental?.net_debt_label ?? "---") },
            { label: "Cash / Revenue", values: results.map((result) => formatNumber(result.fundamental?.cash_to_revenue, 2, "", "x")) },
          ]}
        />
      </ComparisonCard>
    </div>
  )
}

function StatisticalCompareTab({ results }: { results: IndicatorsResult[] }) {
  return (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
      <ComparisonCard title="Risk Metrics">
        <ComparisonTable
          results={results}
          rows={[
            { label: "Risk-Free Rate", values: results.map((result) => formatPercent(result.statistical?.risk_free_rate, 2, true)) },
            { label: "Volatility 30d", values: results.map((result) => formatPercent(result.statistical?.volatility_30d, 2)) },
            { label: "Volatility 1y", values: results.map((result) => formatPercent(result.statistical?.volatility_1y, 2)) },
            { label: "Max Drawdown", values: results.map((result) => formatPercent(result.statistical?.max_drawdown, 2, true)) },
            { label: "VaR 95", values: results.map((result) => formatPercent(result.statistical?.var_95, 2)) },
            { label: "VaR 99", values: results.map((result) => formatPercent(result.statistical?.var_99, 2)) },
          ]}
        />
      </ComparisonCard>

      <ComparisonCard title="Return Profile">
        <ComparisonTable
          results={results}
          rows={[
            { label: "1Y Return", values: results.map((result) => formatPercent(result.statistical?.return_1y, 2, true)) },
            { label: "3M Return", values: results.map((result) => formatPercent(result.statistical?.return_3m, 2, true)) },
            { label: "Sharpe 1Y", values: results.map((result) => formatPercentless(result.statistical?.sharpe_1y)) },
            { label: "Skewness", values: results.map((result) => formatPercentless(result.statistical?.skewness)) },
            { label: "Kurtosis", values: results.map((result) => formatPercentless(result.statistical?.kurtosis)) },
          ]}
        />
      </ComparisonCard>

      <ComparisonCard title="Factor Betas & CAPM">
        <ComparisonTable
          results={results}
          rows={[
            { label: "CAPM Return", values: results.map((result) => formatPercent(result.statistical?.capm_expected_return, 2)) },
            { label: "Beta SPY", values: results.map((result) => formatPercentless(result.statistical?.beta_spy)) },
            { label: "Beta 10Y", values: results.map((result) => formatPercentless(result.statistical?.beta_tnx)) },
            { label: "Beta VIX", values: results.map((result) => formatPercentless(result.statistical?.beta_vix)) },
            { label: "Corr SPY 90d", values: results.map((result) => formatPercentless(result.statistical?.correlation_spy_90d)) },
          ]}
        />
      </ComparisonCard>
    </div>
  )
}

// ── Helpers ──────────────────────────────────────────────────────────────────

type ComparisonCell = string | { label: string; className?: string }

interface ComparisonRow {
  label: string
  values: ComparisonCell[]
}

function ComparisonCard({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <Card className="border-border/50 bg-card/30">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
      </CardHeader>
      <CardContent>
        {children}
      </CardContent>
    </Card>
  )
}

function ComparisonTable({ results, rows }: { results: IndicatorsResult[]; rows: ComparisonRow[] }) {
  return (
    <ScrollArea className="w-full">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="min-w-[180px]">Metric</TableHead>
            {results.map((result) => (
              <TableHead key={result.ticker} className="min-w-[120px] text-right">
                {result.ticker}
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.label}>
              <TableCell className="font-medium text-muted-foreground">{row.label}</TableCell>
              {row.values.map((value, index) => {
                const cell = typeof value === "string" ? { label: value } : value
                return (
                  <TableCell key={`${row.label}-${results[index]?.ticker ?? index}`} className={`text-right font-mono ${cell.className ?? ""}`}>
                    {cell.label}
                  </TableCell>
                )
              })}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </ScrollArea>
  )
}

function EmptyState() {
  return (
    <div className="flex items-center justify-center p-12 mt-4 border border-dashed border-border/50 rounded-lg text-muted-foreground">
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

function MetricRow({ label, value, stringValue, prefix = "", suffix = "", colorize = false, invertedColor = false, valueClass = "" }: MetricRowProps) {
  let colorStr = ""
  if (colorize && value !== undefined && value !== null) {
    if (value > 0) colorStr = invertedColor ? "text-red-400" : "text-green-400"
    if (value < 0) colorStr = invertedColor ? "text-green-400" : "text-red-400"
  }

  return (
    <div className="flex items-center justify-between">
      <span className="text-sm text-muted-foreground">{label}</span>
      <span className={`font-mono font-medium ${colorStr} ${valueClass}`}>
        {stringValue !== undefined && stringValue !== null ? stringValue :
         value !== undefined && value !== null ? `${prefix}${value}${suffix}` : '---'}
      </span>
    </div>
  )
}

function formatCurrency(value?: number | null, digits: number = 2) {
  return value !== undefined && value !== null ? `$${value.toFixed(digits)}` : "---"
}
