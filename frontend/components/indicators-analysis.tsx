"use client"

import { useState, useEffect } from "react"
import { 
  fetchIndicators, 
  IndicatorsResult,
  TechnicalIndicators,
  FundamentalIndicators,
  StatisticalIndicators
} from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Button } from "@/components/ui/button"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Search, Loader2, AlertCircle, TrendingUp, TrendingDown, Activity, DollarSign, BarChart3 } from "lucide-react"

export function IndicatorsAnalysis() {
  const [ticker, setTicker] = useState("AAPL")
  const [searchInput, setSearchInput] = useState("AAPL")
  const [rfrSource, setRfrSource] = useState("irx")
  const [loading, setLoading] = useState(false)
  const [data, setData] = useState<IndicatorsResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchIndicators(ticker, rfrSource)
      .then((result) => {
        if (cancelled) return
        if (!result) setError(`Could not load data for ${ticker}`)
        else setData(result)
      })
      .catch(() => {
        if (!cancelled) setError("Failed to fetch indicators data")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [ticker, rfrSource])

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault()
    if (searchInput.trim()) {
      setTicker(searchInput.trim().toUpperCase())
    }
  }

  return (
    <div className="flex flex-col h-full space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">Indicators Analysis</h2>
          <p className="text-muted-foreground">
            Multi-factor metrics and technicals across the execution timeline.
          </p>
        </div>

        <div className="flex items-center gap-4 max-w-2xl w-full justify-end">
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
          <form onSubmit={handleSearch} className="flex items-center gap-2 max-w-xs w-full">
            <Input 
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              placeholder="Search ticker (e.g. AAPL)"
              className="w-full bg-background"
            />
            <Button type="submit" variant="secondary" disabled={loading}>
              {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Search className="w-4 h-4" />}
            </Button>
          </form>
        </div>
      </div>

      {error ? (
        <Card className="border-red-900/50 bg-red-950/10">
          <CardContent className="flex items-center gap-2 text-red-400 py-6">
            <AlertCircle className="w-5 h-5" />
            <p>{error}</p>
          </CardContent>
        </Card>
      ) : !data ? (
        <div className="flex-1 flex items-center justify-center">
          <Loader2 className="w-8 h-8 animate-spin text-muted-foreground" />
        </div>
      ) : (
        <Tabs defaultValue="technical" className="flex-1 flex flex-col">
          <div className="flex items-center justify-between mb-4">
            <div className="flex items-baseline gap-3">
              <h3 className="text-3xl font-bold text-emerald-400">{data.ticker}</h3>
              <span className="text-xl text-muted-foreground">
                ${data.technical?.latest_price?.toFixed(2) ?? '---'}
              </span>
            </div>
            
            <TabsList className="bg-card/50 border border-border">
              <TabsTrigger value="technical" className="data-[state=active]:bg-emerald-950/50 data-[state=active]:text-emerald-400">
                <Activity className="w-4 h-4 mr-2" />
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
              <TechnicalTab data={data.technical} />
            </TabsContent>
            <TabsContent value="fundamental" className="mt-0 pb-6 focus-visible:outline-none">
              <FundamentalTab data={data.fundamental} />
            </TabsContent>
            <TabsContent value="statistical" className="mt-0 pb-6 focus-visible:outline-none">
              <StatisticalTab data={data.statistical} />
            </TabsContent>
          </ScrollArea>
        </Tabs>
      )}
    </div>
  )
}

// ── Tab Components ──────────────────────────────────────────────────────────

function TechnicalTab({ data }: { data: TechnicalIndicators | null }) {
  if (!data) return <EmptyState />

  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      
      {/* Moving Averages */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground flex items-center gap-2">
            Moving Averages
            {data.sma_trend === 'above' ? <TrendingUp className="w-4 h-4 text-green-400" /> : <TrendingDown className="w-4 h-4 text-red-400" />}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="SMA 20 (Short)" value={data.sma_20} prefix="$" />
          <MetricRow label="SMA 50 (Med)" value={data.sma_50} prefix="$" />
          <MetricRow label="SMA 200 (Long)" value={data.sma_200} prefix="$" />
          
          <div className="pt-2 flex flex-col gap-1">
            <span className="text-xs text-muted-foreground">Current vs SMA 200:</span>
            <div className={`text-sm font-medium px-2 py-1 rounded inline-flex w-fit ${data.sma_trend === 'above' ? 'bg-green-950/40 text-green-400' : 'bg-red-950/40 text-red-400'}`}>
              {data.sma_trend === 'above' ? 'Bullish (Above)' : 'Bearish (Below)'}
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Mean Reversion & Variance */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Mean Reversion</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-1 pb-2">
            <span className="text-3xl font-bold tracking-tight flex items-baseline gap-1">
              {data.mean_reversion_zscore?.toFixed(2) ?? '---'}
              <span className="text-sm font-normal text-muted-foreground">Z-Score</span>
            </span>
            <span className="text-xs text-muted-foreground">
              {data.mean_reversion_zscore !== null && Math.abs(data.mean_reversion_zscore) > 2 ? 'Extreme deviation (Reversion likely)' : 'Within normal variance'}
            </span>
          </div>

          <MetricRow label="Bollinger Band Position" value={data.bollinger_position} suffix="σ" />
          <MetricRow label="Relative Strength (RSI)" value={data.rsi_14} 
            valueClass={data.rsi_14 && data.rsi_14 > 70 ? 'text-red-400' : data.rsi_14 && data.rsi_14 < 30 ? 'text-green-400' : ''} 
          />
        </CardContent>
      </Card>

      {/* Momentum */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Momentum & Flow</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="10-Day Momentum" value={data.momentum_10d} suffix="%" colorize />
          <MetricRow label="20-Day Momentum" value={data.momentum_20d} suffix="%" colorize />
          <MetricRow label="60-Day Momentum" value={data.momentum_60d} suffix="%" colorize />
          
          <div className="pt-2 border-t border-border/50">
            <MetricRow label="Volume Ratio (20d/60d)" value={data.volume_ratio_20_60} suffix="x" 
              valueClass={data.volume_ratio_20_60 && data.volume_ratio_20_60 > 1.2 ? 'text-blue-400' : ''} 
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
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      
      {/* Valuation */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Valuation</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-1 pb-2">
            <span className="text-3xl font-bold tracking-tight text-blue-400">
              {data.market_cap_label ?? '---'}
            </span>
            <span className="text-xs text-muted-foreground">Market Capitalization</span>
          </div>

          <MetricRow label="Price / Sales" value={data.price_to_sales} suffix="x" />
          <MetricRow label="EV / Sales" value={data.ev_to_sales} suffix="x" />
          <MetricRow 
            label="EV/Sales Z-Score" 
            value={data.ev_sales_zscore} 
            colorize invertedColor
          />
        </CardContent>
      </Card>

      {/* Yield & Cash Flow */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Discounted Cash Flow</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex flex-col gap-1 pb-2">
            <span className="text-3xl font-bold tracking-tight flex items-baseline gap-1">
              {data.dcf_npv_gap !== null && data.dcf_npv_gap !== undefined ? (data.dcf_npv_gap * 100).toFixed(1) : '---'}%
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

      {/* Balance Sheet */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Balance Sheet Health</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="Revenue (TTM Proxy)" stringValue={data.revenue_label} />
          <MetricRow label="Net Debt" stringValue={data.net_debt_label} />
          
          <div className="pt-2 border-t border-border/50">
            <MetricRow label="Cash to Revenue Ratio" value={data.cash_to_revenue} suffix="x" />
          </div>
        </CardContent>
      </Card>

      {/* DCF Monthly Breakdown */}
      {data.dcf_breakdown && data.dcf_breakdown.length > 0 && (
        <Card className="bg-card/30 border-border/50 col-span-1 md:col-span-2 lg:col-span-3">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-muted-foreground">DCF Monthly Projection (5-Year Explicit Forecast)</CardTitle>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[300px] rounded-md border border-border/50">
              <Table>
                <TableHeader className="bg-muted/50 sticky top-0 z-10">
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
                      <TableCell className="text-right font-mono text-emerald-400 font-medium">${(row.cumulative_npv / 1e6).toFixed(1)}M</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </ScrollArea>
            <p className="text-xs text-muted-foreground mt-4 text-center">
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
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
      
      {/* Risk Metrics */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Risk Metrics</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="Risk-Free Rate (Selected)" value={data.risk_free_rate} suffix="%" colorize />
          <MetricRow label="Volatility (30d)" value={data.volatility_30d} suffix="%" />
          <MetricRow label="Volatility (1y)" value={data.volatility_1y} suffix="%" />
          <MetricRow label="Max Drawdown (All-Time)" value={data.max_drawdown} suffix="%" colorize />
          
          <div className="pt-2 border-t border-border/50">
            <MetricRow label="95% Value at Risk (1d)" value={data.var_95} suffix="%" />
            <MetricRow label="99% Value at Risk (1d)" value={data.var_99} suffix="%" />
          </div>
        </CardContent>
      </Card>

      {/* Return Profile */}
      <Card className="bg-card/30 border-border/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm font-medium text-muted-foreground">Return Profile</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <MetricRow label="1-Year Return" value={data.return_1y} suffix="%" colorize />
          <MetricRow label="3-Month Return" value={data.return_3m} suffix="%" colorize />
          <MetricRow label="Sharpe Ratio (1y)" value={data.sharpe_1y} />
          
          <div className="pt-2 border-t border-border/50">
            <MetricRow label="Skewness" value={data.skewness} />
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

// ── Helpers ──────────────────────────────────────────────────────────────────

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
