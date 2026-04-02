"use client"

import { useState, useEffect, useCallback } from "react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Calendar } from "@/components/ui/calendar"
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  CalendarIcon,
  Search,
  ArrowDown,
  Database,
  Calculator,
  Brain,
  ShieldAlert,
  Target,
  AlertTriangle,
  CheckCircle2,
  Loader2,
} from "lucide-react"
import { format } from "date-fns"
import { cn } from "@/lib/utils"
import { fetchXrayTickers, fetchXrayData, type XrayResult } from "@/lib/api"

export function XRayInspector() {
  const [date, setDate] = useState<Date>(new Date(2024, 2, 15))
  const [tickers, setTickers] = useState<string[]>([])
  const [selectedTicker, setSelectedTicker] = useState<string>("")
  const [data, setData] = useState<XrayResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Load ticker list on mount
  useEffect(() => {
    fetchXrayTickers().then((t) => {
      setTickers(t)
      if (t.length > 0 && !selectedTicker) {
        setSelectedTicker(t[0])
      }
    })
  }, []) // eslint-disable-line react-hooks/exhaustive-deps

  // Fetch data when ticker or date changes
  const fetchData = useCallback(async () => {
    if (!selectedTicker) return
    setLoading(true)
    setError(null)
    try {
      const dateStr = format(date, "yyyy-MM-dd")
      const result = await fetchXrayData(selectedTicker, dateStr)
      setData(result)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to fetch X-Ray data")
      setData(null)
    } finally {
      setLoading(false)
    }
  }, [selectedTicker, date])

  useEffect(() => {
    if (selectedTicker) {
      fetchData()
    }
  }, [selectedTicker, date, fetchData])

  return (
    <div className="space-y-4">
      {/* Top Controls */}
      <Card className="border-border/50 bg-card/50">
        <CardContent className="p-4">
          <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center">
            <div className="flex items-center gap-2">
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    className="w-[200px] justify-start text-left font-normal border-border/50 bg-secondary/50"
                  >
                    <CalendarIcon className="mr-2 h-4 w-4 text-muted-foreground" />
                    {date ? format(date, "PPP") : <span className="text-muted-foreground">Pick a date</span>}
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-auto p-0 bg-card border-border" align="start">
                  <Calendar
                    mode="single"
                    selected={date}
                    onSelect={(d) => d && setDate(d)}
                    initialFocus
                  />
                </PopoverContent>
              </Popover>
            </div>

            {/* Ticker Dropdown */}
            <div className="w-[200px]">
              <Select value={selectedTicker} onValueChange={setSelectedTicker}>
                <SelectTrigger className="bg-secondary/50 border-border/50">
                  <Search className="w-4 h-4 mr-2 text-muted-foreground" />
                  <SelectValue placeholder="Select ticker..." />
                </SelectTrigger>
                <SelectContent className="max-h-60">
                  {tickers.map((t) => (
                    <SelectItem key={t} value={t} className="text-xs font-mono">
                      {t}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <Badge variant="outline" className="border-primary/50 text-primary">
              Pipeline Verification: {selectedTicker || "—"}
            </Badge>

            {loading && <Loader2 className="w-4 h-4 animate-spin text-primary" />}
          </div>
        </CardContent>
      </Card>

      {/* Error State */}
      {error && (
        <div className="p-3 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive text-sm flex items-center gap-2">
          <AlertTriangle className="w-4 h-4 shrink-0" />
          {error}
        </div>
      )}

      {/* Loading skeleton */}
      {loading && !data && (
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
        </div>
      )}

      {/* No data state */}
      {!loading && !data && !error && selectedTicker && (
        <Card className="border-dashed">
          <CardContent className="flex flex-col items-center justify-center py-12">
            <Database className="w-12 h-12 text-muted-foreground/30 mb-4" />
            <p className="text-muted-foreground">No data found for {selectedTicker} on {format(date, "PPP")}</p>
            <p className="text-xs text-muted-foreground mt-1">Try a different date (market days only)</p>
          </CardContent>
        </Card>
      )}

      {/* Pipeline Funnel */}
      {data && (
        <div className="space-y-2">
          {/* Card 1: Raw Market Data */}
          <FunnelCard
            icon={<Database className="w-5 h-5" />}
            title="Raw Data"
            subtitle="Market & Fundamental Inputs"
            status={data.raw_data ? "complete" : "pending"}
          >
            {data.raw_data ? (
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
                <DataPoint label="Price" value={data.raw_data.price != null ? `$${data.raw_data.price.toLocaleString()}` : "—"} />
                <DataPoint label="Volume" value={data.raw_data.volume != null ? formatLargeNumber(data.raw_data.volume) : "—"} />
                <DataPoint label="Daily Return" value={data.raw_data.daily_return != null ? `${(data.raw_data.daily_return * 100).toFixed(2)}%` : "—"} highlight />
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No market data for this date</p>
            )}
            {data.fundamentals && (
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 mt-4 pt-4 border-t border-border/30">
                <DataPoint label="Filing Date" value={data.fundamentals.filing_date ?? "—"} />
                <DataPoint label="Revenue" value={data.fundamentals.revenue != null ? formatLargeNumber(data.fundamentals.revenue) : "—"} />
                <DataPoint label="Total Debt" value={data.fundamentals.total_debt != null ? formatLargeNumber(data.fundamentals.total_debt) : "—"} />
                <DataPoint label="Cash" value={data.fundamentals.cash != null ? formatLargeNumber(data.fundamentals.cash) : "—"} />
              </div>
            )}
          </FunnelCard>

          <FlowArrow />

          {/* Card 2: Features / Heuristics */}
          <FunnelCard
            icon={<Calculator className="w-5 h-5" />}
            title="Heuristic Calculations"
            subtitle="Factor & Ratio Computation"
            status={data.features ? "complete" : "pending"}
          >
            {data.features ? (
              <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
                <DataPoint label="EV/Sales Z-Score" value={data.features.ev_sales_zscore?.toFixed(2) ?? "—"} highlight />
                <DataPoint label="Dynamic Discount Rate" value={data.features.dynamic_discount_rate != null ? `${(data.features.dynamic_discount_rate * 100).toFixed(1)}%` : "—"} />
                <DataPoint label="DCF NPV Gap" value={data.features.dcf_npv_gap?.toFixed(4) ?? "—"} highlight />
                <DataPoint label="Beta (SPY)" value={data.features.beta_spy?.toFixed(3) ?? "—"} />
                <DataPoint label="Beta (TNX)" value={data.features.beta_tnx?.toFixed(3) ?? "—"} />
                <DataPoint label="Beta (VIX)" value={data.features.beta_vix?.toFixed(3) ?? "—"} />
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No feature data computed for this date</p>
            )}
          </FunnelCard>

          <FlowArrow />

          {/* Card 3: Strategy Intent */}
          <FunnelCard
            icon={<Brain className="w-5 h-5" />}
            title="Strategy Intent"
            subtitle="Raw Weight from Strategy Pipeline"
            status={data.strategy_intent ? "complete" : "pending"}
          >
            {data.strategy_intent ? (
              <div className="flex items-center gap-6">
                <div>
                  <p className="text-xs text-muted-foreground mb-1">Strategy</p>
                  <Badge className="bg-primary/20 text-primary border-primary/30 font-mono">
                    {data.strategy_intent.strategy_id ?? "—"}
                  </Badge>
                </div>
                <div>
                  <p className="text-xs text-muted-foreground mb-1">Raw Desired Weight</p>
                  <p className="text-lg font-mono text-primary font-bold">
                    {data.strategy_intent.raw_weight != null
                      ? `${(data.strategy_intent.raw_weight * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No strategy intent data for this date</p>
            )}
          </FunnelCard>

          <FlowArrow />

          {/* Card 4: Risk Adjustment */}
          <FunnelCard
            icon={<ShieldAlert className="w-5 h-5" />}
            title="Risk APT Bouncer"
            subtitle="Covariance & MCR Enforcement"
            status={data.risk_adjustment ? (data.risk_adjustment.mcr_breach ? "warning" : "complete") : "pending"}
            highlighted={data.risk_adjustment?.mcr_breach}
          >
            {data.risk_adjustment ? (
              <div className="space-y-4">
                <div className="grid grid-cols-2 sm:grid-cols-3 gap-4">
                  <DataPoint
                    label="MCR"
                    value={data.risk_adjustment.mcr != null ? `${(data.risk_adjustment.mcr * 100).toFixed(1)}%` : "—"}
                    warning={data.risk_adjustment.mcr_breach}
                  />
                  <DataPoint label="MCR Threshold" value={`${(data.risk_adjustment.mcr_threshold * 100).toFixed(0)}%`} />
                  <DataPoint label="Scaled" value={data.risk_adjustment.scaled ? "Yes" : "No"} />
                </div>

                {data.risk_adjustment.mcr_breach && (
                  <div className="flex items-start gap-3 p-3 rounded-lg bg-amber-500/10 border border-amber-500/30">
                    <AlertTriangle className="w-5 h-5 text-amber-400 shrink-0 mt-0.5" />
                    <div className="space-y-1">
                      <p className="text-sm text-amber-200 font-medium">MCR Breach Detected</p>
                      <p className="text-xs text-amber-300/80">Position scaled down to maintain portfolio risk limits.</p>
                    </div>
                  </div>
                )}

                <div className="flex items-center justify-center gap-4 pt-2">
                  <div className="text-center">
                    <p className="text-xs text-muted-foreground">Original Weight</p>
                    <p className="text-lg font-mono text-foreground">
                      {(data.risk_adjustment.original_weight * 100).toFixed(1)}%
                    </p>
                  </div>
                  <ArrowDown className="w-6 h-6 text-amber-400 rotate-[-90deg]" />
                  <div className="text-center">
                    <p className="text-xs text-muted-foreground">Target Weight</p>
                    <p className="text-lg font-mono text-amber-400">
                      {data.risk_adjustment.target_weight != null
                        ? `${(data.risk_adjustment.target_weight * 100).toFixed(1)}%`
                        : "—"}
                    </p>
                  </div>
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No risk adjustment data for this date</p>
            )}
          </FunnelCard>

          <FlowArrow />

          {/* Card 5: Final Order */}
          <FunnelCard
            icon={<Target className="w-5 h-5" />}
            title="Final Order"
            subtitle="Execution-Ready Output"
            status={data.final_order ? "complete" : "pending"}
            final
          >
            {data.final_order ? (
              <div className="flex items-center gap-6">
                <div className="text-center">
                  <p className="text-xs text-muted-foreground mb-1">Target Allocation</p>
                  <p className="text-3xl font-mono text-primary font-bold">
                    {data.final_order.target_allocation != null
                      ? `${(data.final_order.target_allocation * 100).toFixed(1)}%`
                      : "—"}
                  </p>
                </div>
                <div className="h-12 w-px bg-border/50" />
                <div>
                  <Badge className="bg-green-500/20 text-green-400 border-green-500/30">
                    {data.final_order.target_allocation != null && data.final_order.target_allocation > 0 ? "BUY" : "HOLD"}
                  </Badge>
                  <span className="font-mono text-foreground ml-2">{selectedTicker}</span>
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground">No final order generated for this date</p>
            )}
          </FunnelCard>
        </div>
      )}
    </div>
  )
}

function FunnelCard({
  icon,
  title,
  subtitle,
  status,
  children,
  highlighted = false,
  final = false,
}: {
  icon: React.ReactNode
  title: string
  subtitle: string
  status: "complete" | "warning" | "pending"
  children: React.ReactNode
  highlighted?: boolean
  final?: boolean
}) {
  return (
    <Card
      className={cn(
        "border-border/50 bg-card/50 transition-all",
        highlighted && "border-amber-500/50 bg-amber-500/5",
        final && "border-primary/50 bg-primary/5"
      )}
    >
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div
              className={cn(
                "p-2 rounded-lg",
                status === "warning"
                  ? "bg-amber-500/20 text-amber-400"
                  : status === "pending"
                  ? "bg-muted/20 text-muted-foreground"
                  : "bg-primary/20 text-primary"
              )}
            >
              {icon}
            </div>
            <div>
              <CardTitle className="text-sm font-medium text-foreground">{title}</CardTitle>
              <CardDescription className="text-xs text-muted-foreground">{subtitle}</CardDescription>
            </div>
          </div>
          {status === "complete" && <CheckCircle2 className="w-5 h-5 text-green-400" />}
          {status === "warning" && <AlertTriangle className="w-5 h-5 text-amber-400" />}
          {status === "pending" && (
            <Badge variant="outline" className="text-[10px] text-muted-foreground">
              No data
            </Badge>
          )}
        </div>
      </CardHeader>
      <CardContent>{children}</CardContent>
    </Card>
  )
}

function FlowArrow() {
  return (
    <div className="flex justify-center py-1">
      <ArrowDown className="w-5 h-5 text-primary/50" />
    </div>
  )
}

function DataPoint({
  label,
  value,
  highlight = false,
  warning = false,
}: {
  label: string
  value: string
  highlight?: boolean
  warning?: boolean
}) {
  return (
    <div className="space-y-1">
      <p className="text-xs text-muted-foreground">{label}</p>
      <p
        className={cn(
          "text-sm font-mono",
          highlight && "text-primary",
          warning && "text-amber-400",
          !highlight && !warning && "text-foreground"
        )}
      >
        {value}
      </p>
    </div>
  )
}

function formatLargeNumber(num: number): string {
  if (num >= 1_000_000_000_000) return `$${(num / 1_000_000_000_000).toFixed(2)}T`
  if (num >= 1_000_000_000) return `$${(num / 1_000_000_000).toFixed(2)}B`
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
  return num.toLocaleString()
}
