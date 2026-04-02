"use client"

import { useState, useEffect, useCallback } from "react"
import {
  TrendingUp,
  TrendingDown,
  DollarSign,
  Wallet,
  RefreshCw,
  AlertCircle,
  Briefcase,
  BarChart3,
  ChevronUp,
  ChevronDown,
  History,
} from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Sheet,
  SheetContent,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"
import { ScrollArea } from "@/components/ui/scroll-area"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { cn } from "@/lib/utils"
import {
  getTraders,
  fetchLivePositions,
  fetchTraderExecutions,
  runPipelineRebalance,
  getPipelineStatus,
  type Trader,
  type LivePositionsResponse,
  type LivePosition,
  type TraderExecution,
} from "@/lib/api"

// ── Helpers ───────────────────────────────────────────────────

function fmt(n: number, decimals = 2) {
  return n.toLocaleString("en-US", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })
}

function fmtUsd(n: number) {
  const abs = Math.abs(n)
  const prefix = n < 0 ? "-$" : "$"
  return `${prefix}${fmt(abs)}`
}

function fmtPct(n: number) {
  return `${n >= 0 ? "+" : ""}${fmt(n, 2)}%`
}

function fmtDateTime(dstr: string) {
  const d = new Date(dstr)
  if (isNaN(d.getTime())) return dstr
  return d.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
  })
}

type SortKey = keyof LivePosition
type SortDir = "asc" | "desc"

// ── Summary Card ──────────────────────────────────────────────

function SummaryCard({
  title,
  value,
  sub,
  icon: Icon,
  accent,
}: {
  title: string
  value: string
  sub?: string
  icon: React.ElementType
  accent: "neutral" | "positive" | "negative"
}) {
  const accentClass = {
    neutral: "text-primary",
    positive: "text-emerald-400",
    negative: "text-rose-400",
  }[accent]

  return (
    <Card className="bg-card/60 border-border/50 backdrop-blur-sm">
      <CardHeader className="flex flex-row items-center justify-between pb-2 space-y-0">
        <CardTitle className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
          {title}
        </CardTitle>
        <div className={cn("p-1.5 rounded-md bg-muted/30", accentClass)}>
          <Icon className="w-4 h-4" />
        </div>
      </CardHeader>
      <CardContent>
        <p className={cn("text-2xl font-bold tabular-nums", accentClass)}>{value}</p>
        {sub && <p className="text-xs text-muted-foreground mt-1">{sub}</p>}
      </CardContent>
    </Card>
  )
}

// ── Main Component ────────────────────────────────────────────

export function LivePositions() {
  const [traders, setTraders] = useState<Trader[]>([])
  const [selectedTraderId, setSelectedTraderId] = useState<number | null>(null)
  const [data, setData] = useState<LivePositionsResponse | null>(null)
  const [executions, setExecutions] = useState<TraderExecution[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [lastRefresh, setLastRefresh] = useState<Date | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>("market_value")
  const [sortDir, setSortDir] = useState<SortDir>("desc")

  // Load traders on mount
  useEffect(() => {
    getTraders()
      .then((ts) => {
        setTraders(ts)
        if (ts.length > 0) setSelectedTraderId(ts[0].id)
      })
      .catch(() => {})
  }, [])

  const refresh = useCallback(async () => {
    if (!selectedTraderId) return
    setLoading(true)
    setError(null)
    try {
      const [posRes, execRes] = await Promise.all([
        fetchLivePositions(selectedTraderId),
        fetchTraderExecutions(selectedTraderId),
      ])
      setData(posRes)
      setExecutions(execRes)
      setLastRefresh(new Date())
    } catch (e: any) {
      setError(e.message ?? "Failed to load positions")
    } finally {
      setLoading(false)
    }
  }, [selectedTraderId])

  // Fetch positions whenever trader changes
  useEffect(() => {
    refresh()
  }, [refresh])

  // Auto-refresh every 30 s
  useEffect(() => {
    const timer = setInterval(refresh, 30_000)
    return () => clearInterval(timer)
  }, [refresh])

  // Sorting
  const handleSort = (key: SortKey) => {
    if (key === sortKey) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"))
    } else {
      setSortKey(key)
      setSortDir("desc")
    }
  }

  const sortedPositions: LivePosition[] = data
    ? [...data.positions].sort((a, b) => {
        const va = a[sortKey] as number
        const vb = b[sortKey] as number
        if (typeof va === "string") return 0
        return sortDir === "asc" ? va - vb : vb - va
      })
    : []

  const totalPnlPositive = data ? data.total_unrealized_pnl >= 0 : true

  const SortIcon = ({ k }: { k: SortKey }) => {
    if (sortKey !== k) return null
    return sortDir === "asc" ? (
      <ChevronUp className="inline w-3 h-3 ml-0.5 text-primary" />
    ) : (
      <ChevronDown className="inline w-3 h-3 ml-0.5 text-primary" />
    )
  }

  return (
    <div className="space-y-6 h-full">
      {/* ── Header bar ── */}
      <div className="flex items-center justify-between gap-4">
        <div>
          <h2 className="text-lg font-semibold text-foreground flex items-center gap-2">
            <Briefcase className="w-5 h-5 text-primary" />
            Live Positions
          </h2>
          <p className="text-xs text-muted-foreground mt-0.5">
            Real-time PnL reconstructed from paper execution ledger
          </p>
        </div>

        <div className="flex items-center gap-3">
          {/* Trader selector */}
          <Select
            value={selectedTraderId?.toString() ?? ""}
            onValueChange={(v) => setSelectedTraderId(Number(v))}
          >
            <SelectTrigger
              id="live-positions-trader-select"
              className="w-48 h-9 text-sm bg-secondary/40 border-border/50"
            >
              <SelectValue placeholder="Select trader…" />
            </SelectTrigger>
            <SelectContent>
              {traders.map((t) => (
                <SelectItem key={t.id} value={t.id.toString()}>
                  {t.name}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          {/* Refresh */}
          <Button
            id="live-positions-refresh-btn"
            variant="outline"
            size="sm"
            className="h-9 gap-2 border-border/50"
            onClick={refresh}
            disabled={loading}
          >
            <RefreshCw className={cn("w-3.5 h-3.5", loading && "animate-spin")} />
            Refresh
          </Button>

          {/* Execution History Sheet */}
          <Sheet>
            <SheetTrigger asChild>
              <Button
                variant="outline"
                size="sm"
                className="h-9 gap-2 border-border/50"
                disabled={!selectedTraderId}
              >
                <History className="w-3.5 h-3.5" />
                History
              </Button>
            </SheetTrigger>
            <SheetContent className="w-[400px] sm:w-[540px] border-border/50 p-0 flex flex-col bg-card/95 backdrop-blur-md">
              <SheetHeader className="p-6 pb-4 border-b border-border/50">
                <SheetTitle className="text-lg flex items-center gap-2">
                  <History className="w-5 h-5 text-primary" />
                  Execution History
                </SheetTitle>
                <p className="text-xs text-muted-foreground mt-1">
                  Recent paper trading executions for {data?.trader_name ?? "this trader"}.
                </p>
              </SheetHeader>
              <ScrollArea className="flex-1">
                {executions.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-20 text-muted-foreground gap-3">
                    <History className="w-10 h-10 opacity-30" />
                    <p className="text-sm">No recent executions found.</p>
                  </div>
                ) : (
                  <div className="divide-y divide-border/30">
                    {executions.map((exec) => {
                      const isBuy = exec.action === "BUY"
                      return (
                        <div key={exec.id} className="p-4 hover:bg-muted/10 transition-colors">
                          <div className="flex justify-between items-start mb-2">
                            <div className="flex items-center gap-2">
                              <Badge
                                variant="outline"
                                className={cn(
                                  "text-[10px] font-bold tracking-wider",
                                  isBuy ? "text-emerald-400 border-emerald-500/30 bg-emerald-500/10" : "text-rose-400 border-rose-500/30 bg-rose-500/10"
                                )}
                              >
                                {exec.action}
                              </Badge>
                              <span className="font-mono font-bold text-sm text-foreground">
                                {exec.ticker}
                              </span>
                            </div>
                            <span className="text-xs text-muted-foreground font-mono">
                              {fmtDateTime(exec.timestamp)}
                            </span>
                          </div>
                          <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
                            <div className="text-muted-foreground text-xs">Quantity</div>
                            <div className="text-right tabular-nums font-medium">{exec.quantity.toLocaleString()}</div>
                            
                            <div className="text-muted-foreground text-xs">Simulated Price</div>
                            <div className="text-right tabular-nums">${fmt(exec.simulated_price, 2)}</div>
                            
                            <div className="text-muted-foreground text-xs">Total Value</div>
                            <div className="text-right tabular-nums text-foreground font-medium">{fmtUsd(exec.quantity * exec.simulated_price)}</div>
                            
                            {exec.portfolio_name && (
                              <>
                                <div className="text-muted-foreground text-[10px] mt-2">Portfolio</div>
                                <div className="text-[11px] font-medium text-primary mt-2">{exec.portfolio_name}</div>
                              </>
                            )}
                            {exec.strategy_id && (
                              <>
                                <div className="text-muted-foreground text-[10px] mt-2">Strategy</div>
                                <div className="text-[11px] font-medium font-mono text-primary mt-2">{exec.strategy_id}</div>
                              </>
                            )}
                          </div>
                        </div>
                      )
                    })}
                  </div>
                )}
              </ScrollArea>
            </SheetContent>
          </Sheet>
        </div>
      </div>

      {/* ── Last refresh timestamp ── */}
      {lastRefresh && (
        <p className="text-[10px] text-muted-foreground -mt-4">
          Last updated: {lastRefresh.toLocaleTimeString()} · Auto-refreshes every 30 s
        </p>
      )}

      {/* ── Error banner ── */}
      {error && (
        <div className="flex items-center gap-2 p-3 rounded-lg bg-rose-500/10 border border-rose-500/30 text-rose-400 text-sm">
          <AlertCircle className="w-4 h-4 shrink-0" />
          {error}
        </div>
      )}

      {/* ── Loading skeleton or no-data ── */}
      {!data && !error && !loading && (
        <div className="flex flex-col items-center justify-center py-24 text-muted-foreground gap-3">
          <BarChart3 className="w-10 h-10 opacity-30" />
          <p className="text-sm">Select a trader to view live positions</p>
        </div>
      )}

      {loading && !data && (
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          {[1, 2, 3].map((i) => (
            <Card key={i} className="bg-card/60 border-border/50 h-28 animate-pulse" />
          ))}
        </div>
      )}

      {data && (
        <>
          {/* ── Summary cards ── */}
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
            <SummaryCard
              title="Total Equity"
              value={fmtUsd(data.total_equity)}
              sub={`Invested: ${fmtUsd(data.total_invested)} · Cash: ${fmtUsd(data.total_cash)}`}
              icon={DollarSign}
              accent="neutral"
            />
            <SummaryCard
              title="Cash Balance"
              value={fmtUsd(data.total_cash)}
              sub={`${((data.total_cash / data.total_equity) * 100).toFixed(1)}% of portfolio`}
              icon={Wallet}
              accent="neutral"
            />
            <SummaryCard
              title="Total Unrealized PnL"
              value={fmtUsd(data.total_unrealized_pnl)}
              sub={
                data.total_invested > 0
                  ? `${fmtPct((data.total_unrealized_pnl / (data.total_invested - data.total_unrealized_pnl)) * 100)} on cost`
                  : "No open positions"
              }
              icon={totalPnlPositive ? TrendingUp : TrendingDown}
              accent={totalPnlPositive ? "positive" : "negative"}
            />
          </div>

          {/* ── Positions table ── */}
          {sortedPositions.length === 0 ? (
            <Card className="bg-card/60 border-border/50">
              <CardContent className="flex flex-col items-center justify-center py-16 text-muted-foreground gap-3">
                <Briefcase className="w-10 h-10 opacity-30" />
                <p className="text-sm">No open positions for {data.trader_name}</p>
                <p className="text-xs mb-4">
                  Run the pipeline and rebalancer to generate paper trades.
                </p>
                <Button 
                  onClick={async () => {
                    setLoading(true)
                    try {
                      await runPipelineRebalance()
                      // Poll status every second until finished
                      const timer = setInterval(async () => {
                        const status = await getPipelineStatus()
                        if (!status.running) {
                          clearInterval(timer)
                          refresh()
                        }
                      }, 1000)
                    } catch (e) {
                      setLoading(false)
                    }
                  }}
                  disabled={loading}
                  variant="outline"
                  className="bg-primary/10 border-primary/20 hover:bg-primary/20 text-primary"
                >
                  <RefreshCw className={cn("w-4 h-4 mr-2", loading && "animate-spin")} />
                  Force Rebalancer
                </Button>
              </CardContent>
            </Card>
          ) : (
            <Card className="bg-card/60 border-border/50 backdrop-blur-sm overflow-hidden">
              <CardHeader className="pb-3">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-sm font-semibold text-foreground">
                    Open Positions — {data.trader_name}
                  </CardTitle>
                  <Badge variant="outline" className="text-[10px] border-border/50 text-muted-foreground">
                    {sortedPositions.length} position{sortedPositions.length !== 1 ? "s" : ""}
                  </Badge>
                </div>
              </CardHeader>
              <CardContent className="p-0">
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="border-border/50 hover:bg-transparent">
                        {(
                          [
                            { key: "ticker" as SortKey, label: "Ticker" },
                            { key: "shares" as SortKey, label: "Shares" },
                            { key: "avg_entry" as SortKey, label: "Avg Entry" },
                            { key: "current_price" as SortKey, label: "Current Price" },
                            { key: "market_value" as SortKey, label: "Market Value" },
                            { key: "unrealized_pnl_usd" as SortKey, label: "Unr. PnL ($)" },
                            { key: "unrealized_pnl_pct" as SortKey, label: "Unr. PnL (%)" },
                          ] as { key: SortKey; label: string }[]
                        ).map(({ key, label }) => (
                          <TableHead
                            key={key}
                            className={cn(
                              "text-[11px] font-semibold text-muted-foreground uppercase tracking-wider cursor-pointer select-none whitespace-nowrap",
                              key !== "ticker" && "text-right"
                            )}
                            onClick={() => handleSort(key)}
                          >
                            {label}
                            <SortIcon k={key} />
                          </TableHead>
                        ))}
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {sortedPositions.map((pos) => {
                        const isUp = pos.unrealized_pnl_usd >= 0
                        return (
                          <TableRow
                            key={pos.ticker}
                            className="border-border/30 hover:bg-muted/20 transition-colors"
                          >
                            {/* Ticker */}
                            <TableCell className="py-3 pl-6">
                              <div className="flex flex-col">
                                <span className="font-mono font-semibold text-sm text-foreground">
                                  {pos.ticker}
                                </span>
                                {pos.strategies && pos.strategies.length > 0 && (
                                  <span className="text-[10px] text-muted-foreground/80 mt-0.5 truncate max-w-[120px]" title={pos.strategies.join(", ")}>
                                    {pos.strategies.join(", ")}
                                  </span>
                                )}
                              </div>
                            </TableCell>

                            {/* Shares */}
                            <TableCell className="text-right text-sm tabular-nums text-muted-foreground">
                              {pos.shares.toLocaleString()}
                            </TableCell>

                            {/* Avg Entry */}
                            <TableCell className="text-right text-sm tabular-nums text-muted-foreground">
                              ${fmt(pos.avg_entry, 2)}
                            </TableCell>

                            {/* Current Price */}
                            <TableCell className="text-right text-sm tabular-nums text-foreground">
                              ${fmt(pos.current_price, 2)}
                            </TableCell>

                            {/* Market Value */}
                            <TableCell className="text-right text-sm tabular-nums font-medium text-foreground">
                              {fmtUsd(pos.market_value)}
                            </TableCell>

                            {/* Unr. PnL $ */}
                            <TableCell
                              className={cn(
                                "text-right text-sm tabular-nums font-semibold",
                                isUp ? "text-emerald-400" : "text-rose-400"
                              )}
                            >
                              <span className="flex items-center justify-end gap-1">
                                {isUp ? (
                                  <ChevronUp className="w-3.5 h-3.5" />
                                ) : (
                                  <ChevronDown className="w-3.5 h-3.5" />
                                )}
                                {fmtUsd(pos.unrealized_pnl_usd)}
                              </span>
                            </TableCell>

                            {/* Unr. PnL % */}
                            <TableCell
                              className={cn(
                                "text-right text-sm tabular-nums font-semibold pr-6",
                                isUp ? "text-emerald-400" : "text-rose-400"
                              )}
                            >
                              <Badge
                                variant="outline"
                                className={cn(
                                  "text-[10px] font-semibold border",
                                  isUp
                                    ? "border-emerald-500/40 text-emerald-400 bg-emerald-500/10"
                                    : "border-rose-500/40 text-rose-400 bg-rose-500/10"
                                )}
                              >
                                {fmtPct(pos.unrealized_pnl_pct)}
                              </Badge>
                            </TableCell>
                          </TableRow>
                        )
                      })}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          )}
        </>
      )}
    </div>
  )
}
