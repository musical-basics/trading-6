"use client"

import { useState, useMemo, useEffect, useCallback, useRef } from "react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Area,
  AreaChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts"
import { Play, TrendingUp, TrendingDown, Loader2, AlertCircle } from "lucide-react"
import {
  fetchStrategies,
  runTournament,
  type Strategy,
  type TournamentResponse,
} from "@/lib/api"

// ── Constants ───────────────────────────────────────────────

const PERIODS = [
  { label: "1M", days: 30 },
  { label: "3M", days: 90 },
  { label: "6M", days: 180 },
  { label: "1Y", days: 365 },
  { label: "3Y", days: 1095 },
  { label: "5Y", days: 1825 },
] as const

/** Dynamic color palette for strategies */
const PALETTE = [
  "#06b6d4", "#10b981", "#f59e0b", "#ec4899", "#8b5cf6",
  "#3b82f6", "#e879f9", "#14b8a6", "#f97316", "#6366f1",
  "#84cc16", "#ef4444",
]

/** Category badges based on strategy ID patterns */
function getCategoryForId(id: string): string {
  if (["ev_sales", "ls_zscore", "sma_crossover", "pullback_rsi"].includes(id)) return "Heuristic"
  if (["xgboost"].includes(id)) return "ML"
  if (["momentum", "low_beta", "dcf_value", "fortress", "buy_hold"].includes(id)) return "Factor"
  if (["macro_regime", "macro_v2"].includes(id)) return "Macro"
  return "Other"
}

// ── Component ───────────────────────────────────────────────

export function StrategyStudio() {
  const formatPct = (value: number | null | undefined, digits = 1): string => {
    return Number.isFinite(value) ? `${(value as number).toFixed(digits)}%` : "--"
  }

  const formatNum = (value: number | null | undefined, digits = 2): string => {
    return Number.isFinite(value) ? (value as number).toFixed(digits) : "--"
  }

  // Strategy list from API
  const [strategies, setStrategies] = useState<Strategy[]>([])
  const [loadingStrategies, setLoadingStrategies] = useState(true)

  // Selection
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())

  // Tournament results
  const [tournamentData, setTournamentData] = useState<TournamentResponse | null>(null)
  const [isRunning, setIsRunning] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Period
  const [selectedPeriod, setSelectedPeriod] = useState<string>("1Y")

  // ── Load strategy list on mount ─────────────────────────
  useEffect(() => {
    fetchStrategies()
      .then((strats) => {
        setStrategies(strats)
        // Select first 4 by default
        setSelectedIds(new Set(strats.slice(0, 4).map((s) => s.id)))
        setLoadingStrategies(false)
      })
      .catch((err) => {
        console.error("Failed to load strategies:", err)
        setError("Could not connect to backend API")
        setLoadingStrategies(false)
      })
  }, [])

  // ── Color mapping ─────────────────────────────────────────
  const colorMap = useMemo(() => {
    const map: Record<string, string> = { SPY: "#6b7280" }
    strategies.forEach((s, i) => {
      map[s.name] = PALETTE[i % PALETTE.length]
    })
    return map
  }, [strategies])

  // ── Toggle strategy selection ─────────────────────────────
  const toggleStrategy = useCallback((id: string) => {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }, [])

  // ── Run backtest ──────────────────────────────────────────
  const hasRun = useRef(false)

  const handleRunBacktest = useCallback(async (periodOverride?: string) => {
    if (selectedIds.size === 0) return
    setIsRunning(true)
    setError(null)

    const activePeriod = periodOverride ?? selectedPeriod
    const period = PERIODS.find((p) => p.label === activePeriod) ?? PERIODS[3]
    const endDate = new Date()
    const startDate = new Date()
    startDate.setDate(startDate.getDate() - period.days)

    try {
      const result = await runTournament({
        strategies: Array.from(selectedIds),
        startDate: startDate.toISOString().split("T")[0],
        endDate: endDate.toISOString().split("T")[0],
      })
      setTournamentData(result)
      hasRun.current = true
    } catch (err) {
      console.error("Tournament failed:", err)
      setError(err instanceof Error ? err.message : "Backtest failed")
    } finally {
      setIsRunning(false)
    }
  }, [selectedIds, selectedPeriod])

  // Auto-rerun when period changes (only after first manual run)
  useEffect(() => {
    if (hasRun.current && !isRunning) {
      handleRunBacktest(selectedPeriod)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedPeriod])

  // ── Build chart data from tournament results ──────────────
  const chartData = useMemo(() => {
    if (!tournamentData) return []

    // Collect all dates from benchmark + strategies
    const dateSet = new Set<string>()
    if (tournamentData.benchmark) {
      tournamentData.benchmark.equity_curve.forEach((p) => dateSet.add(p.date))
    }
    Object.values(tournamentData.strategies).forEach((s) => {
      s.equity_curve.forEach((p) => dateSet.add(p.date))
    })

    const sortedDates = Array.from(dateSet).sort()

    // Build lookup maps for O(1) access
    const spyMap = new Map<string, number>()
    if (tournamentData.benchmark) {
      tournamentData.benchmark.equity_curve.forEach((p) => {
        spyMap.set(p.date, p.value)
      })
    }

    const stratMaps: Record<string, Map<string, number>> = {}
    for (const [, result] of Object.entries(tournamentData.strategies)) {
      stratMaps[result.name] = new Map()
      result.equity_curve.forEach((p) => {
        stratMaps[result.name].set(p.date, p.value)
      })
    }

    return sortedDates.map((date) => {
      const point: Record<string, string | number | undefined> = { date }
      let hasAny = false
      if (spyMap.size > 0) {
        const v = spyMap.get(date)
        if (v !== undefined) { point["SPY"] = v; hasAny = true }
      }
      for (const [name, map] of Object.entries(stratMaps)) {
        const v = map.get(date)
        if (v !== undefined) { point[name] = v; hasAny = true }
      }
      return hasAny ? point : null
    }).filter(Boolean)
  }, [tournamentData])

  // ── Metrics from tournament results ───────────────────────
  const metricsRows = useMemo(() => {
    if (!tournamentData) return []
    return Object.values(tournamentData.strategies).map((s) => ({
      name: s.name,
      totalReturn: s.metrics.total_return * 100,
      cagr: s.metrics.cagr * 100,
      sharpe: s.metrics.sharpe,
      sortino: s.metrics.sortino,
      calmar: s.metrics.calmar,
      maxDrawdown: s.metrics.max_drawdown * 100,
      volatility: s.metrics.volatility * 100,
      winRate: s.metrics.win_rate * 100,
      profitFactor: s.metrics.profit_factor,
    }))
  }, [tournamentData])

  // ── Active strategy names for chart lines ─────────────────
  const activeNames = useMemo(() => {
    if (!tournamentData) return []
    return Object.values(tournamentData.strategies).map((s) => s.name)
  }, [tournamentData])

  // ── Render ────────────────────────────────────────────────
  return (
    <div className="flex flex-col lg:flex-row gap-4 h-full">
      {/* Left Panel - Strategy List */}
      <Card className="lg:w-72 shrink-0 border-border/50 bg-card/50">
        <CardHeader className="pb-3">
          <CardTitle className="text-sm font-medium text-foreground">Strategies</CardTitle>
          <CardDescription className="text-xs text-muted-foreground">
            {loadingStrategies
              ? "Loading..."
              : `${selectedIds.size} of ${strategies.length} selected`}
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-1">
          {loadingStrategies ? (
            <div className="flex items-center justify-center py-8 text-muted-foreground">
              <Loader2 className="w-4 h-4 animate-spin mr-2" />
              <span className="text-sm">Loading strategies...</span>
            </div>
          ) : (
            strategies.map((strategy) => {
              const category = getCategoryForId(strategy.id)
              return (
                <div
                  key={strategy.id}
                  className="flex items-center gap-3 py-2 px-2 rounded-md hover:bg-accent/50 transition-colors cursor-pointer"
                  onClick={() => toggleStrategy(strategy.id)}
                >
                  <Checkbox
                    checked={selectedIds.has(strategy.id)}
                    onCheckedChange={() => toggleStrategy(strategy.id)}
                    className="data-[state=checked]:bg-primary data-[state=checked]:border-primary"
                  />
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-foreground truncate">
                      {strategy.name}
                    </p>
                    <p className="text-xs text-muted-foreground">{category}</p>
                  </div>
                  <Badge
                    variant="outline"
                    className="text-[10px] px-1.5 py-0 h-5 border-border/50 text-muted-foreground"
                  >
                    {category.slice(0, 3)}
                  </Badge>
                </div>
              )
            })
          )}
          <Button
            className="w-full mt-4 bg-primary text-primary-foreground hover:bg-primary/90"
            onClick={() => handleRunBacktest()}
            disabled={isRunning || selectedIds.size === 0 || loadingStrategies}
          >
            {isRunning ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Running...
              </>
            ) : (
              <>
                <Play className="w-4 h-4 mr-2" />
                Run Backtest
              </>
            )}
          </Button>
        </CardContent>
      </Card>

      {/* Main Content */}
      <div className="flex-1 flex flex-col gap-4 min-w-0">
        {/* Error Banner */}
        {error && (
          <Card className="border-red-500/50 bg-red-500/10">
            <CardContent className="py-3 flex items-center gap-2">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0" />
              <p className="text-sm text-red-400">{error}</p>
            </CardContent>
          </Card>
        )}

        {/* Equity Curve Chart */}
        <Card className="flex-1 border-border/50 bg-card/50">
          <CardHeader className="pb-2">
            <div className="flex items-center justify-between">
              <div>
                <CardTitle className="text-sm font-medium text-foreground">
                  Equity Curves
                </CardTitle>
                <CardDescription className="text-xs text-muted-foreground">
                  Strategy performance vs SPY benchmark (dashed)
                </CardDescription>
              </div>
              <div className="flex gap-1">
                {PERIODS.map((period) => (
                  <Button
                    key={period.label}
                    variant={selectedPeriod === period.label ? "default" : "outline"}
                    size="sm"
                    className={`h-7 px-2.5 text-xs ${
                      selectedPeriod === period.label
                        ? "bg-primary text-primary-foreground"
                        : "border-border/50 text-muted-foreground hover:text-foreground"
                    }`}
                    onClick={() => setSelectedPeriod(period.label)}
                  >
                    {period.label}
                  </Button>
                ))}
              </div>
            </div>
          </CardHeader>
          <CardContent className="h-[300px] lg:h-[400px]">
            {chartData.length === 0 ? (
              <div className="flex items-center justify-center h-full text-muted-foreground">
                <p className="text-sm">
                  {isRunning
                    ? "Running backtest..."
                    : "Select strategies and click Run Backtest"}
                </p>
              </div>
            ) : (
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart
                  data={chartData}
                  margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
                >
                  <defs>
                    {activeNames.map((name) => (
                      <linearGradient
                        key={name}
                        id={`gradient-${name.replace(/[^a-zA-Z0-9]/g, "_")}`}
                        x1="0"
                        y1="0"
                        x2="0"
                        y2="1"
                      >
                        <stop
                          offset="5%"
                          stopColor={colorMap[name] ?? "#888"}
                          stopOpacity={0.3}
                        />
                        <stop
                          offset="95%"
                          stopColor={colorMap[name] ?? "#888"}
                          stopOpacity={0}
                        />
                      </linearGradient>
                    ))}
                  </defs>
                  <CartesianGrid
                    strokeDasharray="3 3"
                    stroke="rgba(255,255,255,0.05)"
                  />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: "#6b7280", fontSize: 10 }}
                    tickFormatter={(value) => {
                      const d = new Date(value)
                      return `${d.toLocaleDateString("en-US", { month: "short" })} '${String(d.getFullYear()).slice(2)}`
                    }}
                    tickLine={false}
                    axisLine={false}
                  />
                  <YAxis
                    tick={{ fill: "#6b7280", fontSize: 10 }}
                    tickFormatter={(value) => `$${value.toLocaleString()}`}
                    tickLine={false}
                    axisLine={false}
                    domain={["dataMin - 500", "dataMax + 500"]}
                  />
                  <Tooltip
                    contentStyle={{
                      backgroundColor: "rgba(15, 23, 42, 0.95)",
                      border: "1px solid rgba(255,255,255,0.1)",
                      borderRadius: "8px",
                      fontSize: "12px",
                    }}
                    labelFormatter={(value) =>
                      new Date(value).toLocaleDateString()
                    }
                    formatter={(value: number, name: string) => [
                      `$${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`,
                      name,
                    ]}
                  />
                  <Legend
                    wrapperStyle={{ fontSize: "11px", paddingTop: "10px" }}
                  />
                  {/* SPY Benchmark - dashed */}
                  {"SPY" in (chartData[0] ?? {}) && (
                    <Area
                      type="monotone"
                      dataKey="SPY"
                      stroke={colorMap["SPY"]}
                      strokeWidth={2}
                      strokeDasharray="5 5"
                      fill="none"
                      connectNulls
                    />
                  )}
                  {/* Strategy lines */}
                  {activeNames.map((name) => (
                    <Area
                      key={name}
                      type="monotone"
                      dataKey={name}
                      stroke={colorMap[name] ?? "#888"}
                      strokeWidth={2}
                      fill={`url(#gradient-${name.replace(/[^a-zA-Z0-9]/g, "_")})`}
                      connectNulls
                    />
                  ))}
                </AreaChart>
              </ResponsiveContainer>
            )}
          </CardContent>
        </Card>

        {/* Metrics Table */}
        <Card className="border-border/50 bg-card/50">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-foreground">
              Strategy Metrics (Comprehensive)
            </CardTitle>
            <CardDescription className="text-xs text-muted-foreground">
              Scroll horizontally for all metrics
            </CardDescription>
          </CardHeader>
          <CardContent>
            {metricsRows.length === 0 ? (
              <p className="text-sm text-muted-foreground text-center py-4">
                Run a backtest to see strategy metrics
              </p>
            ) : (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow className="border-border/50 hover:bg-transparent">
                      <TableHead className="text-muted-foreground text-xs font-medium">
                        Strategy
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        Return
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        CAGR
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        Vol
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        Sharpe
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        Sortino
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        Calmar
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        MaxDD
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        WinRate
                      </TableHead>
                      <TableHead className="text-muted-foreground text-xs font-medium text-right">
                        ProfitFactor
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {metricsRows.map((metric) => (
                      <TableRow
                        key={metric.name}
                        className="border-border/50 hover:bg-accent/30"
                      >
                        <TableCell className="font-medium text-sm text-foreground whitespace-nowrap">
                          <div className="flex items-center gap-2">
                            <div
                              className="w-2 h-2 rounded-full"
                              style={{
                                backgroundColor: colorMap[metric.name] ?? "#888",
                              }}
                            />
                            {metric.name}
                          </div>
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs whitespace-nowrap">
                          <span
                            className={
                              Number.isFinite(metric.totalReturn) && metric.totalReturn >= 0
                                ? "text-green-400"
                                : "text-red-400"
                            }
                          >
                            {Number.isFinite(metric.totalReturn) && metric.totalReturn >= 0 ? (
                              <TrendingUp className="w-3 h-3 inline mr-1" />
                            ) : (
                              <TrendingDown className="w-3 h-3 inline mr-1" />
                            )}
                            {formatPct(metric.totalReturn, 1)}
                          </span>
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs text-foreground whitespace-nowrap">
                          {formatPct(metric.cagr, 1)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs text-foreground whitespace-nowrap">
                          {formatPct(metric.volatility, 1)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs text-foreground whitespace-nowrap">
                          {formatNum(metric.sharpe, 2)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs text-foreground whitespace-nowrap">
                          {formatNum(metric.sortino, 2)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs text-foreground whitespace-nowrap">
                          {formatNum(metric.calmar, 2)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs whitespace-nowrap">
                          <span className="text-red-400">
                            <TrendingDown className="w-3 h-3 inline mr-1" />
                            {formatPct(metric.maxDrawdown, 1)}
                          </span>
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs text-foreground whitespace-nowrap">
                          {formatPct(metric.winRate, 1)}
                        </TableCell>
                        <TableCell className="text-right font-mono text-xs text-foreground whitespace-nowrap">
                          {formatNum(metric.profitFactor, 2)}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
