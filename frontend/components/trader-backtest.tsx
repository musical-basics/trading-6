"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from "recharts"
import {
  Play,
  TrendingUp,
  TrendingDown,
  Loader2,
  BarChart3,
} from "lucide-react"
import {
  runTraderBacktest,
  type TraderBacktestResult,
  type Trader,
} from "@/lib/api"

// Colors per portfolio line
const LINE_COLORS = [
  "#06b6d4", "#f59e0b", "#10b981", "#8b5cf6", "#ef4444",
  "#ec4899", "#14b8a6", "#f97316", "#6366f1", "#84cc16",
  "#3b82f6", "#e11d48",
]

interface TraderBacktestProps {
  trader: Trader
}

export function TraderBacktest({ trader }: TraderBacktestProps) {
  const [result, setResult] = useState<TraderBacktestResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleRun = async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await runTraderBacktest(trader.id)
      setResult(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Backtest failed")
    } finally {
      setLoading(false)
    }
  }

  if (!result) {
    return (
      <Card className="border-dashed">
        <CardContent className="flex flex-col items-center justify-center py-8 gap-3">
          <BarChart3 className="w-10 h-10 text-muted-foreground/30" />
          <p className="text-sm text-muted-foreground">
            Run a backtest to see {trader.name}&apos;s strategy mix performance
          </p>
          {error && (
            <p className="text-xs text-destructive">{error}</p>
          )}
          <Button size="sm" onClick={handleRun} disabled={loading}>
            {loading ? (
              <><Loader2 className="w-4 h-4 mr-1 animate-spin" />Running...</>
            ) : (
              <><Play className="w-4 h-4 mr-1" />Run Backtest</>
            )}
          </Button>
        </CardContent>
      </Card>
    )
  }

  // Merge all curves into a single chart
  const dateMap = new Map<string, Record<string, number>>()

  // Combined curve
  for (const pt of result.combined.equity_curve) {
    if (!dateMap.has(pt.date)) dateMap.set(pt.date, {})
    dateMap.get(pt.date)!.combined = pt.value
  }

  // Per-portfolio curves
  const portfolioEntries = Object.entries(result.portfolios)
  for (const [sid, pdata] of portfolioEntries) {
    for (const pt of pdata.equity_curve) {
      if (!dateMap.has(pt.date)) dateMap.set(pt.date, {})
      dateMap.get(pt.date)![sid] = pt.value
    }
  }

  // Benchmark
  if (result.benchmark) {
    for (const pt of result.benchmark.equity_curve) {
      if (!dateMap.has(pt.date)) dateMap.set(pt.date, {})
      dateMap.get(pt.date)!.benchmark = pt.value
    }
  }

  // Sort by date
  const chartData = Array.from(dateMap.entries())
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, vals]) => ({ date, ...vals }))

  // Sample for display
  const step = Math.max(1, Math.floor(chartData.length / 300))
  const sampledData = chartData.filter((_, i) => i % step === 0 || i === chartData.length - 1)

  const cm = result.combined.metrics
  const bm = result.benchmark?.metrics

  return (
    <div className="space-y-4">
      {/* Header with re-run */}
      <div className="flex items-center justify-between">
        <h3 className="text-sm font-semibold flex items-center gap-2">
          <BarChart3 className="w-4 h-4 text-primary" />
          Backtest: {trader.name}
        </h3>
        <Button size="sm" variant="outline" onClick={handleRun} disabled={loading}>
          {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
        </Button>
      </div>

      {/* Metrics Row */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        <MetricCard label="Total Return" value={`${(cm.total_return * 100).toFixed(1)}%`}
          positive={cm.total_return > 0} />
        <MetricCard label="Sharpe" value={cm.sharpe.toFixed(2)}
          positive={cm.sharpe > 1} />
        <MetricCard label="Max Drawdown" value={`${(cm.max_drawdown * 100).toFixed(1)}%`}
          positive={false} />
        <MetricCard label="CAGR" value={`${(cm.cagr * 100).toFixed(1)}%`}
          positive={cm.cagr > 0} />
      </div>

      {/* Chart */}
      <Card>
        <CardContent className="pt-4">
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={sampledData}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.06)" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 10, fill: "#666" }}
                tickFormatter={(d) => {
                  const dt = new Date(d)
                  return `${dt.getFullYear()}`
                }}
                interval={Math.floor(sampledData.length / 6)}
              />
              <YAxis
                tick={{ fontSize: 10, fill: "#666" }}
                tickFormatter={(v) => `$${(v/1000).toFixed(0)}k`}
              />
              <Tooltip
                contentStyle={{
                  background: "rgba(0,0,0,0.9)",
                  border: "1px solid rgba(255,255,255,0.1)",
                  borderRadius: "8px",
                  fontSize: "11px",
                }}
                formatter={(value: number, name: string) => {
                  const label = name === "combined"
                    ? "Combined"
                    : name === "benchmark"
                    ? "SPY"
                    : result.portfolios[name]?.name ?? name
                  return [`$${value.toLocaleString()}`, label]
                }}
                labelFormatter={(d) => new Date(d).toLocaleDateString()}
              />
              <Legend
                formatter={(value: string) => {
                  if (value === "combined") return "Combined"
                  if (value === "benchmark") return "SPY"
                  return result.portfolios[value]?.portfolio_name ?? value
                }}
                wrapperStyle={{ fontSize: "11px" }}
              />

              {/* Combined line (thick, white) */}
              <Line
                type="monotone"
                dataKey="combined"
                stroke="#ffffff"
                strokeWidth={2.5}
                dot={false}
                name="combined"
              />

              {/* Per-portfolio lines */}
              {portfolioEntries.map(([sid], i) => (
                <Line
                  key={sid}
                  type="monotone"
                  dataKey={sid}
                  stroke={LINE_COLORS[i % LINE_COLORS.length]}
                  strokeWidth={1.2}
                  strokeDasharray="4 2"
                  dot={false}
                  name={sid}
                  opacity={0.7}
                />
              ))}

              {/* Benchmark */}
              {result.benchmark && (
                <Line
                  type="monotone"
                  dataKey="benchmark"
                  stroke="#ef4444"
                  strokeWidth={1.5}
                  dot={false}
                  name="benchmark"
                  strokeDasharray="6 3"
                  opacity={0.5}
                />
              )}
            </LineChart>
          </ResponsiveContainer>
        </CardContent>
      </Card>

      {/* Per-Portfolio Table */}
      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Portfolio Breakdown</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border text-muted-foreground">
                  <th className="text-left py-2 pr-4">Portfolio</th>
                  <th className="text-left py-2 pr-4">Strategy</th>
                  <th className="text-right py-2 pr-4">Weight</th>
                  <th className="text-right py-2 pr-4">Return</th>
                  <th className="text-right py-2 pr-4">Sharpe</th>
                  <th className="text-right py-2 pr-4">Max DD</th>
                  <th className="text-right py-2">CAGR</th>
                </tr>
              </thead>
              <tbody>
                {portfolioEntries.map(([sid, pdata], i) => (
                  <tr key={sid} className="border-b border-border/30 hover:bg-card/50">
                    <td className="py-2 pr-4 flex items-center gap-2">
                      <span
                        className="w-2 h-2 rounded-full inline-block"
                        style={{ background: LINE_COLORS[i % LINE_COLORS.length] }}
                      />
                      {pdata.portfolio_name}
                    </td>
                    <td className="py-2 pr-4 text-muted-foreground">{pdata.name}</td>
                    <td className="py-2 pr-4 text-right font-mono">
                      {(pdata.weight * 100).toFixed(0)}%
                    </td>
                    <td className={`py-2 pr-4 text-right font-mono ${pdata.metrics.total_return > 0 ? "text-green-400" : "text-red-400"}`}>
                      {(pdata.metrics.total_return * 100).toFixed(1)}%
                    </td>
                    <td className="py-2 pr-4 text-right font-mono">
                      {pdata.metrics.sharpe.toFixed(2)}
                    </td>
                    <td className="py-2 pr-4 text-right font-mono text-amber-400">
                      {(pdata.metrics.max_drawdown * 100).toFixed(1)}%
                    </td>
                    <td className={`py-2 text-right font-mono ${pdata.metrics.cagr > 0 ? "text-green-400" : "text-red-400"}`}>
                      {(pdata.metrics.cagr * 100).toFixed(1)}%
                    </td>
                  </tr>
                ))}
                {/* Combined row */}
                <tr className="font-semibold border-t border-border">
                  <td className="py-2 pr-4 flex items-center gap-2">
                    <span className="w-2 h-2 rounded-full inline-block bg-white" />
                    Combined
                  </td>
                  <td className="py-2 pr-4 text-muted-foreground">{portfolioEntries.length} strategies</td>
                  <td className="py-2 pr-4 text-right font-mono">100%</td>
                  <td className={`py-2 pr-4 text-right font-mono ${cm.total_return > 0 ? "text-green-400" : "text-red-400"}`}>
                    {(cm.total_return * 100).toFixed(1)}%
                  </td>
                  <td className="py-2 pr-4 text-right font-mono">{cm.sharpe.toFixed(2)}</td>
                  <td className="py-2 pr-4 text-right font-mono text-amber-400">
                    {(cm.max_drawdown * 100).toFixed(1)}%
                  </td>
                  <td className={`py-2 text-right font-mono ${cm.cagr > 0 ? "text-green-400" : "text-red-400"}`}>
                    {(cm.cagr * 100).toFixed(1)}%
                  </td>
                </tr>
                {/* Benchmark row */}
                {bm && (
                  <tr className="text-muted-foreground">
                    <td className="py-2 pr-4 flex items-center gap-2">
                      <span className="w-2 h-2 rounded-full inline-block bg-red-400" />
                      SPY Benchmark
                    </td>
                    <td className="py-2 pr-4">—</td>
                    <td className="py-2 pr-4 text-right font-mono">—</td>
                    <td className={`py-2 pr-4 text-right font-mono ${bm.total_return > 0 ? "text-green-400" : "text-red-400"}`}>
                      {(bm.total_return * 100).toFixed(1)}%
                    </td>
                    <td className="py-2 pr-4 text-right font-mono">{bm.sharpe.toFixed(2)}</td>
                    <td className="py-2 pr-4 text-right font-mono text-amber-400">
                      {(bm.max_drawdown * 100).toFixed(1)}%
                    </td>
                    <td className={`py-2 text-right font-mono ${bm.cagr > 0 ? "text-green-400" : "text-red-400"}`}>
                      {(bm.cagr * 100).toFixed(1)}%
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function MetricCard({ label, value, positive }: { label: string; value: string; positive: boolean }) {
  return (
    <Card className="bg-card/50">
      <CardContent className="p-3">
        <div className="flex items-center justify-between">
          <span className="text-[10px] text-muted-foreground">{label}</span>
          {positive ? (
            <TrendingUp className="w-3 h-3 text-green-400" />
          ) : (
            <TrendingDown className="w-3 h-3 text-red-400" />
          )}
        </div>
        <p className={`text-lg font-mono font-bold ${positive ? "text-green-400" : "text-red-400"}`}>
          {value}
        </p>
      </CardContent>
    </Card>
  )
}
