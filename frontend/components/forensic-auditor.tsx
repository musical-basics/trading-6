"use client"

import { useState, useEffect, useMemo } from "react"
import {
  ShieldAlert,
  ShieldCheck,
  ShieldX,
  AlertTriangle,
  ChevronDown,
  ChevronRight,
  ChevronUp,
  Loader2,
  Search,
  BarChart2,
  Database,
  Code2,
  RefreshCw,
  TrendingUp,
  BrainCircuit,
  Filter,
  Download,
  Layers,
  List,
  ArrowUpDown,
} from "lucide-react"
import { AreaChart, Area, ResponsiveContainer, Tooltip, XAxis } from "recharts"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import { cn } from "@/lib/utils"
import {
  fetchAlphaExperiments,
  fetchAlphaExperiment,
  runForensicAudit,
  fetchExperimentTrades,
  runAlphaBacktest,
  fetchAuditModels,
  type AlphaExperiment,
  type AlphaEquityPoint,
  type AuditReport,
  type TradeLedgerEntry,
  type AuditModel,
} from "@/lib/api"

// ── Helpers ───────────────────────────────────────────────────────────

function statusColor(status?: string) {
  switch (status) {
    case "PASS": return "text-emerald-400 border-emerald-500/40 bg-emerald-500/10"
    case "FAIL": return "text-red-400 border-red-500/40 bg-red-500/10"
    case "WARNING": return "text-amber-400 border-amber-500/40 bg-amber-500/10"
    default: return "text-muted-foreground border-border bg-muted/30"
  }
}

function StatusIcon({ status }: { status?: string }) {
  if (status === "PASS") return <ShieldCheck className="w-5 h-5 text-emerald-400" />
  if (status === "FAIL") return <ShieldX className="w-5 h-5 text-red-400" />
  if (status === "WARNING") return <AlertTriangle className="w-5 h-5 text-amber-400" />
  return <ShieldAlert className="w-5 h-5 text-muted-foreground" />
}

const CATEGORY_META: Record<string, { label: string; icon: React.ElementType; color: string; description: string }> = {
  STRUCTURAL: {
    label: "Structural",
    icon: Database,
    color: "text-red-400",
    description: "Data integrity or P/L math issues across the entire app",
  },
  BACKTEST: {
    label: "Backtest Physics",
    icon: BarChart2,
    color: "text-amber-400",
    description: "Survivorship bias, liquidity hallucination, or frictionless trades",
  },
  STRATEGY: {
    label: "Strategy Code",
    icon: Code2,
    color: "text-purple-400",
    description: "Lookahead bias, earnings leakage, or hallucinated data columns",
  },
  NONE: {
    label: "All Clear",
    icon: ShieldCheck,
    color: "text-emerald-400",
    description: "No issues detected — backtest appears physically and logically sound",
  },
}

// ── Trade Row ─────────────────────────────────────────────────────────

function TradeRow({
  trade,
  flaggedTrades,
}: {
  trade: TradeLedgerEntry
  flaggedTrades: Array<{ ticker: string; date: string; reason: string }>
}) {
  const [expanded, setExpanded] = useState(false)
  const flag = flaggedTrades.find(
    (f) => f.ticker === trade.ticker && f.date === String(trade.date).slice(0, 10)
  )
  const isFlagged = !!flag

  return (
    <>
      <tr
        className={cn(
          "border-b border-border/50 hover:bg-muted/20 cursor-pointer transition-colors",
          isFlagged && "bg-red-500/5 hover:bg-red-500/10"
        )}
        onClick={() => isFlagged && setExpanded(!expanded)}
      >
        <td className="px-4 py-2.5 text-xs text-muted-foreground whitespace-nowrap">
          {String(trade.date).slice(0, 10)}
        </td>
        <td className="px-4 py-2.5 text-xs font-medium">
          {trade.ticker ?? `entity_${trade.entity_id}`}
        </td>
        <td className="px-4 py-2.5">
          <Badge
            variant="outline"
            className={cn(
              "text-[10px] h-5",
              trade.action === "BUY"
                ? "border-emerald-500/40 text-emerald-400"
                : "border-red-500/40 text-red-400"
            )}
          >
            {trade.action}
          </Badge>
        </td>
        <td className="px-4 py-2.5 text-xs font-mono">
          {trade.weight_delta > 0 ? "+" : ""}
          {(trade.weight_delta * 100).toFixed(2)}%
        </td>
        <td className="px-4 py-2.5 text-xs font-mono">
          {trade.adj_close != null ? `$${trade.adj_close.toFixed(2)}` : "—"}
        </td>
        <td className="px-4 py-2.5 text-xs font-mono text-muted-foreground">
          {trade.volume != null ? trade.volume.toLocaleString() : "—"}
        </td>
        {/* P/L column */}
        <td className="px-4 py-2.5 text-xs font-mono">
          {trade.action === "SELL" && trade.pnl_pct != null ? (
            <div className="flex flex-col gap-0.5">
              <span className={cn(
                "font-semibold leading-none",
                trade.pnl_pct >= 0 ? "text-emerald-400" : "text-red-400"
              )}>
                {trade.pnl_pct >= 0 ? "+" : ""}{(trade.pnl_pct * 100).toFixed(2)}%
              </span>
              {trade.pnl_usd != null && (
                <span className={cn(
                  "text-[10px] leading-none",
                  trade.pnl_usd >= 0 ? "text-emerald-400/70" : "text-red-400/70"
                )}>
                  {trade.pnl_usd >= 0 ? "+" : ""}${trade.pnl_usd.toFixed(2)}/sh
                </span>
              )}
            </div>
          ) : trade.action === "BUY" ? (
            <span className="text-[10px] text-muted-foreground/50 italic">open</span>
          ) : (
            <span className="text-muted-foreground">—</span>
          )}
        </td>
        <td className="px-4 py-2.5">
          {isFlagged ? (
            <div className="flex items-center gap-1.5">
              <AlertTriangle className="w-3.5 h-3.5 text-red-400 shrink-0" />
              <span className="text-[10px] text-red-400 font-medium">Flagged</span>
              {expanded ? (
                <ChevronDown className="w-3 h-3 text-red-400 ml-auto" />
              ) : (
                <ChevronRight className="w-3 h-3 text-red-400 ml-auto" />
              )}
            </div>
          ) : (
            <span className="text-[10px] text-emerald-400">Clean</span>
          )}
        </td>
      </tr>
      {isFlagged && expanded && (
        <tr className="bg-red-500/5">
          <td colSpan={8} className="px-6 py-3">
            <div className="flex items-start gap-2 text-xs text-red-300">
              <AlertTriangle className="w-3.5 h-3.5 mt-0.5 shrink-0 text-red-400" />
              <span>{flag?.reason}</span>
            </div>
          </td>
        </tr>
      )}
    </>
  )
}

// ── Main Component ────────────────────────────────────────────────────

export function ForensicAuditor() {
  const [experiments, setExperiments] = useState<AlphaExperiment[]>([])
  const [models, setModels] = useState<AuditModel[]>([])
  const [selectedModel, setSelectedModel] = useState("claude-sonnet-4-6")
  const [selectedId, setSelectedId] = useState<string>("")
  const [auditResult, setAuditResult] = useState<AuditReport | null>(null)
  const [trades, setTrades] = useState<TradeLedgerEntry[]>([])
  const [equityCurve, setEquityCurve] = useState<AlphaEquityPoint[]>([])
  const [isRunning, setIsRunning] = useState(false)
  const [isLoadingTrades, setIsLoadingTrades] = useState(false)
  const [isRerunning, setIsRerunning] = useState(false)
  const [flagFilter, setFlagFilter] = useState<"all" | "flagged" | "clean">("all")
  const [plSort, setPlSort] = useState<"none" | "best" | "worst">("none")
  const [viewMode, setViewMode] = useState<"ledger" | "clusters">("ledger")
  const [errorMsg, setErrorMsg] = useState<string | null>(null)

  // Load passed experiments and models
  useEffect(() => {
    fetchAlphaExperiments()
      .then((exps) => setExperiments(exps.filter((e) => e.status === "passed")))
      .catch(() => setExperiments([]))

    fetchAuditModels()
      .then((res) => {
        if (res.models && res.models.length > 0) {
          setModels(res.models)
          // Default to the first (usually smartest) available
          setSelectedModel(res.models[0].id)
        }
      })
      .catch(() => setModels([]))
  }, [])

  // When experiment changes, load trades and any cached audit
  useEffect(() => {
    if (!selectedId) return
    setAuditResult(null)
    setErrorMsg(null)

    // Load existing audit verdict from experiment metadata
    const exp = experiments.find((e) => e.experiment_id === selectedId)
    if (exp && (exp as any).audit_report_json) {
      try {
        setAuditResult(JSON.parse((exp as any).audit_report_json))
      } catch { /* ignore */ }
    }

    // Load trade ledger
    setIsLoadingTrades(true)
    fetchExperimentTrades(selectedId)
      .then((r) => setTrades(r.trades ?? []))
      .catch(() => setTrades([]))
      .finally(() => setIsLoadingTrades(false))

    // Load equity curve for sparkline
    setEquityCurve([])
    fetchAlphaExperiment(selectedId)
      .then((exp) => {
        if (exp?.equity_curve) setEquityCurve(exp.equity_curve)
      })
      .catch(() => {})
  }, [selectedId, experiments])

  const handleRunAudit = async () => {
    if (!selectedId) return
    setIsRunning(true)
    setErrorMsg(null)
    setAuditResult(null)
    try {
      const result = await runForensicAudit(selectedId, selectedModel)
      if (result.error) {
        setErrorMsg(result.error)
      } else {
        setAuditResult(result)
      }
    } catch (e: any) {
      setErrorMsg(e.message ?? "Audit failed")
    } finally {
      setIsRunning(false)
    }
  }

  const handleRerunBacktest = async () => {
    if (!selectedId) return
    setIsRerunning(true)
    setErrorMsg(null)
    try {
      await runAlphaBacktest(selectedId)
      // After re-run, reload trade ledger
      const r = await fetchExperimentTrades(selectedId)
      setTrades(r.trades ?? [])
    } catch (e: any) {
      setErrorMsg(e.message ?? "Re-run failed")
    } finally {
      setIsRerunning(false)
    }
  }

  const selectedExp = experiments.find((e) => e.experiment_id === selectedId)
  const categoryMeta = auditResult ? CATEGORY_META[auditResult.error_category] ?? CATEGORY_META["NONE"] : null
  const CategoryIcon = categoryMeta?.icon ?? ShieldAlert

  const flaggedTrades = auditResult?.flagged_trades ?? []
  const filteredTrades = useMemo(() => {
    let result = trades.filter((trade) => {
      if (flagFilter === "all") return true
      const isFlagged = !!flaggedTrades.find(
        (f) => f.ticker === trade.ticker && f.date === String(trade.date).slice(0, 10)
      )
      if (flagFilter === "flagged") return isFlagged
      if (flagFilter === "clean") return !isFlagged
      return true
    })
    if (plSort === "best") {
      result = [...result].sort((a, b) => {
        const ap = a.pnl_pct ?? -Infinity
        const bp = b.pnl_pct ?? -Infinity
        return bp - ap
      })
    } else if (plSort === "worst") {
      result = [...result].sort((a, b) => {
        const ap = a.pnl_pct ?? Infinity
        const bp = b.pnl_pct ?? Infinity
        return ap - bp
      })
    }
    return result
  }, [trades, flagFilter, flaggedTrades, plSort])

  const positionClusters = useMemo(() => {
    const clusters: Record<string, {
      ticker: string
      trades: TradeLedgerEntry[]
      totalRealizedPct: number
      totalRealizedUsd: number
      sellCount: number
    }> = {}
    
    trades.forEach(trade => {
      const ticker = trade.ticker ?? `entity_${trade.entity_id}`
      if (!clusters[ticker]) {
        clusters[ticker] = { ticker, trades: [], totalRealizedPct: 0, totalRealizedUsd: 0, sellCount: 0 }
      }
      clusters[ticker].trades.push(trade)
      if (trade.action === "SELL" && trade.pnl_pct != null) {
        clusters[ticker].totalRealizedPct += trade.pnl_pct
        clusters[ticker].totalRealizedUsd += (trade.pnl_usd ?? 0)
        clusters[ticker].sellCount += 1
      }
    })

    return Object.values(clusters).sort((a, b) => b.totalRealizedPct - a.totalRealizedPct)
  }, [trades])

  const summaryStats = useMemo(() => {
    let wins = 0
    let totalSells = 0
    let totalPct = 0
    let totalUsd = 0
    
    trades.forEach(t => {
      if (t.action === "SELL" && t.pnl_pct != null) {
        totalSells++
        totalPct += t.pnl_pct
        totalUsd += (t.pnl_usd ?? 0)
        if (t.pnl_pct > 0) wins++
      }
    })
    
    return {
      wins,
      totalSells,
      winRate: totalSells > 0 ? wins / totalSells : 0,
      avgPct: totalSells > 0 ? totalPct / totalSells : 0,
      totalUsd
    }
  }, [trades])

  const handleExportCsv = () => {
    if (filteredTrades.length === 0) return
    const headers = ["Date", "Ticker", "Action", "Weight Delta (%)", "Price ($)", "Volume", "P/L (%)", "P/L ($)"]
    const rows = filteredTrades.map(t => [
      String(t.date).slice(0, 10),
      t.ticker ?? `entity_${t.entity_id}`,
      t.action,
      (t.weight_delta * 100).toFixed(2),
      t.adj_close?.toFixed(2) ?? "",
      t.volume ?? "",
      t.pnl_pct != null ? (t.pnl_pct * 100).toFixed(2) : "",
      t.pnl_usd != null ? t.pnl_usd.toFixed(2) : "",
    ])
    
    const csvContent = "data:text/csv;charset=utf-8," + 
      [headers.join(","), ...rows.map(e => e.join(","))].join("\n")
    const encodedUri = encodeURI(csvContent)
    const link = document.createElement("a")
    link.setAttribute("href", encodedUri)
    link.setAttribute("download", `alpha_lab_trades_${selectedId}.csv`)
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold flex items-center gap-2">
            <ShieldAlert className="w-5 h-5 text-primary" />
            Forensic AI Backtest Auditor
          </h2>
          <p className="text-xs text-muted-foreground mt-0.5">
            Glass-box AI verification of trade physics, data integrity, and strategy logic
          </p>
        </div>
        {auditResult && (
          <Badge
            variant="outline"
            className={cn("text-sm px-3 py-1 gap-1.5", statusColor(auditResult.status))}
          >
            <StatusIcon status={auditResult.status} />
            {auditResult.status} · {(auditResult.confidence * 100).toFixed(0)}% confidence
          </Badge>
        )}
      </div>

      {/* Experiment Selector + Audit Trigger */}
      <Card className="border-border/50 bg-card/50">
        <CardContent className="pt-4 pb-4">
          <div className="flex flex-col sm:flex-row gap-3 items-start sm:items-end">
            <div className="flex-1 space-y-1.5 flex gap-3">
              <div className="w-1/2 space-y-1.5">
                <label className="text-xs text-muted-foreground font-medium">
                  Select Passed Experiment
                </label>
                <Select value={selectedId} onValueChange={setSelectedId}>
                  <SelectTrigger id="forensic-experiment-select" className="bg-background">
                    <SelectValue placeholder="— choose a strategy —" />
                  </SelectTrigger>
                  <SelectContent>
                    {experiments.length === 0 && (
                      <SelectItem value="__none__" disabled>
                        No passed experiments yet
                      </SelectItem>
                    )}
                    {experiments.map((exp) => (
                      <SelectItem key={exp.experiment_id} value={exp.experiment_id}>
                        <span className="mr-2 font-medium">{exp.strategy_name}</span>
                        <span className="text-muted-foreground text-xs">
                          #{exp.experiment_id} · Sharpe {exp.metrics?.sharpe?.toFixed(2) ?? "—"}
                        </span>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div className="w-1/2 space-y-1.5">
                <label className="text-xs text-muted-foreground font-medium flex items-center gap-1.5">
                  <BrainCircuit className="w-3.5 h-3.5" /> Evaluator Model
                </label>
                <Select value={selectedModel} onValueChange={setSelectedModel}>
                  <SelectTrigger className="bg-background w-full">
                    <SelectValue placeholder="Select Claude model" />
                  </SelectTrigger>
                  <SelectContent>
                    {models.length === 0 ? (
                      <SelectItem value="claude-sonnet-4-6">Claude Sonnet 4.6 (Default)</SelectItem>
                    ) : (
                      models.map((m) => (
                        <SelectItem key={m.id} value={m.id}>{m.display_name}</SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <Button
              id="run-forensic-audit-btn"
              onClick={handleRunAudit}
              disabled={!selectedId || isRunning}
              className="gap-2 min-w-[180px]"
              size="default"
            >
              {isRunning ? (
                <>
                  <Loader2 className="w-4 h-4 animate-spin" />
                  Auditing…
                </>
              ) : (
                <>
                  <Search className="w-4 h-4" />
                  Run Forensic Audit 🔎
                </>
              )}
            </Button>
          </div>

          {errorMsg && (
            <div className="mt-3 flex items-start gap-2 text-xs text-red-400 bg-red-500/10 border border-red-500/20 rounded-md px-3 py-2">
              <ShieldX className="w-3.5 h-3.5 mt-0.5 shrink-0" />
              {errorMsg}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Verdict Dashboard */}
      {auditResult && (
        <div className="space-y-4">
          {/* Three Taxonomy Cards */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {(["STRUCTURAL", "BACKTEST", "STRATEGY"] as const).map((cat) => {
              const meta = CATEGORY_META[cat]
              const Icon = meta.icon
              const isViolated = auditResult.error_category === cat
              return (
                <Card
                  key={cat}
                  className={cn(
                    "border transition-all",
                    isViolated
                      ? "border-red-500/50 bg-red-500/5 shadow-[0_0_20px_rgba(239,68,68,0.1)]"
                      : "border-border/50 bg-card/50"
                  )}
                >
                  <CardHeader className="pb-2 pt-4 px-4">
                    <div className="flex items-center justify-between">
                      <div className={cn("flex items-center gap-2", meta.color)}>
                        <Icon className="w-4 h-4" />
                        <CardTitle className="text-sm font-medium">{meta.label}</CardTitle>
                      </div>
                      {isViolated ? (
                        <Badge variant="outline" className="text-[10px] border-red-500/40 text-red-400">
                          VIOLATED
                        </Badge>
                      ) : (
                        <Badge variant="outline" className="text-[10px] border-emerald-500/40 text-emerald-400">
                          CLEAN
                        </Badge>
                      )}
                    </div>
                  </CardHeader>
                  <CardContent className="px-4 pb-4">
                    <p className="text-[11px] text-muted-foreground leading-relaxed">
                      {meta.description}
                    </p>
                  </CardContent>
                </Card>
              )
            })}
          </div>

          {/* Recommendation Alert */}
          {(auditResult.status === "FAIL" || auditResult.status === "WARNING") && auditResult.recommendation && (
            <Card className={cn(
              "border",
              auditResult.status === "FAIL"
                ? "border-red-500/40 bg-red-500/5"
                : "border-amber-500/40 bg-amber-500/5"
            )}>
              <CardContent className="pt-4 pb-4">
                <div className="flex items-start gap-3">
                  <div className={cn(
                    "p-2 rounded-md",
                    auditResult.status === "FAIL" ? "bg-red-500/20" : "bg-amber-500/20"
                  )}>
                    <AlertTriangle className={cn(
                      "w-4 h-4",
                      auditResult.status === "FAIL" ? "text-red-400" : "text-amber-400"
                    )} />
                  </div>
                  <div>
                    <p className="text-xs font-semibold mb-1">
                      AI Recommendation — Fix at{" "}
                      <span className={categoryMeta?.color}>{categoryMeta?.label}</span>{" "}
                      layer
                    </p>
                    <p className="text-xs text-muted-foreground leading-relaxed">
                      {auditResult.recommendation}
                    </p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {auditResult.status === "PASS" && (
            <Card className="border-emerald-500/30 bg-emerald-500/5">
              <CardContent className="pt-4 pb-4">
                <div className="flex items-center gap-3">
                  <div className="p-2 rounded-md bg-emerald-500/20">
                    <ShieldCheck className="w-4 h-4 text-emerald-400" />
                  </div>
                  <div>
                    <p className="text-xs font-semibold text-emerald-400 mb-0.5">Backtest Passes Forensic Audit</p>
                    <p className="text-xs text-muted-foreground">{auditResult.recommendation}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          )}

          {/* Metrics footer */}
          {auditResult.metrics && (
            <div className="flex items-center gap-4 text-xs font-mono text-muted-foreground bg-muted/20 px-4 py-2 rounded-md border border-border/50">
              <span className="flex items-center gap-1.5"><BrainCircuit className="w-3.5 h-3.5" /> {auditResult.metrics.model}</span>
              <span className="w-px h-3 bg-border" />
              <span>In: {auditResult.metrics.input_tokens.toLocaleString()}</span>
              <span>Out: {auditResult.metrics.output_tokens.toLocaleString()}</span>
              <span className="w-px h-3 bg-border" />
              <span className="text-primary font-medium">Cost: ${auditResult.metrics.cost_usd.toFixed(4)}</span>
            </div>
          )}
        </div>
      )}

      {/* Trade Inspector: The Receipts */}
      {selectedId && (
        <Card className="border-border/50 bg-card/50">
          <CardHeader className="pb-0">
            <div className="flex items-start justify-between gap-4">
              <div className="flex-1 min-w-0">
                <CardTitle className="text-sm flex items-center gap-2">
                  <TrendingUp className="w-4 h-4 text-primary" />
                  Trade Inspector
                </CardTitle>
                <div className="flex items-center gap-3 mt-2">
                  <div className="flex bg-muted/30 p-0.5 rounded-md border border-border/50">
                    <button
                      onClick={() => setViewMode("ledger")}
                      className={cn(
                        "flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium transition-all",
                        viewMode === "ledger" ? "bg-card text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"
                      )}
                    >
                      <List className="w-3 h-3" /> Ledger
                    </button>
                    <button
                      onClick={() => setViewMode("clusters")}
                      className={cn(
                        "flex items-center gap-1.5 px-2.5 py-1 rounded text-xs font-medium transition-all",
                        viewMode === "clusters" ? "bg-card text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"
                      )}
                    >
                      <Layers className="w-3 h-3" /> Clusters
                    </button>
                  </div>
                  <Button
                    variant="outline"
                    size="sm"
                    className="h-6 text-[10px] px-2 gap-1.5 border-border/50 bg-muted/20"
                    onClick={handleExportCsv}
                    disabled={trades.length === 0}
                  >
                    <Download className="w-3 h-3 text-muted-foreground" /> Export CSV
                  </Button>
                </div>
              </div>
              {/* Equity sparkline */}
              {equityCurve.length > 1 && (
                <div className="w-48 h-12 shrink-0">
                  <ResponsiveContainer width="100%" height="100%">
                    <AreaChart data={equityCurve} margin={{ top: 2, right: 2, bottom: 2, left: 2 }}>
                      <defs>
                        <linearGradient id="sparkGrad" x1="0" y1="0" x2="0" y2="1">
                          <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                          <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                        </linearGradient>
                      </defs>
                      <XAxis dataKey="date" hide />
                      <Tooltip
                        contentStyle={{ background: "#0f172a", border: "1px solid #1e293b", borderRadius: 6, fontSize: 10 }}
                        formatter={(v: number) => [`$${v.toFixed(0)}`, "Equity"]}
                        labelFormatter={(l: string) => l?.slice(0, 10) ?? ""}
                      />
                      <Area
                        type="monotone"
                        dataKey="equity"
                        stroke="#10b981"
                        strokeWidth={1.5}
                        fill="url(#sparkGrad)"
                        dot={false}
                        isAnimationActive={false}
                      />
                    </AreaChart>
                  </ResponsiveContainer>
                </div>
              )}
              {isLoadingTrades && <Loader2 className="w-4 h-4 animate-spin text-muted-foreground shrink-0 mt-1" />}
            </div>
          </CardHeader>

          {/* Global Summary Header */}
          {trades.length > 0 && summaryStats.totalSells > 0 && (
            <div className="border-b border-border/50 bg-muted/10 px-4 py-3 flex items-center justify-between text-xs w-full">
              <div className="flex items-center gap-4 text-muted-foreground">
                <span>
                  <strong className="text-foreground">{summaryStats.totalSells}</strong> trades closed
                </span>
                <span>
                  Win Rate: <strong className={summaryStats.winRate >= 0.5 ? "text-emerald-400" : "text-amber-400"}>
                    {(summaryStats.winRate * 100).toFixed(1)}%
                  </strong>
                </span>
                <span>
                  Avg Return: <strong className={summaryStats.avgPct >= 0 ? "text-emerald-400" : "text-red-400"}>
                    {(summaryStats.avgPct * 100).toFixed(2)}%
                  </strong>
                </span>
                {viewMode === "clusters" && (
                  <span className="flex items-center gap-1.5 ml-2 border-l border-border/50 pl-4">
                    <Layers className="w-3.5 h-3.5" /> <strong className="text-foreground">{positionClusters.length}</strong> clusters
                  </span>
                )}
              </div>
              <div className="flex items-center gap-2">
                <span className="text-muted-foreground mr-1">Total Realized P/L per share:</span>
                <Badge variant="outline" className={cn(
                  "font-mono text-[11px] font-semibold py-0.5 shadow-sm",
                  summaryStats.totalUsd >= 0 ? "border-emerald-500/30 text-emerald-400 bg-emerald-500/10" : "border-red-500/30 text-red-400 bg-red-500/10"
                )}>
                  {summaryStats.totalUsd >= 0 ? "+" : ""}${summaryStats.totalUsd.toFixed(2)}
                </Badge>
              </div>
            </div>
          )}

          {trades.length > 0 && viewMode === "ledger" && (
            <CardContent className="p-0 pt-3">
              <ScrollArea className="max-h-[420px]">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border/50 bg-muted/30">
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Date</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Ticker</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Action</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Δ Weight</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Price</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Volume</th>
                      <th
                        className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground cursor-pointer hover:text-foreground transition-colors select-none"
                        onClick={() => setPlSort(s => s === "none" ? "best" : s === "best" ? "worst" : "none")}
                      >
                        <div className="flex items-center gap-1">
                          P/L
                          {plSort === "none" && <ArrowUpDown className="w-3 h-3 opacity-40" />}
                          {plSort === "best" && <ChevronDown className="w-3 h-3 text-emerald-400" />}
                          {plSort === "worst" && <ChevronUp className="w-3 h-3 text-red-400" />}
                        </div>
                      </th>
                      <th 
                        className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground cursor-pointer hover:text-foreground transition-colors group select-none"
                        onClick={() => setFlagFilter(f => f === "all" ? "flagged" : f === "flagged" ? "clean" : "all")}
                      >
                        <div className="flex items-center gap-1.5">
                          AI Flag
                          <div className={cn(
                            "flex items-center gap-1 px-1.5 py-0.5 rounded-full text-[9px] font-bold tracking-wider uppercase transition-colors",
                            flagFilter === "all" ? "bg-muted/50 text-muted-foreground" : 
                            flagFilter === "flagged" ? "bg-red-500/20 text-red-500" : 
                            "bg-emerald-500/20 text-emerald-500"
                          )}>
                            <Filter className="w-2.5 h-2.5" />
                            {flagFilter}
                          </div>
                        </div>
                      </th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredTrades.map((trade, i) => (
                      <TradeRow
                        key={`${trade.entity_id}-${trade.date}-${i}`}
                        trade={trade}
                        flaggedTrades={auditResult?.flagged_trades ?? []}
                      />
                    ))}
                  </tbody>
                </table>
              </ScrollArea>
            </CardContent>
          )}

          {/* Position Clusters View */}
          {trades.length > 0 && viewMode === "clusters" && (
            <CardContent className="p-0 pt-3">
              <ScrollArea className="max-h-[420px]">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-border/50 bg-muted/30">
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Ticker</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Total Trades</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Realized Sells</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Total Realized %</th>
                      <th className="px-4 py-2 text-left text-[11px] font-medium text-muted-foreground">Total Realized $/sh</th>
                    </tr>
                  </thead>
                  <tbody>
                    {positionClusters.map((cluster) => (
                      <tr key={cluster.ticker} className="border-b border-border/50 hover:bg-muted/10 transition-colors">
                        <td className="px-4 py-3 text-xs font-semibold">{cluster.ticker}</td>
                        <td className="px-4 py-3 text-xs text-muted-foreground">{cluster.trades.length} events</td>
                        <td className="px-4 py-3 text-xs text-muted-foreground">{cluster.sellCount}</td>
                        <td className="px-4 py-3 text-xs font-mono">
                          {cluster.sellCount > 0 ? (
                            <span className={cluster.totalRealizedPct >= 0 ? "text-emerald-400" : "text-red-400"}>
                              {cluster.totalRealizedPct >= 0 ? "+" : ""}{(cluster.totalRealizedPct * 100).toFixed(2)}%
                            </span>
                          ) : (
                            <span className="text-muted-foreground/50">open</span>
                          )}
                        </td>
                        <td className="px-4 py-3 text-xs font-mono">
                          {cluster.sellCount > 0 ? (
                            <span className={cluster.totalRealizedUsd >= 0 ? "text-emerald-400" : "text-red-400"}>
                              {cluster.totalRealizedUsd >= 0 ? "+" : ""}${cluster.totalRealizedUsd.toFixed(2)}
                            </span>
                          ) : (
                            <span className="text-muted-foreground/50">—</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </ScrollArea>
            </CardContent>
          )}

          {!isLoadingTrades && trades.length === 0 && selectedId && (
            <CardContent className="py-6">
              <div className="flex flex-col items-center gap-4 text-center">
                <div className="w-12 h-12 rounded-xl bg-amber-500/10 border border-amber-500/20 flex items-center justify-center">
                  <AlertTriangle className="w-6 h-6 text-amber-400" />
                </div>
                <div>
                  <p className="text-sm font-medium text-muted-foreground">No trade ledger found</p>
                  <p className="text-xs text-muted-foreground/60 mt-1 max-w-xs">
                    This experiment was backtested before trade-ledger extraction was enabled.
                    Re-run the backtest to generate the discrete trade log.
                  </p>
                </div>
                <Button
                  id="rerun-backtest-btn"
                  variant="outline"
                  size="sm"
                  className="gap-2 border-amber-500/40 text-amber-400 hover:bg-amber-500/10"
                  onClick={handleRerunBacktest}
                  disabled={isRerunning}
                >
                  {isRerunning ? (
                    <><Loader2 className="w-3.5 h-3.5 animate-spin" /> Re-running Backtest…</>
                  ) : (
                    <><RefreshCw className="w-3.5 h-3.5" /> Re-run Backtest to Generate Ledger</>
                  )}
                </Button>
              </div>
            </CardContent>
          )}
        </Card>
      )}

      {/* Empty state */}
      {!selectedId && (
        <div className="flex flex-col items-center justify-center py-24 text-center gap-3">
          <div className="w-16 h-16 rounded-2xl bg-muted/30 flex items-center justify-center">
            <ShieldAlert className="w-8 h-8 text-muted-foreground/40" />
          </div>
          <p className="text-sm font-medium text-muted-foreground">Select a passed experiment to audit</p>
          <p className="text-xs text-muted-foreground/60 max-w-xs">
            The Forensic Auditor will cross-reference discrete trade logs against raw point-in-time data
            to detect structural errors, backtest physics violations, and strategy-level lookahead bias.
          </p>
        </div>
      )}
    </div>
  )
}
