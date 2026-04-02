"use client"

import { useState, useEffect, useCallback, useRef } from "react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger } from "@/components/ui/dropdown-menu"
import { Loader2, Database, CheckCircle2, AlertTriangle, XCircle, Play, RefreshCw, Terminal, ChevronDown } from "lucide-react"
import { cn } from "@/lib/utils"
import { fetchPipelineCoverage, runPipelineIngest, runPipelineFull, runPipelineScoring, getPipelineLogs, runPipelineIngestEdgar, type TickerCoverage, type ComponentCoverage } from "@/lib/api"

const STAGES = [
  { key: "market_data", label: "Market Data", cols: ["adj_close", "volume", "daily_return"] },
  { key: "fundamental", label: "Fundamentals", cols: ["revenue", "total_debt", "cash", "shares_out"] },
  { key: "feature", label: "Features", cols: ["ev_sales_zscore", "beta_spy", "dcf_npv_gap", "dynamic_discount_rate"] },
  { key: "action_intent", label: "Strategy Intent", cols: ["strategy_id", "raw_weight"] },
  { key: "target_portfolio", label: "Risk / Target", cols: ["target_weight", "mcr"] },
] as const

type StageKey = (typeof STAGES)[number]["key"]

function coverageStatus(c: ComponentCoverage | null): "full" | "partial" | "empty" {
  if (!c || c.rows === 0) return "empty"
  if (c.null_pct) {
    const hasGaps = Object.values(c.null_pct).some((p) => p > 20)
    return hasGaps ? "partial" : "full"
  }
  return "full"
}

function StatusDot({ status }: { status: "full" | "partial" | "empty" }) {
  if (status === "full") return <CheckCircle2 className="w-3.5 h-3.5 text-green-400" />
  if (status === "partial") return <AlertTriangle className="w-3.5 h-3.5 text-amber-400" />
  return <XCircle className="w-3.5 h-3.5 text-red-400/50" />
}

export function DataPipeline() {
  const [data, setData] = useState<TickerCoverage[]>([])
  const [loading, setLoading] = useState(true)
  const [selectedTicker, setSelectedTicker] = useState<string | null>(null)
  const [pipelineRunning, setPipelineRunning] = useState(false)
  const [pipelinePhase, setPipelinePhase] = useState<string | null>(null)
  const [pipelineError, setPipelineError] = useState<string | null>(null)
  const [pipelineMessage, setPipelineMessage] = useState<string | null>(null)
  const [logs, setLogs] = useState<Array<{ ts: string; level: string; msg: string }>>([])
  const [showLogs, setShowLogs] = useState(false)
  const logIndexRef = useRef(0)

  const loadCoverage = useCallback(() => {
    fetchPipelineCoverage().then((d) => {
      setData(d)
      setLoading(false)
    })
  }, [])

  useEffect(() => { loadCoverage() }, [loadCoverage])

  // Poll pipeline logs when running
  useEffect(() => {
    if (!pipelineRunning) return
    const interval = setInterval(async () => {
      try {
        const result = await getPipelineLogs(logIndexRef.current)
        if (result.logs.length > 0) {
          setLogs(prev => [...prev, ...result.logs])
          logIndexRef.current = result.total
        }
        if (!result.running) {
          setPipelineRunning(false)
          setPipelinePhase(null)
          // Check last log for error indicator
          const lastLog = result.logs[result.logs.length - 1]
          if (lastLog?.level === "ERROR") {
            setPipelineError(lastLog.msg)
            setPipelineMessage(null)
          } else {
            setPipelineMessage("✅ Pipeline completed successfully!")
            setPipelineError(null)
            loadCoverage() // Refresh data
          }
          clearInterval(interval)
        }
      } catch { /* ignore polling errors */ }
    }, 2000)
    return () => clearInterval(interval)
  }, [pipelineRunning, loadCoverage])

  // Auto-scroll logs to bottom (within container only, not the page)
  const logContainerRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight
    }
  }, [logs])

  const startPipeline = (phase: string) => {
    setLogs([])
    logIndexRef.current = 0
    setShowLogs(true)
    setPipelineError(null)
    setPipelineMessage(null)
    setPipelineRunning(true)
    setPipelinePhase(phase)
  }

  const handleRunIngest = async () => {
    const result = await runPipelineIngest()
    if (result.ok) {
      startPipeline("ingest")
    } else {
      setPipelineError(result.error ?? "Failed to start")
    }
  }

  const handleRunIngestEdgar = async () => {
    const result = await runPipelineIngestEdgar()
    if (result.ok) {
      startPipeline("ingest_edgar")
    } else {
      setPipelineError(result.error ?? "Failed to start")
    }
  }

  const handleRunCompute = async () => {
    const result = await runPipelineScoring()
    if (result.ok) {
      startPipeline("pipeline")
    } else {
      setPipelineError(result.error ?? "Failed to start")
    }
  }

  const handleRunFull = async () => {
    const result = await runPipelineFull()
    if (result.ok) {
      startPipeline("full")
    } else {
      setPipelineError(result.error ?? "Failed to start")
    }
  }

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  // Summary counters
  const totalTickers = data.length
  const fullCoverage = data.filter((t) =>
    STAGES.every((s) => coverageStatus(t[s.key as StageKey] as ComponentCoverage | null) === "full")
  ).length
  const partialCoverage = totalTickers - fullCoverage

  const selected = data.find((t) => t.ticker === selectedTicker)

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-primary/10">
            <Database className="w-5 h-5 text-primary" />
          </div>
          <div>
            <h2 className="text-lg font-semibold">Data Pipeline</h2>
            <p className="text-xs text-muted-foreground">
              Coverage across {totalTickers} tickers — {fullCoverage} fully covered, {partialCoverage} with gaps
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={loadCoverage}
            disabled={pipelineRunning}
          >
            <RefreshCw className="w-3.5 h-3.5 mr-1.5" />
            Refresh
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button
                variant="outline"
                size="sm"
                disabled={pipelineRunning}
              >
                {pipelineRunning && (pipelinePhase === "ingest" || pipelinePhase === "ingest_edgar") ? (
                  <Loader2 className="w-3.5 h-3.5 mr-1.5 animate-spin" />
                ) : (
                  <Play className="w-3.5 h-3.5 mr-1.5" />
                )}
                Ingest Data
                <ChevronDown className="w-3.5 h-3.5 ml-1.5 opacity-50" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onClick={handleRunIngest}>
                All Sources (Market + Fundamentals + Macro)
              </DropdownMenuItem>
              <DropdownMenuItem onClick={handleRunIngestEdgar}>
                EDGAR Fundamentals Only
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
          <Button
            variant="outline"
            size="sm"
            onClick={handleRunCompute}
            disabled={pipelineRunning}
          >
            {pipelineRunning && pipelinePhase === "pipeline" ? (
              <Loader2 className="w-3.5 h-3.5 mr-1.5 animate-spin" />
            ) : (
              <Play className="w-3.5 h-3.5 mr-1.5" />
            )}
            Compute All
          </Button>
          <Button
            size="sm"
            onClick={handleRunFull}
            disabled={pipelineRunning}
            className="bg-emerald-600 hover:bg-emerald-700 text-white"
          >
            {pipelineRunning && pipelinePhase === "full" ? (
              <Loader2 className="w-3.5 h-3.5 mr-1.5 animate-spin" />
            ) : (
              <Play className="w-3.5 h-3.5 mr-1.5" />
            )}
            Run Full Pipeline
          </Button>
        </div>
      </div>

      {/* Pipeline Live Log Panel */}
      {showLogs && (
        <Card className="border-border/50 bg-card/50">
          <CardHeader className="pb-2 flex flex-row items-center justify-between">
            <div className="flex items-center gap-2">
              <Terminal className="w-4 h-4 text-muted-foreground" />
              <CardTitle className="text-sm">Pipeline Logs</CardTitle>
              {pipelineRunning && (
                <Badge variant="outline" className="text-[10px] h-5 border-emerald-500/30 text-emerald-400">
                  <Loader2 className="w-2.5 h-2.5 mr-1 animate-spin" />
                  Running — {pipelinePhase}
                </Badge>
              )}
              {!pipelineRunning && pipelineMessage && (
                <Badge variant="outline" className="text-[10px] h-5 border-emerald-500/30 text-emerald-400">
                  ✅ Done
                </Badge>
              )}
              {!pipelineRunning && pipelineError && (
                <Badge variant="outline" className="text-[10px] h-5 border-red-500/30 text-red-400">
                  ❌ Error
                </Badge>
              )}
            </div>
            <Button variant="ghost" size="sm" className="h-6 text-xs" onClick={() => setShowLogs(false)}>
              Hide
            </Button>
          </CardHeader>
          <CardContent className="p-0">
            <div ref={logContainerRef} className="max-h-64 overflow-y-auto font-mono text-xs bg-black/30 rounded-b-lg">
              {logs.length === 0 && pipelineRunning && (
                <div className="px-4 py-3 text-muted-foreground">Waiting for logs...</div>
              )}
              {logs.map((log, i) => (
                <div
                  key={i}
                  className={cn(
                    "px-4 py-0.5 border-b border-border/5",
                    log.level === "ERROR" && "text-red-400 bg-red-500/5",
                    log.level === "WARNING" && "text-amber-400",
                    log.level === "INFO" && "text-muted-foreground"
                  )}
                >
                  <span className="text-muted-foreground/50 mr-2">{log.ts}</span>
                  {log.msg}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Coverage Matrix */}
      <Card className="border-border/50 bg-card/50">
        <CardHeader className="pb-2">
          <CardTitle className="text-sm">Coverage Matrix</CardTitle>
          <CardDescription className="text-xs">
            <span className="inline-flex items-center gap-1"><CheckCircle2 className="w-3 h-3 text-green-400" /> Full</span>
            <span className="inline-flex items-center gap-1 ml-3"><AlertTriangle className="w-3 h-3 text-amber-400" /> Gaps (&gt;20% null)</span>
            <span className="inline-flex items-center gap-1 ml-3"><XCircle className="w-3 h-3 text-red-400/50" /> Empty</span>
          </CardDescription>
        </CardHeader>
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border/30">
                  <th className="text-left px-4 py-2 text-muted-foreground font-medium sticky left-0 bg-card/50 z-10">
                    Ticker
                  </th>
                  {STAGES.map((s) => (
                    <th key={s.key} className="text-center px-3 py-2 text-muted-foreground font-medium whitespace-nowrap">
                      {s.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {data.map((ticker) => (
                  <tr
                    key={ticker.ticker}
                    onClick={() => setSelectedTicker(ticker.ticker === selectedTicker ? null : ticker.ticker)}
                    className={cn(
                      "border-b border-border/10 cursor-pointer transition-colors",
                      selectedTicker === ticker.ticker
                        ? "bg-primary/5"
                        : "hover:bg-card/80"
                    )}
                  >
                    <td className="px-4 py-2 font-mono font-semibold text-foreground sticky left-0 bg-card/50 z-10">
                      {ticker.ticker}
                    </td>
                    {STAGES.map((s) => {
                      const comp = ticker[s.key as StageKey] as ComponentCoverage | null
                      const status = coverageStatus(comp)
                      return (
                        <td key={s.key} className="text-center px-3 py-2">
                          <div className="flex items-center justify-center gap-1.5">
                            <StatusDot status={status} />
                            <span className="font-mono text-muted-foreground">
                              {comp?.rows?.toLocaleString() ?? "0"}
                            </span>
                          </div>
                        </td>
                      )
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      {/* Ticker Detail Panel */}
      {selected && (
        <Card className="border-primary/30 bg-primary/5">
          <CardHeader className="pb-3">
            <CardTitle className="text-sm font-mono">{selectedTicker} — Detail Breakdown</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {STAGES.map((stage) => {
                const comp = selected[stage.key as StageKey] as ComponentCoverage | null
                const status = coverageStatus(comp)
                return (
                  <div
                    key={stage.key}
                    className={cn(
                      "rounded-lg border p-3 space-y-2",
                      status === "full" && "border-green-500/30 bg-green-500/5",
                      status === "partial" && "border-amber-500/30 bg-amber-500/5",
                      status === "empty" && "border-border/30 bg-card/30"
                    )}
                  >
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-medium text-foreground">{stage.label}</span>
                      <StatusDot status={status} />
                    </div>
                    {comp && comp.rows > 0 ? (
                      <>
                        <div className="text-xs text-muted-foreground space-y-0.5">
                          <div className="flex justify-between">
                            <span>Rows</span>
                            <span className="font-mono">{comp.rows.toLocaleString()}</span>
                          </div>
                          <div className="flex justify-between">
                            <span>Period</span>
                            <span className="font-mono">{comp.date_start} → {comp.date_end}</span>
                          </div>
                        </div>
                        {comp.null_pct && (
                          <div className="pt-1 border-t border-border/20 space-y-0.5">
                            <p className="text-[10px] text-muted-foreground uppercase tracking-wider">Column Fill</p>
                            {Object.entries(comp.null_pct).map(([col, pct]) => (
                              <div key={col} className="flex items-center justify-between text-xs">
                                <span className="text-muted-foreground font-mono truncate mr-2">{col}</span>
                                <Badge
                                  variant="outline"
                                  className={cn(
                                    "text-[9px] h-4 px-1.5 font-mono",
                                    pct === 0 && "border-green-500/30 text-green-400",
                                    pct > 0 && pct <= 20 && "border-amber-500/30 text-amber-400",
                                    pct > 20 && "border-red-500/30 text-red-400"
                                  )}
                                >
                                  {pct === 0 ? "100%" : `${(100 - pct).toFixed(0)}%`}
                                </Badge>
                              </div>
                            ))}
                          </div>
                        )}
                      </>
                    ) : (
                      <p className="text-xs text-muted-foreground">No data</p>
                    )}
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  )
}
