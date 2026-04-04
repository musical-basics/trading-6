"use client"

import { useState, useEffect, useRef, useCallback } from "react"
import {
  Bot, Brain, Shield, DollarSign, Play, Square, RefreshCw,
  ChevronDown, CheckCircle2, Clock, AlertCircle,
  Loader2, Zap, TrendingUp, BarChart2,
  Activity, Cpu, ArrowRight, Terminal as TerminalIcon,
  Sparkles, Target, XCircle
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { cn } from "@/lib/utils"

// ─── Types ───────────────────────────────────────────────────
type TickStatus = "idle" | "running" | "complete" | "error" | "cancelled"
type AgentStatus = "pending" | "running" | "complete" | "error"
type Phase = "data_fetch" | "consultants" | "commander" | "desks" | "back_office" | "done"

interface AgentCard {
  id: string
  label: string
  role: "consultant" | "auditor" | "scout" | "commander" | "analyst" | "strategist" | "pm" | "back_office"
  desk?: number
  status: AgentStatus
  inputTokens?: number
  outputTokens?: number
  costUsd?: number
  strategy?: string
}

interface LogEntry {
  ts: string
  level: "INFO" | "ERROR" | "WARNING"
  agent: string
  msg: string
}

interface DeskResult {
  desk_id: number
  strategy_id: string
  allocated_capital: number
  confirmation: string
}

interface TickResult {
  tick_date: string
  trader_id: number
  api_cost_deducted_usd: number
  elapsed_seconds: number
  desk_results: DeskResult[]
  total_token_cost: {
    input_tokens: number
    output_tokens: number
    estimated_cost_usd: number
  }
  macro_brief?: {
    macro_regime: string
    vix_level: number
    ten_year_yield: number
    risk_assessment: string
  }
  commander_directive?: {
    commander_reasoning: string
    total_deployed_pct: number
    cash_reserve_pct: number
  }
}

interface ModelOption {
  id: string
  display_name: string
  tier: string
  default_role: string
}

const API = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"

// ─── Parse event data out of a log message string ────────────
// Backend logs: "arena.agent_completed: {'agent_name': 'scout', 'tokens': {'input': 234, 'output': 76}, 'cost_usd': 0.00642}"
// We extract structured info without eval by regex-matching the key fields.
function parseLogEvent(msg: string): { type: string; data: Record<string, unknown> } | null {
  const typeMatch = msg.match(/^(arena\.\w+):/)
  if (!typeMatch) return null
  const type = typeMatch[1]

  // Extract agent_name
  const agentMatch = msg.match(/'agent_name':\s*'([^']+)'/)
  const agentName = agentMatch ? agentMatch[1] : ""

  // Extract cost_usd (float)
  const costMatch = msg.match(/'cost_usd':\s*([\d.]+)/)
  const costUsd = costMatch ? parseFloat(costMatch[1]) : undefined

  // Extract tokens
  const inputMatch = msg.match(/'input':\s*(\d+)/)
  const outputMatch = msg.match(/'output':\s*(\d+)/)
  const inputTokens = inputMatch ? parseInt(inputMatch[1]) : undefined
  const outputTokens = outputMatch ? parseInt(outputMatch[1]) : undefined

  // Extract strategy (for strategist completions)
  const strategyMatch = msg.match(/'strategy':\s*'([^']+)'/)
  const strategy = strategyMatch ? strategyMatch[1] : undefined

  // Extract phase
  const phaseMatch = msg.match(/'phase':\s*'([^']+)'/)
  const phase = phaseMatch ? phaseMatch[1] : undefined

  // Extract phase_name
  const phaseNameMatch = msg.match(/'phase_name':\s*'([^']+)'/)
  const phaseName = phaseNameMatch ? phaseNameMatch[1] : undefined

  return {
    type,
    data: {
      agent_name: agentName,
      cost_usd: costUsd,
      tokens: inputTokens !== undefined ? { input: inputTokens, output: outputTokens } : undefined,
      strategy,
      phase,
      phase_name: phaseName,
    },
  }
}

// ─── Agent DAG Definition ─────────────────────────────────────
const INITIAL_AGENTS: AgentCard[] = [
  { id: "consultant",    label: "Market Consultant", role: "consultant",  status: "pending" },
  { id: "auditor",       label: "Data Auditor",      role: "auditor",     status: "pending" },
  { id: "scout",         label: "Intel Scout",       role: "scout",       status: "pending" },
  { id: "commander",     label: "Commander (CEO)",   role: "commander",   status: "pending" },
  { id: "analyst_d1",    label: "Analyst",           role: "analyst",     desk: 1, status: "pending" },
  { id: "strategist_d1", label: "Strategist",        role: "strategist",  desk: 1, status: "pending" },
  { id: "pm_d1",         label: "PM",                role: "pm",          desk: 1, status: "pending" },
  { id: "analyst_d2",    label: "Analyst",           role: "analyst",     desk: 2, status: "pending" },
  { id: "strategist_d2", label: "Strategist",        role: "strategist",  desk: 2, status: "pending" },
  { id: "pm_d2",         label: "PM",                role: "pm",          desk: 2, status: "pending" },
  { id: "analyst_d3",    label: "Analyst",           role: "analyst",     desk: 3, status: "pending" },
  { id: "strategist_d3", label: "Strategist",        role: "strategist",  desk: 3, status: "pending" },
  { id: "pm_d3",         label: "PM",                role: "pm",          desk: 3, status: "pending" },
  { id: "back_office",   label: "Back Office",       role: "back_office", status: "pending" },
]

const ROLE_ICON: Record<AgentCard["role"], React.ElementType> = {
  consultant:  BarChart2,
  auditor:     Shield,
  scout:       Target,
  commander:   Brain,
  analyst:     Activity,
  strategist:  Sparkles,
  pm:          DollarSign,
  back_office: Shield,
}

const ROLE_COLOR: Record<AgentCard["role"], string> = {
  consultant:  "text-blue-400 bg-blue-400/10",
  auditor:     "text-amber-400 bg-amber-400/10",
  scout:       "text-purple-400 bg-purple-400/10",
  commander:   "text-rose-400 bg-rose-400/10",
  analyst:     "text-cyan-400 bg-cyan-400/10",
  strategist:  "text-violet-400 bg-violet-400/10",
  pm:          "text-emerald-400 bg-emerald-400/10",
  back_office: "text-orange-400 bg-orange-400/10",
}

// ─── Sub-components ─────────────────────────────────────────

function AgentPill({ agent }: { agent: AgentCard }) {
  const Icon = ROLE_ICON[agent.role]
  const colorClass = ROLE_COLOR[agent.role]

  return (
    <div className={cn(
      "flex flex-col gap-1 p-2.5 rounded-lg border transition-all duration-300",
      agent.status === "pending"  && "border-border/30 bg-card/20 opacity-50",
      agent.status === "running"  && "border-primary/60 bg-primary/5 shadow-[0_0_10px_rgba(99,102,241,0.15)] animate-pulse",
      agent.status === "complete" && "border-emerald-500/30 bg-emerald-500/5",
      agent.status === "error"    && "border-red-500/30 bg-red-500/5",
    )}>
      <div className="flex items-center gap-2">
        <div className={cn("p-1 rounded shrink-0", colorClass)}>
          <Icon className="w-3 h-3" />
        </div>
        <span className="text-[11px] font-medium text-foreground truncate">{agent.label}</span>
        <div className="ml-auto shrink-0">
          {agent.status === "pending"  && <Clock className="w-3 h-3 text-muted-foreground/40" />}
          {agent.status === "running"  && <Loader2 className="w-3 h-3 text-primary animate-spin" />}
          {agent.status === "complete" && <CheckCircle2 className="w-3 h-3 text-emerald-400" />}
          {agent.status === "error"    && <AlertCircle className="w-3 h-3 text-red-400" />}
        </div>
      </div>
      {agent.status === "complete" && agent.costUsd !== undefined && (
        <div className="flex items-center gap-1.5 flex-wrap">
          <span className="text-[10px] text-muted-foreground tabular-nums">
            {((agent.inputTokens || 0) + (agent.outputTokens || 0)).toLocaleString()} tok
          </span>
          <span className="text-[10px] text-amber-400 font-mono tabular-nums">${agent.costUsd.toFixed(5)}</span>
          {agent.strategy && (
            <Badge variant="outline" className="text-[9px] px-1 h-4 border-violet-500/30 text-violet-300">
              {agent.strategy}
            </Badge>
          )}
        </div>
      )}
    </div>
  )
}

function PhaseLabel({ label, active, done }: { label: string; active: boolean; done: boolean }) {
  return (
    <div className={cn(
      "flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-widest px-2 py-1 rounded",
      active && "text-primary bg-primary/10",
      done && "text-emerald-400",
      !active && !done && "text-muted-foreground/40",
    )}>
      {done
        ? <CheckCircle2 className="w-3 h-3 shrink-0" />
        : active
        ? <Zap className="w-3 h-3 shrink-0 animate-pulse" />
        : <div className="w-3 h-3 rounded-full border border-current opacity-40 shrink-0" />
      }
      {label}
    </div>
  )
}

// ─── Live animating cost counter ─────────────────────────────
function LiveCostBadge({ total, live }: { total: number; live: boolean }) {
  const prevRef = useRef(total)
  const [flash, setFlash] = useState(false)

  useEffect(() => {
    if (total !== prevRef.current) {
      prevRef.current = total
      setFlash(true)
      const t = setTimeout(() => setFlash(false), 600)
      return () => clearTimeout(t)
    }
  }, [total])

  return (
    <div className={cn(
      "flex items-center gap-2 px-3 py-1.5 rounded-lg border text-xs font-mono tabular-nums transition-colors duration-300",
      live ? "border-amber-500/50 bg-amber-500/8 text-amber-300" : "border-border/30 bg-card/30 text-muted-foreground",
      flash && "border-amber-400 bg-amber-400/15 text-amber-200",
    )}>
      <DollarSign className="w-3.5 h-3.5 shrink-0" />
      <span>${total.toFixed(5)}</span>
      {live && <span className="text-[10px] text-amber-500 animate-pulse">live</span>}
    </div>
  )
}

// ─── Main Component ──────────────────────────────────────────
export function HedgeFundArena() {
  const [traderId, setTraderId] = useState<number>(1)
  const [traders, setTraders] = useState<{id: number; name: string; total_capital: number}[]>([])
  const [models, setModels] = useState<ModelOption[]>([])
  const [selectedCommanderModel, setSelectedCommanderModel] = useState<string>("")
  const [selectedWorkerModel, setSelectedWorkerModel] = useState<string>("")
  const [dryRun, setDryRun] = useState(false)

  const [tickStatus, setTickStatus] = useState<TickStatus>("idle")
  const [tickId, setTickId] = useState<string | null>(null)
  const [currentPhase, setCurrentPhase] = useState<Phase | null>(null)
  const [agents, setAgents] = useState<AgentCard[]>(INITIAL_AGENTS)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [logOffset, setLogOffset] = useState(0)
  const [tickResult, setTickResult] = useState<TickResult | null>(null)
  // allTimeCost: fetched from backend Parquet ledger — persists across all sessions/refreshes
  const [allTimeCost, setAllTimeCost] = useState(0)
  // currentTickCost: live per-tick cost, resets on each Run Tick
  const [currentTickCost, setCurrentTickCost] = useState(0)
  const [elapsedSec, setElapsedSec] = useState(0)

  const pollRef    = useRef<NodeJS.Timeout | null>(null)
  const logPollRef = useRef<NodeJS.Timeout | null>(null)
  const timerRef   = useRef<NodeJS.Timeout | null>(null)
  const logsEndRef = useRef<HTMLDivElement>(null)
  const logOffsetRef = useRef(0)  // stable ref so intervals don't stale-close

  // ── Fetch all-time cost from backend ledger ──────────────────
  const fetchAllTimeCost = useCallback(async (id: number) => {
    try {
      const r = await fetch(`${API}/api/arena/costs/${id}`)
      const data = await r.json()
      // Backend returns { total_cost_usd: float, ... }
      if (typeof data?.total_cost_usd === "number") {
        setAllTimeCost(data.total_cost_usd)
      }
    } catch { /* silently fail — backend may not have ledger yet */ }
  }, [])

  // ── Load traders + models on mount ────────────────────────────
  useEffect(() => {
    fetch(`${API}/api/traders`)
      .then(r => r.json())
      .then(data => { if (Array.isArray(data)) setTraders(data) })
      .catch(() => {})

    // Load all-time cost for the initially-selected trader
    fetchAllTimeCost(traderId)

    fetch(`${API}/api/arena/models`)
      .then(r => r.json())
      .then(data => {
        const modelList: ModelOption[] = data?.models || []
        setModels(modelList)
        const opus   = modelList.find(m => m.tier.includes("Opus"))
        const sonnet = modelList.find(m => m.tier.includes("Sonnet"))
        if (opus)   setSelectedCommanderModel(opus.id)
        if (sonnet) setSelectedWorkerModel(sonnet.id)
      })
      .catch(() => {
        const fallback: ModelOption[] = [
          { id: "claude-opus-4-6-20251001",   display_name: "Claude Opus 4.6",   tier: "Opus (Most Capable)", default_role: "commander" },
          { id: "claude-sonnet-4-6-20251001", display_name: "Claude Sonnet 4.6", tier: "Sonnet (Balanced)",    default_role: "strategist" },
          { id: "claude-haiku-4-5-20251001",  display_name: "Claude Haiku 4.5",  tier: "Haiku (Fastest)",     default_role: "analyst" },
        ]
        setModels(fallback)
        setSelectedCommanderModel(fallback[0].id)
        setSelectedWorkerModel(fallback[1].id)
      })
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  // Re-fetch all-time cost when trader selection changes
  useEffect(() => {
    fetchAllTimeCost(traderId)
  }, [traderId, fetchAllTimeCost])

  // ── Auto-scroll logs ─────────────────────────────────────────
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }, [logs])

  // ── Cleanup ──────────────────────────────────────────────────
  useEffect(() => {
    return () => { stopAllPolling() }
  }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  const stopAllPolling = () => {
    if (pollRef.current)    { clearInterval(pollRef.current);    pollRef.current    = null }
    if (logPollRef.current) { clearInterval(logPollRef.current); logPollRef.current = null }
    if (timerRef.current)   { clearInterval(timerRef.current);   timerRef.current   = null }
  }

  // ── Parse a log entry for agent events → update UI ──────────
  // This is how we get LIVE cost + status updates without a WebSocket.
  // The backend logs emit "arena.agent_completed: {...}" on every agent call.
  const processNewLogs = useCallback((newEntries: LogEntry[]) => {
    for (const entry of newEntries) {
      const parsed = parseLogEvent(entry.msg)
      if (!parsed) continue

      const { type, data } = parsed
      const agentName = data.agent_name as string

      if (type === "arena.agent_started" && agentName) {
        if (data.phase) setCurrentPhase(data.phase as Phase)
        setAgents(prev => prev.map(a =>
          a.id === agentName ? { ...a, status: "running" as AgentStatus } : a
        ))
      } else if (type === "arena.agent_completed" && agentName) {
        const cost = data.cost_usd as number | undefined
        const tokens = data.tokens as { input: number; output: number } | undefined
        setAgents(prev => prev.map(a =>
          a.id === agentName ? {
            ...a,
            status: "complete" as AgentStatus,
            inputTokens: tokens?.input,
            outputTokens: tokens?.output,
            costUsd: cost,
            strategy: data.strategy as string | undefined,
          } : a
        ))
        // Live cost accumulation per-agent from log-parsed events
        if (cost) setCurrentTickCost(prev => prev + cost)
      } else if (type === "arena.phase_completed") {
        const phaseName = (data.phase_name || data.phase) as string
        if (phaseName) setCurrentPhase(phaseName as Phase)
      } else if (type === "arena.tick_completed") {
        setCurrentPhase("done")
      }
    }
  }, [])

  // ── Status polling ───────────────────────────────────────────
  const startStatusPolling = useCallback((id: string) => {
    pollRef.current = setInterval(async () => {
      try {
        const r = await fetch(`${API}/api/arena/tick/${id}/status`)
        const data = await r.json()

        if (data.elapsed_seconds) setElapsedSec(data.elapsed_seconds)

        if (data.status === "complete") {
          stopAllPolling()
          setTickStatus("complete")
          setCurrentPhase("done")
          if (data.result) {
            const result = data.result as TickResult
            setTickResult(result)
            const finalCost = result.api_cost_deducted_usd || result.total_token_cost?.estimated_cost_usd || 0
            setCurrentTickCost(finalCost)
            setElapsedSec(result.elapsed_seconds)
            setAgents(prev => prev.map(a =>
              a.status !== "error" ? { ...a, status: "complete" as AgentStatus } : a
            ))
            // Refresh all-time cost from ledger after tick settles
            setTimeout(() => fetchAllTimeCost(traderId), 1500)
          }
        } else if (data.status === "error") {
          stopAllPolling()
          setTickStatus("error")
        } else if (data.status === "cancelled") {
          stopAllPolling()
          setTickStatus("cancelled")
        }
      } catch { /* network hiccup — retry next interval */ }
    }, 1500)
  }, [])

  // ── Log polling — drives live cost + agent status updates ────
  const startLogPolling = useCallback(() => {
    logPollRef.current = setInterval(async () => {
      try {
        const r = await fetch(`${API}/api/arena/logs?since=${logOffsetRef.current}`)
        const data = await r.json()

        if (data.logs?.length) {
          const newEntries = data.logs as LogEntry[]
          setLogs(prev => [...prev, ...newEntries])
          logOffsetRef.current += newEntries.length
          setLogOffset(logOffsetRef.current)
          // processNewLogs parses cost_usd out of agent_completed events → live cost ticker
          processNewLogs(newEntries)
        }
      } catch { /* retry next interval */ }
    }, 600)  // faster poll = more live feel
  }, [processNewLogs])

  // ── Cancel a running tick ────────────────────────────────────
  const handleCancelTick = async () => {
    if (!tickId || tickStatus !== "running") return
    try {
      await fetch(`${API}/api/arena/tick/${tickId}`, { method: "DELETE" })
    } catch { /* ignore */ }
    stopAllPolling()
    setTickStatus("cancelled")
    setAgents(prev => prev.map(a =>
      a.status === "running" ? { ...a, status: "pending" as AgentStatus } : a
    ))
  }

  // ── Run a tick ───────────────────────────────────────────────
  const handleRunTick = async () => {
    if (tickStatus === "running") return

    // Reset per-tick state (cumulativeCost intentionally NOT reset)
    setTickStatus("running")
    setTickResult(null)
    setCurrentTickCost(0)  // resets for this tick only
    setElapsedSec(0)
    setLogs([])
    setLogOffset(0)
    logOffsetRef.current = 0
    setCurrentPhase("data_fetch")
    setAgents(INITIAL_AGENTS.map(a => ({ ...a, status: "pending" })))
    stopAllPolling()

    // Elapsed counter (visual only, status poll provides authoritative value)
    const startTime = Date.now()
    timerRef.current = setInterval(() => {
      setElapsedSec((Date.now() - startTime) / 1000)
    }, 500)

    const modelOverrides: Record<string, string> = {}
    if (selectedCommanderModel) modelOverrides["commander"] = selectedCommanderModel
    if (selectedWorkerModel) {
      modelOverrides["strategist"] = selectedWorkerModel
      modelOverrides["consultant"] = selectedWorkerModel
      modelOverrides["analyst"]    = selectedWorkerModel
      modelOverrides["pm"]         = selectedWorkerModel
    }

    try {
      const r = await fetch(`${API}/api/arena/tick/${traderId}`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ model_overrides: modelOverrides, dry_run: dryRun }),
      })
      const data = await r.json()
      if (data.tick_id) {
        setTickId(data.tick_id)
        startStatusPolling(data.tick_id)
        startLogPolling()
      } else {
        stopAllPolling()
        setTickStatus("error")
      }
    } catch {
      stopAllPolling()
      setTickStatus("error")
    }
  }

  // ─── Phase helpers ────────────────────────────────────────────
  const phaseOrder: Phase[] = ["data_fetch", "consultants", "commander", "desks", "back_office", "done"]
  const phaseIndex = currentPhase ? phaseOrder.indexOf(currentPhase) : -1
  const isPhaseActive = (p: Phase) => currentPhase === p
  const isPhaseDone   = (p: Phase) => phaseIndex > phaseOrder.indexOf(p)

  const isRunning = tickStatus === "running"

  // ─────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col gap-4 h-full min-h-0">

      {/* ── Header Controls ─────────────────────────────────── */}
      <div className="flex flex-wrap items-end gap-3 shrink-0">

        {/* Fund */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Fund</label>
          <div className="relative">
            <select
              value={traderId}
              onChange={e => setTraderId(Number(e.target.value))}
              disabled={isRunning}
              className="h-9 pl-3 pr-8 text-sm bg-card border border-border rounded-md text-foreground focus:outline-none focus:border-primary disabled:opacity-50 cursor-pointer appearance-none"
            >
              {traders.length > 0
                ? traders.map(t => <option key={t.id} value={t.id}>{t.name} (${(t.total_capital / 1000).toFixed(1)}K)</option>)
                : <option value={1}>Fund-1</option>
              }
            </select>
            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
          </div>
        </div>

        {/* Commander model */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Commander Model</label>
          <div className="relative">
            <select
              value={selectedCommanderModel}
              onChange={e => setSelectedCommanderModel(e.target.value)}
              disabled={isRunning}
              className="h-9 pl-3 pr-8 text-sm bg-card border border-border rounded-md text-foreground focus:outline-none focus:border-primary disabled:opacity-50 cursor-pointer appearance-none"
            >
              {models.map(m => <option key={m.id} value={m.id}>{m.display_name} · {m.tier}</option>)}
            </select>
            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
          </div>
        </div>

        {/* Worker model */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Worker Models</label>
          <div className="relative">
            <select
              value={selectedWorkerModel}
              onChange={e => setSelectedWorkerModel(e.target.value)}
              disabled={isRunning}
              className="h-9 pl-3 pr-8 text-sm bg-card border border-border rounded-md text-foreground focus:outline-none focus:border-primary disabled:opacity-50 cursor-pointer appearance-none"
            >
              {models.map(m => <option key={m.id} value={m.id}>{m.display_name} · {m.tier}</option>)}
            </select>
            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
          </div>
        </div>

        {/* Mode: Live / Dry Run */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Mode</label>
          <button
            onClick={() => setDryRun(d => !d)}
            disabled={isRunning}
            className={cn(
              "h-9 px-3 text-sm rounded-md border transition-colors",
              dryRun ? "border-amber-500/50 bg-amber-500/10 text-amber-300" : "border-border bg-card text-muted-foreground hover:text-foreground",
            )}
          >
            {dryRun ? "Dry Run" : "Live"}
          </button>
        </div>

        {/* Test Mode preset — one click: Haiku + Dry Run */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Preset</label>
          <button
            onClick={() => {
              const haiku = models.find(m => m.tier.includes("Haiku"))
              if (haiku) {
                setSelectedCommanderModel(haiku.id)
                setSelectedWorkerModel(haiku.id)
              }
              setDryRun(true)
            }}
            disabled={isRunning}
            className={cn(
              "h-9 px-3 text-sm rounded-md border transition-colors flex items-center gap-1.5",
              "border-violet-500/40 bg-violet-500/8 text-violet-300 hover:bg-violet-500/15 disabled:opacity-50",
            )}
          >
            <span className="text-[11px]">⚡</span> Test Mode
          </button>
        </div>

        <div className="flex-1" />

        {/* API Cost — all-time persistent (from ledger) + live this-tick */}
        <div className="flex flex-col gap-1 items-end">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">API Cost</label>
          <div className="flex flex-col items-end gap-0.5">
            {/* Main badge: all-time total from Parquet ledger — persists across refreshes */}
            <LiveCostBadge total={allTimeCost + (isRunning ? currentTickCost : 0)} live={isRunning} />
            {/* Sub-line: this tick only, shown live and after completion */}
            {(isRunning || tickStatus === "complete") && currentTickCost > 0 && (
              <span className="text-[10px] text-muted-foreground/60 font-mono tabular-nums">
                this tick ${currentTickCost.toFixed(5)}
              </span>
            )}
          </div>
        </div>

        {/* Elapsed */}
        {(isRunning || tickStatus === "complete" || tickStatus === "cancelled") && (
          <div className="flex flex-col gap-1 items-end">
            <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Elapsed</label>
            <div className="flex items-center gap-1.5 h-9 px-3 rounded-lg border border-border/30 text-xs font-mono text-muted-foreground">
              <Clock className="w-3 h-3" />
              {elapsedSec.toFixed(1)}s
            </div>
          </div>
        )}

        {/* ── Action buttons ─── */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">&nbsp;</label>
          <div className="flex gap-2">
            {/* Pause / Cancel button — only shown while running */}
            {isRunning && (
              <Button
                onClick={handleCancelTick}
                variant="outline"
                className="h-9 gap-2 border-red-500/40 text-red-400 hover:bg-red-500/10 hover:text-red-300"
              >
                <Square className="w-3.5 h-3.5" />
                Stop
              </Button>
            )}

            {/* Run / Re-run button */}
            <Button
              onClick={handleRunTick}
              disabled={isRunning}
              className={cn(
                "h-9 gap-2 font-semibold transition-all",
                isRunning
                  ? "bg-primary/20 text-primary border border-primary/40"
                  : tickStatus === "complete"
                  ? "bg-emerald-600 hover:bg-emerald-500"
                  : tickStatus === "cancelled"
                  ? "bg-amber-600 hover:bg-amber-500"
                  : "bg-primary hover:bg-primary/90",
              )}
            >
              {isRunning ? (
                <><Loader2 className="w-4 h-4 animate-spin" /> Running...</>
              ) : tickStatus === "complete" ? (
                <><RefreshCw className="w-4 h-4" /> Run Again</>
              ) : tickStatus === "cancelled" ? (
                <><Play className="w-4 h-4" /> Retry</>
              ) : (
                <><Play className="w-4 h-4" /> Run Tick</>
              )}
            </Button>
          </div>
        </div>
      </div>

      {/* ── Main Layout ──────────────────────────────────────── */}
      <div className="flex gap-4 flex-1 min-h-0 overflow-hidden">

        {/* ── Left: Agent Pipeline DAG (scrollable) ─────────── */}
        <div className="w-64 shrink-0 flex flex-col gap-2 min-h-0">
          <div className="flex items-center gap-2 shrink-0">
            <Cpu className="w-4 h-4 text-primary" />
            <span className="text-sm font-semibold">Agent Pipeline</span>
          </div>

          <ScrollArea className="flex-1 pr-2">
            <div className="flex flex-col gap-2 pb-4">
              {/* P0: Data Fetch */}
              <PhaseLabel label="Phase 0 · Data Fetch" active={isPhaseActive("data_fetch")} done={isPhaseDone("data_fetch")} />

              {/* P1: Consultants */}
              <PhaseLabel label="Phase 1 · C-Suite" active={isPhaseActive("consultants")} done={isPhaseDone("consultants")} />
              <div className="flex flex-col gap-1 pl-3 border-l-2 border-border/30 ml-2">
                {agents.filter(a => ["consultant","auditor","scout"].includes(a.id)).map(a => (
                  <AgentPill key={a.id} agent={a} />
                ))}
              </div>

              <div className="flex items-center gap-1 pl-4 opacity-40">
                <ArrowRight className="w-3 h-3 text-muted-foreground" />
                <div className="h-px flex-1 bg-border/30" />
              </div>

              {/* P2: Commander */}
              <PhaseLabel label="Phase 2 · Commander" active={isPhaseActive("commander")} done={isPhaseDone("commander")} />
              <div className="pl-3 border-l-2 border-rose-500/20 ml-2">
                <AgentPill agent={agents.find(a => a.id === "commander")!} />
              </div>

              <div className="flex items-center gap-1 pl-4 opacity-40">
                <ArrowRight className="w-3 h-3 text-muted-foreground" />
                <div className="h-px flex-1 bg-border/30" />
              </div>

              {/* P3: 3 Desks — each has its own scroll area if needed */}
              <PhaseLabel label="Phase 3 · Trading Desks" active={isPhaseActive("desks")} done={isPhaseDone("desks")} />
              {[1, 2, 3].map(desk => (
                <div key={desk} className="rounded-lg border border-border/30 bg-card/20 overflow-hidden">
                  <div className="px-2 py-1 bg-muted/10 border-b border-border/20 text-[10px] font-bold text-muted-foreground uppercase tracking-wider flex items-center gap-1.5">
                    <span>Desk {desk}</span>
                    {agents.filter(a => a.desk === desk).every(a => a.status === "complete") && isPhaseDone("desks") && (
                      <CheckCircle2 className="w-3 h-3 text-emerald-400 ml-auto" />
                    )}
                  </div>
                  <div className="flex flex-col gap-1 p-1.5">
                    {agents.filter(a => a.desk === desk).map(a => (
                      <AgentPill key={a.id} agent={a} />
                    ))}
                  </div>
                </div>
              ))}

              <div className="flex items-center gap-1 pl-4 opacity-40">
                <ArrowRight className="w-3 h-3 text-muted-foreground" />
                <div className="h-px flex-1 bg-border/30" />
              </div>

              {/* P4: Back Office */}
              <PhaseLabel label="Phase 4 · Back Office" active={isPhaseActive("back_office")} done={isPhaseDone("back_office")} />
              <div className="pl-3 border-l-2 border-orange-500/20 ml-2">
                <AgentPill agent={agents.find(a => a.id === "back_office")!} />
              </div>
            </div>
          </ScrollArea>
        </div>

        {/* ── Right: Results + Log ─────────────────────────── */}
        <div className="flex flex-col flex-1 gap-3 min-h-0 min-w-0">

          {/* Idle splash */}
          {tickStatus === "idle" && (
            <div className="shrink-0 flex flex-col items-center justify-center gap-4 rounded-xl border border-dashed border-border/40 bg-card/20 py-12">
              <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center">
                <Bot className="w-8 h-8 text-primary/60" />
              </div>
              <div className="text-center">
                <h3 className="text-base font-semibold text-foreground mb-1">Hedge Fund Swarm Ready</h3>
                <p className="text-sm text-muted-foreground max-w-xs">
                  Select a fund and models, then click <strong>Run Tick</strong> to dispatch
                  14 AI agents through the Commander → Desk pipeline.
                </p>
              </div>
              <div className="flex gap-5 text-xs text-muted-foreground">
                <div className="flex items-center gap-1.5"><Brain className="w-3.5 h-3.5 text-rose-400" />Commander</div>
                <div className="flex items-center gap-1.5"><Sparkles className="w-3.5 h-3.5 text-violet-400" />3× Desks</div>
                <div className="flex items-center gap-1.5"><Shield className="w-3.5 h-3.5 text-orange-400" />Risk Bouncer</div>
              </div>
            </div>
          )}

          {/* Cancelled banner */}
          {tickStatus === "cancelled" && (
            <div className="shrink-0 rounded-lg border border-amber-500/30 bg-amber-500/5 px-4 py-3 flex items-center gap-3">
              <XCircle className="w-4 h-4 text-amber-400 shrink-0" />
              <div>
                <p className="text-sm font-medium text-amber-300">Tick Cancelled</p>
                <p className="text-xs text-muted-foreground">Agents stopped. API costs already incurred have been deducted.</p>
              </div>
            </div>
          )}

          {/* Error banner */}
          {tickStatus === "error" && (
            <div className="shrink-0 rounded-lg border border-red-500/30 bg-red-500/5 px-4 py-3 flex items-center gap-3">
              <AlertCircle className="w-4 h-4 text-red-400 shrink-0" />
              <div>
                <p className="text-sm font-medium text-red-300">Tick Failed</p>
                <p className="text-xs text-muted-foreground">Check logs. Common causes: missing ANTHROPIC_API_KEY, data not ingested, or API rate limit.</p>
              </div>
            </div>
          )}

          {/* Result summary cards */}
          {tickStatus === "complete" && tickResult && (
            <div className="shrink-0 grid grid-cols-2 lg:grid-cols-4 gap-3">
              {tickResult.macro_brief && (
                <div className="rounded-lg border border-border/40 bg-card/30 p-3 flex flex-col gap-1">
                  <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Macro Regime</span>
                  <span className={cn("text-sm font-semibold",
                    tickResult.macro_brief.macro_regime === "Risk-On"  ? "text-emerald-400" :
                    tickResult.macro_brief.macro_regime === "Risk-Off" ? "text-red-400" : "text-amber-400"
                  )}>{tickResult.macro_brief.macro_regime}</span>
                  <span className="text-[10px] text-muted-foreground">
                    VIX {tickResult.macro_brief.vix_level?.toFixed(1)} · TNX {tickResult.macro_brief.ten_year_yield?.toFixed(2)}%
                  </span>
                </div>
              )}
              {tickResult.commander_directive && (
                <div className="rounded-lg border border-border/40 bg-card/30 p-3 flex flex-col gap-1">
                  <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Deployed</span>
                  <span className="text-sm font-semibold text-foreground">
                    {(tickResult.commander_directive.total_deployed_pct * 100).toFixed(0)}%
                  </span>
                  <span className="text-[10px] text-muted-foreground">
                    {(tickResult.commander_directive.cash_reserve_pct * 100).toFixed(0)}% cash reserve
                  </span>
                </div>
              )}
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3 flex flex-col gap-1">
                <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Total API Cost</span>
                <span className="text-sm font-semibold text-amber-400 font-mono">${tickResult.api_cost_deducted_usd.toFixed(5)}</span>
                <span className="text-[10px] text-muted-foreground">
                  {((tickResult.total_token_cost?.input_tokens || 0) + (tickResult.total_token_cost?.output_tokens || 0)).toLocaleString()} tokens
                </span>
              </div>
              <div className="rounded-lg border border-border/40 bg-card/30 p-3 flex flex-col gap-1">
                <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Elapsed</span>
                <span className="text-sm font-semibold text-foreground">{tickResult.elapsed_seconds.toFixed(1)}s</span>
                <span className="text-[10px] text-muted-foreground">14 agents</span>
              </div>
            </div>
          )}

          {/* Desk strategy results */}
          {tickStatus === "complete" && (tickResult?.desk_results?.length ?? 0) > 0 && (
            <div className="shrink-0 grid grid-cols-3 gap-3">
              {tickResult!.desk_results.map(desk => (
                <div key={desk.desk_id} className="rounded-lg border border-violet-500/20 bg-violet-500/5 p-3 flex flex-col gap-1.5">
                  <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Desk {desk.desk_id}</span>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className="text-[10px] border-violet-500/40 text-violet-300">{desk.strategy_id}</Badge>
                    <CheckCircle2 className="w-3 h-3 text-emerald-400" />
                  </div>
                  <span className="text-[11px] text-muted-foreground font-mono">${desk.allocated_capital.toLocaleString()}</span>
                </div>
              ))}
            </div>
          )}

          {/* Commander reasoning */}
          {tickStatus === "complete" && tickResult?.commander_directive?.commander_reasoning && (
            <div className="shrink-0 rounded-lg border border-rose-500/20 bg-rose-500/5 p-3">
              <div className="flex items-center gap-2 mb-1.5">
                <Brain className="w-3.5 h-3.5 text-rose-400" />
                <span className="text-xs font-semibold text-rose-300">Commander Reasoning</span>
              </div>
              <p className="text-xs text-muted-foreground leading-relaxed">{tickResult.commander_directive.commander_reasoning}</p>
            </div>
          )}

          {/* ── Log Terminal (scrollable, grows to fill remaining space) ── */}
          <div className="flex-1 min-h-0 flex flex-col rounded-lg border border-border/40 bg-[#080810] overflow-hidden">
            <div className="flex items-center gap-2 px-3 py-2 border-b border-border/25 bg-card/10 shrink-0">
              <TerminalIcon className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold text-muted-foreground">Swarm Log</span>
              {isRunning && (
                <div className="flex items-center gap-1.5 ml-2">
                  <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                  <span className="text-[10px] text-emerald-400">streaming</span>
                </div>
              )}
              <span className="ml-auto text-[10px] text-muted-foreground/50">{logs.length} lines</span>
            </div>

            {/* The key: ScrollArea fills remaining height so logs stay contained */}
            <ScrollArea className="flex-1 min-h-0">
              <div className="p-3 font-mono text-[11px] space-y-px">
                {logs.length === 0 && !isRunning && (
                  <span className="text-muted-foreground/30 select-none">
                    Logs will appear here when a tick starts...
                  </span>
                )}
                {logs.length === 0 && isRunning && (
                  <span className="text-muted-foreground/50 animate-pulse select-none">
                    Initializing agents...
                  </span>
                )}
                {logs.map((log, i) => (
                  <div key={i} className="flex gap-2 leading-5">
                    <span className="text-muted-foreground/30 shrink-0 select-none">{log.ts}</span>
                    <span className={cn(
                      "shrink-0 w-[72px] truncate",
                      log.level === "ERROR"   ? "text-red-400/80" :
                      log.level === "WARNING" ? "text-amber-400/80" : "text-cyan-600/70"
                    )}>
                      [{(log.agent || "sys").slice(0, 8)}]
                    </span>
                    <span className={cn(
                      "break-all",
                      log.level === "ERROR"   ? "text-red-300" :
                      log.level === "WARNING" ? "text-amber-300" : "text-foreground/75"
                    )}>
                      {log.msg}
                    </span>
                  </div>
                ))}
                <div ref={logsEndRef} />
              </div>
            </ScrollArea>
          </div>
        </div>
      </div>
    </div>
  )
}
