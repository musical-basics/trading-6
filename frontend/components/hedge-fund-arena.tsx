"use client"

import { useState, useEffect, useRef, useCallback } from "react"
import {
  Bot, Brain, Shield, DollarSign, Play, Pause, RefreshCw,
  ChevronDown, ChevronRight, CheckCircle2, Clock, AlertCircle,
  Loader2, Zap, TrendingUp, TrendingDown, Users, BarChart2,
  Activity, Cpu, ArrowRight, Terminal as TerminalIcon,
  Sparkles, Target
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { cn } from "@/lib/utils"

// ─── Types ───────────────────────────────────────────────────
type TickStatus = "idle" | "running" | "complete" | "error"
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
  confidence?: number
  elapsedMs?: number
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

// ─── Agent DAG Definition ─────────────────────────────────────
const INITIAL_AGENTS: AgentCard[] = [
  { id: "consultant", label: "Market Consultant", role: "consultant", status: "pending" },
  { id: "auditor",    label: "Data Auditor",      role: "auditor",    status: "pending" },
  { id: "scout",      label: "Intel Scout",       role: "scout",      status: "pending" },
  { id: "commander",  label: "Commander (CEO)",   role: "commander",  status: "pending" },
  { id: "analyst_d1",   label: "Analyst",    role: "analyst",    desk: 1, status: "pending" },
  { id: "strategist_d1",label: "Strategist", role: "strategist", desk: 1, status: "pending" },
  { id: "pm_d1",        label: "PM",         role: "pm",         desk: 1, status: "pending" },
  { id: "analyst_d2",   label: "Analyst",    role: "analyst",    desk: 2, status: "pending" },
  { id: "strategist_d2",label: "Strategist", role: "strategist", desk: 2, status: "pending" },
  { id: "pm_d2",        label: "PM",         role: "pm",         desk: 2, status: "pending" },
  { id: "analyst_d3",   label: "Analyst",    role: "analyst",    desk: 3, status: "pending" },
  { id: "strategist_d3",label: "Strategist", role: "strategist", desk: 3, status: "pending" },
  { id: "pm_d3",        label: "PM",         role: "pm",         desk: 3, status: "pending" },
  { id: "back_office",  label: "Back Office", role: "back_office", status: "pending" },
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
      "relative flex flex-col gap-1 p-3 rounded-lg border transition-all duration-300",
      agent.status === "pending"  && "border-border/40 bg-card/30 opacity-60",
      agent.status === "running"  && "border-primary/60 bg-primary/5 shadow-[0_0_12px_rgba(99,102,241,0.15)] animate-pulse",
      agent.status === "complete" && "border-emerald-500/40 bg-emerald-500/5",
      agent.status === "error"    && "border-red-500/40 bg-red-500/5",
    )}>
      <div className="flex items-center gap-2">
        <div className={cn("p-1 rounded", colorClass)}>
          <Icon className="w-3 h-3" />
        </div>
        <span className="text-xs font-medium text-foreground">{agent.label}</span>
        <div className="ml-auto">
          {agent.status === "pending"  && <Clock className="w-3 h-3 text-muted-foreground/50" />}
          {agent.status === "running"  && <Loader2 className="w-3 h-3 text-primary animate-spin" />}
          {agent.status === "complete" && <CheckCircle2 className="w-3 h-3 text-emerald-400" />}
          {agent.status === "error"    && <AlertCircle className="w-3 h-3 text-red-400" />}
        </div>
      </div>
      {agent.status === "complete" && agent.costUsd !== undefined && (
        <div className="flex items-center gap-2 pt-0.5 border-t border-border/20">
          <span className="text-[10px] text-muted-foreground">
            {(agent.inputTokens || 0) + (agent.outputTokens || 0)} tok
          </span>
          <span className="text-[10px] text-amber-400">${agent.costUsd.toFixed(5)}</span>
          {agent.strategy && (
            <Badge variant="outline" className="text-[9px] px-1 py-0 h-4 border-violet-500/30 text-violet-300">
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
      !active && !done && "text-muted-foreground/50",
    )}>
      {done ? <CheckCircle2 className="w-3 h-3" /> : active ? <Zap className="w-3 h-3 animate-pulse" /> : <div className="w-3 h-3 rounded-full border border-current opacity-40" />}
      {label}
    </div>
  )
}

function CostTicker({ total, ticking }: { total: number; ticking: boolean }) {
  return (
    <div className={cn(
      "flex items-center gap-2 px-3 py-1.5 rounded-lg border text-xs font-mono tabular-nums",
      ticking ? "border-amber-500/40 bg-amber-500/5 text-amber-300" : "border-border/30 text-muted-foreground"
    )}>
      <DollarSign className="w-3.5 h-3.5" />
      <span>${total.toFixed(5)}</span>
      {ticking && <span className="text-[10px] text-amber-400/60">live</span>}
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
  const [totalCost, setTotalCost] = useState(0)
  const [elapsedSec, setElapsedSec] = useState(0)

  const pollRef = useRef<NodeJS.Timeout | null>(null)
  const logPollRef = useRef<NodeJS.Timeout | null>(null)
  const timerRef = useRef<NodeJS.Timeout | null>(null)
  const logsEndRef = useRef<HTMLDivElement>(null)

  // ── Load traders + models on mount ──────────────────────────
  useEffect(() => {
    fetch(`${API}/api/traders`)
      .then(r => r.json())
      .then(data => {
        if (Array.isArray(data)) setTraders(data)
      })
      .catch(() => {})

    fetch(`${API}/api/arena/models`)
      .then(r => r.json())
      .then(data => {
        const modelList: ModelOption[] = data?.models || []
        setModels(modelList)
        const opus = modelList.find(m => m.tier.includes("Opus"))
        const sonnet = modelList.find(m => m.tier.includes("Sonnet"))
        if (opus) setSelectedCommanderModel(opus.id)
        if (sonnet) setSelectedWorkerModel(sonnet.id)
      })
      .catch(() => {
        // Fallback models
        const fallback: ModelOption[] = [
          { id: "claude-opus-4-6-20251001",  display_name: "Claude Opus 4.6",  tier: "Opus (Most Capable)",  default_role: "commander" },
          { id: "claude-sonnet-4-6-20251001",display_name: "Claude Sonnet 4.6",tier: "Sonnet (Balanced)",     default_role: "strategist" },
          { id: "claude-haiku-4-5-20251001", display_name: "Claude Haiku 4.5", tier: "Haiku (Fastest)",       default_role: "analyst" },
        ]
        setModels(fallback)
        setSelectedCommanderModel(fallback[0].id)
        setSelectedWorkerModel(fallback[1].id)
      })
  }, [])

  // ── Scroll logs to bottom ────────────────────────────────────
  useEffect(() => {
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" })
    }
  }, [logs])

  // ── Cleanup on unmount ───────────────────────────────────────
  useEffect(() => {
    return () => {
      if (pollRef.current) clearInterval(pollRef.current)
      if (logPollRef.current) clearInterval(logPollRef.current)
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }, [])

  // ── Apply event updates to agent cards ──────────────────────
  const applyEvent = useCallback((type: string, data: Record<string, unknown>) => {
    if (type === "arena.agent_started") {
      const agentName = data.agent_name as string
      const phase = data.phase as string
      setCurrentPhase(phase as Phase)
      setAgents(prev => prev.map(a =>
        a.id === agentName ? { ...a, status: "running" } : a
      ))
    } else if (type === "arena.agent_completed") {
      const agentName = data.agent_name as string
      const tokens = data.tokens as {input: number; output: number} | undefined
      setAgents(prev => prev.map(a =>
        a.id === agentName ? {
          ...a,
          status: "complete",
          inputTokens: tokens?.input,
          outputTokens: tokens?.output,
          costUsd: data.cost_usd as number,
          strategy: data.strategy as string | undefined,
        } : a
      ))
      setTotalCost(prev => prev + ((data.cost_usd as number) || 0))
    } else if (type === "arena.phase_completed") {
      const phase = data.phase_name as string
      setCurrentPhase(phase as Phase)
    } else if (type === "arena.tick_completed") {
      setCurrentPhase("done")
      setTickStatus("complete")
    }
  }, [])

  // ── Poll tick status ─────────────────────────────────────────
  const pollStatus = useCallback((id: string) => {
    pollRef.current = setInterval(async () => {
      try {
        const r = await fetch(`${API}/api/arena/tick/${id}/status`)
        const data = await r.json()

        if (data.current_agent) {
          setAgents(prev => prev.map(a =>
            a.id === data.current_agent ? { ...a, status: "running" } :
            data.completed_agents?.includes(a.id) ? { ...a, status: "complete" } : a
          ))
        }
        if (data.phase) setCurrentPhase(data.phase as Phase)
        if (data.elapsed_seconds) setElapsedSec(data.elapsed_seconds)

        if (data.status === "complete") {
          clearInterval(pollRef.current!)
          if (timerRef.current) clearInterval(timerRef.current!)
          setTickStatus("complete")
          setCurrentPhase("done")
          if (data.result) {
            const result = data.result as TickResult
            setTickResult(result)
            setTotalCost(result.api_cost_deducted_usd || result.total_token_cost?.estimated_cost_usd || 0)
            // Mark all agents complete
            setAgents(prev => prev.map(a => ({ ...a, status: "complete" as AgentStatus })))
          }
        } else if (data.status === "error") {
          clearInterval(pollRef.current!)
          if (timerRef.current) clearInterval(timerRef.current!)
          setTickStatus("error")
        }
      } catch {}
    }, 1000)
  }, [])

  // ── Poll logs ────────────────────────────────────────────────
  const pollLogs = useCallback(() => {
    logPollRef.current = setInterval(async () => {
      try {
        const r = await fetch(`${API}/api/arena/logs?since=${logOffset}`)
        const data = await r.json()
        if (data.logs?.length) {
          setLogs(prev => [...prev, ...data.logs])
          setLogOffset(prev => prev + data.logs.length)
        }
        if (!data.running && tickStatus !== "running") {
          clearInterval(logPollRef.current!)
        }
      } catch {}
    }, 800)
  }, [logOffset, tickStatus])

  // ── Start a tick ─────────────────────────────────────────────
  const handleRunTick = async () => {
    if (tickStatus === "running") return

    // Reset state
    setTickStatus("running")
    setTickResult(null)
    setTotalCost(0)
    setElapsedSec(0)
    setLogs([])
    setLogOffset(0)
    setCurrentPhase("data_fetch")
    setAgents(INITIAL_AGENTS.map(a => ({ ...a, status: "pending" })))

    // Start elapsed timer
    const startTime = Date.now()
    timerRef.current = setInterval(() => {
      setElapsedSec((Date.now() - startTime) / 1000)
    }, 500)

    const modelOverrides: Record<string, string> = {}
    if (selectedCommanderModel) modelOverrides["commander"] = selectedCommanderModel
    if (selectedWorkerModel) {
      modelOverrides["strategist"] = selectedWorkerModel
      modelOverrides["consultant"] = selectedWorkerModel
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
        pollStatus(data.tick_id)
        pollLogs()
      } else {
        setTickStatus("error")
      }
    } catch (e) {
      setTickStatus("error")
      if (timerRef.current) clearInterval(timerRef.current)
    }
  }

  // ── Computed phase visibility ────────────────────────────────
  const phaseOrder: Phase[] = ["data_fetch", "consultants", "commander", "desks", "back_office", "done"]
  const phaseIndex = currentPhase ? phaseOrder.indexOf(currentPhase) : -1
  const isPhaseActive = (p: Phase) => currentPhase === p
  const isPhaseDone = (p: Phase) => phaseIndex > phaseOrder.indexOf(p)

  const currentFund = traders.find(t => t.id === traderId)

  // ─────────────────────────────────────────────────────────────
  return (
    <div className="flex flex-col gap-4 h-full">
      {/* ── Header Controls ───────────────────────────────────── */}
      <div className="flex flex-wrap items-start gap-3">
        {/* Fund selector */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Fund</label>
          <div className="relative">
            <select
              value={traderId}
              onChange={e => setTraderId(Number(e.target.value))}
              disabled={tickStatus === "running"}
              className="h-9 pl-3 pr-8 text-sm bg-card border border-border rounded-md text-foreground focus:outline-none focus:border-primary disabled:opacity-50 cursor-pointer appearance-none"
            >
              {traders.length > 0
                ? traders.map(t => (
                    <option key={t.id} value={t.id}>
                      {t.name} (${(t.total_capital / 1000).toFixed(1)}K)
                    </option>
                  ))
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
              disabled={tickStatus === "running"}
              className="h-9 pl-3 pr-8 text-sm bg-card border border-border rounded-md text-foreground focus:outline-none focus:border-primary disabled:opacity-50 cursor-pointer appearance-none"
            >
              {models.map(m => (
                <option key={m.id} value={m.id}>{m.display_name} · {m.tier}</option>
              ))}
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
              disabled={tickStatus === "running"}
              className="h-9 pl-3 pr-8 text-sm bg-card border border-border rounded-md text-foreground focus:outline-none focus:border-primary disabled:opacity-50 cursor-pointer appearance-none"
            >
              {models.map(m => (
                <option key={m.id} value={m.id}>{m.display_name} · {m.tier}</option>
              ))}
            </select>
            <ChevronDown className="absolute right-2 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted-foreground pointer-events-none" />
          </div>
        </div>

        {/* Dry run toggle */}
        <div className="flex flex-col gap-1">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Mode</label>
          <button
            onClick={() => setDryRun(d => !d)}
            disabled={tickStatus === "running"}
            className={cn(
              "h-9 px-3 text-sm rounded-md border transition-colors",
              dryRun
                ? "border-amber-500/50 bg-amber-500/10 text-amber-300"
                : "border-border bg-card text-muted-foreground hover:text-foreground"
            )}
          >
            {dryRun ? "Dry Run" : "Live"}
          </button>
        </div>

        {/* Spacer */}
        <div className="flex-1" />

        {/* Cost ticker */}
        <div className="flex flex-col gap-1 items-end">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">API Cost</label>
          <CostTicker total={totalCost} ticking={tickStatus === "running"} />
        </div>

        {/* Elapsed */}
        {(tickStatus === "running" || tickStatus === "complete") && (
          <div className="flex flex-col gap-1 items-end">
            <label className="text-[10px] text-muted-foreground uppercase tracking-wider">Elapsed</label>
            <div className="flex items-center gap-1.5 h-9 px-3 rounded-lg border border-border/30 text-xs font-mono text-muted-foreground">
              <Clock className="w-3 h-3" />
              {elapsedSec.toFixed(1)}s
            </div>
          </div>
        )}

        {/* Run button */}
        <div className="flex flex-col gap-1 items-end">
          <label className="text-[10px] text-muted-foreground uppercase tracking-wider">&nbsp;</label>
          <Button
            onClick={handleRunTick}
            disabled={tickStatus === "running"}
            className={cn(
              "h-9 gap-2 font-semibold transition-all",
              tickStatus === "running"
                ? "bg-primary/20 text-primary border border-primary/40"
                : tickStatus === "complete"
                ? "bg-emerald-600 hover:bg-emerald-500"
                : "bg-primary hover:bg-primary/90"
            )}
          >
            {tickStatus === "running" ? (
              <><Loader2 className="w-4 h-4 animate-spin" /> Running...</>
            ) : tickStatus === "complete" ? (
              <><RefreshCw className="w-4 h-4" /> Run Again</>
            ) : (
              <><Play className="w-4 h-4" /> Run Tick</>
            )}
          </Button>
        </div>
      </div>

      {/* ── Content Grid ──────────────────────────────────────── */}
      <div className="flex gap-4 flex-1 min-h-0">

        {/* ── Left: Agent Pipeline DAG ────────────────────────── */}
        <div className="flex flex-col gap-3 w-72 shrink-0">
          <div className="flex items-center gap-2">
            <Cpu className="w-4 h-4 text-primary" />
            <span className="text-sm font-semibold">Agent Pipeline</span>
          </div>

          {/* Phase 0: Data */}
          <PhaseLabel label="Phase 0 · Data Fetch" active={isPhaseActive("data_fetch")} done={isPhaseDone("data_fetch")} />

          {/* Phase 1: Consultants */}
          <PhaseLabel label="Phase 1 · C-Suite" active={isPhaseActive("consultants")} done={isPhaseDone("consultants")} />
          <div className="grid grid-cols-1 gap-1.5 pl-3 border-l-2 border-border/30 ml-2">
            {agents.filter(a => ["consultant","auditor","scout"].includes(a.id)).map(a => (
              <AgentPill key={a.id} agent={a} />
            ))}
          </div>

          {/* Flow arrow */}
          <div className="flex items-center gap-1.5 pl-4">
            <ArrowRight className="w-3.5 h-3.5 text-muted-foreground/40" />
            <div className="h-px flex-1 bg-border/30" />
          </div>

          {/* Phase 2: Commander */}
          <PhaseLabel label="Phase 2 · Commander" active={isPhaseActive("commander")} done={isPhaseDone("commander")} />
          <div className="pl-3 border-l-2 border-rose-500/20 ml-2">
            <AgentPill key="commander" agent={agents.find(a => a.id === "commander")!} />
          </div>

          {/* Flow arrow */}
          <div className="flex items-center gap-1.5 pl-4">
            <ArrowRight className="w-3.5 h-3.5 text-muted-foreground/40" />
            <div className="h-px flex-1 bg-border/30" />
          </div>

          {/* Phase 3: Desks */}
          <PhaseLabel label="Phase 3 · Trading Desks" active={isPhaseActive("desks")} done={isPhaseDone("desks")} />
          {[1,2,3].map(desk => (
            <div key={desk} className="rounded-lg border border-border/30 bg-card/20 overflow-hidden">
              <div className="px-2 py-1 bg-muted/10 border-b border-border/20 text-[10px] font-semibold text-muted-foreground uppercase tracking-wider">
                Desk {desk}
              </div>
              <div className="flex flex-col gap-1 p-1.5">
                {agents.filter(a => a.desk === desk).map(a => (
                  <AgentPill key={a.id} agent={a} />
                ))}
              </div>
            </div>
          ))}

          {/* Flow arrow */}
          <div className="flex items-center gap-1.5 pl-4">
            <ArrowRight className="w-3.5 h-3.5 text-muted-foreground/40" />
            <div className="h-px flex-1 bg-border/30" />
          </div>

          {/* Phase 4: Back Office */}
          <PhaseLabel label="Phase 4 · Back Office" active={isPhaseActive("back_office")} done={isPhaseDone("back_office")} />
          <div className="pl-3 border-l-2 border-orange-500/20 ml-2">
            <AgentPill key="back_office" agent={agents.find(a => a.id === "back_office")!} />
          </div>
        </div>

        {/* ── Right: Results + Logs ────────────────────────────── */}
        <div className="flex flex-col flex-1 gap-3 min-w-0">

          {/* Idle state */}
          {tickStatus === "idle" && (
            <div className="flex-1 flex flex-col items-center justify-center gap-4 rounded-xl border border-dashed border-border/40 bg-card/20">
              <div className="w-16 h-16 rounded-full bg-primary/10 flex items-center justify-center">
                <Bot className="w-8 h-8 text-primary/60" />
              </div>
              <div className="text-center">
                <h3 className="text-base font-semibold text-foreground mb-1">Hedge Fund Swarm Ready</h3>
                <p className="text-sm text-muted-foreground max-w-xs">
                  Select a fund and models, then click <strong>Run Tick</strong> to dispatch
                  <br />13 AI agents across the Commander → Desk pipeline.
                </p>
              </div>
              <div className="flex gap-4 text-xs text-muted-foreground">
                <div className="flex items-center gap-1.5"><Brain className="w-3.5 h-3.5 text-rose-400" />Commander (CEO)</div>
                <div className="flex items-center gap-1.5"><Sparkles className="w-3.5 h-3.5 text-violet-400" />3× Strategist</div>
                <div className="flex items-center gap-1.5"><Shield className="w-3.5 h-3.5 text-orange-400" />Risk Bouncer</div>
              </div>
            </div>
          )}

          {/* Result cards — shown when complete */}
          {tickStatus === "complete" && tickResult && (
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
              {/* Macro regime */}
              {tickResult.macro_brief && (
                <div className="rounded-lg border border-border/40 bg-card/30 p-3 flex flex-col gap-1">
                  <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Macro Regime</span>
                  <span className={cn(
                    "text-sm font-semibold",
                    tickResult.macro_brief.macro_regime === "Risk-On" ? "text-emerald-400" :
                    tickResult.macro_brief.macro_regime === "Risk-Off" ? "text-red-400" : "text-amber-400"
                  )}>
                    {tickResult.macro_brief.macro_regime}
                  </span>
                  <span className="text-[10px] text-muted-foreground">
                    VIX {tickResult.macro_brief.vix_level?.toFixed(1)} · TNX {tickResult.macro_brief.ten_year_yield?.toFixed(2)}%
                  </span>
                </div>
              )}
              {/* Deployment */}
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
              {/* Cost */}
              <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 p-3 flex flex-col gap-1">
                <span className="text-[10px] text-muted-foreground uppercase tracking-wider">API Cost</span>
                <span className="text-sm font-semibold text-amber-400 font-mono">
                  ${tickResult.api_cost_deducted_usd.toFixed(5)}
                </span>
                <span className="text-[10px] text-muted-foreground">
                  {(tickResult.total_token_cost?.input_tokens || 0) + (tickResult.total_token_cost?.output_tokens || 0)} tokens
                </span>
              </div>
              {/* Elapsed */}
              <div className="rounded-lg border border-border/40 bg-card/30 p-3 flex flex-col gap-1">
                <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Elapsed</span>
                <span className="text-sm font-semibold text-foreground">{tickResult.elapsed_seconds.toFixed(1)}s</span>
                <span className="text-[10px] text-muted-foreground">across 13 agents</span>
              </div>
            </div>
          )}

          {/* Desk strategies — shown when complete */}
          {tickStatus === "complete" && tickResult?.desk_results?.length && (
            <div className="grid grid-cols-3 gap-3">
              {tickResult.desk_results.map(desk => (
                <div key={desk.desk_id} className="rounded-lg border border-violet-500/20 bg-violet-500/5 p-3 flex flex-col gap-1">
                  <span className="text-[10px] text-muted-foreground uppercase tracking-wider">Desk {desk.desk_id}</span>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline" className="text-[10px] border-violet-500/40 text-violet-300">
                      {desk.strategy_id}
                    </Badge>
                    <CheckCircle2 className="w-3 h-3 text-emerald-400" />
                  </div>
                  <span className="text-[10px] text-muted-foreground font-mono">
                    ${desk.allocated_capital.toLocaleString()}
                  </span>
                </div>
              ))}
            </div>
          )}

          {/* Commander reasoning */}
          {tickStatus === "complete" && tickResult?.commander_directive?.commander_reasoning && (
            <div className="rounded-lg border border-rose-500/20 bg-rose-500/5 p-3">
              <div className="flex items-center gap-2 mb-1.5">
                <Brain className="w-3.5 h-3.5 text-rose-400" />
                <span className="text-xs font-semibold text-rose-300">Commander Reasoning</span>
              </div>
              <p className="text-xs text-muted-foreground leading-relaxed">
                {tickResult.commander_directive.commander_reasoning}
              </p>
            </div>
          )}

          {/* Error state */}
          {tickStatus === "error" && (
            <div className="rounded-lg border border-red-500/30 bg-red-500/5 p-4 flex items-start gap-3">
              <AlertCircle className="w-5 h-5 text-red-400 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-red-300">Tick Failed</p>
                <p className="text-xs text-muted-foreground mt-1">
                  Check the logs below. Common causes: missing ANTHROPIC_API_KEY, data pipeline not run, or network error.
                </p>
              </div>
            </div>
          )}

          {/* Live Log Stream */}
          <div className="flex-1 min-h-0 flex flex-col rounded-lg border border-border/40 bg-[#0a0a0f] overflow-hidden">
            <div className="flex items-center gap-2 px-3 py-2 border-b border-border/30 bg-card/20">
              <TerminalIcon className="w-3.5 h-3.5 text-muted-foreground" />
              <span className="text-xs font-semibold text-muted-foreground">Swarm Log</span>
              {tickStatus === "running" && (
                <div className="flex items-center gap-1 ml-auto">
                  <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                  <span className="text-[10px] text-emerald-400">live</span>
                </div>
              )}
              {logs.length > 0 && (
                <span className="text-[10px] text-muted-foreground ml-auto">{logs.length} lines</span>
              )}
            </div>
            <ScrollArea className="flex-1">
              <div className="p-3 font-mono text-[11px] space-y-0.5">
                {logs.length === 0 && tickStatus === "idle" && (
                  <span className="text-muted-foreground/40">Logs will appear here when tick starts...</span>
                )}
                {logs.length === 0 && tickStatus === "running" && (
                  <span className="text-muted-foreground/60 animate-pulse">Waiting for agents to initialize...</span>
                )}
                {logs.map((log, i) => (
                  <div key={i} className="flex gap-2 leading-5">
                    <span className="text-muted-foreground/40 shrink-0">{log.ts}</span>
                    <span className={cn(
                      "shrink-0 w-14",
                      log.level === "ERROR" ? "text-red-400" :
                      log.level === "WARNING" ? "text-amber-400" : "text-emerald-400/70"
                    )}>[{log.agent?.slice(0, 8)}]</span>
                    <span className={cn(
                      "break-all",
                      log.level === "ERROR" ? "text-red-300" :
                      log.level === "WARNING" ? "text-amber-300" : "text-foreground/80"
                    )}>{log.msg}</span>
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
