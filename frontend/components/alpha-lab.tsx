"use client"

import { useState, useEffect, useCallback, useRef } from "react"
import {
  fetchAlphaExperiments,
  fetchAlphaExperiment,
  fetchFailedGenerations,
  deleteFailedGeneration,
  generateAlphaStrategy,
  getSwarmStreamUrl,
  saveSwarmResult,
  runAlphaBacktest,
  deleteAlphaExperiment,
  updateAlphaCode,
  updateAlphaName,
  promoteAlphaExperiment,
  combineAlphaStrategies,
  runStandaloneBacktest,
  saveStandaloneExperiment,
  getEditorSetting,
  saveEditorSetting,
  type AlphaExperiment,
  type FailedGeneration,
  type AlphaEquityPoint,
} from "@/lib/api"
import Editor from "@monaco-editor/react"
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts"

const TIER_CONFIG = {
  haiku: {
    label: "Haiku",
    sublabel: "Fast · Low Cost",
    color: "#22d3ee",
    inputCost: "$1.00/M",
    outputCost: "$5.00/M",
    icon: "⚡",
  },
  sonnet: {
    label: "Sonnet",
    sublabel: "Balanced",
    color: "#a78bfa",
    inputCost: "$3.00/M",
    outputCost: "$15.00/M",
    icon: "🎯",
  },
  opus: {
    label: "Opus",
    sublabel: "Premium Quality",
    color: "#f59e0b",
    inputCost: "$15.00/M",
    outputCost: "$75.00/M",
    icon: "👑",
  },
} as const

type TierKey = keyof typeof TIER_CONFIG
type Tab = "generate" | "swarm" | "results" | "failed" | "backtest"

const DEFAULT_STANDALONE_CODE = `import polars as pl
import numpy as np

def execute(data: pl.DataFrame) -> pl.DataFrame:
    """
    Standalone Backtest Execution Sandbox.
    Input: DataFrame with 'date', 'entity_id', 'adj_close' and features.
    Output: DataFrame with 'raw_weight_X' column defining portfolio weights.
    """
    print("Running standalone logic...")
    return data.with_columns(
        pl.lit(1.0).alias("raw_weight_equal")
    )`

const SWARM_AGENT_ROLES = [
  {
    id: "researcher",
    label: "Researcher",
    icon: "🔬",
    color: "text-blue-400",
    borderColor: "border-blue-500/30",
    bgColor: "bg-blue-500/5",
    description: "Proposes strategy hypotheses and selects alpha signals.",
  },
  {
    id: "risk_manager",
    label: "Risk Manager",
    icon: "🛡️",
    color: "text-amber-400",
    borderColor: "border-amber-500/30",
    bgColor: "bg-amber-500/5",
    description: "Enforces drawdown limits, regime filters, and concentration rules.",
  },
  {
    id: "developer",
    label: "Developer",
    icon: "💻",
    color: "text-emerald-400",
    borderColor: "border-emerald-500/30",
    bgColor: "bg-emerald-500/5",
    description: "Implements the finalized strategy in Polars code.",
  },
] as const

const STATUS_BADGES: Record<string, { bg: string; text: string; label: string }> = {
  generated: { bg: "bg-blue-500/20", text: "text-blue-400", label: "Generated" },
  backtesting: { bg: "bg-yellow-500/20", text: "text-yellow-400", label: "Backtesting…" },
  passed: { bg: "bg-emerald-500/20", text: "text-emerald-400", label: "Passed" },
  promoted: { bg: "bg-cyan-500/20", text: "text-cyan-400", label: "Promoted" },
  failed: { bg: "bg-red-500/20", text: "text-red-400", label: "Failed" },
  error: { bg: "bg-orange-500/20", text: "text-orange-400", label: "Error" },
}

const SWARM_TIER_ABBR_TO_KEY: Record<string, TierKey> = {
  hai: "haiku",
  son: "sonnet",
  opu: "opus",
}

function isSwarmExperiment(exp: AlphaExperiment): boolean {
  return (exp.model_tier || "").startsWith("swarm/")
}

function parseSwarmModelConfig(modelTier?: string): { researcher?: TierKey; risk_manager?: TierKey; developer?: TierKey } {
  if (!modelTier?.startsWith("swarm/")) return {}
  const raw = modelTier.replace("swarm/", "")
  const parts = raw.split("-")

  if (parts.length === 3) {
    return {
      researcher: SWARM_TIER_ABBR_TO_KEY[parts[0]] || undefined,
      risk_manager: SWARM_TIER_ABBR_TO_KEY[parts[1]] || undefined,
      developer: SWARM_TIER_ABBR_TO_KEY[parts[2]] || undefined,
    }
  }

  const single = raw as TierKey
  if (single in TIER_CONFIG) {
    return { researcher: single, risk_manager: single, developer: single }
  }

  return {}
}

export default function AlphaLab() {
  const [activeTab, setActiveTab] = useState<Tab>("generate")
  const [experiments, setExperiments] = useState<AlphaExperiment[]>([])
  const [failedGenerations, setFailedGenerations] = useState<FailedGeneration[]>([])
  const [selectedFailed, setSelectedFailed] = useState<FailedGeneration | null>(null)
  const [selectedExp, setSelectedExp] = useState<AlphaExperiment | null>(null)
  const [prompt, setPrompt] = useState("")
  const [selectedTier, setSelectedTier] = useState<TierKey>("sonnet")
  const [strategyStyle, setStrategyStyle] = useState<"academic" | "hedge_fund">("academic")
  const [generating, setGenerating] = useState(false)
  const [backtesting, setBacktesting] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [editedCode, setEditedCode] = useState<string>("")
  const [isEditing, setIsEditing] = useState(false)
  const [isRenaming, setIsRenaming] = useState(false)
  const [newName, setNewName] = useState("")
  const [saving, setSaving] = useState(false)
  const [promoting, setPromoting] = useState(false)
  const [combineMode, setCombineMode] = useState(false)
  const [selectedForCombine, setSelectedForCombine] = useState<Set<string>>(new Set())
  const [combining, setCombining] = useState(false)
  const [combineTier, setCombineTier] = useState<TierKey>("sonnet")
  const [generatingSwarm, setGeneratingSwarm] = useState(false)
  const swarmAbortRef = useRef<AbortController | null>(null)
  type SwarmLogEntry = { agent: string; label: string; status: "running" | "done" | "error"; preview?: string; tokens?: number }
  const [swarmLogs, setSwarmLogs] = useState<SwarmLogEntry[]>([])
  // Swarm config state — initialized from DB via useEffect
  const [swarmAgentTiers, setSwarmAgentTiers] = useState<Record<string, TierKey>>({ researcher: "haiku", risk_manager: "haiku", developer: "sonnet" })
  const [swarmAgentNotes, setSwarmAgentNotes] = useState<Record<string, string>>({ researcher: "", risk_manager: "", developer: "" })
  const [configSaved, setConfigSaved] = useState(false)

  useEffect(() => {
    getEditorSetting("swarm_agent_tiers").then(data => {
      if (data) setSwarmAgentTiers(data)
    }).catch(() => {})
    
    getEditorSetting("swarm_agent_notes").then(data => {
      if (data) setSwarmAgentNotes(data)
    }).catch(() => {})
  }, [])

  // Standalone Backtester state
  const [standaloneCode, setStandaloneCode] = useState(DEFAULT_STANDALONE_CODE)
  const [runningStandalone, setRunningStandalone] = useState(false)
  const [savingStandalone, setSavingStandalone] = useState(false)
  const [standaloneResult, setStandaloneResult] = useState<{
    metrics?: any;
    equity_curve?: any[];
    error?: string;
  } | null>(null)

  const loadExperiments = useCallback(async () => {
    const data = await fetchAlphaExperiments()
    setExperiments(data)
  }, [])

  const loadFailedGenerations = useCallback(async () => {
    const data = await fetchFailedGenerations()
    setFailedGenerations(data)
  }, [])

  useEffect(() => {
    loadExperiments()
    loadFailedGenerations()
  }, [loadExperiments, loadFailedGenerations])

  const handleSaveSwarmConfig = async () => {
    try {
      setConfigSaved(false)
      await saveEditorSetting("swarm_agent_tiers", swarmAgentTiers)
      await saveEditorSetting("swarm_agent_notes", swarmAgentNotes)
      setConfigSaved(true)
      setTimeout(() => setConfigSaved(false), 2000)
    } catch { /* ignore */ }
  }

  const handleGenerate = async () => {
    setGenerating(true)
    setError(null)
    try {
      const result = await generateAlphaStrategy(prompt, selectedTier, strategyStyle)
      if (result.error) {
        setError(result.error)
        await loadFailedGenerations()
      } else {
        setPrompt("")
        await loadExperiments()
        if (result.experiment_id) {
          const exp = await fetchAlphaExperiment(result.experiment_id)
          if (exp) {
            setSelectedExp(exp)
            setActiveTab("results")
          }
        }
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Generation failed")
    } finally {
      setGenerating(false)
    }
  }

  const handleGenerateSwarm = async () => {
    setGeneratingSwarm(true)
    setSwarmLogs([])
    setError(null)
    const ctrl = new AbortController()
    swarmAbortRef.current = ctrl

    const url = getSwarmStreamUrl(prompt, strategyStyle, swarmAgentTiers, swarmAgentNotes)
    try {
      const res = await fetch(url, { signal: ctrl.signal })
      if (!res.body) throw new Error("No stream body")
      const reader = res.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ""

      while (true) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split("\n")
        buffer = lines.pop() ?? ""
        for (const line of lines) {
          if (!line.startsWith("data: ")) continue
          const evt = JSON.parse(line.slice(6))

          if (evt.type === "start") {
            setSwarmLogs(prev => [...prev, { agent: evt.agent, label: evt.label, status: "running" }])
          } else if (evt.type === "done") {
            setSwarmLogs(prev => prev.map(l =>
              l.agent === evt.agent ? { ...l, label: evt.label, status: "done", tokens: evt.tokens, preview: evt.preview } : l
            ))
          } else if (evt.type === "result") {
            // Save to backend
            const saved = await saveSwarmResult({
              name: evt.name,
              hypothesis: prompt || "(swarm-generated)",
              rationale: evt.rationale,
              code: evt.code,
              model_tier: evt.model_tier,
              input_tokens: evt.input_tokens,
              output_tokens: evt.output_tokens,
              cost_usd: evt.cost_usd,
            })
            setPrompt("")
            await loadExperiments()
            if (saved.experiment_id) {
              const exp = await fetchAlphaExperiment(saved.experiment_id)
              if (exp) { setSelectedExp(exp); setActiveTab("results") }
            } else if (saved.error) {
              setError(saved.error)
              await loadFailedGenerations()
            }
          } else if (evt.type === "error") {
            setSwarmLogs(prev => [...prev, { agent: "system", label: `❌ ${evt.message}`, status: "error" }])
            setError(evt.message)
          }
        }
      }
    } catch (e: unknown) {
      if ((e as Error).name === "AbortError") {
        setSwarmLogs(prev => [...prev, { agent: "system", label: "⛔ Swarm killed by user", status: "error" }])
      } else {
        setError(e instanceof Error ? e.message : "Swarm stream failed")
      }
    } finally {
      setGeneratingSwarm(false)
      swarmAbortRef.current = null
    }
  }

  const handleKillSwarm = () => {
    swarmAbortRef.current?.abort()
  }

  const handleBacktest = async (experimentId: string) => {
    setBacktesting(experimentId)
    try {
      const result = await runAlphaBacktest(experimentId)
      await loadExperiments()
      if (result.error) {
        setError(result.error)
      }
      const updated = await fetchAlphaExperiment(experimentId)
      if (updated) setSelectedExp(updated)
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Backtest failed")
    } finally {
      setBacktesting(null)
    }
  }

  const handleDelete = async (experimentId: string) => {
    await deleteAlphaExperiment(experimentId)
    if (selectedExp?.experiment_id === experimentId) setSelectedExp(null)
    await loadExperiments()
  }

  const handlePromote = async (experimentId: string) => {
    setPromoting(true)
    setError(null)
    try {
      const result = await promoteAlphaExperiment(experimentId)
      if (result.error) {
        setError(result.error)
      } else {
        await loadExperiments()
        const updated = await fetchAlphaExperiment(experimentId)
        if (updated) setSelectedExp(updated)
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Promote failed")
    } finally {
      setPromoting(false)
    }
  }

  const toggleCombineSelect = (experimentId: string) => {
    setSelectedForCombine(prev => {
      const next = new Set(prev)
      if (next.has(experimentId)) {
        next.delete(experimentId)
      } else if (next.size < 5) {
        next.add(experimentId)
      }
      return next
    })
  }

  const handleCombine = async () => {
    if (selectedForCombine.size < 2) return
    setCombining(true)
    setError(null)
    try {
      const result = await combineAlphaStrategies(
        Array.from(selectedForCombine),
        combineTier,
      )
      if (result.error) {
        setError(result.error)
      } else {
        setCombineMode(false)
        setSelectedForCombine(new Set())
        await loadExperiments()
        if (result.experiment_id) {
          const exp = await fetchAlphaExperiment(result.experiment_id)
          if (exp) {
            setSelectedExp(exp)
            setActiveTab("results")
          }
        }
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Combine failed")
    } finally {
      setCombining(false)
    }
  }

  const handleSelectExp = async (exp: AlphaExperiment) => {
    const full = await fetchAlphaExperiment(exp.experiment_id)
    if (full) {
      setSelectedExp(full)
      setEditedCode(full.strategy_code || "")
      setIsEditing(false)
    }
  }

  const handleSaveCode = async () => {
    if (!selectedExp) return
    setSaving(true)
    try {
      await updateAlphaCode(selectedExp.experiment_id, editedCode)
      // Reload to reflect updated code + status reset to 'generated'
      const updated = await fetchAlphaExperiment(selectedExp.experiment_id)
      if (updated) {
        setSelectedExp(updated)
        setIsEditing(false)
      }
      await loadExperiments()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Save failed")
    } finally {
      setSaving(false)
    }
  }

  const handleSaveAndBacktest = async () => {
    if (!selectedExp) return
    setSaving(true)
    setBacktesting(selectedExp.experiment_id)
    try {
      // Save code first
      await updateAlphaCode(selectedExp.experiment_id, editedCode)
      setIsEditing(false)
      // Then run backtest
      const result = await runAlphaBacktest(selectedExp.experiment_id)
      await loadExperiments()
      if (result.error) setError(result.error)
      const updated = await fetchAlphaExperiment(selectedExp.experiment_id)
      if (updated) {
        setSelectedExp(updated)
        setEditedCode(updated.strategy_code || "")
      }
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Save & backtest failed")
    } finally {
      setSaving(false)
      setBacktesting(null)
    }
  }

  const handleRename = async () => {
    if (!selectedExp || !newName.trim()) return
    setError(null)
    try {
      await updateAlphaName(selectedExp.experiment_id, newName.trim())
      setSelectedExp({ ...selectedExp, strategy_name: newName.trim() })
      setIsRenaming(false)
      await loadExperiments()
    } catch (e: unknown) {
      setError(e instanceof Error ? e.message : "Rename failed")
    }
  }

  const handleRunStandalone = async () => {
    setRunningStandalone(true)
    setStandaloneResult(null)
    setError(null)
    try {
      const res = await runStandaloneBacktest(standaloneCode)
      setStandaloneResult(res)
    } catch (e: unknown) {
      setStandaloneResult({ error: e instanceof Error ? e.message : "Run failed" })
    } finally {
      setRunningStandalone(false)
    }
  }

  const handleSaveStandalone = async () => {
    if (!standaloneCode) return
    setSavingStandalone(true)
    setError(null)
    try {
      const res = await saveStandaloneExperiment(standaloneCode)
      if (res.error) {
        setStandaloneResult({ ...standaloneResult, error: res.error })
      } else {
        await loadExperiments()
        if (res.experiment_id) {
          const exp = await fetchAlphaExperiment(res.experiment_id)
          if (exp) {
            setSelectedExp(exp)
            setActiveTab("results")
            // Optionally, tell the backend to run standard backtest to fill metrics
            await runAlphaBacktest(res.experiment_id)
            await loadExperiments()
            const updated = await fetchAlphaExperiment(res.experiment_id)
            if (updated) setSelectedExp(updated)
          }
        }
      }
    } catch (e: unknown) {
      setStandaloneResult({ ...standaloneResult, error: e instanceof Error ? e.message : "Save failed" })
    } finally {
      setSavingStandalone(false)
    }
  }

  const totalCost = experiments.reduce((sum, e) => sum + (e.cost_usd || 0), 0)

  return (
    <div className="flex flex-col h-full gap-4 p-1">
      {/* Header + Tabs */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-6">
          <div>
            <h1 className="text-2xl font-bold text-white flex items-center gap-3">
              <span className="text-3xl">🧪</span>
              Alpha Lab
            </h1>
            <p className="text-sm text-zinc-400 mt-1">
              Autonomous strategy discovery — generate, backtest, evaluate
            </p>
          </div>

          {/* Tab Switcher */}
          <div className="flex bg-zinc-900/80 rounded-lg border border-zinc-800 p-1 ml-4">
            <button
              onClick={() => setActiveTab("generate")}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-all flex items-center gap-2 ${
                activeTab === "generate"
                  ? "bg-violet-600 text-white shadow-lg shadow-violet-500/20"
                  : "text-zinc-400 hover:text-white"
              }`}
            >
              🚀 Generate
            </button>
            <button
              onClick={() => setActiveTab("swarm")}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-all flex items-center gap-2 ${
                activeTab === "swarm"
                  ? "bg-cyan-600 text-white shadow-lg shadow-cyan-500/20"
                  : "text-zinc-400 hover:text-white"
              }`}
            >
              🤖 Swarm
            </button>
            <button
              onClick={() => setActiveTab("backtest")}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-all flex items-center gap-2 ${
                activeTab === "backtest"
                  ? "bg-amber-600 text-white shadow-lg shadow-amber-500/20"
                  : "text-zinc-400 hover:text-white"
              }`}
            >
              💻 Backtest
            </button>
            <button
              onClick={() => setActiveTab("results")}
              className={`px-4 py-2 rounded-md text-sm font-medium transition-all flex items-center gap-2 ${
                activeTab === "results"
                  ? "bg-emerald-600 text-white shadow-lg shadow-emerald-500/20"
                  : "text-zinc-400 hover:text-white"
              }`}
            >
              📊 Results
              {experiments.length > 0 && (
                <span className={`text-xs px-1.5 py-0.5 rounded-full ${
                  activeTab === "results" ? "bg-white/20" : "bg-zinc-700"
                }`}>
                  {experiments.length}
                </span>
              )}
            </button>
              <button
                onClick={() => setActiveTab("failed")}
                className={`px-4 py-2 rounded-md text-sm font-medium transition-all flex items-center gap-2 ${
                  activeTab === "failed"
                    ? "bg-rose-600 text-white shadow-lg shadow-rose-500/20"
                    : "text-zinc-400 hover:text-white"
                }`}
              >
                ⛔ Failed Generations
                {failedGenerations.length > 0 && (
                  <span className={`text-xs px-1.5 py-0.5 rounded-full ${
                    activeTab === "failed" ? "bg-white/20" : "bg-zinc-700"
                  }`}>
                    {failedGenerations.length}
                  </span>
                )}
              </button>
          </div>
        </div>

        <div className="flex items-center gap-4">
          <div className="text-right">
            <div className="text-xs text-zinc-500">Experiments</div>
            <div className="text-lg font-semibold text-white">{experiments.length}</div>
          </div>
          <div className="text-right">
            <div className="text-xs text-zinc-500">API Cost</div>
            <div className="text-lg font-semibold text-emerald-400">${totalCost.toFixed(4)}</div>
          </div>
        </div>
      </div>

      {/* ─── GENERATE TAB ──────────────────────────────────── */}
      {activeTab === "generate" && (
        <div className="flex-1 flex flex-col gap-6">
          <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-6 max-w-4xl">
            <h2 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-4">
              Strategy Hypothesis
            </h2>

            <textarea
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              placeholder="Describe your strategy idea... e.g. 'Find mean-reverting signals using RSI + Bollinger bands for undervalued stocks with low beta'"
              className="w-full bg-zinc-800/50 border border-zinc-700 rounded-lg p-4 text-sm text-white placeholder-zinc-500 focus:border-violet-500 focus:outline-none resize-none"
              rows={4}
            />

            <h3 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mt-5 mb-3">
              Strategy Style
            </h3>
            <div className="flex items-center gap-3">
              <button
                onClick={() => setStrategyStyle("academic")}
                className={`flex-1 p-3 rounded-lg border transition-all ${
                  strategyStyle === "academic"
                    ? "border-blue-500 bg-blue-500/10"
                    : "border-zinc-700 bg-zinc-800/30 hover:border-zinc-600"
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className="text-xl">🎓</span>
                  <div className="text-left">
                    <div className="text-sm font-semibold text-white">Academic</div>
                    <div className="text-xs text-zinc-500">Market-neutral, diversified</div>
                  </div>
                </div>
              </button>
              <button
                onClick={() => setStrategyStyle("hedge_fund")}
                className={`flex-1 p-3 rounded-lg border transition-all ${
                  strategyStyle === "hedge_fund"
                    ? "border-amber-500 bg-amber-500/10"
                    : "border-zinc-700 bg-zinc-800/30 hover:border-zinc-600"
                }`}
              >
                <div className="flex items-center gap-2">
                  <span className="text-xl">🏦</span>
                  <div className="text-left">
                    <div className="text-sm font-semibold text-white">Hedge Fund</div>
                    <div className="text-xs text-zinc-500">Concentrated, alpha-seeking</div>
                  </div>
                </div>
              </button>
            </div>

            <h3 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mt-5 mb-3">
              Model Tier
            </h3>
            <div className="flex items-center gap-3">
              {(Object.entries(TIER_CONFIG) as [TierKey, (typeof TIER_CONFIG)[TierKey]][]).map(([key, tier]) => (
                <button
                  key={key}
                  onClick={() => setSelectedTier(key)}
                  className={`flex-1 p-4 rounded-lg border transition-all ${
                    selectedTier === key
                      ? "border-violet-500 bg-violet-500/10"
                      : "border-zinc-700 bg-zinc-800/30 hover:border-zinc-600"
                  }`}
                >
                  <div className="flex items-center gap-2">
                    <span className="text-xl">{tier.icon}</span>
                    <div className="text-left">
                      <div className="text-sm font-semibold text-white">{tier.label}</div>
                      <div className="text-xs text-zinc-500">{tier.sublabel}</div>
                    </div>
                  </div>
                  <div className="text-xs text-zinc-500 mt-2">
                    In: {tier.inputCost} · Out: {tier.outputCost}
                  </div>
                </button>
              ))}
            </div>

            <div className="flex gap-3 mt-5">
              <button
                onClick={handleGenerate}
                disabled={generating}
                className="w-full px-6 py-4 bg-gradient-to-r from-violet-600 to-purple-600 hover:from-violet-500 hover:to-purple-500 disabled:from-zinc-700 disabled:to-zinc-700 disabled:text-zinc-500 text-white font-semibold rounded-lg transition-all text-base shadow-lg shadow-violet-900/30"
              >
                {generating ? (
                  <span className="flex items-center justify-center gap-2">
                    <span className="animate-spin text-lg">⏳</span> Generating (1-shot)…
                  </span>
                ) : (
                  "🚀 Generate Strategy"
                )}
              </button>
            </div>

            {error && (
              <div className="mt-4 p-3 bg-red-500/10 border border-red-500/30 rounded-lg text-sm text-red-400">
                {error}
              </div>
            )}
          </div>

          {/* Quick tip */}
          <div className="text-xs text-zinc-600 max-w-4xl">
            💡 Leave the prompt empty for the AI to generate a novel strategy from scratch, or describe specific
            signals/factors you want explored. After generation, you'll be switched to the Results tab to review and backtest.
          </div>
        </div>
      )}

      {/* ─── FAILED GENERATIONS TAB ───────────────────────── */}
      {activeTab === "failed" && (
        <div className="flex gap-5 flex-1 min-h-0">
          <div className="w-[360px] flex-shrink-0 flex flex-col">
            <h2 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-3">
              Failed Generations ({failedGenerations.length})
            </h2>
            <div className="flex-1 overflow-y-auto space-y-2 pr-1">
              {failedGenerations.length === 0 && (
                <div className="text-center py-12 text-zinc-500 text-sm">
                  No failed generations recorded.
                </div>
              )}
              {failedGenerations.map((fg) => {
                const isSelected = selectedFailed?.failed_id === fg.failed_id
                return (
                  <button
                    key={fg.failed_id}
                    className={`w-full text-left p-3 rounded-lg border transition-all ${
                      isSelected
                        ? "border-rose-500 bg-rose-500/10"
                        : "border-zinc-800 bg-zinc-900/50 hover:border-zinc-700"
                    }`}
                    onClick={() => setSelectedFailed(fg)}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <div className="text-sm font-semibold text-white truncate">
                        {fg.strategy_name || "Unnamed"}
                      </div>
                      <span className="text-xs px-2 py-0.5 rounded-full bg-rose-500/20 text-rose-300">
                        Rejected
                      </span>
                    </div>
                    <div className="text-xs text-zinc-500 mt-1 line-clamp-2">{fg.rejection_reason}</div>
                    <div className="flex items-center gap-3 mt-2 text-xs text-zinc-500">
                      <span className="text-emerald-400">${(fg.cost_usd || 0).toFixed(4)}</span>
                      <span>·</span>
                      <span>{fg.source}</span>
                      <span>·</span>
                      <span>{fg.failed_id}</span>
                    </div>
                  </button>
                )
              })}
            </div>
          </div>

          <div className="flex-1 min-w-0 flex flex-col">
            {!selectedFailed ? (
              <div className="flex-1 flex items-center justify-center text-zinc-500 text-sm">
                Select a failed generation to inspect details
              </div>
            ) : (
              <div className="flex-1 overflow-y-auto space-y-4 pr-1">
                <div className="flex items-center justify-between">
                  <div>
                    <h2 className="text-xl font-bold text-white">{selectedFailed.strategy_name}</h2>
                    <p className="text-sm text-zinc-400 mt-1">{selectedFailed.hypothesis}</p>
                  </div>
                  <button
                    onClick={async () => {
                      await deleteFailedGeneration(selectedFailed.failed_id)
                      await loadFailedGenerations()
                      setSelectedFailed(null)
                    }}
                    className="px-4 py-2 bg-zinc-800 hover:bg-red-500/20 text-zinc-400 hover:text-red-400 text-sm rounded-lg transition-colors border border-zinc-700"
                  >
                    🗑 Delete
                  </button>
                </div>

                <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
                  <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-3">Rejection Reason</h3>
                  <div className="text-sm text-rose-300 whitespace-pre-wrap">{selectedFailed.rejection_reason}</div>
                </div>

                <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
                  <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-3">Cost & Metadata</h3>
                  <div className="grid grid-cols-4 gap-4 text-sm">
                    <div>
                      <div className="text-xs text-zinc-500">Model</div>
                      <div className="text-white font-semibold">{selectedFailed.model_tier}</div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500">Input Tokens</div>
                      <div className="text-white font-mono">{(selectedFailed.cost_input_tokens || 0).toLocaleString()}</div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500">Output Tokens</div>
                      <div className="text-white font-mono">{(selectedFailed.cost_output_tokens || 0).toLocaleString()}</div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500">API Cost</div>
                      <div className="text-emerald-400 font-semibold">${(selectedFailed.cost_usd || 0).toFixed(4)}</div>
                    </div>
                  </div>
                </div>

                <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
                  <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-3">Rejected Strategy Code</h3>
                  <Editor
                    height="420px"
                    defaultLanguage="python"
                    value={selectedFailed.strategy_code || ""}
                    options={{
                      readOnly: true,
                      minimap: { enabled: false },
                      fontSize: 13,
                      wordWrap: "on",
                      scrollBeyondLastLine: false,
                      lineNumbers: "on",
                      padding: { top: 10 },
                    }}
                    theme="vs-dark"
                  />
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* ─── RESULTS TAB ───────────────────────────────────── */}
      {activeTab === "results" && (
        <div className="flex gap-5 flex-1 min-h-0">
          {/* Experiments List */}
          <div className="w-[340px] flex-shrink-0 flex flex-col">
            <div className="flex items-center justify-between mb-3">
              <h2 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider">
                Experiments ({experiments.length})
              </h2>
              <button
                onClick={() => { setCombineMode(!combineMode); setSelectedForCombine(new Set()) }}
                className={`text-xs px-3 py-1 rounded-md font-medium transition-all ${
                  combineMode
                    ? "bg-amber-500/20 text-amber-400 border border-amber-500/50"
                    : "bg-zinc-800 text-zinc-400 hover:text-white border border-zinc-700"
                }`}
              >
                {combineMode ? "✕ Cancel" : "🧬 Combine"}
              </button>
            </div>

            {/* Combine toolbar */}
            {combineMode && (
              <div className="mb-3 p-3 bg-amber-500/5 border border-amber-500/20 rounded-lg space-y-2">
                <div className="text-xs text-amber-300">Select 2-5 passed strategies to combine via AI</div>
                <div className="flex items-center gap-2">
                  <select
                    value={combineTier}
                    onChange={(e) => setCombineTier(e.target.value as TierKey)}
                    className="flex-1 bg-zinc-800 border border-zinc-700 rounded-md px-2 py-1 text-xs text-white"
                  >
                    {Object.entries(TIER_CONFIG).map(([key, tier]) => (
                      <option key={key} value={key}>{tier.icon} {tier.label}</option>
                    ))}
                  </select>
                  <button
                    onClick={handleCombine}
                    disabled={selectedForCombine.size < 2 || combining}
                    className="px-3 py-1 bg-amber-600 hover:bg-amber-500 disabled:bg-zinc-700 disabled:text-zinc-500 text-white text-xs font-semibold rounded-md transition-colors"
                  >
                    {combining ? "⏳ Combining…" : `🧬 Combine (${selectedForCombine.size})`}
                  </button>
                </div>
              </div>
            )}

            <div className="flex-1 overflow-y-auto space-y-2 pr-1">
              {experiments.length === 0 && (
                <div className="text-center py-12 text-zinc-500 text-sm">
                  No experiments yet.
                  <button
                    onClick={() => setActiveTab("generate")}
                    className="text-violet-400 hover:text-violet-300 ml-1 underline"
                  >
                    Generate one →
                  </button>
                </div>
              )}
              {experiments.map((exp) => {
                const badge = STATUS_BADGES[exp.status] || STATUS_BADGES.error
                const tierInfo = TIER_CONFIG[exp.model_tier as TierKey]
                const swarmConfig = parseSwarmModelConfig(exp.model_tier)
                const swarmLabel = swarmConfig.researcher && swarmConfig.risk_manager && swarmConfig.developer
                  ? `R:${TIER_CONFIG[swarmConfig.researcher].label} · RM:${TIER_CONFIG[swarmConfig.risk_manager].label} · Dev:${TIER_CONFIG[swarmConfig.developer].label}`
                  : "R/ RM/ Dev config unavailable"
                const isSelected = selectedExp?.experiment_id === exp.experiment_id

                const isCombineSelected = selectedForCombine.has(exp.experiment_id)
                const isPassed = exp.status === "passed"

                return (
                  <div
                    key={exp.experiment_id}
                    className={`w-full text-left p-3 rounded-lg border transition-all cursor-pointer ${
                      isCombineSelected
                        ? "border-amber-500 bg-amber-500/10"
                        : isSelected
                          ? "border-violet-500 bg-violet-500/10"
                          : "border-zinc-800 bg-zinc-900/50 hover:border-zinc-700"
                    }`}
                    onClick={() => {
                      if (combineMode && isPassed) {
                        toggleCombineSelect(exp.experiment_id)
                      } else {
                        handleSelectExp(exp)
                      }
                    }}
                  >
                    <div className="flex items-start justify-between gap-2">
                      {combineMode && isPassed && (
                        <div className={`w-5 h-5 rounded border-2 flex-shrink-0 flex items-center justify-center mt-0.5 transition-all ${
                          isCombineSelected
                            ? "bg-amber-500 border-amber-500 text-black"
                            : "border-zinc-600"
                        }`}>
                          {isCombineSelected && "✓"}
                        </div>
                      )}
                      <div className="flex-1 min-w-0">
                        <div className="text-sm font-semibold text-white truncate">
                          {exp.strategy_name || "Unnamed"}
                        </div>
                        <div className="text-xs text-zinc-500 mt-0.5 truncate">
                          {exp.hypothesis}
                        </div>
                      </div>
                      <span className={`text-xs px-2 py-0.5 rounded-full ${badge.bg} ${badge.text} whitespace-nowrap`}>
                        {badge.label}
                      </span>
                    </div>

                    <div className="flex items-center gap-3 mt-2 text-xs text-zinc-500">
                      {isSwarmExperiment(exp) ? (
                        <span className="text-cyan-300">🤖 Swarm</span>
                      ) : (
                        <span>{tierInfo?.icon} {tierInfo?.label}</span>
                      )}
                      <span>·</span>
                      <span className="text-emerald-400">${exp.cost_usd?.toFixed(4)}</span>
                      <span>·</span>
                      <span>{exp.experiment_id}</span>
                    </div>

                    {isSwarmExperiment(exp) && (
                      <div className="mt-1 text-[11px] text-cyan-200/80 truncate" title={swarmLabel}>
                        {swarmLabel}
                      </div>
                    )}

                    {exp.metrics && !exp.metrics.error && (
                      <div className="flex gap-3 mt-2 text-xs">
                        <span className={exp.metrics.sharpe > 0 ? "text-emerald-400" : "text-red-400"}>
                          Sharpe {exp.metrics.sharpe.toFixed(2)}
                        </span>
                        <span className="text-zinc-500">
                          MaxDD {(exp.metrics.max_drawdown * 100).toFixed(1)}%
                        </span>
                        <span className={exp.metrics.total_return > 0 ? "text-emerald-400" : "text-red-400"}>
                          {(exp.metrics.total_return * 100).toFixed(1)}% Return
                        </span>
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </div>

          {/* Detail View — full width */}
          <div className="flex-1 min-w-0 flex flex-col">
            {!selectedExp ? (
              <div className="flex-1 flex items-center justify-center text-zinc-500 text-sm">
                Select an experiment to view details
              </div>
            ) : (
              <div className="flex-1 overflow-y-auto space-y-4 pr-1">
                {/* Detail Header */}
                <div className="flex items-center justify-between">
                  <div className="flex-1 mr-4">
                    {isRenaming ? (
                      <div className="flex items-center gap-2 mb-1">
                        <input
                          type="text"
                          value={newName}
                          onChange={(e) => setNewName(e.target.value)}
                          className="bg-zinc-800 text-white rounded px-2 py-1 text-xl font-bold border border-zinc-600 focus:outline-none focus:border-violet-500 w-full max-w-sm"
                          autoFocus
                          onKeyDown={(e) => {
                            if (e.key === "Enter") handleRename()
                            if (e.key === "Escape") setIsRenaming(false)
                          }}
                        />
                        <button
                          onClick={handleRename}
                          className="p-1 px-2 bg-emerald-600/20 text-emerald-400 hover:bg-emerald-600/40 rounded transition-colors text-sm font-bold"
                          title="Save Name"
                        >
                          ✓
                        </button>
                        <button
                          onClick={() => setIsRenaming(false)}
                          className="p-1 px-2 bg-zinc-800 text-zinc-400 hover:text-white rounded transition-colors text-sm font-bold"
                          title="Cancel"
                        >
                          ✕
                        </button>
                      </div>
                    ) : (
                      <div className="flex items-center gap-2 group mb-1 min-w-0">
                        <h2 className="text-xl font-bold text-white truncate shrink-0 max-w-[80%]">
                          {selectedExp.strategy_name}
                        </h2>
                        <button
                          onClick={() => {
                            setNewName(selectedExp.strategy_name || "")
                            setIsRenaming(true)
                          }}
                          className="text-zinc-500 hover:text-zinc-300 opacity-0 group-hover:opacity-100 transition-opacity flex-shrink-0"
                          title="Rename Strategy"
                        >
                          ✏️
                        </button>
                      </div>
                    )}
                    <p className="text-sm text-zinc-400 mt-0.5 max-w-2xl">
                      {selectedExp.rationale}
                    </p>
                  </div>
                  <div className="flex gap-2">
                    <button
                      onClick={() => handleBacktest(selectedExp.experiment_id)}
                      disabled={backtesting === selectedExp.experiment_id}
                      className="px-4 py-2 bg-emerald-600 hover:bg-emerald-500 disabled:bg-zinc-700 text-white text-sm font-semibold rounded-lg transition-colors"
                    >
                      {backtesting === selectedExp.experiment_id
                        ? "⏳ Running…"
                        : selectedExp.status === "generated"
                          ? "▶ Backtest"
                          : "🔄 Re-run Backtest"}
                    </button>
                    {selectedExp.status === "passed" && (
                      <button
                        onClick={() => handlePromote(selectedExp.experiment_id)}
                        disabled={promoting}
                        className="px-4 py-2 bg-cyan-600 hover:bg-cyan-500 disabled:bg-zinc-700 text-white text-sm font-semibold rounded-lg transition-colors"
                      >
                        {promoting ? "⏳ Promoting…" : "🚀 Promote to Live"}
                      </button>
                    )}
                    <button
                      onClick={() => handleDelete(selectedExp.experiment_id)}
                      className="px-4 py-2 bg-zinc-800 hover:bg-red-500/20 text-zinc-400 hover:text-red-400 text-sm rounded-lg transition-colors border border-zinc-700"
                    >
                      🗑 Delete
                    </button>
                  </div>
                </div>

                {/* Cost Card */}
                <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
                  {(() => {
                    const swarmConfig = parseSwarmModelConfig(selectedExp.model_tier)
                    const hasSwarmConfig = Boolean(swarmConfig.researcher && swarmConfig.risk_manager && swarmConfig.developer)
                    return (
                  <div className="grid grid-cols-4 gap-4 text-sm">
                    <div>
                      <div className="text-xs text-zinc-500">Model</div>
                      <div className="text-white font-semibold">
                        {isSwarmExperiment(selectedExp)
                          ? "🤖 Swarm"
                          : `${TIER_CONFIG[selectedExp.model_tier as TierKey]?.icon ?? ""} ${TIER_CONFIG[selectedExp.model_tier as TierKey]?.label ?? selectedExp.model_tier}`}
                      </div>
                      {isSwarmExperiment(selectedExp) && (
                        <div className="text-[11px] text-cyan-200 mt-1 leading-4">
                          {hasSwarmConfig
                            ? `Researcher: ${TIER_CONFIG[swarmConfig.researcher!].label} | Risk Manager: ${TIER_CONFIG[swarmConfig.risk_manager!].label} | Developer: ${TIER_CONFIG[swarmConfig.developer!].label}`
                            : "Swarm config unavailable"}
                        </div>
                      )}
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500">Input Tokens</div>
                      <div className="text-white font-mono">{selectedExp.cost_input_tokens?.toLocaleString()}</div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500">Output Tokens</div>
                      <div className="text-white font-mono">{selectedExp.cost_output_tokens?.toLocaleString()}</div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500">API Cost</div>
                      <div className="text-emerald-400 font-semibold">${selectedExp.cost_usd?.toFixed(4)}</div>
                    </div>
                  </div>
                    )
                  })()}
                </div>

                {/* Metrics Card */}
                {selectedExp.metrics && !selectedExp.metrics.error && (
                  <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
                    <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-3">
                      Backtest Results
                    </h3>
                    <div className="grid grid-cols-5 gap-4 text-sm">
                      <div>
                        <div className="text-xs text-zinc-500">Sharpe Ratio</div>
                        <div className={`text-lg font-bold ${selectedExp.metrics.sharpe > 0 ? "text-emerald-400" : "text-red-400"}`}>
                          {selectedExp.metrics.sharpe.toFixed(3)}
                        </div>
                      </div>
                      <div>
                        <div className="text-xs text-zinc-500">CAGR</div>
                        <div className={`text-lg font-bold ${selectedExp.metrics.cagr > 0 ? "text-emerald-400" : "text-red-400"}`}>
                          {(selectedExp.metrics.cagr * 100).toFixed(2)}%
                        </div>
                      </div>
                      <div>
                        <div className="text-xs text-zinc-500">Total Return</div>
                        <div className={`text-lg font-bold ${selectedExp.metrics.total_return > 0 ? "text-emerald-400" : "text-red-400"}`}>
                          {(selectedExp.metrics.total_return * 100).toFixed(2)}%
                        </div>
                      </div>
                      <div>
                        <div className="text-xs text-zinc-500">Max Drawdown</div>
                        <div className="text-lg font-bold text-orange-400">
                          {(selectedExp.metrics.max_drawdown * 100).toFixed(2)}%
                        </div>
                      </div>
                      <div>
                        <div className="text-xs text-zinc-500">Trading Days</div>
                        <div className="text-lg font-bold text-white">
                          {selectedExp.metrics.trading_days?.toLocaleString()}
                        </div>
                      </div>
                    </div>
                  </div>
                )}

                {/* Equity Curve Chart */}
                {selectedExp.equity_curve && selectedExp.equity_curve.length > 0 && (
                  <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
                    <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-3">
                      Equity Curve
                    </h3>
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={selectedExp.equity_curve.map((p: AlphaEquityPoint) => ({
                        date: p.date?.split("T")[0],
                        equity: Number(p.equity?.toFixed(2)),
                      }))}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
                        <XAxis
                          dataKey="date"
                          tick={{ fontSize: 10, fill: "#71717a" }}
                          tickFormatter={(v: string) => v?.slice(5)}
                          interval="preserveStartEnd"
                        />
                        <YAxis
                          tick={{ fontSize: 10, fill: "#71717a" }}
                          tickFormatter={(v: number) => `$${(v / 1000).toFixed(1)}k`}
                        />
                        <Tooltip
                          contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: "8px", fontSize: 12 }}
                          labelStyle={{ color: "#a1a1aa" }}
                          formatter={(v: number) => [`$${v.toLocaleString()}`, "Equity"]}
                        />
                        <Line
                          type="monotone"
                          dataKey="equity"
                          stroke="#a78bfa"
                          strokeWidth={2}
                          dot={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}

                {/* Error */}
                {selectedExp.metrics?.error && (
                  <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-4 text-sm text-red-400">
                    <div className="font-semibold mb-1">Backtest Error</div>
                    {selectedExp.metrics.error}
                  </div>
                )}

                {/* Strategy Code — Editable */}
                <div className="bg-zinc-900/50 border border-zinc-800 rounded-lg p-4">
                  <div className="flex items-center justify-between mb-3">
                    <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider">
                      Strategy Code
                    </h3>
                    <div className="flex gap-2">
                      {!isEditing ? (
                        <button
                          onClick={() => { setEditedCode(selectedExp.strategy_code || ""); setIsEditing(true); }}
                          className="px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-zinc-300 text-xs font-medium rounded-md border border-zinc-700 transition-colors"
                        >
                          ✏️ Edit Code
                        </button>
                      ) : (
                        <>
                          <button
                            onClick={() => setIsEditing(false)}
                            className="px-3 py-1.5 bg-zinc-800 hover:bg-zinc-700 text-zinc-400 text-xs font-medium rounded-md border border-zinc-700 transition-colors"
                          >
                            Cancel
                          </button>
                          <button
                            onClick={handleSaveCode}
                            disabled={saving}
                            className="px-3 py-1.5 bg-blue-600 hover:bg-blue-500 disabled:bg-zinc-700 text-white text-xs font-medium rounded-md transition-colors"
                          >
                            {saving ? "Saving…" : "💾 Save Code"}
                          </button>
                          <button
                            onClick={handleSaveAndBacktest}
                            disabled={saving || backtesting !== null}
                            className="px-3 py-1.5 bg-emerald-600 hover:bg-emerald-500 disabled:bg-zinc-700 text-white text-xs font-medium rounded-md transition-colors"
                          >
                            {backtesting ? "⏳ Running…" : "💾 Save & Backtest"}
                          </button>
                        </>
                      )}
                    </div>
                  </div>
                  {isEditing ? (
                    <textarea
                      value={editedCode}
                      onChange={(e) => setEditedCode(e.target.value)}
                      className="w-full bg-black/50 rounded-lg p-4 text-xs text-emerald-300 font-mono leading-relaxed border border-zinc-700 focus:border-violet-500 focus:outline-none resize-y min-h-[200px]"
                      rows={Math.max(15, (editedCode.match(/\n/g) || []).length + 2)}
                      spellCheck={false}
                    />
                  ) : (
                    <pre className="bg-black/50 rounded-lg p-4 overflow-x-auto text-xs text-emerald-300 font-mono leading-relaxed">
                      {selectedExp.strategy_code}
                    </pre>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      )}
      {activeTab === "swarm" && (
        <div className="flex-1 flex flex-col gap-6 max-w-4xl">
          <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-6 shadow-xl">
            <div className="flex items-center gap-3 mb-8">
              <span className="text-3xl">🤖</span>
              <div>
                <h2 className="text-xl font-bold bg-gradient-to-r from-cyan-400 to-blue-400 bg-clip-text text-transparent">Agent Swarm Generation</h2>
                <p className="text-sm text-zinc-400 mt-1">Configure your hedge fund pod. Changes persist automatically.</p>
              </div>
            </div>

            {/* Prompt */}
            <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-4">
              Strategy Hypothesis
            </h3>
            <textarea
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              placeholder="Describe your strategy idea... or leave empty to let the Swarm brainstorm from scratch"
              className="w-full bg-black/40 border border-zinc-700/60 rounded-lg p-4 text-sm text-cyan-50 placeholder-zinc-500 focus:border-cyan-500 focus:ring-1 focus:ring-cyan-500/50 focus:outline-none resize-none transition-all shadow-inner"
              rows={4}
            />

            {/* Strategy Style */}
            <h3 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mt-6 mb-3">
              Strategy Style
            </h3>
            <div className="flex items-center gap-3">
              <button
                onClick={() => setStrategyStyle("academic")}
                className={`flex-1 p-3 rounded-xl border transition-all ${
                  strategyStyle === "academic"
                    ? "border-cyan-500 bg-cyan-500/10 shadow-lg shadow-cyan-900/20"
                    : "border-zinc-700/60 bg-zinc-800/30 hover:border-zinc-500 hover:bg-zinc-800/50"
                }`}
              >
                <div className="flex items-center gap-3">
                  <span className="text-2xl drop-shadow-md">🎓</span>
                  <div className="text-left">
                    <div className="text-sm font-semibold text-white">Academic</div>
                    <div className="text-xs text-zinc-400 mt-0.5">Market-neutral, diversified</div>
                  </div>
                </div>
              </button>
              <button
                onClick={() => setStrategyStyle("hedge_fund")}
                className={`flex-1 p-3 rounded-xl border transition-all ${
                  strategyStyle === "hedge_fund"
                    ? "border-amber-500 bg-amber-500/10 shadow-lg shadow-amber-900/20"
                    : "border-zinc-700/60 bg-zinc-800/30 hover:border-zinc-500 hover:bg-zinc-800/50"
                }`}
              >
                <div className="flex items-center gap-3">
                  <span className="text-2xl drop-shadow-md">🏦</span>
                  <div className="text-left">
                    <div className="text-sm font-semibold text-white">Hedge Fund</div>
                    <div className="text-xs text-zinc-400 mt-0.5">Concentrated, alpha-seeking</div>
                  </div>
                </div>
              </button>
            </div>

            <div className="w-full h-px bg-zinc-800 my-8"></div>

            <h3 className="text-sm font-semibold text-zinc-300 uppercase tracking-wider mb-5 flex items-center gap-2">
              <span>Swarm Configuration</span>
              <button
                onClick={handleSaveSwarmConfig}
                className={`px-3 py-1 rounded text-xs font-semibold border transition-all ${
                  configSaved
                    ? "bg-emerald-600/20 border-emerald-500/50 text-emerald-400"
                    : "bg-zinc-800 border-zinc-700/50 text-zinc-400 hover:text-white"
                }`}
              >
                {configSaved ? "✅ Saved" : "💾 Save"}
              </button>
            </h3>

            <div className="mt-6 space-y-5">
              {SWARM_AGENT_ROLES.map((agent) => (
                <div key={agent.id} className={`rounded-xl border ${agent.borderColor} ${agent.bgColor} p-5`}>
                  <div className="flex items-start justify-between gap-4 mb-4">
                    <div className="flex items-center gap-2">
                      <span className="text-2xl">{agent.icon}</span>
                      <div>
                        <div className={`font-semibold text-base ${agent.color}`}>{agent.label}</div>
                        <div className="text-xs text-zinc-500 mt-0.5">{agent.description}</div>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs text-zinc-500">Model:</span>
                      <div className="flex gap-1">
                        {(Object.entries(TIER_CONFIG) as [TierKey, (typeof TIER_CONFIG)[TierKey]][]).map(([key, tier]) => (
                          <button
                            key={key}
                            onClick={() => setSwarmAgentTiers(prev => ({ ...prev, [agent.id]: key }))}
                            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all border ${
                              swarmAgentTiers[agent.id] === key
                                ? "border-violet-500 bg-violet-500/20 text-white"
                                : "border-zinc-700 bg-zinc-800/30 text-zinc-400 hover:text-white"
                            }`}
                          >
                            {tier.icon} {tier.label}
                          </button>
                        ))}
                      </div>
                    </div>
                  </div>
                  <div>
                    <label className="text-xs text-zinc-500 block mb-1.5">Additional instructions (optional)</label>
                    <textarea
                      value={swarmAgentNotes[agent.id]}
                      onChange={(e) => setSwarmAgentNotes(prev => ({ ...prev, [agent.id]: e.target.value }))}
                      placeholder={`Extra guidance for the ${agent.label}…`}
                      className="w-full bg-zinc-800/50 border border-zinc-700 rounded-lg p-3 text-sm text-white placeholder-zinc-600 focus:border-violet-500 focus:outline-none resize-none"
                      rows={2}
                    />
                  </div>
                </div>
              ))}
            </div>

            <div className="mt-6 p-4 bg-zinc-800/40 rounded-lg border border-zinc-700">
              <div className="text-xs text-zinc-400 mb-3 font-semibold uppercase tracking-wider">Pipeline Flow</div>
              <div className="flex items-center gap-2 text-sm flex-wrap">
                {SWARM_AGENT_ROLES.map((agent, i) => (
                  <div key={agent.id} className="flex items-center gap-2">
                    <span className={`flex items-center gap-1 px-3 py-1.5 rounded-lg border ${agent.borderColor} ${agent.bgColor} ${agent.color} text-xs font-medium`}>
                      {agent.icon} {agent.label}
                      <span className="text-zinc-500 ml-1">[{TIER_CONFIG[swarmAgentTiers[agent.id]]?.label}]</span>
                    </span>
                    {i < SWARM_AGENT_ROLES.length - 1 && <span className="text-zinc-600">→</span>}
                  </div>
                ))}
              </div>
            </div>

            <div className="mt-4 text-xs text-zinc-600">
              🧠 The Swarm runs 3 sequential LLM calls. Total cost ≈ 3× the cost of a single generation at the configured model tiers.
            </div>
          </div>

          {/* Action Buttons */}
          <div className="flex items-center justify-between gap-4 mt-2">
            {!generatingSwarm ? (
              <button
                onClick={handleGenerateSwarm}
                disabled={generating}
                className="flex-1 px-6 py-4 bg-gradient-to-r from-cyan-600 to-blue-600 hover:from-cyan-500 hover:to-blue-500 disabled:from-zinc-700 disabled:to-zinc-700 disabled:text-zinc-500 text-white font-semibold rounded-xl transition-all text-base border-t border-cyan-400/30 shadow-lg shadow-cyan-900/40 flex items-center justify-center gap-2"
              >
                <span className="text-xl">🚀</span> Deploy Swarm
              </button>
            ) : (
              <button
                onClick={handleKillSwarm}
                className="flex-1 px-6 py-4 bg-gradient-to-r from-red-700 to-rose-700 hover:from-red-600 hover:to-rose-600 text-white font-semibold rounded-xl transition-all text-base border-t border-red-400/30 shadow-lg shadow-red-900/40 flex items-center justify-center gap-2 animate-pulse"
              >
                <span className="text-xl">⛔</span> Abort Protocol
              </button>
            )}
          </div>

          {error && (
            <div className="p-3 bg-red-500/10 border border-red-500/30 rounded-lg text-sm text-red-400">
              {error}
            </div>
          )}

          {/* Live Swarm Log */}
          {swarmLogs.length > 0 && (
            <div className="mt-2 bg-black/60 border border-zinc-700/50 rounded-xl p-5 space-y-3 font-mono text-sm shadow-2xl backdrop-blur-sm">
              <div className="text-cyan-400 font-semibold tracking-wider text-xs mb-4 flex items-center gap-2">
                <span className="relative flex h-2 w-2">
                  <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-cyan-400 opacity-75"></span>
                  <span className="relative inline-flex rounded-full h-2 w-2 bg-cyan-500"></span>
                </span>
                LIVE SWARM TELEMETRY
              </div>
              
              <div className="space-y-4">
                {swarmLogs.map((log, i) => (
                  <div key={i} className={`flex items-start gap-4 ${
                    log.status === "running" ? "text-cyan-300" :
                    log.status === "done" ? "text-emerald-400" : "text-red-400"
                  }`}>
                    <span className={log.status === "running" ? "animate-spin inline-block text-lg" : "inline-block text-lg"}>
                      {log.status === "running" ? "🌀" : log.status === "done" ? "✅" : "❌"}
                    </span>
                    <div className="flex-1 min-w-0 pt-0.5">
                      <div className="font-semibold text-[13px]">{log.label}
                        {log.tokens ? <span className="text-zinc-500 ml-2 font-normal text-[11px] bg-zinc-800 px-1.5 py-0.5 rounded">[{log.tokens} tkns]</span> : null}
                      </div>
                      {log.preview && (
                        <div className="text-zinc-400 mt-2 truncate bg-zinc-900/80 p-2.5 rounded border border-zinc-800/80 text-[11px] leading-relaxed italic border-l-2 border-l-emerald-500/50 shadow-inner">
                          "{log.preview}"
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
              
              {generatingSwarm && (
                <div className="text-zinc-500 animate-pulse ml-9 text-xs flex items-center gap-2 mt-4 font-semibold">
                  <span className="inline-block animate-bounce">↓</span> Agent thinking...
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {/* ─── STANDALONE BACKTEST TAB ────────────────────────── */}
      {activeTab === "backtest" && (
        <div className="flex gap-5 flex-1 min-h-0">
          <div className="flex-1 flex flex-col bg-zinc-900/50 border border-zinc-800 rounded-xl overflow-hidden shadow-xl">
            {/* Toolbar */}
            <div className="flex items-center justify-between p-3 bg-zinc-900 border-b border-zinc-800">
              <div className="flex items-center gap-2">
                <span className="text-xl">💻</span>
                <div className="text-sm font-semibold text-white">Standalone Editor</div>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={handleSaveStandalone}
                  disabled={savingStandalone || !standaloneResult || !!standaloneResult.error}
                  className="px-4 py-1.5 bg-zinc-800 hover:bg-zinc-700 disabled:opacity-50 text-white text-xs font-semibold rounded-md transition-all flex items-center gap-2"
                  title="Run backtest first to save results"
                >
                  {savingStandalone ? "⏳ Saving..." : "💾 Save Experiment"}
                </button>
                <button
                  onClick={handleRunStandalone}
                  disabled={runningStandalone}
                  className="px-4 py-1.5 bg-amber-600 hover:bg-amber-500 disabled:bg-zinc-700 text-white text-xs font-semibold rounded-md transition-all flex items-center gap-2"
                >
                  {runningStandalone ? (
                    <><span className="animate-spin text-sm">⏳</span> Running...</>
                  ) : (
                    <><span className="text-sm">▶</span> Run Backtest</>
                  )}
                </button>
              </div>
            </div>
            
            {/* Editor */}
            <div className="flex-1 min-h-[400px]">
              <Editor
                height="100%"
                defaultLanguage="python"
                theme="vs-dark"
                value={standaloneCode}
                onChange={(val) => setStandaloneCode(val || "")}
                options={{
                  minimap: { enabled: false },
                  fontSize: 13,
                  fontFamily: "var(--font-mono)",
                  lineHeight: 22,
                  padding: { top: 16 },
                  scrollBeyondLastLine: false,
                }}
              />
            </div>
          </div>

          <div className="w-[480px] flex-shrink-0 flex flex-col gap-4 overflow-y-auto pr-1">
            {!standaloneResult ? (
              <div className="flex-1 flex items-center justify-center text-zinc-500 text-sm border border-dashed border-zinc-800 rounded-xl">
                Run backtest to view results
              </div>
            ) : standaloneResult.error ? (
              <div className="bg-red-500/10 border border-red-500/30 rounded-xl p-5 text-sm text-red-400 font-mono shadow-xl relative mt-0">
                <div className="font-semibold mb-2 flex items-center gap-2">
                  <span>❌</span> Execution Error
                </div>
                <div className="whitespace-pre-wrap">{standaloneResult.error}</div>
              </div>
            ) : (
              // Results UI
              <>
                <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-5 shadow-xl">
                  <h3 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mb-4">
                    Performance Metrics
                  </h3>
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <div className="text-xs text-zinc-500 mb-1">Sharpe Ratio</div>
                      <div className={`text-xl font-bold ${(standaloneResult.metrics?.sharpe || 0) > 0 ? "text-emerald-400" : "text-red-400"}`}>
                        {standaloneResult.metrics?.sharpe?.toFixed(3) || "0.000"}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500 mb-1">Total Return</div>
                      <div className={`text-xl font-bold ${(standaloneResult.metrics?.total_return || 0) > 0 ? "text-emerald-400" : "text-red-400"}`}>
                        {((standaloneResult.metrics?.total_return || 0) * 100).toFixed(2)}%
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500 mb-1">Max Drawdown</div>
                      <div className="text-xl font-bold text-orange-400">
                        {((standaloneResult.metrics?.max_drawdown || 0) * 100).toFixed(2)}%
                      </div>
                    </div>
                    <div>
                      <div className="text-xs text-zinc-500 mb-1">Trading Days</div>
                      <div className="text-xl font-bold text-white">
                        {standaloneResult.metrics?.trading_days?.toLocaleString() || "0"}
                      </div>
                    </div>
                  </div>
                </div>

                {standaloneResult.equity_curve && standaloneResult.equity_curve.length > 0 && (
                  <div className="bg-zinc-900/50 border border-zinc-800 rounded-xl p-5 flex-1 shadow-xl">
                    <h3 className="text-xs font-semibold text-zinc-400 uppercase tracking-wider mb-4">
                      Equity Curve
                    </h3>
                    <ResponsiveContainer width="100%" height={300}>
                      <LineChart data={standaloneResult.equity_curve.map((p: any) => ({
                        date: p.date?.split("T")[0],
                        equity: Number(p.equity?.toFixed(2)),
                      }))}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
                        <XAxis
                          dataKey="date"
                          tick={{ fontSize: 10, fill: "#71717a" }}
                          tickFormatter={(v: string) => v?.slice(5)}
                          interval="preserveStartEnd"
                        />
                        <YAxis
                          tick={{ fontSize: 10, fill: "#71717a" }}
                          domain={['auto', 'auto']}
                          tickFormatter={(v: number) => `$${(v / 1000).toFixed(1)}k`}
                        />
                        <Tooltip
                          contentStyle={{ background: "#18181b", border: "1px solid #3f3f46", borderRadius: "8px", fontSize: 12 }}
                          labelStyle={{ color: "#a1a1aa" }}
                          formatter={(v: number) => [`$${v.toLocaleString()}`, "Equity"]}
                        />
                        <Line
                          type="monotone"
                          dataKey="equity"
                          stroke="#a78bfa"
                          strokeWidth={2}
                          dot={false}
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
