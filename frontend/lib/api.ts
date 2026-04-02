/**
 * api.ts — Typed API client for the FastAPI backend.
 *
 * Uses NEXT_PUBLIC_API_URL from .env.local (default: http://localhost:8000).
 */

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"

// ── Types ─────────────────────────────────────────────────

export interface Strategy {
  id: string
  name: string
}

export interface EquityCurvePoint {
  date: string
  value: number
}

export interface StrategyMetrics {
  sharpe: number
  max_drawdown: number
  cagr: number
  total_return: number
  trading_days: number
}

export interface StrategyResult {
  name: string
  metrics: StrategyMetrics
  equity_curve: EquityCurvePoint[]
}

export interface TournamentResponse {
  strategies: Record<string, StrategyResult>
  benchmark: {
    name: string
    metrics: StrategyMetrics
    equity_curve: EquityCurvePoint[]
  } | null
}

// ── API Functions ─────────────────────────────────────────

/**
 * Fetch all available strategies from the backend.
 */
export async function fetchStrategies(): Promise<Strategy[]> {
  const res = await fetch(`${API_BASE}/api/strategies/list`)
  if (!res.ok) throw new Error(`Failed to fetch strategies: ${res.status}`)
  const data = await res.json()
  return data.strategies as Strategy[]
}

/**
 * Run a tournament backtest for the selected strategies.
 */
export async function runTournament(params: {
  strategies?: string[]
  startDate?: string
  endDate?: string
  startingCapital?: number
}): Promise<TournamentResponse> {
  const res = await fetch(`${API_BASE}/api/strategies/tournament`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      strategies: params.strategies ?? null,
      start_date: params.startDate ?? null,
      end_date: params.endDate ?? null,
      starting_capital: params.startingCapital ?? 10000,
    }),
  })
  if (!res.ok) {
    const text = await res.text().catch(() => "Unknown error")
    throw new Error(`Tournament failed (${res.status}): ${text}`)
  }
  return (await res.json()) as TournamentResponse
}

// ── Trader & Portfolio Types ──────────────────────────────

export interface TraderConstraint {
  trader_id: number
  max_drawdown_pct: number
  max_open_positions: number
  max_capital_per_trade: number
  halt_trading_flag: boolean | number
}

export interface Trader {
  id: number
  name: string
  total_capital: number
  unallocated_capital: number
  created_at?: string
  constraints?: TraderConstraint
  portfolios_count: number
}

export interface Portfolio {
  id: number
  trader_id: number
  name: string
  allocated_capital: number
  strategy_id: string | null
  strategy_name: string | null
  rebalance_freq: string
  next_rebalance_date: string | null
}

// ── Trader API ────────────────────────────────────────────

export async function getTraders(): Promise<Trader[]> {
  const res = await fetch(`${API_BASE}/api/traders/`)
  if (!res.ok) throw new Error(`Failed to fetch traders: ${res.status}`)
  return (await res.json()) as Trader[]
}

export async function createTrader(
  name: string,
  capital: number,
  numPortfolios: number = 10,
  capitalPerPortfolio?: number,
): Promise<Trader> {
  const res = await fetch(`${API_BASE}/api/traders/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name,
      total_capital: capital,
      num_portfolios: numPortfolios,
      capital_per_portfolio: capitalPerPortfolio ?? null,
    }),
  })
  if (!res.ok) {
    const text = await res.text().catch(() => "Unknown error")
    throw new Error(`Failed to create trader: ${text}`)
  }
  return (await res.json()) as Trader
}

export async function updateConstraints(
  traderId: number,
  data: Partial<TraderConstraint>
): Promise<void> {
  const res = await fetch(`${API_BASE}/api/traders/${traderId}/constraints`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  })
  if (!res.ok) throw new Error(`Failed to update constraints: ${res.status}`)
}

export async function deleteTrader(traderId: number): Promise<void> {
  const res = await fetch(`${API_BASE}/api/traders/${traderId}`, {
    method: "DELETE",
  })
  if (!res.ok) throw new Error(`Failed to delete trader: ${res.status}`)
}

// ── Portfolio API ─────────────────────────────────────────

export async function getPortfolios(traderId: number): Promise<Portfolio[]> {
  const res = await fetch(`${API_BASE}/api/traders/${traderId}/portfolios`)
  if (!res.ok) throw new Error(`Failed to fetch portfolios: ${res.status}`)
  return (await res.json()) as Portfolio[]
}

export async function updatePortfolioStrategy(
  portfolioId: number,
  strategyId: string
): Promise<void> {
  const res = await fetch(`${API_BASE}/api/portfolios/${portfolioId}/strategy`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ strategy_id: strategyId }),
  })
  if (!res.ok) throw new Error(`Failed to assign strategy: ${res.status}`)
}

export async function updatePortfolioSchedule(
  portfolioId: number,
  freq: string
): Promise<void> {
  const res = await fetch(`${API_BASE}/api/portfolios/${portfolioId}/schedule`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ rebalance_freq: freq }),
  })
  if (!res.ok) throw new Error(`Failed to update schedule: ${res.status}`)
}

// ── Trader Backtest API ───────────────────────────────────

export interface TraderBacktestResult {
  trader: { id: number; name: string; total_capital: number }
  portfolios: Record<string, {
    name: string
    portfolio_name: string
    allocated_capital: number
    weight: number
    metrics: StrategyMetrics
    equity_curve: EquityCurvePoint[]
  }>
  combined: {
    name: string
    metrics: StrategyMetrics
    equity_curve: EquityCurvePoint[]
  }
  benchmark: {
    name: string
    metrics: StrategyMetrics
    equity_curve: EquityCurvePoint[]
  } | null
}

export async function runTraderBacktest(
  traderId: number,
  startDate?: string,
  endDate?: string,
): Promise<TraderBacktestResult> {
  const res = await fetch(`${API_BASE}/api/traders/${traderId}/backtest`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      start_date: startDate ?? null,
      end_date: endDate ?? null,
    }),
  })
  if (!res.ok) {
    const text = await res.text().catch(() => "Unknown error")
    throw new Error(`Backtest failed: ${text}`)
  }
  return (await res.json()) as TraderBacktestResult
}

// ── X-Ray Diagnostics API ─────────────────────────────────

export async function fetchXrayTickers(): Promise<string[]> {
  const res = await fetch(`${API_BASE}/api/diagnostics/tickers`)
  if (!res.ok) return []
  const data = await res.json()
  return data.tickers ?? []
}

export interface XrayResult {
  ticker: string
  date: string
  entity_id: number
  raw_data: {
    price: number | null
    volume: number | null
    daily_return: number | null
  } | null
  fundamentals: {
    filing_date: string
    revenue: number | null
    total_debt: number | null
    cash: number | null
    shares_outstanding: number | null
  } | null
  features: {
    ev_sales_zscore: number | null
    dynamic_discount_rate: number | null
    dcf_npv_gap: number | null
    beta_spy: number | null
    beta_tnx: number | null
    beta_vix: number | null
  } | null
  strategy_intent: {
    strategy_id: string | null
    raw_weight: number | null
  } | null
  risk_adjustment: {
    target_weight: number | null
    mcr: number | null
    mcr_threshold: number
    mcr_breach: boolean
    original_weight: number
    scaled: boolean
  } | null
  final_order: {
    target_allocation: number | null
  } | null
}

export async function fetchXrayData(ticker: string, date: string): Promise<XrayResult> {
  const res = await fetch(`${API_BASE}/api/diagnostics/xray/${ticker}/${date}`)
  if (!res.ok) {
    const text = await res.text().catch(() => "Unknown error")
    throw new Error(`X-Ray failed: ${text}`)
  }
  return (await res.json()) as XrayResult
}

// ── Pipeline Coverage API ─────────────────────────────────

export interface ComponentCoverage {
  rows: number
  date_start?: string
  date_end?: string
  null_pct?: Record<string, number>
}

export interface TickerCoverage {
  ticker: string
  entity_id: number
  market_data: ComponentCoverage | null
  fundamental: ComponentCoverage | null
  feature: ComponentCoverage | null
  action_intent: ComponentCoverage | null
  target_portfolio: ComponentCoverage | null
}

export async function fetchPipelineCoverage(): Promise<TickerCoverage[]> {
  const res = await fetch(`${API_BASE}/api/diagnostics/pipeline-coverage`)
  if (!res.ok) return []
  const data = await res.json()
  return data.tickers ?? []
}

// ── Indicators API ───────────────────────────────────────────

export interface TechnicalIndicators {
  latest_price: number
  sma_20: number | null
  sma_50: number | null
  sma_200: number | null
  sma_trend: string | null
  rsi_14: number | null
  momentum_10d: number | null
  momentum_20d: number | null
  momentum_60d: number | null
  bollinger_position: number | null
  mean_reversion_zscore: number | null
  volume_ratio_20_60: number | null
}

export interface DCFBreakdownMonth {
  month: number
  cash_flow: number
  discount_factor: number
  present_value: number
  terminal_value: number
  cumulative_npv: number
}

export interface FundamentalIndicators {
  filing_date?: string
  market_cap?: number
  market_cap_label?: string
  price_to_sales?: number
  ev_to_sales?: number
  net_debt?: number
  net_debt_label?: string
  revenue?: number
  revenue_label?: string
  cash_to_revenue?: number
  ev_sales_zscore?: number
  dcf_npv_gap?: number
  dynamic_discount_rate?: number
  dcf_breakdown?: DCFBreakdownMonth[]
  feature_date?: string
}

export interface StatisticalIndicators {
  risk_free_rate?: number
  volatility_30d: number | null
  volatility_90d: number | null
  volatility_1y: number | null
  return_1y?: number
  return_3m?: number
  sharpe_1y?: number
  max_drawdown: number
  var_95?: number
  var_99?: number
  skewness?: number
  kurtosis?: number
  beta_spy?: number
  beta_tnx?: number
  beta_vix?: number
  capm_expected_return?: number
  correlation_spy_90d?: number
}

export interface IndicatorsResult {
  ticker: string
  entity_id: number
  technical: TechnicalIndicators | null
  fundamental: FundamentalIndicators | null
  statistical: StatisticalIndicators | null
}

export async function fetchIndicatorTickers(): Promise<string[]> {
  const res = await fetch(`${API_BASE}/api/indicators/tickers`)
  if (!res.ok) return []
  const data = await res.json()
  return data.tickers ?? []
}

export async function fetchIndicators(ticker: string, rfrSource: string = "irx"): Promise<IndicatorsResult | null> {
  const res = await fetch(`${API_BASE}/api/indicators/${ticker}?rfr_source=${rfrSource}`)
  if (!res.ok) return null
  return (await res.json()) as IndicatorsResult
}

// ── Alpha Lab ─────────────────────────────────────────────

export interface AlphaExperiment {
  experiment_id: string
  hypothesis: string
  strategy_code: string
  strategy_name: string
  model_tier: string
  status: string
  created_at: string
  metrics_json?: string | null
  metrics?: AlphaMetrics | null
  cost_input_tokens: number
  cost_output_tokens: number
  cost_usd: number
  rationale: string
  equity_curve?: AlphaEquityPoint[] | null
}

export interface AlphaMetrics {
  sharpe: number
  max_drawdown: number
  cagr: number
  total_return: number
  trading_days: number
  error?: string
}

export interface AlphaEquityPoint {
  date: string
  daily_return: number
  equity: number
}

export interface AlphaGenerateResult {
  experiment_id?: string
  strategy_name?: string
  rationale?: string
  code?: string
  model_tier?: string
  input_tokens?: number
  output_tokens?: number
  cost_usd?: number
  error?: string
}

export interface AlphaModelTier {
  label: string
  input_cost_per_mtok: number
  output_cost_per_mtok: number
}

export async function fetchAlphaModelTiers(): Promise<Record<string, AlphaModelTier>> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/tiers`)
  if (!res.ok) return {}
  return await res.json()
}

export async function generateAlphaStrategy(prompt: string, modelTier: string, strategyStyle: string = "academic"): Promise<AlphaGenerateResult> {
  const res = await fetch(
    `${API_BASE}/api/alpha-lab/generate?prompt=${encodeURIComponent(prompt)}&model_tier=${modelTier}&strategy_style=${strategyStyle}`,
    { method: "POST" }
  )
  return await res.json()
}

export function getSwarmStreamUrl(
  prompt: string,
  strategyStyle: string,
  agentTiers: Record<string, string>,
  agentNotes: Record<string, string>
): string {
  const params = new URLSearchParams()
  if (prompt) params.append("prompt", prompt)
  params.append("strategy_style", strategyStyle)
  params.append("agent_tiers", JSON.stringify(agentTiers))
  params.append("agent_notes", JSON.stringify(agentNotes))
  return `${API_BASE}/api/alpha-lab/generate-swarm-stream?${params.toString()}`
}

export async function saveSwarmResult(data: {
  name: string
  hypothesis: string
  rationale: string
  code: string
  model_tier: string
  input_tokens: number
  output_tokens: number
  cost_usd: number
}): Promise<AlphaGenerateResult> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/generate-swarm-save`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      name: data.name,
      hypothesis: data.hypothesis,
      rationale: data.rationale,
      code: data.code,
      model_tier: data.model_tier,
      input_tokens: data.input_tokens,
      output_tokens: data.output_tokens,
      cost_usd: data.cost_usd,
    }),
  })
  return await res.json()
}

export async function runStandaloneBacktest(code: string): Promise<{
  metrics?: AlphaMetrics;
  equity_curve?: AlphaEquityPoint[];
  final_code?: string;
  error?: string;
}> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/standalone-backtest`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code }),
  })
  return await res.json()
}

export async function saveStandaloneExperiment(code: string): Promise<{ experiment_id?: string; strategy_name?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/save-standalone`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ code }),
  })
  return await res.json()
}

export async function runAlphaBacktest(experimentId: string): Promise<{ metrics?: AlphaMetrics; equity_curve?: AlphaEquityPoint[]; status?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/${experimentId}/backtest`, { method: "POST" })
  return await res.json()
}

export async function fetchAlphaExperiments(): Promise<AlphaExperiment[]> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/experiments`)
  if (!res.ok) return []
  return await res.json()
}

export async function fetchAlphaExperiment(experimentId: string): Promise<AlphaExperiment | null> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/${experimentId}`)
  if (!res.ok) return null
  return await res.json()
}

export async function deleteAlphaExperiment(experimentId: string): Promise<boolean> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/${experimentId}`, { method: "DELETE" })
  const data = await res.json()
  return data.deleted === true
}

export async function updateAlphaCode(experimentId: string, code: string): Promise<{ ok?: boolean; error?: string }> {
  const res = await fetch(
    `${API_BASE}/api/alpha-lab/${experimentId}/code`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ code }),
    }
  )
  return await res.json()
}

export async function updateAlphaName(experimentId: string, name: string): Promise<{ ok?: boolean; error?: string }> {
  const res = await fetch(
    `${API_BASE}/api/alpha-lab/${experimentId}/name`,
    {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name }),
    }
  )
  return await res.json()
}

// ── Level 5: Promotion API ─────────────────────────────────

export async function promoteAlphaExperiment(experimentId: string): Promise<{
  status?: string
  strategy_id?: string
  file?: string
  error?: string
}> {
  const res = await fetch(
    `${API_BASE}/api/alpha-lab/${experimentId}/promote`,
    { method: "POST" }
  )
  return await res.json()
}

export async function combineAlphaStrategies(
  experimentIds: string[],
  modelTier: string = "sonnet",
  guidance: string = "",
): Promise<{
  experiment_id?: string
  strategy_name?: string
  rationale?: string
  code?: string
  parent_strategies?: string[]
  cost_usd?: number
  error?: string
}> {
  const params = new URLSearchParams({
    experiment_ids: experimentIds.join(","),
    model_tier: modelTier,
    guidance,
  })
  const res = await fetch(
    `${API_BASE}/api/alpha-lab/combine?${params.toString()}`,
    { method: "POST" }
  )
  return await res.json()
}

// ── Editor Settings ────────────────────────────────────────

export async function getEditorSetting(key: string): Promise<any> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/settings/${key}`)
  const data = await res.json()
  return data.value
}

export async function saveEditorSetting(key: string, value: any): Promise<void> {
  await fetch(`${API_BASE}/api/alpha-lab/settings/${key}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ value }),
  })
}

// ── Pipeline Triggers ──────────────────────────────────────

export async function runPipelineIngest(): Promise<{ ok: boolean; message?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/api/pipeline/run/ingest`, { method: "POST" })
  return await res.json()
}

export async function runPipelineIngestEdgar(): Promise<{ ok: boolean; message?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/api/pipeline/run/ingest_edgar`, { method: "POST" })
  return await res.json()
}

export async function runPipelineFull(): Promise<{ ok: boolean; message?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/api/pipeline/run/full`, { method: "POST" })
  return await res.json()
}

export async function runPipelineScoring(): Promise<{ ok: boolean; message?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/api/pipeline/run/pipeline`, { method: "POST" })
  return await res.json()
}

export async function runPipelineRebalance(): Promise<{ ok: boolean; message?: string; error?: string }> {
  const res = await fetch(`${API_BASE}/api/pipeline/run/rebalance`, { method: "POST" })
  return await res.json()
}

export async function getPipelineStatus(): Promise<{ running: boolean; phase: string | null; error: string | null }> {
  const res = await fetch(`${API_BASE}/api/pipeline/status`)
  return await res.json()
}

export async function getPipelineLogs(since: number = 0): Promise<{
  logs: Array<{ ts: string; level: string; msg: string }>
  total: number
  running: boolean
}> {
  const res = await fetch(`${API_BASE}/api/pipeline/logs?since=${since}`)
  return await res.json()
}

// ── Aligned Data Pipeline API ────────────────────────────────

export interface FeatureStat {
  min: number | null
  max: number | null
  mean: number | null
  median: number | null
  std_dev: number | null
  null_pct: number
}

export interface FeatureProfile {
  dtype: string
  category: string
  description: string
  source: string
  source_rows: number
  stats: FeatureStat
}

export interface AlignedProfileResponse {
  sources?: Record<string, number>
  universe_size?: number
  features?: Record<string, FeatureProfile>
  error?: string
}

export async function fetchAlignedProfile(): Promise<AlignedProfileResponse> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/aligned-profile`)
  if (!res.ok) throw new Error(`Failed to fetch profile: ${res.status}`)
  return await res.json()
}

// ── Level 5.5: Forensic AI Backtest Auditor ──────────────────

export interface AuditModel {
  id: string
  display_name: string
}

export interface FlaggedTrade {
  ticker: string
  date: string
  reason: string
}

export interface AuditReport {
  status: "PASS" | "FAIL" | "WARNING"
  error_category: "STRUCTURAL" | "BACKTEST" | "STRATEGY" | "NONE"
  confidence: number
  flagged_trades: FlaggedTrade[]
  recommendation: string
  metrics?: {
    model: string
    input_tokens: number
    output_tokens: number
    cost_usd: number
  }
  error?: string
}

export interface TradeLedgerEntry {
  date: string
  entity_id: number
  ticker?: string
  action: "BUY" | "SELL"
  weight_delta: number
  norm_weight: number
  adj_close?: number
  volume?: number
  pnl_pct?: number | null  // realized P/L % for SELL trades (BUY = null = open)
  pnl_usd?: number | null  // realized P/L $ per share for SELL trades
}

export async function fetchAuditModels(): Promise<{ models: AuditModel[] }> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/audit/models`)
  if (!res.ok) throw new Error("Failed to fetch models")
  return await res.json()
}

export async function runForensicAudit(experimentId: string, modelId?: string): Promise<AuditReport> {
  const body = modelId ? JSON.stringify({ model_id: modelId }) : undefined
  const res = await fetch(
    `${API_BASE}/api/alpha-lab/${experimentId}/audit`,
    { 
      method: "POST",
      headers: body ? { "Content-Type": "application/json" } : undefined,
      body
    }
  )
  return await res.json()
}

export async function fetchExperimentTrades(experimentId: string): Promise<{
  trades: TradeLedgerEntry[]
  message?: string
  error?: string
}> {
  const res = await fetch(`${API_BASE}/api/alpha-lab/${experimentId}/trades`)
  if (!res.ok) return { trades: [], error: `HTTP ${res.status}` }
  return await res.json()
}

// ── Live Positions ────────────────────────────────────────────

export interface LivePosition {
  ticker: string
  shares: number
  avg_entry: number
  current_price: number
  market_value: number
  cost_basis: number
  unrealized_pnl_usd: number
  unrealized_pnl_pct: number
  strategies?: string[]
}

export interface LivePositionsResponse {
  trader_id: number
  trader_name: string
  total_equity: number
  total_cash: number
  total_invested: number
  total_unrealized_pnl: number
  positions: LivePosition[]
}

export async function fetchLivePositions(traderId: number): Promise<LivePositionsResponse> {
  const res = await fetch(`${API_BASE}/api/traders/${traderId}/positions`)
  if (!res.ok) {
    const text = await res.text().catch(() => "Unknown error")
    throw new Error(`Failed to fetch positions: ${text}`)
  }
  return (await res.json()) as LivePositionsResponse
}

export interface TraderExecution {
  id: number
  timestamp: string
  ticker: string
  action: "BUY" | "SELL"
  quantity: number
  simulated_price: number
  strategy_id: string | null
  portfolio_id: number | null
  portfolio_name: string | null
}

export async function fetchTraderExecutions(traderId: number): Promise<TraderExecution[]> {
  const res = await fetch(`${API_BASE}/api/traders/${traderId}/executions`)
  if (!res.ok) throw new Error(`Failed to fetch executions: ${res.status}`)
  return (await res.json()) as TraderExecution[]
}
