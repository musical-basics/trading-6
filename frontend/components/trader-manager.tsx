"use client"

import { useState, useEffect, useCallback } from "react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Label } from "@/components/ui/label"
import { Slider } from "@/components/ui/slider"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Plus,
  DollarSign,
  ShieldAlert,
  BarChart3,
  Calendar,
  Loader2,
  Users,
  Wallet,
  AlertTriangle,
  Trash2,
} from "lucide-react"
import {
  Trader,
  Portfolio,
  Strategy,
  getTraders,
  createTrader,
  updateConstraints,
  getPortfolios,
  updatePortfolioStrategy,
  updatePortfolioSchedule,
  fetchStrategies,
  deleteTrader,
} from "@/lib/api"
import { TraderBacktest } from "./trader-backtest"

export function TraderManager() {
  // ── State ────────────────────────────────────────────────
  const [traders, setTraders] = useState<Trader[]>([])
  const [selectedTrader, setSelectedTrader] = useState<Trader | null>(null)
  const [portfolios, setPortfolios] = useState<Portfolio[]>([])
  const [strategies, setStrategies] = useState<Strategy[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  // Create dialog
  const [showCreateDialog, setShowCreateDialog] = useState(false)
  const [newTraderName, setNewTraderName] = useState("")
  const [newTraderCapital, setNewTraderCapital] = useState("10000")
  const [numPortfolios, setNumPortfolios] = useState("10")
  const [capitalPerPortfolio, setCapitalPerPortfolio] = useState("")
  const [creating, setCreating] = useState(false)

  // Delete dialog
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)
  const [deleting, setDeleting] = useState(false)

  // Constraints
  const [maxDrawdown, setMaxDrawdown] = useState(20)
  const [maxPositions, setMaxPositions] = useState(50)

  // Sub-tab: "portfolios" or "backtest"
  const [activeTab, setActiveTab] = useState<"portfolios" | "backtest">("portfolios")

  // ── Load data ────────────────────────────────────────────
  const loadTraders = useCallback(async () => {
    try {
      const [t, s] = await Promise.all([getTraders(), fetchStrategies()])
      setTraders(t)
      setStrategies(s)
      if (t.length > 0 && !selectedTrader) {
        setSelectedTrader(t[0])
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load traders")
    } finally {
      setLoading(false)
    }
  }, [selectedTrader])

  const loadPortfolios = useCallback(async (traderId: number) => {
    try {
      const p = await getPortfolios(traderId)
      setPortfolios(p)
    } catch (e) {
      console.error("Failed to load portfolios:", e)
    }
  }, [])

  useEffect(() => {
    loadTraders()
  }, [loadTraders])

  useEffect(() => {
    if (selectedTrader) {
      loadPortfolios(selectedTrader.id)
      if (selectedTrader.constraints) {
        setMaxDrawdown(Math.round(selectedTrader.constraints.max_drawdown_pct * 100))
        setMaxPositions(selectedTrader.constraints.max_open_positions)
      }
    }
  }, [selectedTrader, loadPortfolios])

  // ── Handlers ─────────────────────────────────────────────
  const handleCreateTrader = async () => {
    setCreating(true)
    try {
      const numP = Math.max(1, Math.min(12, parseInt(numPortfolios) || 10))
      const capPer = capitalPerPortfolio ? parseFloat(capitalPerPortfolio) : undefined
      const trader = await createTrader(
        newTraderName,
        parseFloat(newTraderCapital),
        numP,
        capPer,
      )
      setTraders((prev) => [trader, ...prev])
      setSelectedTrader(trader)
      setShowCreateDialog(false)
      setNewTraderName("")
      setNewTraderCapital("10000")
      setNumPortfolios("10")
      setCapitalPerPortfolio("")
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to create trader")
    } finally {
      setCreating(false)
    }
  }

  const handleDeleteTrader = async () => {
    if (!selectedTrader) return
    setDeleting(true)
    try {
      await deleteTrader(selectedTrader.id)
      const updatedTraders = traders.filter(t => t.id !== selectedTrader.id)
      setTraders(updatedTraders)
      setSelectedTrader(updatedTraders.length > 0 ? updatedTraders[0] : null)
      setShowDeleteDialog(false)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete trader")
    } finally {
      setDeleting(false)
    }
  }

  const handleConstraintUpdate = async (field: string, value: number) => {
    if (!selectedTrader) return
    try {
      const data: Record<string, number> = {}
      if (field === "max_drawdown_pct") data.max_drawdown_pct = value / 100
      if (field === "max_open_positions") data.max_open_positions = value
      await updateConstraints(selectedTrader.id, data)
    } catch (e) {
      console.error("Failed to update constraint:", e)
    }
  }

  const handleStrategyChange = async (portfolioId: number, strategyId: string) => {
    try {
      await updatePortfolioStrategy(portfolioId, strategyId)
      setPortfolios((prev) =>
        prev.map((p) =>
          p.id === portfolioId
            ? {
                ...p,
                strategy_id: strategyId,
                strategy_name:
                  strategies.find((s) => s.id === strategyId)?.name ?? strategyId,
              }
            : p
        )
      )
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Failed to assign strategy"
      setError(msg)
    }
  }

  // Compute which strategies are already used by other portfolios
  const usedStrategies = new Set(
    portfolios
      .filter((p) => p.strategy_id)
      .map((p) => p.strategy_id as string)
  )

  const handleScheduleChange = async (portfolioId: number, freq: string) => {
    try {
      await updatePortfolioSchedule(portfolioId, freq)
      setPortfolios((prev) =>
        prev.map((p) =>
          p.id === portfolioId ? { ...p, rebalance_freq: freq } : p
        )
      )
    } catch (e) {
      console.error("Failed to update schedule:", e)
    }
  }

  // ── Loading state ────────────────────────────────────────
  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {error && (
        <div className="p-3 rounded-lg bg-destructive/10 border border-destructive/20 text-destructive text-sm flex items-center gap-2">
          <AlertTriangle className="w-4 h-4 shrink-0" />
          {error}
          <button onClick={() => setError(null)} className="ml-auto underline text-xs">
            Dismiss
          </button>
        </div>
      )}

      {/* ── Trader Header ────────────────────────────────── */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-primary/10">
            <Users className="w-5 h-5 text-primary" />
          </div>
          <div>
            <h2 className="text-lg font-semibold">Traders & Portfolios</h2>
            <p className="text-xs text-muted-foreground">
              Manage capital allocation across isolated sub-portfolios
            </p>
          </div>
        </div>

        <Dialog open={showCreateDialog} onOpenChange={setShowCreateDialog}>
          <DialogTrigger asChild>
            <Button size="sm">
              <Plus className="w-4 h-4 mr-1" />
              New Trader
            </Button>
          </DialogTrigger>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Create Trader</DialogTitle>
              <DialogDescription>
                Creates a trader with 10 equally-allocated sub-portfolios.
              </DialogDescription>
            </DialogHeader>
            <div className="space-y-4 py-4">
              <div className="space-y-2">
                <Label>Trader Name</Label>
                <Input
                  value={newTraderName}
                  onChange={(e) => setNewTraderName(e.target.value)}
                  placeholder="e.g. Alpha Trader"
                />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>Total Capital ($)</Label>
                  <Input
                    type="number"
                    value={newTraderCapital}
                    onChange={(e) => setNewTraderCapital(e.target.value)}
                    placeholder="10000"
                  />
                </div>
                <div className="space-y-2">
                  <Label># Portfolios (1-12)</Label>
                  <Input
                    type="number"
                    value={numPortfolios}
                    onChange={(e) => setNumPortfolios(e.target.value)}
                    min={1}
                    max={12}
                    placeholder="10"
                  />
                </div>
              </div>
              <div className="space-y-2">
                <Label>Capital per Portfolio ($)</Label>
                <Input
                  type="number"
                  value={capitalPerPortfolio}
                  onChange={(e) => setCapitalPerPortfolio(e.target.value)}
                  placeholder={`Auto: $${(parseFloat(newTraderCapital || "0") / (parseInt(numPortfolios) || 10)).toLocaleString()}`}
                />
                <p className="text-xs text-muted-foreground">
                  Leave blank to auto-split: ${(parseFloat(newTraderCapital || "0") / (parseInt(numPortfolios) || 10)).toLocaleString()} each
                </p>
              </div>
            </div>
            <DialogFooter>
              <Button
                onClick={handleCreateTrader}
                disabled={creating || !newTraderName.trim()}
              >
                {creating ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-1 animate-spin" />
                    Creating...
                  </>
                ) : (
                  "Create Trader"
                )}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>

      {/* ── Trader Selector ──────────────────────────────── */}
      {traders.length > 0 && (
        <div className="flex gap-2 flex-wrap">
          {traders.map((trader) => (
            <button
              key={trader.id}
              onClick={() => setSelectedTrader(trader)}
              className={`
                flex items-center gap-2 px-4 py-2 rounded-lg border text-sm transition-all
                ${
                  selectedTrader?.id === trader.id
                    ? "bg-primary/10 border-primary/40 text-primary"
                    : "bg-card border-border text-muted-foreground hover:border-primary/20 hover:text-foreground"
                }
              `}
            >
              <Wallet className="w-4 h-4" />
              <span className="font-medium">{trader.name}</span>
              <Badge variant="outline" className="text-[10px] h-5">
                ${trader.total_capital.toLocaleString()}
              </Badge>
            </button>
          ))}
        </div>
      )}

      {traders.length === 0 && (
        <Card className="border-dashed">
          <CardContent className="flex flex-col items-center justify-center py-12">
            <Users className="w-12 h-12 text-muted-foreground/30 mb-4" />
            <p className="text-muted-foreground mb-2">No traders yet</p>
            <Button size="sm" onClick={() => setShowCreateDialog(true)}>
              <Plus className="w-4 h-4 mr-1" />
              Create Your First Trader
            </Button>
          </CardContent>
        </Card>
      )}

      {/* ── Selected Trader Details ───────────────────────── */}
      {selectedTrader && (
        <div className="space-y-4">
          {/* Sub-tab navigation */}
          <div className="flex items-center justify-between">
            <div className="flex gap-1 p-1 bg-card/50 rounded-lg border border-border w-fit">
              <button
                onClick={() => setActiveTab("portfolios")}
                className={`
                  px-4 py-1.5 rounded-md text-sm font-medium transition-all
                  ${activeTab === "portfolios"
                    ? "bg-primary/10 text-primary shadow-sm"
                    : "text-muted-foreground hover:text-foreground"}
                `}
              >
                <Wallet className="w-3.5 h-3.5 inline mr-1.5 -mt-0.5" />
                Portfolios
              </button>
              <button
                onClick={() => setActiveTab("backtest")}
                className={`
                  px-4 py-1.5 rounded-md text-sm font-medium transition-all
                  ${activeTab === "backtest"
                    ? "bg-primary/10 text-primary shadow-sm"
                    : "text-muted-foreground hover:text-foreground"}
                `}
              >
                <BarChart3 className="w-3.5 h-3.5 inline mr-1.5 -mt-0.5" />
                Backtest
              </button>
            </div>

            <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
              <DialogTrigger asChild>
                <Button variant="outline" size="sm" className="text-destructive border-destructive/20 hover:bg-destructive/10">
                  <Trash2 className="w-4 h-4 mr-1" />
                  Delete Trader
                </Button>
              </DialogTrigger>
              <DialogContent>
                <DialogHeader>
                  <DialogTitle>Delete Trader</DialogTitle>
                  <DialogDescription>
                    Are you sure you want to delete <strong>{selectedTrader.name}</strong>? This action will permanently remove all associated constraints and sub-portfolios.
                  </DialogDescription>
                </DialogHeader>
                <DialogFooter>
                  <Button variant="outline" onClick={() => setShowDeleteDialog(false)}>
                    Cancel
                  </Button>
                  <Button
                    variant="destructive"
                    onClick={handleDeleteTrader}
                    disabled={deleting}
                  >
                    {deleting ? (
                      <>
                        <Loader2 className="w-4 h-4 mr-1 animate-spin" />
                        Deleting...
                      </>
                    ) : (
                      "Delete"
                    )}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>

          {/* Portfolios Tab */}
          {activeTab === "portfolios" && (
          <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
          {/* Constraints Card */}
          <Card className="lg:col-span-1">
            <CardHeader className="pb-3">
              <CardTitle className="text-sm flex items-center gap-2">
                <ShieldAlert className="w-4 h-4 text-amber-400" />
                Risk Constraints
              </CardTitle>
              <CardDescription className="text-[10px]">
                Global limits for {selectedTrader.name}
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-2">
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Max Drawdown</span>
                  <span className="font-mono text-amber-400">{maxDrawdown}%</span>
                </div>
                <Slider
                  value={[maxDrawdown]}
                  onValueChange={([v]) => setMaxDrawdown(v)}
                  onValueCommit={([v]) =>
                    handleConstraintUpdate("max_drawdown_pct", v)
                  }
                  min={5}
                  max={50}
                  step={1}
                  className="cursor-pointer"
                />
              </div>

              <div className="space-y-2">
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Max Positions</span>
                  <span className="font-mono">{maxPositions}</span>
                </div>
                <Slider
                  value={[maxPositions]}
                  onValueChange={([v]) => setMaxPositions(v)}
                  onValueCommit={([v]) =>
                    handleConstraintUpdate("max_open_positions", v)
                  }
                  min={5}
                  max={100}
                  step={5}
                  className="cursor-pointer"
                />
              </div>

              <div className="pt-2 border-t border-border">
                <div className="flex justify-between text-xs mb-1">
                  <span className="text-muted-foreground">Capital per Portfolio</span>
                  <span className="font-mono">
                    ${portfolios.length > 0
                      ? portfolios[0].allocated_capital.toLocaleString()
                      : (selectedTrader.total_capital / 10).toLocaleString()}
                  </span>
                </div>
                <div className="flex justify-between text-xs">
                  <span className="text-muted-foreground">Active Portfolios</span>
                  <span className="font-mono">
                    {portfolios.filter((p) => p.strategy_id).length} / {portfolios.length}
                  </span>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Portfolio Grid */}
          <div className="lg:col-span-3">
            <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-5 gap-3">
              {portfolios.map((portfolio) => (
                <Card
                  key={portfolio.id}
                  className={`relative transition-all ${
                    portfolio.strategy_id
                      ? "border-primary/20 bg-card"
                      : "border-dashed border-border/50 bg-card/50"
                  }`}
                >
                  <CardContent className="p-4 space-y-3">
                    {/* Header */}
                    <div className="flex items-center justify-between">
                      <span className="text-xs font-semibold text-foreground">
                        {portfolio.name}
                      </span>
                      <Badge
                        variant={portfolio.strategy_id ? "default" : "outline"}
                        className="text-[9px] h-4 px-1.5"
                      >
                        {portfolio.strategy_id ? "Active" : "Unassigned"}
                      </Badge>
                    </div>

                    {/* Capital */}
                    <div className="flex items-center gap-1.5">
                      <DollarSign className="w-3 h-3 text-green-400" />
                      <span className="text-sm font-mono text-green-400">
                        {portfolio.allocated_capital.toLocaleString()}
                      </span>
                    </div>

                    {/* Strategy Dropdown */}
                    <div className="space-y-1">
                      <Label className="text-[10px] text-muted-foreground flex items-center gap-1">
                        <BarChart3 className="w-3 h-3" />
                        Strategy
                      </Label>
                      <Select
                        value={portfolio.strategy_id ?? ""}
                        onValueChange={(v) => handleStrategyChange(portfolio.id, v)}
                      >
                        <SelectTrigger className="h-7 text-xs">
                          <SelectValue placeholder="Select..." />
                        </SelectTrigger>
                        <SelectContent>
                          {strategies.map((s) => {
                            const isUsed = usedStrategies.has(s.id) && s.id !== portfolio.strategy_id
                            return (
                              <SelectItem
                                key={s.id}
                                value={s.id}
                                className="text-xs"
                                disabled={isUsed}
                              >
                                {s.name}{isUsed ? " (in use)" : ""}
                              </SelectItem>
                            )
                          })}
                        </SelectContent>
                      </Select>
                    </div>

                    {/* Schedule Dropdown */}
                    <div className="space-y-1">
                      <Label className="text-[10px] text-muted-foreground flex items-center gap-1">
                        <Calendar className="w-3 h-3" />
                        Rebalance
                      </Label>
                      <Select
                        value={portfolio.rebalance_freq}
                        onValueChange={(v) =>
                          handleScheduleChange(portfolio.id, v)
                        }
                      >
                        <SelectTrigger className="h-7 text-xs">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="Daily" className="text-xs">
                            Daily
                          </SelectItem>
                          <SelectItem value="Weekly" className="text-xs">
                            Weekly
                          </SelectItem>
                          <SelectItem value="Monthly" className="text-xs">
                            Monthly
                          </SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    {/* Next rebalance */}
                    {portfolio.next_rebalance_date && (
                      <p className="text-[10px] text-muted-foreground">
                        Next: {portfolio.next_rebalance_date}
                      </p>
                    )}
                  </CardContent>
                </Card>
              ))}
            </div>
          </div>
          </div>
          )}

          {/* Backtest Tab */}
          {activeTab === "backtest" && (
            <TraderBacktest trader={selectedTrader} />
          )}
        </div>
      )}
    </div>
  )
}
