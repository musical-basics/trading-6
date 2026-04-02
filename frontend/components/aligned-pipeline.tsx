"use client"

import { useState, useEffect } from "react"
import { fetchAlignedProfile, type AlignedProfileResponse } from "@/lib/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import {
  Loader2,
  BookOpen,
  Binary,
  Database,
  AlertTriangle,
  RefreshCw,
} from "lucide-react"
import { cn } from "@/lib/utils"

const CATEGORY_COLORS: Record<string, string> = {
  market: "border-emerald-500/30 text-emerald-400 bg-emerald-500/10",
  fundamental: "border-purple-500/30 text-purple-400 bg-purple-500/10",
  statistical: "border-blue-500/30 text-blue-400 bg-blue-500/10",
  macro: "border-amber-500/30 text-amber-400 bg-amber-500/10",
  other: "border-zinc-500/30 text-zinc-400 bg-zinc-500/10",
}

const SOURCE_LABELS: Record<string, string> = {
  market_data: "Market",
  feature: "Feature",
  macro: "Macro",
  fundamental: "Fundamental",
}

function fmt(val: number | null | undefined, dec = 2): string {
  if (val === null || val === undefined) return "—"
  if (Math.abs(val) >= 1_000_000_000) return `${(val / 1_000_000_000).toFixed(1)}B`
  if (Math.abs(val) >= 1_000_000) return `${(val / 1_000_000).toFixed(1)}M`
  if (Math.abs(val) >= 1_000) return `${(val / 1_000).toFixed(1)}K`
  if (Math.abs(val) < 0.01 && val !== 0) return val.toExponential(2)
  return val.toFixed(val % 1 === 0 ? 0 : dec)
}

export function AlignedDataPipeline() {
  const [data, setData] = useState<AlignedProfileResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await fetchAlignedProfile()
      if (result.error) {
        setError(result.error)
      } else {
        setData(result)
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    setError(null)
    fetchAlignedProfile()
      .then((result) => {
        if (cancelled) return
        if (result.error) setError(result.error)
        else setData(result)
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })
    return () => { cancelled = true }
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !data?.features) {
    return (
      <div className="p-4 bg-red-500/10 border border-red-500/30 rounded-lg text-red-400">
        <AlertTriangle className="w-5 h-5 inline mr-2" />
        Failed to load Data Dictionary: {error || "No features found."}
      </div>
    )
  }

  const featureEntries = Object.entries(data.features)

  return (
    <div className="space-y-6 pb-12">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <div className="p-2 rounded-lg bg-blue-500/10 border border-blue-500/20">
            <BookOpen className="w-5 h-5 text-blue-400" />
          </div>
          <div>
            <h2 className="text-xl font-bold">Data Dictionary & Context</h2>
            <p className="text-sm text-muted-foreground">
              This is the exact statistical profile injected into Alpha Lab for AI prompt context.
            </p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          {data.sources && Object.entries(data.sources).map(([src, rows]) => (
            <Badge
              key={src}
              variant="outline"
              className="text-xs border-border/50 text-muted-foreground px-2 py-0.5"
            >
              {SOURCE_LABELS[src] || src}: {(rows as number).toLocaleString()} rows
            </Badge>
          ))}
          {data.universe_size && (
            <Badge variant="outline" className="border-blue-500/30 text-blue-400 text-xs px-2 py-0.5">
              <Database className="w-3 h-3 mr-1 inline" />
              {data.universe_size} Tickers
            </Badge>
          )}
          <Badge variant="outline" className="border-border/50 text-muted-foreground text-xs px-2 py-0.5">
            {featureEntries.length} Features
          </Badge>
          <Button
            variant="outline"
            size="sm"
            onClick={load}
            className="text-xs gap-1.5"
          >
            <RefreshCw className="w-3 h-3" />
            Refresh
          </Button>
        </div>
      </div>

      {/* Feature Card Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
        {featureEntries.map(([colName, info]) => {
          const stats = info.stats
          const categoryClass = CATEGORY_COLORS[info.category] || CATEGORY_COLORS.other

          return (
            <Card key={colName} className="bg-card/50 border-border/50 hover:border-border transition-colors">
              <CardHeader className="pb-2 pt-4 px-4 flex flex-row items-start justify-between">
                <div className="min-w-0 flex-1">
                  <CardTitle className="text-sm font-mono text-emerald-400 tracking-tight">
                    {colName}
                  </CardTitle>
                  <p
                    className="text-xs text-muted-foreground mt-1 line-clamp-2"
                    title={info.description}
                  >
                    {info.description}
                  </p>
                </div>
              </CardHeader>
              <CardContent className="px-4 pb-4">
                {/* Badges */}
                <div className="flex items-center gap-1.5 mb-3 flex-wrap">
                  <Badge variant="outline" className="text-[10px] bg-secondary/50 border-border/50 text-muted-foreground">
                    <Binary className="w-3 h-3 mr-1 inline" />
                    {info.dtype}
                  </Badge>
                  <Badge
                    variant="outline"
                    className={cn("text-[10px]", categoryClass)}
                  >
                    {info.category}
                  </Badge>
                  <Badge variant="outline" className="text-[10px] border-border/50 text-muted-foreground">
                    {SOURCE_LABELS[info.source] || info.source}
                  </Badge>
                  {stats.null_pct > 0 && (
                    <Badge
                      variant="outline"
                      className={cn(
                        "text-[10px]",
                        stats.null_pct > 20
                          ? "border-red-500/30 text-red-400"
                          : stats.null_pct > 5
                            ? "border-amber-500/30 text-amber-400"
                            : "border-zinc-500/30 text-zinc-400"
                      )}
                    >
                      {stats.null_pct}% Null
                    </Badge>
                  )}
                </div>

                {/* Stat Grid */}
                <div className="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
                  <div className="flex justify-between items-center border-b border-border/30 pb-1">
                    <span className="text-muted-foreground">Min</span>
                    <span className="font-mono text-foreground">{fmt(stats.min)}</span>
                  </div>
                  <div className="flex justify-between items-center border-b border-border/30 pb-1">
                    <span className="text-muted-foreground">Max</span>
                    <span className="font-mono text-foreground">{fmt(stats.max)}</span>
                  </div>
                  <div className="flex justify-between items-center border-b border-border/30 pb-1">
                    <span className="text-muted-foreground">Median</span>
                    <span className="font-mono text-foreground">{fmt(stats.median)}</span>
                  </div>
                  <div className="flex justify-between items-center border-b border-border/30 pb-1">
                    <span className="text-muted-foreground">Mean</span>
                    <span className="font-mono text-foreground">{fmt(stats.mean)}</span>
                  </div>
                  <div className="flex justify-between items-center col-span-2 pt-1">
                    <span className="text-muted-foreground">Std Dev</span>
                    <span className="font-mono text-foreground">{fmt(stats.std_dev)}</span>
                  </div>
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}
