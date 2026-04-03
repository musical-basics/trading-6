"use client"

import { useEffect, useMemo, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Loader2, Sigma, BrainCircuit } from "lucide-react"
import { fetchMetricsLibrary, type MetricsLibraryResponse } from "@/lib/api"

export function MetricsLibrary() {
  const [data, setData] = useState<MetricsLibraryResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    fetchMetricsLibrary()
      .then((res) => {
        if (!cancelled) {
          setData(res)
          setError(null)
        }
      })
      .catch((err) => {
        if (!cancelled) setError(err instanceof Error ? err.message : "Failed to load Metrics Library")
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [])

  const accessRows = useMemo(() => {
    if (!data?.access_matrix) return []
    return Object.entries(data.access_matrix)
  }, [data])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error || !data) {
    return (
      <Card className="border-red-500/50 bg-red-500/10">
        <CardContent className="py-4 text-sm text-red-300">{error ?? "No data"}</CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4 pb-8">
      <Card className="border-border/50 bg-card/40">
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <Sigma className="w-4 h-4 text-cyan-400" />
            Metrics Library
          </CardTitle>
          <CardDescription>
            Complete metric dictionary and which AI subsystem can access each metric set.
          </CardDescription>
        </CardHeader>
      </Card>

      <MetricGroup
        title={data.metrics.tournament_pipeline.label}
        subtitle={data.metrics.tournament_pipeline.consumer}
        keys={data.metrics.tournament_pipeline.keys}
        tone="cyan"
      />

      <MetricGroup
        title={data.metrics.alpha_lab_backtester.label}
        subtitle={data.metrics.alpha_lab_backtester.consumer}
        keys={data.metrics.alpha_lab_backtester.keys}
        tone="emerald"
      />

      <MetricGroup
        title={data.metrics.forensic_audit.label}
        subtitle={data.metrics.forensic_audit.consumer}
        keys={data.metrics.forensic_audit.keys}
        tone="amber"
      />

      <Card className="border-border/50 bg-card/40">
        <CardHeader>
          <CardTitle className="text-sm flex items-center gap-2">
            <BrainCircuit className="w-4 h-4 text-violet-400" />
            AI Access Matrix
          </CardTitle>
          <CardDescription>
            Answers: which subsystem has access to tournament metrics vs Alpha Lab metrics.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {accessRows.map(([name, info]) => (
            <div key={name} className="border border-border/50 rounded-md p-3 bg-background/30">
              <div className="flex items-center justify-between gap-2">
                <div className="text-sm font-medium text-foreground">{name}</div>
                <div className="flex gap-2">
                  <Badge
                    variant="outline"
                    className={info.has_tournament_metrics ? "border-cyan-500/40 text-cyan-300" : "border-zinc-600 text-zinc-400"}
                  >
                    tournament: {info.has_tournament_metrics ? "yes" : "no"}
                  </Badge>
                  <Badge
                    variant="outline"
                    className={info.has_alpha_lab_metrics ? "border-emerald-500/40 text-emerald-300" : "border-zinc-600 text-zinc-400"}
                  >
                    alpha-lab: {info.has_alpha_lab_metrics ? "yes" : "no"}
                  </Badge>
                </div>
              </div>
              <div className="text-xs text-muted-foreground mt-2">{info.notes}</div>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card className="border-border/50 bg-card/40">
        <CardHeader>
          <CardTitle className="text-sm">Tournament vs Alpha Lab Comparison</CardTitle>
          <CardDescription>
            Tournament-only metrics are available in Strategy Studio but not in current Alpha Lab backtester output.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          <div>
            <div className="text-xs text-muted-foreground mb-1">Shared keys</div>
            <div className="flex flex-wrap gap-2">
              {data.comparison.shared_keys.map((k) => (
                <Badge key={`shared-${k}`} variant="outline" className="text-[10px] border-emerald-500/40 text-emerald-300">{k}</Badge>
              ))}
            </div>
          </div>
          <div>
            <div className="text-xs text-muted-foreground mb-1">Tournament-only keys</div>
            <div className="flex flex-wrap gap-2">
              {data.comparison.tournament_only_keys.map((k) => (
                <Badge key={`tour-only-${k}`} variant="outline" className="text-[10px] border-cyan-500/40 text-cyan-300">{k}</Badge>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function MetricGroup({
  title,
  subtitle,
  keys,
  tone,
}: {
  title: string
  subtitle: string
  keys: string[]
  tone: "cyan" | "emerald" | "amber"
}) {
  const toneClass =
    tone === "cyan"
      ? "border-cyan-500/40 text-cyan-300"
      : tone === "emerald"
        ? "border-emerald-500/40 text-emerald-300"
        : "border-amber-500/40 text-amber-300"

  return (
    <Card className="border-border/50 bg-card/40">
      <CardHeader className="pb-2">
        <CardTitle className="text-sm">{title}</CardTitle>
        <CardDescription>{subtitle}</CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-2">
          {keys.map((k) => (
            <Badge key={`${title}-${k}`} variant="outline" className={`text-[10px] ${toneClass}`}>
              {k}
            </Badge>
          ))}
        </div>
      </CardContent>
    </Card>
  )
}
