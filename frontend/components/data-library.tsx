"use client"

import { useEffect, useMemo, useState } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Loader2, BookOpen, Database } from "lucide-react"
import {
  fetchDataLibrary,
  type DataLibraryResponse,
  type DataLibraryComponent,
} from "@/lib/api"

const ORDER = ["fundamental", "action_intent", "target_portfolio", "feature", "market_data"]

export function DataLibrary() {
  const [data, setData] = useState<DataLibraryResponse | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    fetchDataLibrary()
      .then((result) => {
        if (!cancelled) {
          setData(result)
          setError(null)
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to load Data Library")
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false)
      })

    return () => {
      cancelled = true
    }
  }, [])

  const components = useMemo(() => {
    if (!data?.components) return []
    const keys = Object.keys(data.components)
    keys.sort((a, b) => {
      const ia = ORDER.indexOf(a)
      const ib = ORDER.indexOf(b)
      if (ia === -1 && ib === -1) return a.localeCompare(b)
      if (ia === -1) return 1
      if (ib === -1) return -1
      return ia - ib
    })
    return keys.map((k) => data.components[k])
  }, [data])

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    )
  }

  if (error) {
    return (
      <Card className="border-red-500/50 bg-red-500/10">
        <CardContent className="py-4 text-sm text-red-300">{error}</CardContent>
      </Card>
    )
  }

  return (
    <div className="space-y-4 pb-8">
      <Card className="border-border/50 bg-card/40">
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            <BookOpen className="w-4 h-4 text-primary" />
            Data Library
          </CardTitle>
          <CardDescription>
            Header-level dictionary only. No row-level data is shown here.
            This helps decode counts like "Strategy Intent = 173" for a ticker.
          </CardDescription>
        </CardHeader>
      </Card>

      {components.map((comp) => (
        <ComponentCard key={comp.component} comp={comp} />
      ))}
    </div>
  )
}

function ComponentCard({ comp }: { comp: DataLibraryComponent }) {
  return (
    <Card className="border-border/50 bg-card/40">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between gap-3">
          <CardTitle className="text-sm flex items-center gap-2">
            <Database className="w-4 h-4 text-cyan-400" />
            {comp.label}
            <Badge variant="outline" className="text-[10px] border-border/50 text-muted-foreground">
              {comp.component}
            </Badge>
          </CardTitle>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="text-[10px] border-border/50 text-muted-foreground">
              rows: {comp.row_count.toLocaleString()}
            </Badge>
            <Badge variant="outline" className="text-[10px] border-border/50 text-muted-foreground">
              entities: {comp.entity_count ?? "-"}
            </Badge>
          </div>
        </div>
        <CardDescription>
          {comp.count_meaning}
          {comp.date_start && comp.date_end ? ` Date range: ${comp.date_start} to ${comp.date_end}.` : ""}
        </CardDescription>
      </CardHeader>
      <CardContent>
        <div className="flex flex-wrap gap-2 mb-3">
          <Badge variant="outline" className="text-[10px] border-emerald-500/30 text-emerald-400">
            date column: {comp.date_col}
          </Badge>
          <Badge variant="outline" className="text-[10px] border-amber-500/30 text-amber-400">
            key columns: {comp.key_cols.join(", ")}
          </Badge>
        </div>

        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow className="border-border/50 hover:bg-transparent">
                <TableHead className="text-xs">Header</TableHead>
                <TableHead className="text-xs">Type</TableHead>
                <TableHead className="text-xs text-right">Null %</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {comp.columns.map((col) => (
                <TableRow key={`${comp.component}-${col.name}`} className="border-border/50 hover:bg-accent/20">
                  <TableCell className="font-mono text-xs text-foreground">{col.name}</TableCell>
                  <TableCell className="text-xs text-muted-foreground">{col.dtype}</TableCell>
                  <TableCell className="text-right text-xs">
                    <span
                      className={
                        col.null_pct > 20
                          ? "text-amber-400"
                          : col.null_pct > 0
                            ? "text-zinc-300"
                            : "text-emerald-400"
                      }
                    >
                      {col.null_pct.toFixed(1)}%
                    </span>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  )
}
