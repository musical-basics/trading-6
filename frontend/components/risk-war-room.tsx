"use client"

import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import {
  Bar,
  BarChart,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  ReferenceLine,
} from "recharts"
import { 
  TrendingUp, 
  TrendingDown, 
  Activity, 
  Gauge,
  Shield
} from "lucide-react"
import { riskData } from "@/lib/mock-data"
import { cn } from "@/lib/utils"

export function RiskWarRoom() {
  const data = riskData

  return (
    <div className="space-y-4">
      {/* Top Macro Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        <MacroCard
          title="VIX"
          value={data.vix.toFixed(2)}
          change={data.vixChange}
          icon={<Activity className="w-5 h-5" />}
          description="Volatility Index"
        />
        <MacroCard
          title="10Y Yield"
          value={`${data.tenYearYield.toFixed(2)}%`}
          change={data.yieldChange}
          icon={<Gauge className="w-5 h-5" />}
          description="Treasury Yield"
        />
        <Card className="border-border/50 bg-card/50">
          <CardContent className="p-4">
            <div className="flex items-start justify-between">
              <div>
                <p className="text-xs text-muted-foreground uppercase tracking-wider">Macro Regime</p>
                <div className="flex items-center gap-2 mt-2">
                  <Badge className={cn(
                    "text-sm font-medium px-3 py-1",
                    data.macroRegime === "Risk-On" 
                      ? "bg-green-500/20 text-green-400 border-green-500/30"
                      : "bg-red-500/20 text-red-400 border-red-500/30"
                  )}>
                    {data.macroRegime}
                  </Badge>
                </div>
                <p className="text-xs text-muted-foreground mt-2">
                  Confidence: {(data.regimeConfidence * 100).toFixed(0)}%
                </p>
              </div>
              <div className={cn(
                "p-2 rounded-lg",
                data.macroRegime === "Risk-On" ? "bg-green-500/20" : "bg-red-500/20"
              )}>
                <Shield className={cn(
                  "w-5 h-5",
                  data.macroRegime === "Risk-On" ? "text-green-400" : "text-red-400"
                )} />
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Main Content - Covariance Matrix and MCR Chart */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Covariance Matrix Heatmap */}
        <Card className="border-border/50 bg-card/50">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-foreground">Covariance Matrix</CardTitle>
            <CardDescription className="text-xs text-muted-foreground">
              Portfolio correlation structure
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="overflow-x-auto">
              <div className="min-w-[400px]">
                {/* Column headers */}
                <div className="flex mb-1">
                  <div className="w-14 shrink-0" />
                  {data.tickers.map((ticker) => (
                    <div 
                      key={ticker} 
                      className="flex-1 text-center text-[10px] font-mono text-muted-foreground"
                    >
                      {ticker}
                    </div>
                  ))}
                </div>
                
                {/* Matrix rows */}
                {data.covarianceMatrix.map((row, i) => (
                  <div key={i} className="flex gap-0.5 mb-0.5">
                    <div className="w-14 shrink-0 text-[10px] font-mono text-muted-foreground flex items-center">
                      {data.tickers[i]}
                    </div>
                    {row.map((value, j) => (
                      <div
                        key={j}
                        className="flex-1 aspect-square rounded-sm flex items-center justify-center text-[9px] font-mono"
                        style={{
                          backgroundColor: getHeatmapColor(value),
                          color: value > 0.5 ? 'rgba(255,255,255,0.9)' : 'rgba(255,255,255,0.7)'
                        }}
                        title={`${data.tickers[i]} ↔ ${data.tickers[j]}: ${value.toFixed(2)}`}
                      >
                        {value.toFixed(2)}
                      </div>
                    ))}
                  </div>
                ))}
                
                {/* Legend */}
                <div className="flex items-center justify-center gap-2 mt-4 pt-4 border-t border-border/50">
                  <span className="text-xs text-muted-foreground">Low</span>
                  <div className="flex h-3 rounded-sm overflow-hidden">
                    {[0, 0.25, 0.5, 0.75, 1].map((v, i) => (
                      <div 
                        key={i}
                        className="w-8 h-full"
                        style={{ backgroundColor: getHeatmapColor(v) }}
                      />
                    ))}
                  </div>
                  <span className="text-xs text-muted-foreground">High</span>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* MCR Bar Chart */}
        <Card className="border-border/50 bg-card/50">
          <CardHeader className="pb-2">
            <CardTitle className="text-sm font-medium text-foreground">
              Top 5 Marginal Contributors to Risk
            </CardTitle>
            <CardDescription className="text-xs text-muted-foreground">
              MCR threshold at 5% (red dashed line)
            </CardDescription>
          </CardHeader>
          <CardContent className="h-[300px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart 
                data={data.mcrData} 
                layout="vertical"
                margin={{ top: 10, right: 30, left: 50, bottom: 10 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" horizontal={false} />
                <XAxis 
                  type="number" 
                  tick={{ fill: '#6b7280', fontSize: 11 }}
                  tickFormatter={(value) => `${value}%`}
                  domain={[0, 8]}
                  tickLine={false}
                  axisLine={false}
                />
                <YAxis 
                  type="category" 
                  dataKey="ticker" 
                  tick={{ fill: '#e5e7eb', fontSize: 12, fontFamily: 'monospace' }}
                  tickLine={false}
                  axisLine={false}
                  width={45}
                />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'rgba(15, 23, 42, 0.95)', 
                    border: '1px solid rgba(255,255,255,0.1)',
                    borderRadius: '8px',
                    fontSize: '12px'
                  }}
                  formatter={(value: number) => [`${value.toFixed(1)}%`, 'MCR']}
                />
                <ReferenceLine 
                  x={5} 
                  stroke="#ef4444" 
                  strokeDasharray="5 5" 
                  strokeWidth={2}
                  label={{ 
                    value: '5% Threshold', 
                    position: 'top', 
                    fill: '#ef4444',
                    fontSize: 10
                  }}
                />
                <Bar 
                  dataKey="mcr" 
                  radius={[0, 4, 4, 0]}
                  fill="url(#mcrGradient)"
                />
                <defs>
                  <linearGradient id="mcrGradient" x1="0" y1="0" x2="1" y2="0">
                    <stop offset="0%" stopColor="#06b6d4" />
                    <stop offset="60%" stopColor="#06b6d4" />
                    <stop offset="100%" stopColor="#f59e0b" />
                  </linearGradient>
                </defs>
              </BarChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>
      </div>

      {/* Risk Summary */}
      <Card className="border-border/50 bg-card/50">
        <CardContent className="p-4">
          <div className="flex flex-wrap gap-6 items-center justify-between">
            <div className="flex items-center gap-4">
              <div className="p-3 rounded-lg bg-primary/20">
                <Shield className="w-6 h-6 text-primary" />
              </div>
              <div>
                <p className="text-sm font-medium text-foreground">Portfolio Risk Status</p>
                <p className="text-xs text-muted-foreground">2 positions exceeding MCR threshold</p>
              </div>
            </div>
            <div className="flex gap-6">
              <div className="text-center">
                <p className="text-xs text-muted-foreground">Total Positions</p>
                <p className="text-lg font-mono text-foreground">8</p>
              </div>
              <div className="text-center">
                <p className="text-xs text-muted-foreground">Breach Count</p>
                <p className="text-lg font-mono text-amber-400">2</p>
              </div>
              <div className="text-center">
                <p className="text-xs text-muted-foreground">Avg Correlation</p>
                <p className="text-lg font-mono text-foreground">0.45</p>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function MacroCard({ 
  title, 
  value, 
  change, 
  icon, 
  description 
}: { 
  title: string
  value: string
  change: number
  icon: React.ReactNode
  description: string
}) {
  const isPositive = change >= 0
  
  return (
    <Card className="border-border/50 bg-card/50">
      <CardContent className="p-4">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-xs text-muted-foreground uppercase tracking-wider">{title}</p>
            <p className="text-2xl font-mono text-foreground mt-1">{value}</p>
            <div className="flex items-center gap-1 mt-1">
              {isPositive ? (
                <TrendingUp className="w-3 h-3 text-green-400" />
              ) : (
                <TrendingDown className="w-3 h-3 text-red-400" />
              )}
              <span className={cn(
                "text-xs font-mono",
                isPositive ? "text-green-400" : "text-red-400"
              )}>
                {isPositive ? "+" : ""}{change.toFixed(2)}
              </span>
            </div>
            <p className="text-xs text-muted-foreground mt-1">{description}</p>
          </div>
          <div className="p-2 rounded-lg bg-primary/20">
            <div className="text-primary">{icon}</div>
          </div>
        </div>
      </CardContent>
    </Card>
  )
}

function getHeatmapColor(value: number): string {
  // Gradient from dark blue (low) to cyan (medium) to yellow/orange (high)
  if (value < 0.3) {
    return `rgba(30, 64, 175, ${0.3 + value})`
  } else if (value < 0.6) {
    return `rgba(6, 182, 212, ${0.4 + value * 0.5})`
  } else {
    return `rgba(245, 158, 11, ${0.5 + value * 0.4})`
  }
}
