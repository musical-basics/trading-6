"use client"

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { 
  Send, 
  ArrowUpRight, 
  ArrowDownRight, 
  Minus,
  CheckCircle2,
  Clock,
  AlertCircle
} from "lucide-react"
import { executionOrders } from "@/lib/mock-data"
import { cn } from "@/lib/utils"

export function ExecutionLedger() {
  const [selectedOrders, setSelectedOrders] = useState<number[]>(
    executionOrders.filter(o => o.status === "pending").map(o => o.id)
  )
  const [isRouting, setIsRouting] = useState(false)

  const toggleOrder = (id: number) => {
    setSelectedOrders(prev => 
      prev.includes(id) 
        ? prev.filter(x => x !== id)
        : [...prev, id]
    )
  }

  const toggleAll = () => {
    const pendingIds = executionOrders.filter(o => o.status === "pending").map(o => o.id)
    if (selectedOrders.length === pendingIds.length) {
      setSelectedOrders([])
    } else {
      setSelectedOrders(pendingIds)
    }
  }

  const routeOrders = () => {
    setIsRouting(true)
    setTimeout(() => setIsRouting(false), 2000)
  }

  const pendingOrders = executionOrders.filter(o => o.status === "pending")
  const totalBuys = pendingOrders.filter(o => o.action === "BUY").length
  const totalSells = pendingOrders.filter(o => o.action === "SELL").length

  return (
    <div className="space-y-4">
      {/* Header with Action Button */}
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
        <div className="space-y-1">
          <h2 className="text-lg font-semibold text-foreground">Pending Orders</h2>
          <p className="text-sm text-muted-foreground">
            {selectedOrders.length} of {pendingOrders.length} orders selected for routing
          </p>
        </div>
        <Button 
          size="lg"
          className="bg-primary text-primary-foreground hover:bg-primary/90 shadow-lg shadow-primary/25"
          onClick={routeOrders}
          disabled={isRouting || selectedOrders.length === 0}
        >
          <Send className="w-4 h-4 mr-2" />
          {isRouting ? "Routing..." : "Route Paper Trades"}
        </Button>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <Card className="border-border/50 bg-card/50">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground">Total Orders</p>
                <p className="text-2xl font-mono text-foreground">{pendingOrders.length}</p>
              </div>
              <Clock className="w-5 h-5 text-muted-foreground" />
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/50 bg-card/50">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground">Buy Orders</p>
                <p className="text-2xl font-mono text-green-400">{totalBuys}</p>
              </div>
              <ArrowUpRight className="w-5 h-5 text-green-400" />
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/50 bg-card/50">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground">Sell Orders</p>
                <p className="text-2xl font-mono text-red-400">{totalSells}</p>
              </div>
              <ArrowDownRight className="w-5 h-5 text-red-400" />
            </div>
          </CardContent>
        </Card>
        <Card className="border-border/50 bg-card/50">
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-muted-foreground">Selected</p>
                <p className="text-2xl font-mono text-primary">{selectedOrders.length}</p>
              </div>
              <CheckCircle2 className="w-5 h-5 text-primary" />
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Orders Table */}
      <Card className="border-border/50 bg-card/50">
        <CardHeader className="pb-2">
          <div className="flex items-center justify-between">
            <div>
              <CardTitle className="text-sm font-medium text-foreground">Order Queue</CardTitle>
              <CardDescription className="text-xs text-muted-foreground">
                Review and route pending rebalancing orders
              </CardDescription>
            </div>
            <Badge variant="outline" className="border-primary/50 text-primary">
              Paper Trading Mode
            </Badge>
          </div>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <Table>
              <TableHeader>
                <TableRow className="border-border/50 hover:bg-transparent">
                  <TableHead className="w-12">
                    <Checkbox 
                      checked={selectedOrders.length === pendingOrders.length && pendingOrders.length > 0}
                      onCheckedChange={toggleAll}
                      className="data-[state=checked]:bg-primary data-[state=checked]:border-primary"
                    />
                  </TableHead>
                  <TableHead className="text-muted-foreground text-xs font-medium">Ticker</TableHead>
                  <TableHead className="text-muted-foreground text-xs font-medium">Action</TableHead>
                  <TableHead className="text-muted-foreground text-xs font-medium text-right">Quantity</TableHead>
                  <TableHead className="text-muted-foreground text-xs font-medium text-right">Current Wt</TableHead>
                  <TableHead className="text-muted-foreground text-xs font-medium text-right">Target Wt</TableHead>
                  <TableHead className="text-muted-foreground text-xs font-medium text-right">Change</TableHead>
                  <TableHead className="text-muted-foreground text-xs font-medium text-center">Status</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {executionOrders.map((order) => {
                  const weightChange = order.targetWeight - order.currentWeight
                  const isPending = order.status === "pending"
                  
                  return (
                    <TableRow 
                      key={order.id} 
                      className={cn(
                        "border-border/50 transition-colors",
                        isPending ? "hover:bg-accent/30 cursor-pointer" : "opacity-60"
                      )}
                      onClick={() => isPending && toggleOrder(order.id)}
                    >
                      <TableCell>
                        {isPending && (
                          <Checkbox 
                            checked={selectedOrders.includes(order.id)}
                            onCheckedChange={() => toggleOrder(order.id)}
                            onClick={(e) => e.stopPropagation()}
                            className="data-[state=checked]:bg-primary data-[state=checked]:border-primary"
                          />
                        )}
                      </TableCell>
                      <TableCell className="font-mono font-medium text-foreground">
                        {order.ticker}
                      </TableCell>
                      <TableCell>
                        <ActionBadge action={order.action} />
                      </TableCell>
                      <TableCell className="text-right font-mono text-foreground">
                        {order.quantity > 0 ? order.quantity.toLocaleString() : "—"}
                      </TableCell>
                      <TableCell className="text-right font-mono text-muted-foreground">
                        {order.currentWeight.toFixed(1)}%
                      </TableCell>
                      <TableCell className="text-right font-mono text-foreground">
                        {order.targetWeight.toFixed(1)}%
                      </TableCell>
                      <TableCell className="text-right font-mono">
                        <span className={cn(
                          weightChange > 0 ? "text-green-400" : 
                          weightChange < 0 ? "text-red-400" : 
                          "text-muted-foreground"
                        )}>
                          {weightChange > 0 ? "+" : ""}{weightChange.toFixed(1)}%
                        </span>
                      </TableCell>
                      <TableCell className="text-center">
                        <StatusBadge status={order.status} />
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      {/* Footer Note */}
      <Card className="border-border/50 bg-amber-500/5 border-amber-500/30">
        <CardContent className="p-4">
          <div className="flex items-start gap-3">
            <AlertCircle className="w-5 h-5 text-amber-400 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-medium text-amber-200">Paper Trading Notice</p>
              <p className="text-xs text-amber-300/80 mt-1">
                Orders will be simulated and tracked in the paper trading ledger. No real trades will be executed.
                Review all positions carefully before routing.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  )
}

function ActionBadge({ action }: { action: string }) {
  if (action === "BUY") {
    return (
      <Badge className="bg-green-500/20 text-green-400 border-green-500/30 gap-1">
        <ArrowUpRight className="w-3 h-3" />
        BUY
      </Badge>
    )
  }
  if (action === "SELL") {
    return (
      <Badge className="bg-red-500/20 text-red-400 border-red-500/30 gap-1">
        <ArrowDownRight className="w-3 h-3" />
        SELL
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="border-border/50 text-muted-foreground gap-1">
      <Minus className="w-3 h-3" />
      HOLD
    </Badge>
  )
}

function StatusBadge({ status }: { status: string }) {
  if (status === "pending") {
    return (
      <Badge variant="outline" className="border-amber-500/50 text-amber-400 gap-1">
        <Clock className="w-3 h-3" />
        Pending
      </Badge>
    )
  }
  return (
    <Badge variant="outline" className="border-green-500/50 text-green-400 gap-1">
      <CheckCircle2 className="w-3 h-3" />
      Complete
    </Badge>
  )
}
