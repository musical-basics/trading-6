"use client"

import { useState, useEffect } from "react"
import { 
  FlaskConical, 
  Search, 
  ShieldCheck, 
  ShieldAlert,
  FileText, 
  Terminal,
  Command,
  Bell,
  Settings,
  User,
  ChevronRight,
  Activity,
  Users,
  Database,
  Sparkles,
  BookOpen,
  Briefcase,
  Sigma,
} from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Separator } from "@/components/ui/separator"
import { cn } from "@/lib/utils"
import { StrategyStudio } from "./strategy-studio"
import { XRayInspector } from "./xray-inspector"
import { RiskWarRoom } from "./risk-war-room"
import { ExecutionLedger } from "./execution-ledger"
import { TraderManager } from "./trader-manager"
import { DataPipeline } from "./data-pipeline"
import { IndicatorsAnalysis } from "./indicators-analysis"
import AlphaLab from "./alpha-lab"
import { AlignedDataPipeline } from "./aligned-pipeline"
import { ForensicAuditor } from "./forensic-auditor"
import { LivePositions } from "./live-positions"
import { DataLibrary } from "./data-library"
import { MetricsLibrary } from "./metrics-library"

type View = "data-pipeline" | "data-library" | "metrics-library" | "aligned-pipeline" | "indicators-analysis" | "alpha-lab" | "forensic-auditor" | "strategy-studio" | "xray-inspector" | "risk-war-room" | "execution-ledger" | "trader-manager" | "live-positions"

const navItems = [
  {
    id: "data-pipeline" as const,
    label: "Data Pipeline",
    icon: Database,
    description: "Coverage & Quality",
    badge: null
  },
  {
    id: "indicators-analysis" as const,
    label: "Indicators Analysis",
    icon: Activity,
    description: "Technical & Fundamental",
    badge: null
  },
  {
    id: "aligned-pipeline" as const,
    label: "Data Dictionary",
    icon: BookOpen,
    description: "LLM Statistical Context",
    badge: "Profile"
  },
  {
    id: "data-library" as const,
    label: "Data Library",
    icon: BookOpen,
    description: "Header Viewer",
    badge: "Schema"
  },
  {
    id: "metrics-library" as const,
    label: "Metrics Library",
    icon: Sigma,
    description: "AI Access Matrix",
    badge: "Metrics"
  },
  {
    id: "alpha-lab" as const,
    label: "Alpha Lab",
    icon: Sparkles,
    description: "Strategy Discovery",
    badge: "AI"
  },
  {
    id: "forensic-auditor" as const,
    label: "Forensic Auditor",
    icon: ShieldAlert,
    description: "AI Strategy Verification",
    badge: "Audit"
  },
  { 
    id: "strategy-studio" as const, 
    label: "Strategy Studio", 
    icon: FlaskConical, 
    description: "The Sandbox",
    badge: "12 strategies"
  },
  { 
    id: "xray-inspector" as const, 
    label: "X-Ray Inspector", 
    icon: Search, 
    description: "Verification Engine",
    badge: null
  },
  { 
    id: "risk-war-room" as const, 
    label: "Risk War Room", 
    icon: ShieldCheck, 
    description: "Macro & Covariance",
    badge: "2 alerts"
  },
  { 
    id: "execution-ledger" as const, 
    label: "Execution Ledger", 
    icon: FileText, 
    description: "Order Management",
    badge: "7 pending"
  },
  { 
    id: "trader-manager" as const, 
    label: "Traders & Portfolios", 
    icon: Users, 
    description: "Capital Management",
    badge: null
  },
  {
    id: "live-positions" as const,
    label: "Live Positions",
    icon: Briefcase,
    description: "Real-time PnL & Holdings",
    badge: null
  },
]

export function DashboardShell() {
  const [currentView, setCurrentView] = useState<View>("strategy-studio")
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false)

  // Restore saved view on client mount (avoids SSR hydration mismatch)
  useEffect(() => {
    const saved = localStorage.getItem("quantprime-view")
    if (saved && navItems.some(item => item.id === saved)) {
      setCurrentView(saved as View)
    }
  }, [])

  const handleViewChange = (view: View) => {
    setCurrentView(view)
    localStorage.setItem("quantprime-view", view)
  }

  const currentNavItem = navItems.find(item => item.id === currentView)

  return (
    <div className="flex h-screen bg-background">
      {/* Sidebar */}
      <aside className={cn(
        "flex flex-col border-r border-sidebar-border bg-sidebar transition-all duration-300",
        isSidebarCollapsed ? "w-16" : "w-64"
      )}>
        {/* Logo */}
        <div className="h-14 flex items-center gap-3 px-4 border-b border-sidebar-border">
          <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center shrink-0">
            <Terminal className="w-4 h-4 text-primary-foreground" />
          </div>
          {!isSidebarCollapsed && (
            <div className="flex flex-col">
              <span className="text-sm font-semibold text-sidebar-foreground">QuantPrime</span>
              <span className="text-[10px] text-muted-foreground">Terminal v2.4</span>
            </div>
          )}
        </div>

        {/* Navigation */}
        <ScrollArea className="flex-1 px-2 py-4">
          <div className="space-y-1">
            {navItems.map((item) => {
              const Icon = item.icon
              const isActive = currentView === item.id
              
              return (
                <button
                  key={item.id}
                  onClick={() => handleViewChange(item.id)}
                  className={cn(
                    "w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-left transition-all",
                    isActive 
                      ? "bg-sidebar-accent text-sidebar-accent-foreground" 
                      : "text-muted-foreground hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
                  )}
                >
                  <div className={cn(
                    "p-1.5 rounded-md shrink-0",
                    isActive ? "bg-sidebar-primary text-sidebar-primary-foreground" : "bg-transparent"
                  )}>
                    <Icon className="w-4 h-4" />
                  </div>
                  {!isSidebarCollapsed && (
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium truncate">{item.label}</p>
                      <p className="text-[10px] text-muted-foreground truncate">{item.description}</p>
                    </div>
                  )}
                  {!isSidebarCollapsed && item.badge && (
                    <Badge 
                      variant="outline" 
                      className={cn(
                        "text-[10px] px-1.5 py-0 h-5 shrink-0",
                        item.id === "risk-war-room" 
                          ? "border-amber-500/50 text-amber-400" 
                          : "border-sidebar-border text-muted-foreground"
                      )}
                    >
                      {item.badge}
                    </Badge>
                  )}
                </button>
              )
            })}
          </div>
        </ScrollArea>

        {/* Collapse Toggle */}
        <div className="p-2 border-t border-sidebar-border">
          <Button
            variant="ghost"
            size="sm"
            className="w-full justify-center text-muted-foreground hover:text-foreground"
            onClick={() => setIsSidebarCollapsed(!isSidebarCollapsed)}
          >
            <ChevronRight className={cn(
              "w-4 h-4 transition-transform",
              isSidebarCollapsed ? "" : "rotate-180"
            )} />
          </Button>
        </div>
      </aside>

      {/* Main Content */}
      <div className="flex-1 flex flex-col min-w-0">
        {/* Top Command Bar */}
        <header className="h-14 flex items-center justify-between px-4 border-b border-border bg-card/30">
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-2">
              {currentNavItem && (
                <>
                  <currentNavItem.icon className="w-5 h-5 text-primary" />
                  <h1 className="text-sm font-semibold text-foreground">{currentNavItem.label}</h1>
                  <span className="text-xs text-muted-foreground">/ {currentNavItem.description}</span>
                </>
              )}
            </div>
          </div>

          <div className="flex items-center gap-3">
            {/* Command Palette Trigger */}
            <div className="relative hidden md:block">
              <Command className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-muted-foreground" />
              <Input 
                placeholder="Search or command..."
                className="w-64 pl-9 pr-12 h-9 bg-secondary/50 border-border/50 text-sm"
              />
              <kbd className="absolute right-2 top-1/2 -translate-y-1/2 text-[10px] text-muted-foreground bg-background px-1.5 py-0.5 rounded border border-border">
                ⌘K
              </kbd>
            </div>

            <Separator orientation="vertical" className="h-6" />

            {/* Status Indicator */}
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1.5">
                <div className="w-2 h-2 rounded-full bg-green-400 animate-pulse" />
                <span className="text-xs text-muted-foreground hidden sm:inline">Live</span>
              </div>
              <Activity className="w-4 h-4 text-muted-foreground" />
            </div>

            <Separator orientation="vertical" className="h-6" />

            {/* Actions */}
            <Button variant="ghost" size="icon" className="text-muted-foreground hover:text-foreground">
              <Bell className="w-4 h-4" />
            </Button>
            <Button variant="ghost" size="icon" className="text-muted-foreground hover:text-foreground">
              <Settings className="w-4 h-4" />
            </Button>
            <Button variant="ghost" size="icon" className="text-muted-foreground hover:text-foreground">
              <User className="w-4 h-4" />
            </Button>
          </div>
        </header>

        {/* Page Content */}
        <main className="flex-1 overflow-auto p-4 lg:p-6">
          {currentView === "data-pipeline" && <DataPipeline />}
          {currentView === "data-library" && <DataLibrary />}
          {currentView === "metrics-library" && <MetricsLibrary />}
          {currentView === "aligned-pipeline" && <AlignedDataPipeline />}
          {currentView === "indicators-analysis" && <IndicatorsAnalysis />}
          {currentView === "alpha-lab" && <AlphaLab />}
          {currentView === "forensic-auditor" && <ForensicAuditor />}
          {currentView === "strategy-studio" && <StrategyStudio />}
          {currentView === "xray-inspector" && <XRayInspector />}
          {currentView === "risk-war-room" && <RiskWarRoom />}
          {currentView === "execution-ledger" && <ExecutionLedger />}
          {currentView === "trader-manager" && <TraderManager />}
          {currentView === "live-positions" && <LivePositions />}
        </main>

        {/* Status Bar */}
        <footer className="h-8 flex items-center justify-between px-4 border-t border-border bg-card/30 text-[10px] text-muted-foreground">
          <div className="flex items-center gap-4">
            <span>Market: <span className="text-green-400">Open</span></span>
            <span>Last Update: <span suppressHydrationWarning>{new Date().toLocaleTimeString()}</span></span>
          </div>
          <div className="flex items-center gap-4">
            <span>Paper Trading Mode</span>
            <span>v2.4.1</span>
          </div>
        </footer>
      </div>
    </div>
  )
}
