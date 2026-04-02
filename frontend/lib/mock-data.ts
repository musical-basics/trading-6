// Mock data for the quantitative trading terminal
// Strategy data is now fetched from the backend API (see lib/api.ts).
// This file retains mock data for X-Ray, Risk, and Execution components.


// X-Ray Inspector data
export const xrayData = {
  rawData: {
    ticker: "NVDA",
    date: "2024-03-15",
    price: 878.35,
    volume: 42_500_000,
    q3Revenue: 18_120_000_000,
    totalDebt: 9_700_000_000,
    marketCap: 2_180_000_000_000,
  },
  heuristics: {
    evSalesRatio: 12.4,
    zScore: 2.34,
    dynamicDiscountRate: 8.5,
    priceToBook: 45.2,
    debtToEquity: 0.41,
  },
  xgboost: {
    predictedReturn: 0.182,
    confidence: 0.76,
    rawDesiredWeight: 0.18,
    featureImportance: [
      { feature: "Momentum_30D", importance: 0.24 },
      { feature: "EV/Sales", importance: 0.19 },
      { feature: "Volume_Surge", importance: 0.15 },
      { feature: "Earnings_Surprise", importance: 0.14 },
      { feature: "Sector_Momentum", importance: 0.11 },
      { feature: "Other", importance: 0.17 },
    ],
  },
  riskBouncer: {
    covariancePenalty: 0.032,
    mcr: 0.068,
    mcrThreshold: 0.05,
    mcrBreach: true,
    originalWeight: 0.18,
    scaledWeight: 0.095,
    reason: "MCR breach detected. Position scaled down to maintain portfolio risk limits.",
  },
  finalOrder: {
    targetAllocation: 0.095,
    action: "BUY",
    shares: 235,
    estimatedValue: 206_412,
  },
}

// Risk War Room data
export const riskData = {
  vix: 14.82,
  vixChange: -0.45,
  tenYearYield: 4.32,
  yieldChange: 0.02,
  macroRegime: "Risk-On",
  regimeConfidence: 0.78,
  
  // Covariance matrix (simplified 8x8 for visualization)
  covarianceMatrix: [
    [1.0, 0.65, 0.42, 0.38, 0.28, 0.55, 0.33, 0.41],
    [0.65, 1.0, 0.58, 0.45, 0.32, 0.48, 0.29, 0.52],
    [0.42, 0.58, 1.0, 0.72, 0.55, 0.38, 0.44, 0.35],
    [0.38, 0.45, 0.72, 1.0, 0.68, 0.42, 0.51, 0.28],
    [0.28, 0.32, 0.55, 0.68, 1.0, 0.35, 0.62, 0.22],
    [0.55, 0.48, 0.38, 0.42, 0.35, 1.0, 0.38, 0.58],
    [0.33, 0.29, 0.44, 0.51, 0.62, 0.38, 1.0, 0.31],
    [0.41, 0.52, 0.35, 0.28, 0.22, 0.58, 0.31, 1.0],
  ],
  tickers: ["NVDA", "AAPL", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "AMD"],
  
  // MCR data
  mcrData: [
    { ticker: "NVDA", mcr: 6.8, threshold: 5.0 },
    { ticker: "TSLA", mcr: 5.2, threshold: 5.0 },
    { ticker: "AAPL", mcr: 4.1, threshold: 5.0 },
    { ticker: "MSFT", mcr: 3.8, threshold: 5.0 },
    { ticker: "META", mcr: 3.2, threshold: 5.0 },
  ],
}

// Execution Ledger data
export const executionOrders = [
  { id: 1, ticker: "NVDA", action: "BUY", quantity: 235, targetWeight: 9.5, currentWeight: 7.2, status: "pending" },
  { id: 2, ticker: "AAPL", action: "SELL", quantity: 150, targetWeight: 8.0, currentWeight: 10.5, status: "pending" },
  { id: 3, ticker: "MSFT", action: "BUY", quantity: 80, targetWeight: 7.5, currentWeight: 6.2, status: "pending" },
  { id: 4, ticker: "GOOGL", action: "BUY", quantity: 45, targetWeight: 5.5, currentWeight: 4.8, status: "pending" },
  { id: 5, ticker: "TSLA", action: "SELL", quantity: 120, targetWeight: 3.0, currentWeight: 5.8, status: "pending" },
  { id: 6, ticker: "META", action: "BUY", quantity: 65, targetWeight: 4.5, currentWeight: 3.2, status: "pending" },
  { id: 7, ticker: "AMD", action: "BUY", quantity: 180, targetWeight: 4.0, currentWeight: 2.5, status: "pending" },
  { id: 8, ticker: "AMZN", action: "HOLD", quantity: 0, targetWeight: 6.0, currentWeight: 6.1, status: "complete" },
]
