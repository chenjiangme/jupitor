import Foundation

// MARK: - API Response Types

struct SymbolStatsJSON: Codable, Equatable {
    let symbol: String
    let trades: Int
    let high: Double
    let low: Double
    let open: Double
    let close: Double
    let size: Int64
    let turnover: Double
    let maxGain: Double
    let maxLoss: Double
}

struct CombinedStatsJSON: Codable, Identifiable, Equatable {
    let symbol: String
    let tier: String
    let pre: SymbolStatsJSON?
    let reg: SymbolStatsJSON?
    let news: Int?

    var id: String { symbol }
}

struct TierGroupJSON: Codable, Identifiable, Equatable {
    let name: String
    let count: Int
    let symbols: [CombinedStatsJSON]

    var id: String { name }
}

struct DayDataJSON: Codable, Equatable {
    let label: String
    let date: String?
    let preCount: Int
    let regCount: Int
    let tiers: [TierGroupJSON]
}

struct DashboardResponse: Codable {
    let date: String
    let today: DayDataJSON
    let next: DayDataJSON?
    let sortMode: Int
    let sortLabel: String
}

struct DatesResponse: Codable {
    let dates: [String]
}

struct WatchlistResponse: Codable {
    let symbols: [String]
}

struct NewsArticleJSON: Codable, Identifiable {
    let time: Int64
    let source: String
    let headline: String
    let content: String?

    var id: Int64 { time }

    var date: Date {
        Date(timeIntervalSince1970: Double(time) / 1000.0)
    }
}

struct NewsResponse: Codable {
    let symbol: String
    let date: String
    let articles: [NewsArticleJSON]
}

// MARK: - Symbol History

struct SymbolDateStats: Codable, Identifiable {
    let date: String
    let pre: SymbolStatsJSON?
    let reg: SymbolStatsJSON?

    var id: String { date }
}

struct SymbolHistoryResponse: Codable {
    let symbol: String
    let dates: [SymbolDateStats]
    let hasMore: Bool
}

// MARK: - Sort Modes

enum SortMode: Int, CaseIterable {
    case preTrades = 0
    case preGain = 1
    case regTrades = 2
    case regGain = 3
    case preTurnover = 4
    case regTurnover = 5
    case news = 6

    var label: String {
        switch self {
        case .preTrades: return "PRE:TRD"
        case .preGain: return "PRE:GAIN"
        case .regTrades: return "REG:TRD"
        case .regGain: return "REG:GAIN"
        case .preTurnover: return "PRE:TO"
        case .regTurnover: return "REG:TO"
        case .news: return "NEWS"
        }
    }

    var next: SortMode {
        SortMode(rawValue: (rawValue + 1) % SortMode.allCases.count)!
    }
}

// MARK: - Session Toggle

enum SessionView: String, CaseIterable {
    case pre = "PRE"
    case reg = "REG"
}

// MARK: - Session Mode (bubble chart)

enum SessionMode: CaseIterable {
    case pre, reg, day, next

    var label: String {
        switch self {
        case .pre: return "PRE"
        case .reg: return "REG"
        case .day: return "DAY"
        case .next: return "NEXT"
        }
    }
}
