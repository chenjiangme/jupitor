import Foundation

struct CNHeatmapStock: Codable, Identifiable {
    let symbol: String
    let name: String
    let index: String
    let turn: Double
    let pctChg: Double
    let close: Double
    let amount: Double
    let peTTM: Double
    let isST: Bool

    var id: String { symbol }
}

struct CNHeatmapStats: Codable {
    let turnP50: Double
    let turnP90: Double
    let turnMax: Double
}

struct CNHeatmapResponse: Codable {
    let date: String
    let stocks: [CNHeatmapStock]?
    let stats: CNHeatmapStats
}

struct CNDatesResponse: Codable {
    let dates: [String]
}
