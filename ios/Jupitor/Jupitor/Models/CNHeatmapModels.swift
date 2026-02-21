import Foundation

struct CNHeatmapStock: Codable, Identifiable {
    let symbol: String
    let name: String
    let index: String
    let industry: String
    let turn: Double
    let pctChg: Double
    let close: Double
    let amount: Double
    let peTTM: Double
    let isST: Bool

    var id: String { symbol }

    init(from decoder: Decoder) throws {
        let c = try decoder.container(keyedBy: CodingKeys.self)
        symbol = try c.decode(String.self, forKey: .symbol)
        name = try c.decode(String.self, forKey: .name)
        index = try c.decode(String.self, forKey: .index)
        industry = try c.decodeIfPresent(String.self, forKey: .industry) ?? ""
        turn = try c.decode(Double.self, forKey: .turn)
        pctChg = try c.decode(Double.self, forKey: .pctChg)
        close = try c.decode(Double.self, forKey: .close)
        amount = try c.decode(Double.self, forKey: .amount)
        peTTM = try c.decode(Double.self, forKey: .peTTM)
        isST = try c.decode(Bool.self, forKey: .isST)
    }
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

struct CNSymbolDay: Codable, Identifiable {
    let date: String
    let turn: Double
    let pctChg: Double
    let close: Double
    var id: String { date }
}

struct CNSymbolHistoryResponse: Codable {
    let symbol: String
    let name: String
    let days: [CNSymbolDay]
}

struct CNIndustryFilterResponse: Codable {
    let selected: [String]
    let excluded: [String]
}

struct CNIndustryPreset: Codable, Identifiable {
    let name: String
    let selected: [String]
    let excluded: [String]
    var id: String { name }
}

struct CNIndustryPresetsResponse: Codable {
    let presets: [CNIndustryPreset]
}
