import Foundation

actor CNAPIService {
    private let baseURL: URL
    private let session: URLSession

    init(baseURL: URL) {
        self.baseURL = baseURL
        let config = URLSessionConfiguration.default
        config.timeoutIntervalForRequest = 30
        self.session = URLSession(configuration: config)
    }

    func fetchHeatmap(date: String? = nil) async throws -> CNHeatmapResponse {
        let url = baseURL.appendingPathComponent("api/cn/heatmap")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        if let date {
            components.queryItems = [URLQueryItem(name: "date", value: date)]
        }
        return try await fetch(components.url!)
    }

    func fetchDates() async throws -> CNDatesResponse {
        let url = baseURL.appendingPathComponent("api/cn/dates")
        return try await fetch(url)
    }

    func fetchSymbolHistory(symbol: String, days: Int = 250, end: String? = nil) async throws -> CNSymbolHistoryResponse {
        let url = baseURL.appendingPathComponent("api/cn/symbol-history/\(symbol)")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        var items = [URLQueryItem(name: "days", value: "\(days)")]
        if let end { items.append(URLQueryItem(name: "end", value: end)) }
        components.queryItems = items
        return try await fetch(components.url!)
    }

    private func fetch<T: Decodable>(_ url: URL) async throws -> T {
        let (data, response) = try await session.data(from: url)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
        return try JSONDecoder().decode(T.self, from: data)
    }
}
