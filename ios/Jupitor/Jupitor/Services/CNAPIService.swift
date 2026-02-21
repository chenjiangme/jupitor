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

    func fetchIndustryFilter() async throws -> CNIndustryFilterResponse {
        let url = baseURL.appendingPathComponent("api/cn/industry-filter")
        return try await fetch(url)
    }

    func saveIndustryFilter(selected: [String], excluded: [String]) async throws {
        let url = baseURL.appendingPathComponent("api/cn/industry-filter")
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body = CNIndustryFilterResponse(selected: selected, excluded: excluded)
        request.httpBody = try JSONEncoder().encode(body)
        let (_, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
    }

    func fetchPresets() async throws -> CNIndustryPresetsResponse {
        let url = baseURL.appendingPathComponent("api/cn/industry-presets")
        return try await fetch(url)
    }

    func savePreset(name: String, selected: [String], excluded: [String]) async throws {
        let url = baseURL.appendingPathComponent("api/cn/industry-presets")
        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body = CNIndustryPreset(name: name, selected: selected, excluded: excluded)
        request.httpBody = try JSONEncoder().encode(body)
        let (_, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
    }

    func deletePreset(name: String) async throws {
        let encoded = name.addingPercentEncoding(withAllowedCharacters: .urlPathAllowed) ?? name
        let url = baseURL.appendingPathComponent("api/cn/industry-presets/\(encoded)")
        var request = URLRequest(url: url)
        request.httpMethod = "DELETE"
        let (_, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
    }

    private func fetch<T: Decodable>(_ url: URL) async throws -> T {
        let (data, response) = try await session.data(from: url)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
        return try JSONDecoder().decode(T.self, from: data)
    }
}
