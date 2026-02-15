import Foundation

actor APIService {
    private let baseURL: URL
    private let session: URLSession
    private let longSession: URLSession

    init(baseURL: URL) {
        self.baseURL = baseURL
        let config = URLSessionConfiguration.default
        config.timeoutIntervalForRequest = 15
        self.session = URLSession(configuration: config)

        let longConfig = URLSessionConfiguration.default
        longConfig.timeoutIntervalForRequest = 120
        self.longSession = URLSession(configuration: longConfig)
    }

    // MARK: - Dashboard

    func fetchDashboard(sortMode: Int = 0) async throws -> DashboardResponse {
        let url = baseURL.appendingPathComponent("api/dashboard")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "sort", value: "\(sortMode)")]
        return try await fetch(components.url!)
    }

    func fetchHistory(date: String, sortMode: Int = 0) async throws -> DashboardResponse {
        let url = baseURL.appendingPathComponent("api/dashboard/history/\(date)")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "sort", value: "\(sortMode)")]
        return try await fetch(components.url!)
    }

    // MARK: - Dates

    func fetchDates() async throws -> DatesResponse {
        let url = baseURL.appendingPathComponent("api/dates")
        return try await fetch(url)
    }

    // MARK: - Watchlist

    func fetchWatchlist() async throws -> WatchlistResponse {
        let url = baseURL.appendingPathComponent("api/watchlist")
        return try await fetch(url)
    }

    func addToWatchlist(symbol: String) async throws {
        let url = baseURL.appendingPathComponent("api/watchlist/\(symbol)")
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        let (_, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
    }

    func removeFromWatchlist(symbol: String) async throws {
        let url = baseURL.appendingPathComponent("api/watchlist/\(symbol)")
        var request = URLRequest(url: url)
        request.httpMethod = "DELETE"
        let (_, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
    }

    // MARK: - News

    func fetchNews(symbol: String, date: String) async throws -> NewsResponse {
        let url = baseURL.appendingPathComponent("api/news/\(symbol)")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        components.queryItems = [URLQueryItem(name: "date", value: date)]
        return try await fetch(components.url!)
    }

    // MARK: - Symbol History

    func fetchSymbolHistory(symbol: String, before: String? = nil) async throws -> SymbolHistoryResponse {
        let url = baseURL.appendingPathComponent("api/symbol-history/\(symbol)")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        if let before {
            components.queryItems = [URLQueryItem(name: "before", value: before)]
        }
        let (data, response) = try await longSession.data(from: components.url!)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
        return try JSONDecoder().decode(SymbolHistoryResponse.self, from: data)
    }

    // MARK: - Private

    private func fetch<T: Decodable>(_ url: URL) async throws -> T {
        let (data, response) = try await session.data(from: url)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw APIError.requestFailed
        }
        return try JSONDecoder().decode(T.self, from: data)
    }
}

enum APIError: Error, LocalizedError {
    case requestFailed

    var errorDescription: String? {
        switch self {
        case .requestFailed: return "Request failed"
        }
    }
}
