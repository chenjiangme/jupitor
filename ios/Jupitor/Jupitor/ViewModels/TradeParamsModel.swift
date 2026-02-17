import Foundation

/// SSE event from the /api/targets/stream endpoint.
private struct TradeParamsEvent: Decodable {
    let type: String                           // "snapshot", "set", "delete"
    let date: String?
    let key: String?
    let value: Double?
    let data: [String: [String: Double]]?      // snapshot only
}

@MainActor @Observable
final class TradeParamsModel {
    var targets: [String: [String: Double]] = [:]  // date -> key -> value
    var isConnected = false

    private let baseURL: URL
    private var streamTask: Task<Void, Never>?
    private let session: URLSession

    init(baseURL: URL) {
        self.baseURL = baseURL
        let config = URLSessionConfiguration.default
        config.timeoutIntervalForRequest = 15
        self.session = URLSession(configuration: config)
    }

    func start() {
        guard streamTask == nil else { return }
        streamTask = Task { await connectLoop() }
    }

    func stop() {
        streamTask?.cancel()
        streamTask = nil
        isConnected = false
    }

    // MARK: - Write Methods (REST)

    func setTarget(key: String, value: Double, date: String) async {
        // Optimistic update for instant UI feedback during drag.
        if targets[date] == nil { targets[date] = [:] }
        targets[date]?[key] = value

        do {
            try await restPUT(date: date, key: key, value: value)
        } catch {
            // Revert — SSE will correct if server has different state.
            targets[date]?.removeValue(forKey: key)
        }
    }

    func deleteTarget(key: String, date: String) async {
        let old = targets[date]?[key]
        targets[date]?.removeValue(forKey: key)

        do {
            try await restDELETE(date: date, key: key)
        } catch {
            if let old { revert(date: date, key: key, value: old) }
        }
    }

    func deleteAllTargets(symbol: String, date: String) async {
        let preKey = "\(symbol):PRE"
        let regKey = "\(symbol):REG"
        let oldPre = targets[date]?[preKey]
        let oldReg = targets[date]?[regKey]

        // Optimistic removal.
        targets[date]?.removeValue(forKey: preKey)
        targets[date]?.removeValue(forKey: regKey)

        // Send REST DELETEs directly (don't go through deleteTarget which
        // would try to capture old values that we already removed).
        do {
            if oldPre != nil { try await restDELETE(date: date, key: preKey) }
        } catch {
            if let oldPre { revert(date: date, key: preKey, value: oldPre) }
        }
        do {
            if oldReg != nil { try await restDELETE(date: date, key: regKey) }
        } catch {
            if let oldReg { revert(date: date, key: regKey, value: oldReg) }
        }
    }

    // MARK: - REST Helpers

    private func restPUT(date: String, key: String, value: Double) async throws {
        let url = baseURL.appendingPathComponent("api/targets")
        var request = URLRequest(url: url)
        request.httpMethod = "PUT"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        let body: [String: Any] = ["date": date, "key": key, "value": value]
        request.httpBody = try JSONSerialization.data(withJSONObject: body)
        let (_, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }

    private func restDELETE(date: String, key: String) async throws {
        let url = baseURL.appendingPathComponent("api/targets")
        var components = URLComponents(url: url, resolvingAgainstBaseURL: false)!
        components.queryItems = [
            URLQueryItem(name: "date", value: date),
            URLQueryItem(name: "key", value: key),
        ]
        var request = URLRequest(url: components.url!)
        request.httpMethod = "DELETE"
        let (_, response) = try await session.data(for: request)
        guard let http = response as? HTTPURLResponse, (200..<300).contains(http.statusCode) else {
            throw URLError(.badServerResponse)
        }
    }

    // MARK: - SSE Connection

    private func connectLoop() async {
        while !Task.isCancelled {
            await connectOnce()
            guard !Task.isCancelled else { return }
            isConnected = false
            try? await Task.sleep(for: .seconds(2))
        }
    }

    private func connectOnce() async {
        let url = baseURL.appendingPathComponent("api/targets/stream")
        // Use a long-timeout session for SSE (server sends heartbeats every 30s).
        let sseConfig = URLSessionConfiguration.default
        sseConfig.timeoutIntervalForRequest = 60
        let sseSession = URLSession(configuration: sseConfig)
        do {
            let (bytes, response) = try await sseSession.bytes(from: url)
            guard let http = response as? HTTPURLResponse, http.statusCode == 200 else { return }
            isConnected = true

            for try await line in bytes.lines {
                guard !Task.isCancelled else { return }
                guard line.hasPrefix("data: ") else {
                    // Heartbeat lines (": keepalive") are ignored automatically.
                    continue
                }
                let json = String(line.dropFirst(6))
                guard let data = json.data(using: .utf8) else { continue }
                if let event = try? JSONDecoder().decode(TradeParamsEvent.self, from: data) {
                    handleEvent(event)
                }
            }
        } catch {
            // Connection failed or dropped — will reconnect.
        }
    }

    private func handleEvent(_ event: TradeParamsEvent) {
        switch event.type {
        case "snapshot":
            targets = event.data ?? [:]
        case "set":
            guard let date = event.date, let key = event.key, let value = event.value else { return }
            if targets[date] == nil { targets[date] = [:] }
            targets[date]?[key] = value
        case "delete":
            guard let date = event.date, let key = event.key else { return }
            targets[date]?.removeValue(forKey: key)
            if targets[date]?.isEmpty == true { targets.removeValue(forKey: date) }
        default:
            break
        }
    }

    private func revert(date: String, key: String, value: Double) {
        if targets[date] == nil { targets[date] = [:] }
        targets[date]?[key] = value
    }
}
