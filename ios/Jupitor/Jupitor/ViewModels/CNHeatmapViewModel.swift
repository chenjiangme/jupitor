import Foundation

@MainActor @Observable
final class CNHeatmapViewModel {
    var dates: [String] = []
    var currentDate: String = ""
    var heatmapData: CNHeatmapResponse?
    var isLoading = false
    var error: String?
    var indexFilter: CNIndexFilter = .all

    enum CNIndexFilter: String, CaseIterable {
        case all = "ALL"
        case csi300 = "300"
        case csi500 = "500"

        var next: CNIndexFilter {
            switch self {
            case .all: return .csi300
            case .csi300: return .csi500
            case .csi500: return .all
            }
        }
    }

    /// Stocks filtered by current index filter.
    var filteredStocks: [CNHeatmapStock]? {
        guard let stocks = heatmapData?.stocks else { return nil }
        switch indexFilter {
        case .all: return stocks
        case .csi300: return stocks.filter { $0.index == "csi300" }
        case .csi500: return stocks.filter { $0.index == "csi500" }
        }
    }

    let api: CNAPIService
    private var cache: [String: CNHeatmapResponse] = [:]
    private let maxCacheSize = 5

    init(baseURL: URL) {
        self.api = CNAPIService(baseURL: baseURL)
    }

    // MARK: - Lifecycle

    func start() {
        Task { await loadInitial() }
    }

    // MARK: - Navigation

    var currentIndex: Int {
        dates.firstIndex(of: currentDate) ?? max(0, dates.count - 1)
    }

    var canGoBack: Bool { currentIndex > 0 }
    var canGoForward: Bool { currentIndex < dates.count - 1 }

    func navigate(by delta: Int) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < dates.count else { return }
        let date = dates[newIndex]
        currentDate = date
        Task {
            await loadDate(date)
            preloadAdjacent()
        }
    }

    // MARK: - Data Loading

    private func loadInitial() async {
        isLoading = true
        defer { isLoading = false }

        do {
            let resp = try await api.fetchDates()
            self.dates = resp.dates
            if let latest = resp.dates.last {
                currentDate = latest
                await loadDate(latest)
                preloadAdjacent()
            }
        } catch {
            self.error = error.localizedDescription
        }
    }

    func loadDate(_ date: String) async {
        if let cached = cache[date] {
            heatmapData = cached
            return
        }

        isLoading = true
        defer { isLoading = false }

        do {
            let resp = try await api.fetchHeatmap(date: date)
            cache[date] = resp
            trimCache()
            if currentDate == date {
                heatmapData = resp
            }
        } catch {
            self.error = error.localizedDescription
        }
    }

    // MARK: - Preload & Cache

    /// Preload one date ahead and one behind for smooth swiping.
    private func preloadAdjacent() {
        let idx = currentIndex
        let toPreload = [idx - 1, idx + 1].filter { $0 >= 0 && $0 < dates.count }
        for i in toPreload {
            let date = dates[i]
            guard cache[date] == nil else { continue }
            Task {
                do {
                    let resp = try await api.fetchHeatmap(date: date)
                    cache[date] = resp
                    trimCache()
                } catch {
                    // Non-fatal.
                }
            }
        }
    }

    /// Keep cache bounded: evict dates farthest from current position.
    private func trimCache() {
        guard cache.count > maxCacheSize else { return }
        let idx = currentIndex
        let sorted = cache.keys.sorted { a, b in
            let ai = dates.firstIndex(of: a) ?? 0
            let bi = dates.firstIndex(of: b) ?? 0
            return abs(ai - idx) > abs(bi - idx)
        }
        for key in sorted.prefix(cache.count - maxCacheSize) {
            cache.removeValue(forKey: key)
        }
    }
}
