import Foundation

@MainActor @Observable
final class CNHeatmapViewModel {
    var dates: [String] = []
    var currentDate: String = ""
    var heatmapData: CNHeatmapResponse?
    var isLoading = false
    var error: String?
    var indexFilter: CNIndexFilter = .csi300
    var showDatePicker = false
    var isZoomed = false

    // Industry filter state.
    var industries: [String] = []
    var excludedIndustries: Set<String> = []
    var selectedIndustries: Set<String> = []
    var showIndustryFilter = false
    var showingHistory = false

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

        static var `default`: CNIndexFilter { .csi300 }
    }

    /// Stocks filtered by current index and industry filters.
    var filteredStocks: [CNHeatmapStock]? {
        guard var stocks = heatmapData?.stocks else { return nil }
        switch indexFilter {
        case .all: break
        case .csi300: stocks = stocks.filter { $0.index == "csi300" }
        case .csi500: stocks = stocks.filter { $0.index == "csi500" }
        }
        if !selectedIndustries.isEmpty {
            stocks = stocks.filter { selectedIndustries.contains($0.industry) }
        } else if !excludedIndustries.isEmpty {
            stocks = stocks.filter { !excludedIndustries.contains($0.industry) }
        }
        return stocks
    }

    /// Whether any industry filter is active.
    var hasIndustryFilter: Bool {
        !selectedIndustries.isEmpty || !excludedIndustries.isEmpty
    }

    /// Industry stock counts from currently index-filtered stocks.
    var industryCounts: [String: Int] {
        guard let stocks = heatmapData?.stocks else { return [:] }
        var filtered: [CNHeatmapStock]
        switch indexFilter {
        case .all: filtered = stocks
        case .csi300: filtered = stocks.filter { $0.index == "csi300" }
        case .csi500: filtered = stocks.filter { $0.index == "csi500" }
        }
        var counts: [String: Int] = [:]
        for s in filtered {
            guard !s.industry.isEmpty else { continue }
            counts[s.industry, default: 0] += 1
        }
        return counts
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
            updateIndustries()
            preloadAdjacent()
        }
    }

    func navigateTo(_ date: String) {
        guard dates.contains(date) else { return }
        currentDate = date
        Task {
            await loadDate(date)
            updateIndustries()
            preloadAdjacent()
        }
    }

    // MARK: - Data Loading

    private func loadInitial() async {
        isLoading = true
        defer { isLoading = false }

        // Load persisted industry filter.
        do {
            let filter = try await api.fetchIndustryFilter()
            selectedIndustries = Set(filter.selected)
            excludedIndustries = Set(filter.excluded)
        } catch {
            // Non-fatal — start with empty filter.
        }

        do {
            let resp = try await api.fetchDates()
            self.dates = resp.dates
            if let latest = resp.dates.last {
                currentDate = latest
                await loadDate(latest)
                // If latest date has no data, step back to find one that does.
                if heatmapData?.stocks?.isEmpty != false {
                    for i in stride(from: resp.dates.count - 2, through: 0, by: -1) {
                        let date = resp.dates[i]
                        currentDate = date
                        await loadDate(date)
                        if let stocks = heatmapData?.stocks, !stocks.isEmpty { break }
                    }
                }
                updateIndustries()
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

    // MARK: - Industry Filter

    private func updateIndustries() {
        guard let stocks = heatmapData?.stocks else {
            industries = []
            return
        }
        let unique = Set(stocks.compactMap { $0.industry.isEmpty ? nil : $0.industry })
        industries = unique.sorted()
    }

    func resetIndustryFilter() {
        selectedIndustries.removeAll()
        excludedIndustries.removeAll()
        saveIndustryFilter()
    }

    /// Three-state cycle: normal → selected → excluded → normal.
    func toggleIndustry(_ industry: String) {
        if selectedIndustries.contains(industry) {
            selectedIndustries.remove(industry)
            excludedIndustries.insert(industry)
        } else if excludedIndustries.contains(industry) {
            excludedIndustries.remove(industry)
        } else {
            selectedIndustries.insert(industry)
        }
        saveIndustryFilter()
    }

    private func saveIndustryFilter() {
        let selected = Array(selectedIndustries)
        let excluded = Array(excludedIndustries)
        Task {
            try? await api.saveIndustryFilter(selected: selected, excluded: excluded)
        }
    }
}
