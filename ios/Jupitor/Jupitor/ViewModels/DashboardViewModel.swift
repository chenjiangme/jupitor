import Foundation
import Combine

@MainActor @Observable
final class DashboardViewModel {
    // MARK: - Live State

    var today: DayDataJSON?
    var next: DayDataJSON?
    var date: String = ""

    // MARK: - Sort & Session

    var sortMode: SortMode = .preTrades
    var sortLabel: String = "PRE:TRD"
    var sessionView: SessionView = .pre

    // MARK: - History

    var historyDates: [String] = []
    var selectedHistoryDate: String?
    var historyDay: DayDataJSON?
    var historyNext: DayDataJSON?
    var isLoadingHistory = false

    // MARK: - Watchlist

    var watchlistSymbols: Set<String> = []

    // MARK: - Status

    var isLoading = false
    var error: String?

    // MARK: - Private

    private let api: APIService
    private var refreshTimer: AnyCancellable?
    private var historyCache: [String: (today: DayDataJSON, next: DayDataJSON?)] = [:]
    private var preloadTask: Task<Void, Never>?

    // MARK: - Init

    init(baseURL: URL) {
        self.api = APIService(baseURL: baseURL)
    }

    // MARK: - Lifecycle

    func start() {
        Task {
            await loadInitial()
            startAutoRefresh()
        }
    }

    func stop() {
        refreshTimer?.cancel()
        refreshTimer = nil
        preloadTask?.cancel()
        preloadTask = nil
    }

    // MARK: - Auto Refresh

    private func startAutoRefresh() {
        refreshTimer?.cancel()
        refreshTimer = Timer.publish(every: 5, on: .main, in: .common)
            .autoconnect()
            .sink { [weak self] _ in
                guard let self else { return }
                Task { await self.refreshLive() }
            }
    }

    // MARK: - Data Loading

    private func loadInitial() async {
        isLoading = true
        defer { isLoading = false }

        async let dashTask: () = refreshLive()
        async let datesTask: () = loadDates()
        async let watchlistTask: () = loadWatchlist()

        _ = await (dashTask, datesTask, watchlistTask)

        startPreloadingHistory()
    }

    func refreshLive() async {
        do {
            let resp = try await api.fetchDashboard(sortMode: sortMode.rawValue)
            self.date = resp.date
            self.today = resp.today
            self.next = resp.next
            self.sortLabel = resp.sortLabel
            self.error = nil
        } catch {
            self.error = error.localizedDescription
        }
    }

    private func loadDates() async {
        do {
            let resp = try await api.fetchDates()
            self.historyDates = resp.dates
        } catch {
            // Non-fatal.
        }
    }

    func loadWatchlist() async {
        do {
            let resp = try await api.fetchWatchlist()
            self.watchlistSymbols = Set(resp.symbols)
        } catch {
            // Non-fatal.
        }
    }

    // MARK: - History

    func loadHistory(date: String) async {
        selectedHistoryDate = date

        let cacheKey = "\(date):\(sortMode.rawValue)"
        if let cached = historyCache[cacheKey] {
            self.historyDay = cached.today
            self.historyNext = cached.next
            return
        }

        isLoadingHistory = true
        defer { isLoadingHistory = false }

        do {
            let resp = try await api.fetchHistory(date: date, sortMode: sortMode.rawValue)
            self.historyDay = resp.today
            self.historyNext = resp.next
            historyCache[cacheKey] = (resp.today, resp.next)
        } catch {
            self.historyDay = nil
            self.historyNext = nil
        }
    }

    /// Preload all history dates in background (latest first).
    private func startPreloadingHistory() {
        guard !historyDates.isEmpty else { return }
        let dates = historyDates.reversed() as [String]
        let mode = sortMode.rawValue
        let api = self.api

        preloadTask?.cancel()
        preloadTask = Task {
            for d in dates {
                guard !Task.isCancelled else { break }
                let key = "\(d):\(mode)"
                guard historyCache[key] == nil else { continue }
                do {
                    let resp = try await api.fetchHistory(date: d, sortMode: mode)
                    historyCache[key] = (resp.today, resp.next)
                } catch {
                    // Non-fatal.
                }
            }
        }
    }

    // MARK: - Sort

    func setSortMode(_ mode: SortMode) async {
        sortMode = mode
        sortLabel = mode.label
        await refreshLive()
        if let date = selectedHistoryDate {
            await loadHistory(date: date)
        }
    }

    // MARK: - Watchlist

    func toggleWatchlist(symbol: String) async {
        let wasInWatchlist = watchlistSymbols.contains(symbol)

        // Optimistic update.
        if wasInWatchlist {
            watchlistSymbols.remove(symbol)
        } else {
            watchlistSymbols.insert(symbol)
        }

        do {
            if wasInWatchlist {
                try await api.removeFromWatchlist(symbol: symbol)
            } else {
                try await api.addToWatchlist(symbol: symbol)
            }
        } catch {
            // Revert on error.
            if wasInWatchlist {
                watchlistSymbols.insert(symbol)
            } else {
                watchlistSymbols.remove(symbol)
            }
        }
    }

    func removeWatchlistSymbols(_ symbols: Set<String>) async {
        let toRemove = symbols.intersection(watchlistSymbols)
        guard !toRemove.isEmpty else { return }

        // Optimistic update.
        watchlistSymbols.subtract(toRemove)

        for symbol in toRemove {
            do {
                try await api.removeFromWatchlist(symbol: symbol)
            } catch {
                watchlistSymbols.insert(symbol)
            }
        }
    }

    // MARK: - Symbol History

    func fetchSymbolHistory(symbol: String, before: String? = nil, until: String? = nil) async -> SymbolHistoryResponse? {
        do {
            return try await api.fetchSymbolHistory(symbol: symbol, before: before, until: until)
        } catch {
            return nil
        }
    }

    // MARK: - News

    func fetchNewsArticles(symbol: String, date: String) async -> [NewsArticleJSON] {
        do {
            let resp = try await api.fetchNews(symbol: symbol, date: date)
            return resp.articles
        } catch {
            return []
        }
    }
}
