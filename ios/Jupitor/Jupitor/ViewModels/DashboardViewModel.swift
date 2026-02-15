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
    private var prefetchTask: Task<Void, Never>?

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
            prefetchNearby(date: date)
            return
        }

        isLoadingHistory = true
        defer { isLoadingHistory = false }

        do {
            let resp = try await api.fetchHistory(date: date, sortMode: sortMode.rawValue)
            self.historyDay = resp.today
            self.historyNext = resp.next
            historyCache[cacheKey] = (resp.today, resp.next)
            prefetchNearby(date: date)
        } catch {
            self.historyDay = nil
            self.historyNext = nil
        }
    }

    /// Prefetch up to 5 dates around the given date in background.
    private func prefetchNearby(date: String) {
        guard let idx = historyDates.firstIndex(of: date) else { return }
        let mode = sortMode.rawValue
        let start = max(0, idx - 5)
        let end = min(historyDates.count - 1, idx + 5)
        let datesToFetch = (start...end).map { historyDates[$0] }
            .filter { historyCache["\($0):\(mode)"] == nil }

        guard !datesToFetch.isEmpty else { return }

        prefetchTask?.cancel()
        prefetchTask = Task.detached { [api] in
            for d in datesToFetch {
                guard !Task.isCancelled else { break }
                do {
                    let resp = try await api.fetchHistory(date: d, sortMode: mode)
                    await MainActor.run { [weak self] in
                        self?.historyCache["\(d):\(mode)"] = (resp.today, resp.next)
                    }
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
