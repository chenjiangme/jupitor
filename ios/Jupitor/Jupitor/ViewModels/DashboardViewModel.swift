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
        isLoadingHistory = true
        defer { isLoadingHistory = false }

        do {
            let resp = try await api.fetchHistory(date: date, sortMode: sortMode.rawValue)
            self.historyDay = resp.today
            self.historyNext = resp.next
        } catch {
            self.historyDay = nil
            self.historyNext = nil
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
