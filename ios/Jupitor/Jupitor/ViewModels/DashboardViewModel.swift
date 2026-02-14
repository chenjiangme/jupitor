import Foundation
import Combine

@MainActor @Observable
final class DashboardViewModel {
    // MARK: - State

    var today: DayDataJSON?
    var next: DayDataJSON?
    var date: String = ""
    var sortMode: SortMode = .preTrades
    var sortLabel: String = "PRE:TRD"
    var sessionView: SessionView = .pre

    var historyDates: [String] = []
    var historyIndex: Int = -1  // -1 = live mode
    var isHistoryMode: Bool { historyIndex >= 0 }

    var watchlistSymbols: Set<String> = []

    var selectedSymbol: String?
    var newsArticles: [NewsArticleJSON] = []
    var showingNews = false

    var isLoading = false
    var error: String?

    // MARK: - Private

    private let api: APIService
    private var refreshTimer: AnyCancellable?
    private var cancellables = Set<AnyCancellable>()

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
        guard !isHistoryMode else { return }
        refreshTimer = Timer.publish(every: 5, on: .main, in: .common)
            .autoconnect()
            .sink { [weak self] _ in
                guard let self, !self.isHistoryMode else { return }
                Task {
                    await self.refreshLive()
                }
            }
    }

    private func stopAutoRefresh() {
        refreshTimer?.cancel()
        refreshTimer = nil
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

    // MARK: - History Navigation

func navigateHistory(delta: Int) async {
        if !isHistoryMode {
            if delta > 0 { return }
            guard !historyDates.isEmpty else { return }
            historyIndex = historyDates.count - 1
            stopAutoRefresh()
        } else {
            let newIdx = historyIndex + delta
            if newIdx >= historyDates.count {
                // Return to live.
                historyIndex = -1
                startAutoRefresh()
                await refreshLive()
                return
            }
            if newIdx < 0 { return }
            historyIndex = newIdx
        }

        await loadHistoryDate()
    }

func goToLive() async {
        historyIndex = -1
        startAutoRefresh()
        await refreshLive()
    }

private func loadHistoryDate() async {
        guard historyIndex >= 0, historyIndex < historyDates.count else { return }
        let date = historyDates[historyIndex]

        isLoading = true
        defer { isLoading = false }

        do {
            let resp = try await api.fetchHistory(date: date, sortMode: sortMode.rawValue)
            self.date = resp.date
            self.today = resp.today
            self.next = resp.next
            self.sortLabel = resp.sortLabel
            self.error = nil
        } catch {
            self.error = error.localizedDescription
        }
    }

    // MARK: - Sort

func cycleSortMode() async {
        sortMode = sortMode.next
        if isHistoryMode {
            await loadHistoryDate()
        } else {
            await refreshLive()
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

func loadNews(symbol: String) async {
        selectedSymbol = symbol
        do {
            let resp = try await api.fetchNews(symbol: symbol, date: date)
            newsArticles = resp.articles
            showingNews = true
        } catch {
            newsArticles = []
            showingNews = true
        }
    }
}
