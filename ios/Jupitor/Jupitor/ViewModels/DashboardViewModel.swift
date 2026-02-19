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
    var watchlistDate: String = "" // date the current watchlist is for
    private var watchlistCache: [String: Set<String>] = [:] // date -> symbols

    // MARK: - Replay

    var isReplaying = false
    var replayTime: Int64?           // current scrub position (Unix ms)
    var replayTimeRange: TimeRange?  // full time bounds from API
    var replayDayData: DayDataJSON?  // snapshot at current scrub time

    // MARK: - Status

    var isLoading = false
    var error: String?

    // MARK: - Private

    private let api: APIService
    private var refreshTimer: AnyCancellable?
    private var historyCache: [String: (today: DayDataJSON, next: DayDataJSON?)] = [:]
    private var preloadTask: Task<Void, Never>?
    private var replayDebounceTask: Task<Void, Never>?

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

        _ = await (dashTask, datesTask)

        // Load watchlist for the live date (targets already arrived with dashboard response).
        await loadWatchlist(for: date)

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

        // Refresh watchlist from server for cross-device sync.
        if !watchlistDate.isEmpty {
            await loadWatchlist(for: watchlistDate, forceRefresh: true)
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

    func loadWatchlist(for date: String, forceRefresh: Bool = false) async {
        guard !date.isEmpty else { return }

        // Check in-memory cache (skip if force-refreshing for cross-device sync).
        if !forceRefresh, let cached = watchlistCache[date] {
            watchlistDate = date
            watchlistSymbols = cached
            return
        }

        do {
            let resp = try await api.fetchWatchlist(date: date)
            let symbols = Set(resp.symbols)
            watchlistCache[date] = symbols
            watchlistDate = date
            watchlistSymbols = symbols
        } catch {
            // Non-fatal.
        }
    }

    /// Called by views when the display date changes; reloads watchlist if needed.
    /// Targets arrive with dashboard/history responses automatically.
    func updateDisplayDate(_ date: String) async {
        guard !date.isEmpty, date != watchlistDate else { return }
        await loadWatchlist(for: date)
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

    func toggleWatchlist(symbol: String, date: String) async {
        let wasInWatchlist = watchlistSymbols.contains(symbol)

        // Optimistic update.
        if wasInWatchlist {
            watchlistSymbols.remove(symbol)
        } else {
            watchlistSymbols.insert(symbol)
        }
        watchlistCache[date] = watchlistSymbols

        do {
            if wasInWatchlist {
                try await api.removeFromWatchlist(symbol: symbol, date: date)
            } else {
                try await api.addToWatchlist(symbol: symbol, date: date)
            }
        } catch {
            // Revert on error.
            if wasInWatchlist {
                watchlistSymbols.insert(symbol)
            } else {
                watchlistSymbols.remove(symbol)
            }
            watchlistCache[date] = watchlistSymbols
        }
    }

    func removeWatchlistSymbols(_ symbols: Set<String>, date: String) async {
        let toRemove = symbols.intersection(watchlistSymbols)
        guard !toRemove.isEmpty else { return }

        // Optimistic update.
        watchlistSymbols.subtract(toRemove)
        watchlistCache[date] = watchlistSymbols

        for symbol in toRemove {
            do {
                try await api.removeFromWatchlist(symbol: symbol, date: date)
            } catch {
                watchlistSymbols.insert(symbol)
            }
        }
        watchlistCache[date] = watchlistSymbols
    }

    // MARK: - Replay

    func toggleReplay() {
        if isReplaying {
            isReplaying = false
            replayTime = nil
            replayTimeRange = nil
            replayDayData = nil
            replayDebounceTask?.cancel()
            replayDebounceTask = nil
        }
    }

    func startReplay(date: String) async {
        isReplaying = true
        // Fetch with max timestamp to get full timeRange.
        await fetchReplayData(date: date, until: Int64.max)
        // Set scrub position to end of time range.
        if let tr = replayTimeRange {
            replayTime = tr.end
        }
    }

    func scrubTo(fraction: Double, date: String, sessionMode: SessionMode) {
        let clamped = min(max(fraction, 0), 1)
        let (sessionStart, sessionEnd) = sessionBounds(date: date, sessionMode: sessionMode)

        // Clamp session bounds to actual time range if available.
        let rangeStart: Int64
        let rangeEnd: Int64
        if let tr = replayTimeRange {
            rangeStart = max(sessionStart, tr.start)
            rangeEnd = min(sessionEnd, tr.end)
        } else {
            rangeStart = sessionStart
            rangeEnd = sessionEnd
        }

        guard rangeEnd > rangeStart else { return }
        let ts = rangeStart + Int64(clamped * Double(rangeEnd - rangeStart))
        replayTime = ts

        // Debounce API call.
        replayDebounceTask?.cancel()
        replayDebounceTask = Task {
            try? await Task.sleep(nanoseconds: 150_000_000)
            guard !Task.isCancelled else { return }
            await fetchReplayData(date: date, until: ts)
        }
    }

    func replaySessionChanged(date: String, sessionMode: SessionMode) {
        guard isReplaying, let rt = replayTime else { return }
        let (sessionStart, sessionEnd) = sessionBounds(date: date, sessionMode: sessionMode)
        let rangeStart: Int64
        let rangeEnd: Int64
        if let tr = replayTimeRange {
            rangeStart = max(sessionStart, tr.start)
            rangeEnd = min(sessionEnd, tr.end)
        } else {
            rangeStart = sessionStart
            rangeEnd = sessionEnd
        }
        let clamped = min(max(rt, rangeStart), rangeEnd)
        replayTime = clamped
        replayDebounceTask?.cancel()
        replayDebounceTask = Task {
            await fetchReplayData(date: date, until: clamped)
        }
    }

    private func fetchReplayData(date: String, until: Int64) async {
        do {
            let resp = try await api.fetchReplay(date: date, until: until, sortMode: sortMode.rawValue)
            replayDayData = resp.today
            if let tr = resp.timeRange {
                replayTimeRange = tr
            }
        } catch {
            // Non-fatal.
        }
    }

    /// Returns (start, end) ET-shifted timestamps in Unix ms for the given session mode.
    /// The backend uses "ET-shifted" convention: ET clock time stored as if it were UTC.
    /// So 9:30 AM ET is stored as date 09:30:00 UTC milliseconds.
    private func sessionBounds(date: String, sessionMode: SessionMode) -> (Int64, Int64) {
        let parts = date.split(separator: "-")
        guard parts.count == 3,
              let y = Int(parts[0]), let m = Int(parts[1]), let d = Int(parts[2]) else {
            return (0, Int64.max)
        }

        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = TimeZone(abbreviation: "UTC")!
        var comps = DateComponents()
        comps.year = y
        comps.month = m
        comps.day = d

        func msAt(hour: Int, minute: Int) -> Int64 {
            comps.hour = hour
            comps.minute = minute
            comps.second = 0
            guard let dt = cal.date(from: comps) else { return 0 }
            return Int64(dt.timeIntervalSince1970 * 1000)
        }

        switch sessionMode {
        case .pre:
            return (msAt(hour: 4, minute: 0), msAt(hour: 9, minute: 30))
        case .reg:
            return (msAt(hour: 9, minute: 30), msAt(hour: 16, minute: 0))
        case .day:
            return (msAt(hour: 4, minute: 0), msAt(hour: 16, minute: 0))
        case .next:
            return (msAt(hour: 16, minute: 0), msAt(hour: 20, minute: 0))
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
