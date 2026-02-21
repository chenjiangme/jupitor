import Foundation

@MainActor @Observable
final class CNHeatmapViewModel {
    var dates: [String] = []
    var currentDate: String = ""
    var heatmapData: CNHeatmapResponse?
    var isLoading = false
    var error: String?

    private let api: CNAPIService
    private var cache: [String: CNHeatmapResponse] = [:]

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
        Task { await loadDate(date) }
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
            if currentDate == date {
                heatmapData = resp
            }
        } catch {
            self.error = error.localizedDescription
        }
    }
}
