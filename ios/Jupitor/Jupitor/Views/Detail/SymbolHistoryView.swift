import SwiftUI

struct SymbolHistoryView: View {
    @Environment(DashboardViewModel.self) private var vm
    let symbol: String
    let date: String

    @State private var dates: [SymbolDateStats] = []
    @State private var isLoading = true
    @State private var hasMore = false
    @State private var isLoadingMore = false

    private let minRatio: CGFloat = 0.33
    private let minInnerRatio: CGFloat = 0.15

    private var maxTurnover: Double {
        dates.map { ($0.pre?.turnover ?? 0) + ($0.reg?.turnover ?? 0) }.max() ?? 1
    }

    /// Build week rows from dates. Each row has 5 slots (Mon–Fri).
    private var weekRows: [WeekRow] {
        guard !dates.isEmpty else { return [] }
        let cal = Calendar(identifier: .gregorian)
        let df = DateFormatter()
        df.dateFormat = "yyyy-MM-dd"
        df.timeZone = TimeZone(identifier: "America/New_York")

        // Map dates by ISO year-week + weekday (Mon=0..Fri=4).
        var lookup: [String: SymbolDateStats] = [:]
        for d in dates { lookup[d.date] = d }

        var rows: [WeekRow] = []
        var currentWeek: (year: Int, week: Int)? = nil
        var slots: [SymbolDateStats?] = Array(repeating: nil, count: 5)

        for d in dates {
            guard let date = df.date(from: d.date) else { continue }
            let comps = cal.dateComponents([.yearForWeekOfYear, .weekOfYear, .weekday], from: date)
            guard let y = comps.yearForWeekOfYear, let w = comps.weekOfYear, let wd = comps.weekday else { continue }
            // weekday: 1=Sun..7=Sat → Mon=0, Tue=1, Wed=2, Thu=3, Fri=4
            let col = (wd + 5) % 7  // Mon=0..Sun=6
            guard col < 5 else { continue }

            let thisWeek = (y, w)
            if currentWeek == nil {
                currentWeek = thisWeek
            } else if currentWeek! != thisWeek {
                rows.append(WeekRow(id: "\(currentWeek!.year)-\(currentWeek!.week)", slots: slots))
                slots = Array(repeating: nil, count: 5)
                currentWeek = thisWeek
            }
            slots[col] = d
        }
        if currentWeek != nil {
            rows.append(WeekRow(id: "\(currentWeek!.year)-\(currentWeek!.week)", slots: slots))
        }
        return rows
    }

    var body: some View {
        Group {
            if isLoading {
                VStack { Spacer(); ProgressView(); Spacer() }
            } else if dates.isEmpty {
                VStack {
                    Spacer()
                    Text("No trading history")
                        .font(.caption).foregroundStyle(.secondary)
                    Spacer()
                }
            } else {
                GeometryReader { geo in
                    let cellSize = geo.size.width / 5
                    ScrollView {
                        LazyVStack(spacing: 0) {
                            if hasMore {
                                Color.clear.frame(height: 1)
                                    .onAppear { loadMore() }
                            }

                            ForEach(weekRows) { row in
                                HStack(spacing: 0) {
                                    ForEach(0..<5, id: \.self) { col in
                                        if let entry = row.slots[col] {
                                            ringView(entry, maxDiameter: cellSize)
                                                .frame(width: cellSize, height: cellSize)
                                        } else {
                                            Color.clear
                                                .frame(width: cellSize, height: cellSize)
                                        }
                                    }
                                }
                            }
                        }
                    }
                    .defaultScrollAnchor(.bottom)
                }
            }
        }
        .navigationTitle(symbol)
        .navigationBarTitleDisplayMode(.inline)
        .task { await loadInitial() }
    }

    // MARK: - Ring View

    @ViewBuilder
    private func ringView(_ entry: SymbolDateStats, maxDiameter: CGFloat) -> some View {
        let preTurnover = entry.pre?.turnover ?? 0
        let regTurnover = entry.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let sizeRatio = maxTurnover > 0 ? sqrt(CGFloat(total / maxTurnover)) : 0
        let minDiameter = maxDiameter * minRatio
        let diameter = minDiameter + (maxDiameter - minDiameter) * sizeRatio
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0
        let lineWidth = max(3, diameter * 0.12)
        let outerDia = diameter - lineWidth
        let innerDia = outerDia * max(minInnerRatio, preRatio)

        ZStack {
            Circle().fill(Color.white.opacity(0.04))

            SessionRingView(
                gain: entry.reg?.maxGain ?? 0,
                loss: entry.reg?.maxLoss ?? 0,
                hasData: entry.reg != nil,
                diameter: outerDia,
                lineWidth: lineWidth
            )

            SessionRingView(
                gain: entry.pre?.maxGain ?? 0,
                loss: entry.pre?.maxLoss ?? 0,
                hasData: entry.pre != nil,
                diameter: innerDia,
                lineWidth: lineWidth
            )
        }
        .frame(width: diameter, height: diameter)
    }

    // MARK: - Data Loading

    private func loadInitial() async {
        guard let resp = await vm.fetchSymbolHistory(symbol: symbol, until: date) else {
            isLoading = false
            return
        }
        dates = resp.dates
        hasMore = resp.hasMore
        isLoading = false
    }

    private func loadMore() {
        guard hasMore, !isLoadingMore, let oldest = dates.first else { return }
        isLoadingMore = true
        Task {
            if let resp = await vm.fetchSymbolHistory(symbol: symbol, before: oldest.date, until: date) {
                if !resp.dates.isEmpty {
                    dates = resp.dates + dates
                }
                hasMore = resp.hasMore
            }
            isLoadingMore = false
        }
    }
}

// MARK: - Week Row

private struct WeekRow: Identifiable {
    let id: String
    let slots: [SymbolDateStats?]  // [Mon, Tue, Wed, Thu, Fri]
}
