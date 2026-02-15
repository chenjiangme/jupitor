import SwiftUI

struct SymbolHistoryView: View {
    @Environment(DashboardViewModel.self) private var vm
    let symbol: String

    @State private var dates: [SymbolDateStats] = []
    @State private var isLoading = true
    @State private var hasMore = false
    @State private var isLoadingMore = false

    private let maxDiameter: CGFloat = 60
    private let minDiameter: CGFloat = 20
    private let minInnerRatio: CGFloat = 0.15

    private var maxTurnover: Double {
        dates.map { ($0.pre?.turnover ?? 0) + ($0.reg?.turnover ?? 0) }.max() ?? 1
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
                ScrollView {
                    let cellSize = maxDiameter + 2
                    LazyVGrid(columns: [GridItem(.adaptive(minimum: cellSize, maximum: cellSize), spacing: 0)], spacing: 0) {
                        if hasMore {
                            Color.clear.frame(width: 1, height: 1)
                                .onAppear { loadMore() }
                        }

                        ForEach(dates) { entry in
                            ringView(entry)
                                .frame(width: cellSize, height: cellSize)
                        }
                    }
                }
                .defaultScrollAnchor(.bottom)
            }
        }
        .navigationTitle(symbol)
        .navigationBarTitleDisplayMode(.inline)
        .task { await loadInitial() }
    }

    // MARK: - Ring View

    @ViewBuilder
    private func ringView(_ entry: SymbolDateStats) -> some View {
        let preTurnover = entry.pre?.turnover ?? 0
        let regTurnover = entry.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let ratio = maxTurnover > 0 ? sqrt(CGFloat(total / maxTurnover)) : 0
        let diameter = minDiameter + (maxDiameter - minDiameter) * ratio
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
        guard let resp = await vm.fetchSymbolHistory(symbol: symbol) else {
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
            if let resp = await vm.fetchSymbolHistory(symbol: symbol, before: oldest.date) {
                if !resp.dates.isEmpty {
                    dates = resp.dates + dates
                }
                hasMore = resp.hasMore
            }
            isLoadingMore = false
        }
    }
}
