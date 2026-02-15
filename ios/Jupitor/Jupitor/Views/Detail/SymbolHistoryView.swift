import SwiftUI

struct SymbolHistoryView: View {
    @Environment(DashboardViewModel.self) private var vm
    let symbol: String

    @State private var dates: [SymbolDateStats] = []
    @State private var isLoading = true
    @State private var hasMore = false
    @State private var isLoadingMore = false

    private let maxRingDiameter: CGFloat = 60
    private let minRingDiameter: CGFloat = 20
    private let minInnerRatio: CGFloat = 0.15

    private var maxTurnover: Double {
        dates.map { ($0.pre?.turnover ?? 0) + ($0.reg?.turnover ?? 0) }.max() ?? 1
    }

    var body: some View {
        Group {
            if isLoading {
                VStack {
                    Spacer()
                    ProgressView()
                    Spacer()
                }
            } else if dates.isEmpty {
                VStack {
                    Spacer()
                    Text("No trading history")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Spacer()
                }
            } else {
                GeometryReader { geo in
                    let cols = max(1, Int(geo.size.width / maxRingDiameter))
                    let cellSize = geo.size.width / CGFloat(cols)
                    let rowHeight = cellSize * 0.87

                    ScrollViewReader { proxy in
                        ScrollView {
                            LazyVStack(spacing: 0) {
                                // Sentinel for loading more.
                                if hasMore {
                                    Color.clear.frame(height: 1)
                                        .onAppear { loadMore() }
                                }

                                ForEach(rowIndices(count: dates.count, cols: cols), id: \.self) { rowIdx in
                                    hexRow(rowIdx: rowIdx, cols: cols, cellSize: cellSize)
                                        .frame(height: rowHeight)
                                }
                            }
                            .padding(.bottom, 4)

                            // Anchor at the bottom.
                            Color.clear.frame(height: 1).id("bottom")
                        }
                        .defaultScrollAnchor(.bottom)
                    }
                }
            }
        }
        .navigationTitle(symbol)
        .navigationBarTitleDisplayMode(.inline)
        .task {
            await loadInitial()
        }
    }

    // MARK: - Hex Row

    @ViewBuilder
    private func hexRow(rowIdx: Int, cols: Int, cellSize: CGFloat) -> some View {
        let start = rowIdx * cols
        let end = min(start + cols, dates.count)
        let isOffset = rowIdx % 2 == 1

        HStack(spacing: 0) {
            if isOffset {
                Spacer().frame(width: cellSize / 2)
            }
            ForEach(start..<end, id: \.self) { i in
                ringCell(dates[i], cellSize: cellSize)
                    .frame(width: cellSize, height: cellSize)
            }
            Spacer(minLength: 0)
        }
    }

    private func rowIndices(count: Int, cols: Int) -> [Int] {
        guard cols > 0 else { return [] }
        let rows = (count + cols - 1) / cols
        return Array(0..<rows)
    }

    // MARK: - Ring Cell

    @ViewBuilder
    private func ringCell(_ entry: SymbolDateStats, cellSize: CGFloat) -> some View {
        let preTurnover = entry.pre?.turnover ?? 0
        let regTurnover = entry.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0

        let ratio = maxTurnover > 0 ? sqrt(CGFloat(total / maxTurnover)) : 0
        let diameter = minRingDiameter + (maxRingDiameter - minRingDiameter) * ratio
        let lineWidth = max(3, diameter * 0.09)
        let outerDia = diameter - lineWidth
        let innerDia = outerDia * max(minInnerRatio, preRatio)

        ZStack {
            Circle()
                .fill(Color.white.opacity(0.04))

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
