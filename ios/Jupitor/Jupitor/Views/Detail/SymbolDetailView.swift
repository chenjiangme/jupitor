import SwiftUI

struct SymbolDetailView: View {
    @Environment(DashboardViewModel.self) private var vm
    let symbols: [CombinedStatsJSON]
    let initialSymbol: String
    let date: String

    @State private var currentIndex: Int = 0
    @State private var newsArticles: [NewsArticleJSON] = []
    @State private var isLoadingNews = false
    @State private var panOffset: CGFloat = 0
    @State private var isTransitioning = false

    private var currentSymbol: String { symbols[currentIndex].symbol }

    /// For live dates, look up fresh stats from vm (updated every 5s).
    private var combined: CombinedStatsJSON {
        if date == vm.date, let today = vm.today {
            if let fresh = today.tiers.flatMap(\.symbols).first(where: { $0.symbol == currentSymbol }) {
                return fresh
            }
        }
        return symbols[currentIndex]
    }

    private var news: [NewsArticleJSON] {
        newsArticles.filter { $0.source != "stocktwits" }
    }
    private var stocktwitsCount: Int {
        newsArticles.filter { $0.source == "stocktwits" }.count
    }

    private var canGoBack: Bool { currentIndex > 0 }
    private var canGoForward: Bool { currentIndex < symbols.count - 1 }

    private func commitSwipe(offset: CGFloat) {
        let threshold: CGFloat = 80
        let w = UIScreen.main.bounds.width

        if offset < -threshold && canGoForward {
            isTransitioning = true
            withAnimation(.easeOut(duration: 0.15)) { panOffset = -w }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                currentIndex += 1
                panOffset = w
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { panOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isTransitioning = false
            }
        } else if offset > threshold && canGoBack {
            isTransitioning = true
            withAnimation(.easeOut(duration: 0.15)) { panOffset = w }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                currentIndex -= 1
                panOffset = -w
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { panOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isTransitioning = false
            }
        } else {
            withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) { panOffset = 0 }
        }
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                // Header.
                HStack {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(combined.symbol)
                            .font(.title2.bold())
                        Text(combined.tier)
                            .font(.caption)
                            .foregroundStyle(Color.tierColor(for: combined.tier))
                    }
                    Spacer()
                    Button {
                        Task { await vm.toggleWatchlist(symbol: combined.symbol, date: date) }
                    } label: {
                        Image(systemName: vm.watchlistSymbols.contains(combined.symbol) ? "star.fill" : "star")
                            .font(.title3)
                            .foregroundStyle(
                                vm.watchlistSymbols.contains(combined.symbol) ? Color.watchlistColor : .secondary
                            )
                    }
                }
                .padding(.horizontal)

                // Ring visualization.
                DetailRingView(combined: combined)
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 8)

                // Session cards.
                if let pre = combined.pre {
                    SessionCard(label: "Pre-Market", stats: pre)
                }
                if let reg = combined.reg {
                    SessionCard(label: "Regular", stats: reg)
                }

                // StockTwits count.
                if !isLoadingNews, stocktwitsCount > 0 {
                    HStack {
                        Text("StockTwits")
                            .font(.headline)
                        Spacer()
                        Text("\(stocktwitsCount)")
                            .font(.headline.monospacedDigit())
                            .foregroundStyle(.secondary)
                    }
                    .padding(.horizontal)
                }

                // News articles.
                if isLoadingNews {
                    ProgressView()
                        .frame(maxWidth: .infinity)
                        .padding()
                } else if !news.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("News")
                            .font(.headline)
                            .padding(.horizontal)

                        ForEach(news) { article in
                            VStack(alignment: .leading, spacing: 4) {
                                HStack {
                                    Text(article.source)
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                    Spacer()
                                    Text(article.date, style: .time)
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                                Text(article.headline)
                                    .font(.subheadline.bold())
                                    .lineLimit(3)
                                if let content = article.content, !content.isEmpty {
                                    Text(content)
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                        .lineLimit(4)
                                }
                            }
                            .padding(.horizontal)
                            .padding(.vertical, 4)
                        }
                    }
                }
            }
            .padding(.vertical)
        }
        .offset(x: panOffset)
        .background(Color.black)
        .navigationTitle(combined.symbol)
        .navigationBarTitleDisplayMode(.inline)
        .simultaneousGesture(
            DragGesture(minimumDistance: 30)
                .onChanged { value in
                    guard !isTransitioning else { return }
                    let t = value.translation
                    guard abs(t.width) > abs(t.height) else { return }
                    if (t.width < 0 && canGoForward) || (t.width > 0 && canGoBack) {
                        panOffset = t.width
                    }
                }
                .onEnded { value in
                    guard !isTransitioning else {
                        panOffset = 0
                        return
                    }
                    let t = value.translation
                    guard abs(t.width) > abs(t.height) else {
                        withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) { panOffset = 0 }
                        return
                    }
                    commitSwipe(offset: t.width)
                }
        )
        .onAppear {
            if let idx = symbols.firstIndex(where: { $0.symbol == initialSymbol }) {
                currentIndex = idx
            }
        }
        .task(id: combined.symbol) {
            isLoadingNews = true
            newsArticles = await vm.fetchNewsArticles(symbol: combined.symbol, date: date)
            isLoadingNews = false
        }
    }
}

// MARK: - Detail Ring

private struct DetailRingView: View {
    let combined: CombinedStatsJSON

    private let maxDiameter: CGFloat = 140
    private let minDiameter: CGFloat = 50
    private let ringWidth: CGFloat = 12

    private var preTurnover: Double { combined.pre?.turnover ?? 0 }
    private var regTurnover: Double { combined.reg?.turnover ?? 0 }

    /// Diameter scaled by turnover relative to the larger session.
    private func diameter(for turnover: Double, maxTurnover: Double) -> CGFloat {
        guard maxTurnover > 0 else { return minDiameter }
        let ratio = sqrt(CGFloat(turnover / maxTurnover))
        return max(minDiameter, maxDiameter * ratio)
    }

    var body: some View {
        let maxT = max(preTurnover, regTurnover)
        let preDia = diameter(for: preTurnover, maxTurnover: maxT)
        let regDia = diameter(for: regTurnover, maxTurnover: maxT)

        HStack(spacing: 24) {
            if let pre = combined.pre {
                ringWithDial(
                    label: "PRE",
                    stats: pre,
                    dia: preDia
                )
            }
            if let reg = combined.reg {
                ringWithDial(
                    label: "REG",
                    stats: reg,
                    dia: regDia
                )
            }
        }
    }

    @ViewBuilder
    private func ringWithDial(label: String, stats: SymbolStatsJSON, dia: CGFloat) -> some View {
        let outerDia = dia - ringWidth
        VStack(spacing: 6) {
            ZStack {
                SessionRingView(
                    gain: stats.maxGain,
                    loss: stats.maxLoss,
                    hasData: true,
                    diameter: outerDia,
                    lineWidth: ringWidth
                )

                if stats.high > stats.low {
                    CloseDialView(
                        fraction: (stats.close - stats.low) / (stats.high - stats.low),
                        needleRadius: outerDia / 2,
                        lineWidth: max(1.5, ringWidth * 0.4)
                    )
                    .frame(width: dia, height: dia)
                }
            }
            .frame(width: dia, height: dia)

            Text(label)
                .font(.caption2.bold())
                .foregroundStyle(.secondary)
        }
    }
}
