import SwiftUI

struct SymbolDetailView: View {
    @Environment(DashboardViewModel.self) private var vm
    let combined: CombinedStatsJSON
    let date: String

    @State private var newsArticles: [NewsArticleJSON] = []
    @State private var isLoadingNews = false

    private var news: [NewsArticleJSON] {
        newsArticles.filter { $0.source != "stocktwits" }
    }
    private var stocktwitsCount: Int {
        newsArticles.filter { $0.source == "stocktwits" }.count
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
        .background(Color.black)
        .navigationTitle(combined.symbol)
        .navigationBarTitleDisplayMode(.inline)
        .task {
            isLoadingNews = true
            newsArticles = await vm.fetchNewsArticles(symbol: combined.symbol, date: date)
            isLoadingNews = false
        }
    }
}

// MARK: - Detail Ring

private struct DetailRingView: View {
    let combined: CombinedStatsJSON

    private let diameter: CGFloat = 160
    private let ringWidth: CGFloat = 14
    private let minInnerRatio: CGFloat = 0.15

    private var hasPre: Bool { combined.pre != nil }
    private var hasReg: Bool { combined.reg != nil }
    private var dualRing: Bool { hasPre && hasReg }

    private var preTurnover: Double { combined.pre?.turnover ?? 0 }
    private var regTurnover: Double { combined.reg?.turnover ?? 0 }
    private var total: Double { preTurnover + regTurnover }
    private var preRatio: CGFloat { total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0 }

    private var outerDia: CGFloat { diameter - ringWidth }
    private var innerDia: CGFloat {
        min(outerDia - 3 * ringWidth, outerDia * max(minInnerRatio, preRatio))
    }
    private var blackFillDia: CGFloat { innerDia + ringWidth }

    /// Combined stats for close dial.
    private var dialStats: (close: Double, low: Double, high: Double)? {
        if let reg = combined.reg, reg.high > reg.low {
            return (reg.close, min(combined.pre?.low ?? reg.low, reg.low), max(combined.pre?.high ?? reg.high, reg.high))
        }
        if let pre = combined.pre, pre.high > pre.low {
            return (pre.close, pre.low, pre.high)
        }
        return nil
    }

    var body: some View {
        ZStack {
            if dualRing {
                // Outer ring (regular session).
                SessionRingView(
                    gain: combined.reg?.maxGain ?? 0,
                    loss: combined.reg?.maxLoss ?? 0,
                    hasData: true,
                    diameter: outerDia,
                    lineWidth: ringWidth
                )

                // Black fill for clean separation.
                Circle()
                    .fill(Color.black)
                    .frame(width: blackFillDia, height: blackFillDia)

                // Inner ring (pre-market).
                SessionRingView(
                    gain: combined.pre?.maxGain ?? 0,
                    loss: combined.pre?.maxLoss ?? 0,
                    hasData: true,
                    diameter: innerDia,
                    lineWidth: ringWidth
                )
            } else {
                // Single ring for whichever session exists.
                SessionRingView(
                    gain: combined.pre?.maxGain ?? combined.reg?.maxGain ?? 0,
                    loss: combined.pre?.maxLoss ?? combined.reg?.maxLoss ?? 0,
                    hasData: hasPre || hasReg,
                    diameter: outerDia,
                    lineWidth: ringWidth
                )
            }

            // Close dial needle.
            if let s = dialStats {
                CloseDialView(
                    fraction: (s.close - s.low) / (s.high - s.low),
                    needleRadius: outerDia / 2,
                    lineWidth: max(1.5, ringWidth * 0.4)
                )
                .frame(width: diameter, height: diameter)
            }
        }
        .frame(width: diameter, height: diameter)
    }
}
