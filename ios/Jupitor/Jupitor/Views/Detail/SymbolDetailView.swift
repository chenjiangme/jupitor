import SwiftUI

struct SymbolDetailView: View {
    @Environment(DashboardViewModel.self) private var vm
    let combined: CombinedStatsJSON
    let date: String

    @State private var newsArticles: [NewsArticleJSON] = []
    @State private var isLoadingNews = false

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
                        Task { await vm.toggleWatchlist(symbol: combined.symbol) }
                    } label: {
                        Image(systemName: vm.watchlistSymbols.contains(combined.symbol) ? "star.fill" : "star")
                            .font(.title3)
                            .foregroundStyle(
                                vm.watchlistSymbols.contains(combined.symbol) ? Color.watchlistColor : .secondary
                            )
                    }
                }
                .padding(.horizontal)

                // Session cards.
                if let pre = combined.pre {
                    SessionCard(label: "Pre-Market", stats: pre)
                }
                if let reg = combined.reg {
                    SessionCard(label: "Regular", stats: reg)
                }

                // News count.
                if !isLoadingNews, !newsArticles.isEmpty {
                    HStack {
                        Text("News")
                            .font(.headline)
                        Spacer()
                        Text("\(newsArticles.count)")
                            .font(.headline.monospacedDigit())
                            .foregroundStyle(.secondary)
                    }
                    .padding(.horizontal)
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
