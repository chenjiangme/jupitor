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

                // News section.
                if isLoadingNews {
                    ProgressView()
                        .frame(maxWidth: .infinity)
                        .padding()
                } else if !newsArticles.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("News")
                            .font(.headline)
                            .padding(.horizontal)

                        ForEach(newsArticles) { article in
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
