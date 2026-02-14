import SwiftUI

struct NewsSheetView: View {
    let symbol: String
    let date: String
    let articles: [NewsArticleJSON]

    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            Group {
                if articles.isEmpty {
                    VStack(spacing: 12) {
                        Image(systemName: "newspaper")
                            .font(.largeTitle)
                            .foregroundStyle(.secondary)
                        Text("No articles found")
                            .foregroundStyle(.secondary)
                    }
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
                } else {
                    List(articles) { article in
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
                        .padding(.vertical, 4)
                        .listRowBackground(Color.clear)
                    }
                    .listStyle(.plain)
                }
            }
            .navigationTitle("\(symbol) - \(date)")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}
