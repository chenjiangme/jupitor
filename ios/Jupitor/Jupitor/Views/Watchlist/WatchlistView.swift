import SwiftUI

struct WatchlistView: View {
    @Environment(DashboardViewModel.self) private var vm

    private var watchlistSymbols: [CombinedStatsJSON] {
        guard let today = vm.today else { return [] }
        return today.tiers.flatMap(\.symbols)
            .filter { vm.watchlistSymbols.contains($0.symbol) }
    }

    var body: some View {
        ScrollView {
            if watchlistSymbols.isEmpty {
                ContentUnavailableView(
                    "No Watchlist Symbols",
                    systemImage: "star.slash",
                    description: Text("Star symbols from the Live or Detail view to add them here.")
                )
            } else {
                LazyVStack(spacing: 4) {
                    ForEach(watchlistSymbols) { combined in
                        NavigationLink {
                            let sorted = watchlistSymbols.sorted {
                                ($0.pre?.turnover ?? 0) + ($0.reg?.turnover ?? 0) >
                                ($1.pre?.turnover ?? 0) + ($1.reg?.turnover ?? 0)
                            }
                            SymbolDetailView(symbols: sorted, initialSymbol: combined.symbol, date: vm.date)
                        } label: {
                            SymbolCardView(
                                combined: combined,
                                session: vm.sessionView,
                                isWatchlist: true
                            )
                        }
                        .buttonStyle(.plain)
                        .padding(.horizontal, 8)
                    }
                }
                .padding(.top, 8)
            }
        }
        .background(Color.black)
        .navigationTitle("Watchlist")
        .navigationBarTitleDisplayMode(.inline)
    }
}
