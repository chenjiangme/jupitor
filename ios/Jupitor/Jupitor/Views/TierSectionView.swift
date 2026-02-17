import SwiftUI

struct TierSectionView: View {
    @Environment(DashboardViewModel.self) private var vm
    let tier: TierGroupJSON
    let date: String

    private var sortedByTurnover: [CombinedStatsJSON] {
        tier.symbols.sorted { a, b in
            totalTurnover(a) > totalTurnover(b)
        }
    }

    private func totalTurnover(_ s: CombinedStatsJSON) -> Double {
        (s.pre?.turnover ?? 0) + (s.reg?.turnover ?? 0)
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            // Tier header.
            HStack(spacing: 6) {
                Circle()
                    .fill(Color.tierColor(for: tier.name))
                    .frame(width: 8, height: 8)
                Text(tier.name)
                    .font(.caption.bold())
                    .foregroundStyle(Color.tierColor(for: tier.name))
                Text("\(tier.count)")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                Spacer()
            }
            .padding(.horizontal)
            .padding(.top, 10)
            .padding(.bottom, 4)

            // Symbol cards.
            ForEach(tier.symbols) { combined in
                NavigationLink {
                    SymbolDetailView(symbols: sortedByTurnover, initialSymbol: combined.symbol, date: date)
                } label: {
                    SymbolCardView(
                        combined: combined,
                        session: vm.sessionView,
                        isWatchlist: vm.watchlistSymbols.contains(combined.symbol)
                    )
                }
                .buttonStyle(.plain)
                .padding(.horizontal, 8)
            }
        }
    }
}
