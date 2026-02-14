import SwiftUI

struct TierSectionView: View {
    let tier: TierGroupJSON
    let session: SessionView
    let watchlist: Set<String>
    let onSelect: (String) -> Void
    let onToggleWatchlist: (String) -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
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

            // Column header.
            HStack(spacing: 0) {
                Text("")
                    .frame(width: 14) // watchlist marker
                Text("Symbol")
                    .frame(width: 56, alignment: .leading)
                Text("Gain%")
                    .frame(width: 52, alignment: .trailing)
                Text("Trd")
                    .frame(width: 48, alignment: .trailing)
                Text("TO")
                    .frame(width: 56, alignment: .trailing)
                Text("News")
                    .frame(width: 36, alignment: .trailing)
                Spacer()
            }
            .font(.caption2)
            .foregroundStyle(.secondary)
            .padding(.horizontal)
            .padding(.bottom, 2)

            // Symbol rows.
            ForEach(tier.symbols) { combined in
                SymbolRowView(
                    combined: combined,
                    session: session,
                    isWatchlist: watchlist.contains(combined.symbol),
                    onTap: { onSelect(combined.symbol) },
                    onLongPress: { onToggleWatchlist(combined.symbol) }
                )
            }
        }
    }
}
