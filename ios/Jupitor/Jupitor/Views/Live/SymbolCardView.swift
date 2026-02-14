import SwiftUI

struct SymbolCardView: View {
    let combined: CombinedStatsJSON
    let session: SessionView
    let isWatchlist: Bool

    private var stats: SymbolStatsJSON? {
        session == .pre ? combined.pre : combined.reg
    }

    private var isDim: Bool {
        guard let s = stats else { return true }
        return s.trades < 1000 || s.turnover < 1_000_000 || s.maxGain < 0.10
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            // Line 1: Symbol, Gain, Trades.
            HStack {
                Text(combined.symbol)
                    .font(.subheadline.bold())
                    .foregroundStyle(isWatchlist ? Color.watchlistColor : .blue)

                Spacer()

                if let s = stats, s.maxGain > 0 {
                    Text(Fmt.gain(s.maxGain))
                        .font(.subheadline.monospacedDigit())
                        .foregroundStyle(s.maxGain >= 0.10 ? Color.gainColor : .secondary)
                }

                if let s = stats {
                    Text("\(Fmt.count(s.trades)) trd")
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(s.trades >= 1000 ? .cyan : .secondary)
                        .frame(width: 80, alignment: .trailing)
                }
            }

            // Line 2: Turnover, Loss, News, Star.
            HStack {
                if let s = stats {
                    Text(Fmt.turnover(s.turnover))
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(s.turnover >= 1_000_000 ? .purple : .secondary)
                }

                if let s = stats, s.maxLoss > 0 {
                    Text(Fmt.loss(s.maxLoss))
                        .font(.caption.monospacedDigit())
                        .foregroundStyle(Color.lossColor)
                }

                Spacer()

                if let n = combined.news, n > 0 {
                    Label("\(n)", systemImage: "newspaper")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }

                if isWatchlist {
                    Image(systemName: "star.fill")
                        .font(.caption2)
                        .foregroundStyle(Color.watchlistColor)
                }
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(
            RoundedRectangle(cornerRadius: 8)
                .fill(Color.white.opacity(0.05))
        )
        .opacity(isDim ? 0.6 : 1.0)
    }
}
