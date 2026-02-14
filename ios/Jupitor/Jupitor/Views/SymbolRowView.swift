import SwiftUI

struct SymbolRowView: View {
    let combined: CombinedStatsJSON
    let session: SessionView
    let isWatchlist: Bool
    let onTap: () -> Void
    let onLongPress: () -> Void

    @State private var isExpanded = false

    private var stats: SymbolStatsJSON? {
        session == .pre ? combined.pre : combined.reg
    }

    private var isDim: Bool {
        guard let s = stats else { return true }
        return s.trades < 1000 || s.turnover < 1_000_000 || s.maxGain < 0.10
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Compact row.
            Button {
                withAnimation(.easeInOut(duration: 0.2)) {
                    isExpanded.toggle()
                }
            } label: {
                compactRow
            }
            .buttonStyle(.plain)
            .simultaneousGesture(
                LongPressGesture(minimumDuration: 0.5)
                    .onEnded { _ in
                        let generator = UIImpactFeedbackGenerator(style: .medium)
                        generator.impactOccurred()
                        onLongPress()
                    }
            )

            // Expanded detail.
            if isExpanded {
                expandedDetail
                    .transition(.opacity.combined(with: .move(edge: .top)))
            }
        }
    }

    // MARK: - Compact Row

    private var compactRow: some View {
        HStack(spacing: 0) {
            // Watchlist marker.
            Text(isWatchlist ? "*" : " ")
                .font(.caption2.monospaced())
                .foregroundStyle(Color.watchlistColor)
                .frame(width: 14)

            // Symbol.
            Text(combined.symbol)
                .font(.caption.bold().monospaced())
                .foregroundStyle(isWatchlist ? Color.watchlistColor : .blue)
                .frame(width: 56, alignment: .leading)

            // Gain%.
            if let s = stats, s.maxGain > 0 {
                Text(Fmt.gain(s.maxGain))
                    .font(.caption.monospaced())
                    .foregroundStyle(s.maxGain >= 0.10 ? Color.gainColor : .secondary)
                    .frame(width: 52, alignment: .trailing)
            } else {
                Text("-")
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
                    .frame(width: 52, alignment: .trailing)
            }

            // Trades.
            if let s = stats {
                Text(Fmt.count(s.trades))
                    .font(.caption.monospaced())
                    .foregroundStyle(s.trades >= 1000 ? .cyan : .secondary)
                    .frame(width: 48, alignment: .trailing)
            } else {
                Text("-")
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
                    .frame(width: 48, alignment: .trailing)
            }

            // Turnover.
            if let s = stats {
                Text(Fmt.turnover(s.turnover))
                    .font(.caption.monospaced())
                    .foregroundStyle(s.turnover >= 1_000_000 ? .purple : .secondary)
                    .frame(width: 56, alignment: .trailing)
            } else {
                Text("-")
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
                    .frame(width: 56, alignment: .trailing)
            }

            // News count.
            if let n = combined.news, n > 0 {
                Text("\(n)")
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary)
                    .frame(width: 36, alignment: .trailing)
            } else {
                Text("-")
                    .font(.caption.monospaced())
                    .foregroundStyle(.secondary.opacity(0.5))
                    .frame(width: 36, alignment: .trailing)
            }

            Spacer()

            // News tap target.
            Button {
                onTap()
            } label: {
                Image(systemName: "newspaper")
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }
            .buttonStyle(.plain)
            .padding(.trailing, 4)
        }
        .padding(.horizontal)
        .padding(.vertical, 5)
        .opacity(isDim ? 0.6 : 1.0)
    }

    // MARK: - Expanded Detail

    private var expandedDetail: some View {
        VStack(alignment: .leading, spacing: 4) {
            if let pre = combined.pre {
                sessionDetail(label: "PRE", s: pre)
            }
            if let reg = combined.reg {
                sessionDetail(label: "REG", s: reg)
            }
        }
        .padding(.horizontal, 30)
        .padding(.vertical, 6)
        .background(Color.white.opacity(0.03))
    }

    private func sessionDetail(label: String, s: SymbolStatsJSON) -> some View {
        VStack(alignment: .leading, spacing: 2) {
            Text(label)
                .font(.caption2.bold())
                .foregroundStyle(.secondary)

            HStack(spacing: 12) {
                VStack(alignment: .leading, spacing: 1) {
                    labelValue("O", Fmt.price(s.open))
                    labelValue("H", Fmt.price(s.high))
                }
                VStack(alignment: .leading, spacing: 1) {
                    labelValue("L", Fmt.price(s.low))
                    labelValue("C", Fmt.price(s.close))
                }
                VStack(alignment: .leading, spacing: 1) {
                    labelValue("Trd", Fmt.count(s.trades))
                    labelValue("TO", Fmt.turnover(s.turnover))
                }
                VStack(alignment: .leading, spacing: 1) {
                    Text(Fmt.gain(s.maxGain))
                        .font(.caption2.monospaced())
                        .foregroundStyle(.green)
                    Text(Fmt.loss(s.maxLoss))
                        .font(.caption2.monospaced())
                        .foregroundStyle(.red)
                }
            }
        }
    }

    private func labelValue(_ label: String, _ value: String) -> some View {
        HStack(spacing: 4) {
            Text(label)
                .font(.caption2)
                .foregroundStyle(.secondary)
                .frame(width: 16, alignment: .trailing)
            Text(value)
                .font(.caption2.monospaced())
                .foregroundStyle(.primary)
        }
    }
}
