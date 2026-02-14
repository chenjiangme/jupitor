import SwiftUI

struct SessionCard: View {
    let label: String
    let stats: SymbolStatsJSON

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text(label)
                .font(.subheadline.bold())
                .foregroundStyle(.secondary)

            // OHLC grid.
            LazyVGrid(columns: Array(repeating: GridItem(.flexible()), count: 4), spacing: 8) {
                MetricCell(label: "Open", value: Fmt.price(stats.open))
                MetricCell(label: "High", value: Fmt.price(stats.high))
                MetricCell(label: "Low", value: Fmt.price(stats.low))
                MetricCell(label: "Close", value: Fmt.price(stats.close))
            }

            Divider()

            // Trades / Volume / Turnover.
            LazyVGrid(columns: Array(repeating: GridItem(.flexible()), count: 3), spacing: 8) {
                MetricCell(label: "Trades", value: Fmt.count(stats.trades))
                MetricCell(label: "Volume", value: Fmt.count(Int(stats.size)))
                MetricCell(label: "Turnover", value: Fmt.turnover(stats.turnover))
            }

            Divider()

            // Gain / Loss.
            HStack(spacing: 16) {
                VStack(alignment: .leading, spacing: 2) {
                    Text("Max Gain")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                    Text(stats.maxGain > 0 ? Fmt.gain(stats.maxGain) : "-")
                        .font(.subheadline.bold().monospacedDigit())
                        .foregroundStyle(stats.maxGain >= 0.10 ? Color.gainColor : .secondary)
                }
                VStack(alignment: .leading, spacing: 2) {
                    Text("Max Loss")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                    Text(stats.maxLoss > 0 ? Fmt.loss(stats.maxLoss) : "-")
                        .font(.subheadline.bold().monospacedDigit())
                        .foregroundStyle(stats.maxLoss >= 0.10 ? Color.lossColor : .secondary)
                }
                Spacer()
            }
        }
        .padding()
        .background(
            RoundedRectangle(cornerRadius: 12)
                .fill(Color.white.opacity(0.05))
        )
        .padding(.horizontal)
    }
}
