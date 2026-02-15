import SwiftUI

struct SymbolHistoryView: View {
    @Environment(DashboardViewModel.self) private var vm
    let symbol: String

    @State private var dates: [SymbolDateStats] = []
    @State private var isLoading = true

    private let maxRingDiameter: CGFloat = 60
    private let minRingDiameter: CGFloat = 20
    private let ringLineWidth: CGFloat = 5
    private let minInnerRatio: CGFloat = 0.15
    private let cellSpacing: CGFloat = 12

    private let columns = [GridItem(.adaptive(minimum: 68), spacing: 12)]

    private var maxTurnover: Double {
        dates.map { ($0.pre?.turnover ?? 0) + ($0.reg?.turnover ?? 0) }.max() ?? 1
    }

    var body: some View {
        Group {
            if isLoading {
                VStack {
                    Spacer()
                    ProgressView()
                    Spacer()
                }
            } else if dates.isEmpty {
                VStack {
                    Spacer()
                    Text("No trading history")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Spacer()
                }
            } else {
                ScrollView {
                    LazyVGrid(columns: columns, spacing: cellSpacing) {
                        ForEach(dates.reversed()) { entry in
                            ringCell(entry)
                        }
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 8)
                }
            }
        }
        .navigationTitle(symbol)
        .navigationBarTitleDisplayMode(.inline)
        .task {
            if let resp = await vm.fetchSymbolHistory(symbol: symbol) {
                dates = resp.dates
            }
            isLoading = false
        }
    }

    @ViewBuilder
    private func ringCell(_ entry: SymbolDateStats) -> some View {
        let preTurnover = entry.pre?.turnover ?? 0
        let regTurnover = entry.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0

        // Scale ring diameter by sqrt(turnover / maxTurnover) for area-proportional sizing.
        let ratio = maxTurnover > 0 ? sqrt(CGFloat(total / maxTurnover)) : 0
        let diameter = minRingDiameter + (maxRingDiameter - minRingDiameter) * ratio
        let lineWidth = max(3, diameter * 0.09)
        let outerDia = diameter - lineWidth
        let innerDia = outerDia * max(minInnerRatio, preRatio)

        VStack(spacing: 4) {
            ZStack {
                Circle()
                    .fill(Color.white.opacity(0.04))

                SessionRingView(
                    gain: entry.reg?.maxGain ?? 0,
                    loss: entry.reg?.maxLoss ?? 0,
                    hasData: entry.reg != nil,
                    diameter: outerDia,
                    lineWidth: lineWidth
                )

                SessionRingView(
                    gain: entry.pre?.maxGain ?? 0,
                    loss: entry.pre?.maxLoss ?? 0,
                    hasData: entry.pre != nil,
                    diameter: innerDia,
                    lineWidth: lineWidth
                )
            }
            .frame(width: maxRingDiameter, height: maxRingDiameter)

            Text(shortDate(entry.date))
                .font(.system(size: 10))
                .foregroundStyle(.secondary)
        }
    }

    private func shortDate(_ dateStr: String) -> String {
        let parts = dateStr.split(separator: "-")
        guard parts.count == 3,
              let m = Int(parts[1]),
              let d = Int(parts[2]) else { return dateStr }
        return "\(m)/\(d)"
    }
}
