import SwiftUI

struct SymbolHistoryView: View {
    @Environment(DashboardViewModel.self) private var vm
    let symbol: String

    @State private var dates: [SymbolDateStats] = []
    @State private var isLoading = true

    private let ringDiameter: CGFloat = 60
    private let ringLineWidth: CGFloat = 5
    private let minInnerRatio: CGFloat = 0.15

    var body: some View {
        VStack(spacing: 16) {
            if isLoading {
                Spacer()
                ProgressView()
                Spacer()
            } else if dates.isEmpty {
                Spacer()
                Text("No trading history")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                ScrollViewReader { proxy in
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 16) {
                            ForEach(dates) { entry in
                                ringPair(entry)
                                    .id(entry.date)
                            }
                        }
                        .padding(.horizontal, 16)
                        .padding(.vertical, 8)
                    }
                    .onAppear {
                        if let last = dates.last {
                            proxy.scrollTo(last.date, anchor: .trailing)
                        }
                    }
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
    private func ringPair(_ entry: SymbolDateStats) -> some View {
        let preTurnover = entry.pre?.turnover ?? 0
        let regTurnover = entry.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0
        let outerDia = ringDiameter - ringLineWidth
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
                    lineWidth: ringLineWidth
                )

                SessionRingView(
                    gain: entry.pre?.maxGain ?? 0,
                    loss: entry.pre?.maxLoss ?? 0,
                    hasData: entry.pre != nil,
                    diameter: innerDia,
                    lineWidth: ringLineWidth
                )
            }
            .frame(width: ringDiameter, height: ringDiameter)

            Text(shortDate(entry.date))
                .font(.system(size: 10))
                .foregroundStyle(.secondary)
        }
    }

    private func shortDate(_ dateStr: String) -> String {
        // "2025-02-10" → "2/10"
        let parts = dateStr.split(separator: "-")
        guard parts.count == 3,
              let m = Int(parts[1]),
              let d = Int(parts[2]) else { return dateStr }
        return "\(m)/\(d)"
    }
}
