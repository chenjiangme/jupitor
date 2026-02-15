import SwiftUI

struct BubbleChartView: View {
    @Environment(DashboardViewModel.self) private var vm
    let day: DayDataJSON
    let date: String

    private var symbols: [(combined: CombinedStatsJSON, tier: String)] {
        day.tiers
            .filter { $0.name == "MODERATE" || $0.name == "SPORADIC" }
            .flatMap { tier in tier.symbols.map { (combined: $0, tier: tier.name) } }
    }

    var body: some View {
        VStack(spacing: 0) {
            // Day header.
            HStack {
                Text(day.label)
                    .font(.caption.bold())
                    .foregroundStyle(.white)
                Spacer()
                if day.preCount > 0 {
                    Text("pre: \(Fmt.intWithCommas(day.preCount))")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                if day.regCount > 0 {
                    Text("reg: \(Fmt.intWithCommas(day.regCount))")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
            .padding(.horizontal)
            .padding(.vertical, 6)
            .background(Color.cyan.opacity(0.3))

            if symbols.isEmpty {
                Spacer()
                Text("(no matching symbols)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                // Bubble chart.
                GeometryReader { geo in
                    let bubbles = packBubbles(in: geo.size)
                    ZStack {
                        ForEach(bubbles, id: \.combined.symbol) { bubble in
                            NavigationLink {
                                SymbolDetailView(combined: bubble.combined, date: date)
                            } label: {
                                bubbleLabel(bubble)
                            }
                            .buttonStyle(.plain)
                            .position(bubble.center)
                        }
                    }
                    .frame(width: geo.size.width, height: geo.size.height)
                }

                // Legend.
                HStack(spacing: 4) {
                    Text("Turnover:")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                    legendGradient
                        .frame(width: 120, height: 6)
                        .clipShape(Capsule())
                    HStack {
                        Text("$10K")
                        Spacer()
                        Text("$100M")
                    }
                    .font(.system(size: 8))
                    .foregroundStyle(.secondary)
                    .frame(width: 120)

                    Spacer()

                    HStack(spacing: 8) {
                        tierLegendDot("MOD", color: .tierModerate)
                        tierLegendDot("SPO", color: .tierSporadic)
                    }
                }
                .padding(.horizontal)
                .padding(.vertical, 6)
            }
        }
    }

    // MARK: - Bubble Label

    private func bubbleLabel(_ bubble: Bubble) -> some View {
        let isWatchlist = vm.watchlistSymbols.contains(bubble.combined.symbol)
        let diameter = bubble.radius * 2

        return ZStack {
            Circle()
                .fill(bubble.color.opacity(0.85))

            Circle()
                .strokeBorder(
                    isWatchlist ? Color.watchlistColor : Color.tierColor(for: bubble.tier).opacity(0.5),
                    lineWidth: isWatchlist ? 2.5 : 1
                )

            VStack(spacing: 0) {
                Text(bubble.combined.symbol)
                    .font(bubble.radius > 28 ? .caption2.bold() : .system(size: 9, weight: .bold))
                    .foregroundStyle(.white)
                    .minimumScaleFactor(0.6)

                if bubble.radius > 24, let s = sessionStats(bubble.combined), s.maxGain > 0 {
                    Text(Fmt.gain(s.maxGain))
                        .font(.system(size: bubble.radius > 32 ? 10 : 8, weight: .semibold))
                        .foregroundStyle(.white.opacity(0.85))
                }
            }
            .padding(2)
        }
        .frame(width: diameter, height: diameter)
    }

    // MARK: - Legend

    private var legendGradient: some View {
        LinearGradient(
            colors: [
                turnoverColor(1e4),
                turnoverColor(1e5),
                turnoverColor(1e6),
                turnoverColor(1e7),
                turnoverColor(1e8),
            ],
            startPoint: .leading,
            endPoint: .trailing
        )
    }

    private func tierLegendDot(_ label: String, color: Color) -> some View {
        HStack(spacing: 3) {
            Circle()
                .fill(color)
                .frame(width: 6, height: 6)
            Text(label)
                .font(.system(size: 8))
                .foregroundStyle(.secondary)
        }
    }

    // MARK: - Data

    private func sessionStats(_ combined: CombinedStatsJSON) -> SymbolStatsJSON? {
        vm.sessionView == .pre ? combined.pre : combined.reg
    }

    // MARK: - Packing Algorithm

    private struct Bubble {
        let combined: CombinedStatsJSON
        let tier: String
        let radius: CGFloat
        let color: Color
        var center: CGPoint
    }

    private func packBubbles(in size: CGSize) -> [Bubble] {
        let viewCenter = CGPoint(x: size.width / 2, y: size.height / 2)

        // Build items with stats.
        var items: [(combined: CombinedStatsJSON, tier: String, trades: Int, turnover: Double)] = []
        for (combined, tier) in symbols {
            guard let s = sessionStats(combined), s.trades > 0 else { continue }
            items.append((combined, tier, s.trades, s.turnover))
        }
        guard !items.isEmpty else { return [] }

        // Compute radii (sqrt scaling for area-proportional sizing).
        let maxTrades = items.map(\.trades).max()!
        let count = items.count
        let maxRadius: CGFloat = count > 25 ? 26 : count > 15 ? 34 : 44
        let minRadius: CGFloat = 16

        let sorted = items.sorted { $0.trades > $1.trades }

        var placed: [(center: CGPoint, radius: CGFloat)] = []
        var result: [Bubble] = []

        for item in sorted {
            let norm = sqrt(Double(item.trades)) / sqrt(Double(maxTrades))
            let radius = minRadius + CGFloat(norm) * (maxRadius - minRadius)
            let color = turnoverColor(item.turnover)

            var bestPos = viewCenter
            let pad: CGFloat = 3

            if !placed.isEmpty {
                var bestDist = CGFloat.infinity

                // Try positions touching each existing circle.
                for existing in placed {
                    let touchDist = existing.radius + radius + pad
                    let steps = max(18, Int(touchDist * 0.8))
                    for i in 0..<steps {
                        let angle = CGFloat(i) * 2 * .pi / CGFloat(steps)
                        let candidate = CGPoint(
                            x: existing.center.x + cos(angle) * touchDist,
                            y: existing.center.y + sin(angle) * touchDist
                        )

                        // Bounds check.
                        guard candidate.x - radius >= 0,
                              candidate.x + radius <= size.width,
                              candidate.y - radius >= 0,
                              candidate.y + radius <= size.height else { continue }

                        // Overlap check.
                        let overlaps = placed.contains { other in
                            hypot(candidate.x - other.center.x, candidate.y - other.center.y) < radius + other.radius + pad
                        }

                        if !overlaps {
                            let dist = hypot(candidate.x - viewCenter.x, candidate.y - viewCenter.y)
                            if dist < bestDist {
                                bestDist = dist
                                bestPos = candidate
                            }
                        }
                    }
                }

                // Fallback: spiral outward if no touching position found.
                if bestDist == .infinity {
                    outer: for r in stride(from: radius, through: max(size.width, size.height), by: radius * 0.5) {
                        for i in 0..<36 {
                            let angle = CGFloat(i) * 2 * .pi / 36
                            let candidate = CGPoint(
                                x: viewCenter.x + cos(angle) * r,
                                y: viewCenter.y + sin(angle) * r
                            )
                            guard candidate.x - radius >= 0,
                                  candidate.x + radius <= size.width,
                                  candidate.y - radius >= 0,
                                  candidate.y + radius <= size.height else { continue }
                            let overlaps = placed.contains { other in
                                hypot(candidate.x - other.center.x, candidate.y - other.center.y) < radius + other.radius + pad
                            }
                            if !overlaps {
                                bestPos = candidate
                                break outer
                            }
                        }
                    }
                }
            }

            placed.append((bestPos, radius))
            result.append(Bubble(
                combined: item.combined,
                tier: item.tier,
                radius: radius,
                color: color,
                center: bestPos
            ))
        }

        return result
    }

    private func turnoverColor(_ turnover: Double) -> Color {
        // Log scale: $10K (blue) → $100M (red).
        let logVal = log10(max(turnover, 1e4))
        let t = max(0, min(1, (logVal - 4) / 4))
        let hue = 0.6 * (1 - t)
        return Color(hue: hue, saturation: 0.75, brightness: 0.85)
    }
}
