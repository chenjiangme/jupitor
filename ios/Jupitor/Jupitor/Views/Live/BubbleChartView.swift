import SwiftUI

struct BubbleChartView: View {
    @Environment(DashboardViewModel.self) private var vm
    let day: DayDataJSON
    let date: String
    var sessionMode: SessionMode = .day

    @State private var bubbles: [BubbleState] = []
    @State private var viewSize: CGSize = .zero
    @State private var showDetail = false
    @State private var detailCombined: CombinedStatsJSON?
    @State private var showHistory = false
    @State private var historySymbol: String = ""
    @State private var isSettled = false
    @State private var showWatchlistOnly = false

    private let minInnerRatio: CGFloat = 0.15

    private var symbolData: [(combined: CombinedStatsJSON, tier: String)] {
        let all = day.tiers
            .filter { $0.name == "MODERATE" || $0.name == "SPORADIC" }
            .flatMap { tier in tier.symbols.map { (combined: $0, tier: tier.name) } }
            .filter { item in
                switch sessionMode {
                case .pre, .next: return item.combined.pre != nil
                case .reg: return item.combined.reg != nil
                case .day: return true
                }
            }
        let watchlist = all.filter { vm.watchlistSymbols.contains($0.combined.symbol) }
        if showWatchlistOnly { return watchlist }
        let rest = all.filter { !vm.watchlistSymbols.contains($0.combined.symbol) }
        return watchlist + rest
    }

    var body: some View {
        VStack(spacing: 0) {
            if symbolData.isEmpty {
                Spacer()
                Text("(no matching symbols)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                Spacer()
            } else {
                GeometryReader { geo in
                    ZStack {
                        // Invisible background to receive gestures on empty areas.
                        Color.clear.contentShape(Rectangle())

                        ForEach(bubbles) { bubble in
                            bubbleView(bubble)
                        }
                    }
                    .coordinateSpace(name: "canvas")
                    .frame(width: geo.size.width, height: geo.size.height)
                    .gesture(
                        MagnifyGesture()
                            .onEnded { value in
                                let newValue: Bool
                                if value.magnification < 0.7 {
                                    newValue = true
                                } else if value.magnification > 1.3 {
                                    newValue = false
                                } else {
                                    return
                                }
                                guard newValue != showWatchlistOnly else { return }
                                showWatchlistOnly = newValue
                                if viewSize.width > 0 { syncBubbles(in: viewSize) }
                            }
                    )
                    .onAppear {
                        viewSize = geo.size
                        if bubbles.isEmpty { syncBubbles(in: geo.size) }
                    }
                    .onChange(of: geo.size) { _, newSize in
                        viewSize = newSize
                    }
                }
                .task(id: isSettled) {
                    guard !isSettled else { return }
                    while !Task.isCancelled {
                        if viewSize.width > 0 { simulationStep() }
                        try? await Task.sleep(for: .milliseconds(16))
                    }
                }

            }
        }
        .onChange(of: day) { _, _ in
            if viewSize.width > 0 { syncBubbles(in: viewSize) }
        }
        .onChange(of: sessionMode) { _, _ in
            if viewSize.width > 0 { syncBubbles(in: viewSize) }
        }
        .onChange(of: vm.watchlistSymbols) { old, new in
            if viewSize.width > 0 {
                onWatchlistChanged(added: new.subtracting(old), removed: old.subtracting(new))
            }
        }
        .navigationDestination(isPresented: $showDetail) {
            if let combined = detailCombined {
                SymbolDetailView(combined: combined, date: date)
            }
        }
        .navigationDestination(isPresented: $showHistory) {
            SymbolHistoryView(symbol: historySymbol, date: date)
        }
        .onShake {
            let onScreen = Set(symbolData.map(\.combined.symbol))
            let toRemove = onScreen.intersection(vm.watchlistSymbols)
            guard !toRemove.isEmpty else { return }
            Task { await vm.removeWatchlistSymbols(toRemove, date: date) }
        }
    }

    // MARK: - Bubble View

    @ViewBuilder
    private func bubbleView(_ bubble: BubbleState) -> some View {
        let isWatchlist = vm.watchlistSymbols.contains(bubble.id)
        let diameter = bubble.radius * 2
        let ringWidth = max(4, bubble.radius * 0.18)
        let outerDia = diameter - ringWidth
        let preTurnover = bubble.combined.pre?.turnover ?? 0
        let regTurnover = bubble.combined.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0
        // Cap innerDia so inner ring stroke doesn't overlap outer ring stroke.
        let innerDia = min(outerDia - 3 * ringWidth, outerDia * max(minInnerRatio, preRatio))
        // Black fill covers inner ring stroke fully (center to outer stroke edge).
        let blackFillDia = innerDia + ringWidth

        ZStack {
            // Subtle background.
            let hasPre_ = bubble.combined.pre != nil
            let hasReg_ = bubble.combined.reg != nil
            let bgOpacity = (sessionMode == .day && hasPre_ && hasReg_) ? 0.08 : 0.04
            if isWatchlist {
                RoundedRectangle(cornerRadius: diameter * 0.15)
                    .fill(Color.white.opacity(bgOpacity))
            } else {
                Circle()
                    .fill(Color.white.opacity(bgOpacity))
            }

            let hasPre = bubble.combined.pre != nil
            let hasReg = bubble.combined.reg != nil
            let dualRing = sessionMode == .day && hasPre && hasReg

            if dualRing {
                // Outer ring (regular session).
                SessionRingView(
                    gain: bubble.combined.reg?.maxGain ?? 0,
                    loss: bubble.combined.reg?.maxLoss ?? 0,
                    hasData: true,
                    diameter: outerDia,
                    lineWidth: ringWidth,
                    isSquare: isWatchlist
                )

                // Black fill covers inner ring area for clean separation.
                if isWatchlist {
                    RoundedRectangle(cornerRadius: blackFillDia * 0.15)
                        .fill(Color.black)
                        .frame(width: blackFillDia, height: blackFillDia)
                } else {
                    Circle()
                        .fill(Color.black)
                        .frame(width: blackFillDia, height: blackFillDia)
                }

                // Inner ring (pre-market session).
                SessionRingView(
                    gain: bubble.combined.pre?.maxGain ?? 0,
                    loss: bubble.combined.pre?.maxLoss ?? 0,
                    hasData: true,
                    diameter: innerDia,
                    lineWidth: ringWidth,
                    isSquare: isWatchlist
                )
            } else {
                // Single ring for the relevant session.
                SessionRingView(
                    gain: singleRingGain(bubble.combined),
                    loss: singleRingLoss(bubble.combined),
                    hasData: hasPre || hasReg,
                    diameter: outerDia,
                    lineWidth: ringWidth,
                    isSquare: isWatchlist
                )
            }

            // Symbol label + price for watchlist.
            VStack(spacing: 0) {
                Text(bubble.id)
                    .font(.system(size: max(7, bubble.radius * 0.3), weight: .heavy))
                    .foregroundStyle((isWatchlist ? Color.watchlistColor : .white).opacity(0.5))
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                if isWatchlist, let stats = sessionStats(bubble.combined) {
                    Text("\(Fmt.compactPrice(stats.open)) \(Fmt.compactPrice(stats.high)) \(Fmt.compactPrice(stats.close))")
                        .font(.system(size: max(5, bubble.radius * 0.16)))
                        .foregroundStyle(Color.watchlistPriceColor.opacity(0.4))
                        .lineLimit(1)
                        .minimumScaleFactor(0.5)
                }
            }
            .padding(2)
        }
        .frame(width: diameter, height: diameter)
        .position(bubble.position)
        .onTapGesture(count: 2) {
            historySymbol = bubble.id
            showHistory = true
        }
        .onTapGesture(count: 1) {
            Task { await vm.toggleWatchlist(symbol: bubble.id, date: date) }
        }
        .onLongPressGesture {
            detailCombined = bubble.combined
            showDetail = true
        }
    }

    // MARK: - Helpers

    private func sessionStats(_ c: CombinedStatsJSON) -> SymbolStatsJSON? {
        switch sessionMode {
        case .pre, .next: return c.pre
        case .reg: return c.reg
        case .day: return c.reg ?? c.pre
        }
    }


    private func singleRingGain(_ c: CombinedStatsJSON) -> Double {
        switch sessionMode {
        case .pre, .next: return c.pre?.maxGain ?? 0
        case .reg: return c.reg?.maxGain ?? 0
        case .day: return c.pre?.maxGain ?? c.reg?.maxGain ?? 0
        }
    }

    private func singleRingLoss(_ c: CombinedStatsJSON) -> Double {
        switch sessionMode {
        case .pre, .next: return c.pre?.maxLoss ?? 0
        case .reg: return c.reg?.maxLoss ?? 0
        case .day: return c.pre?.maxLoss ?? c.reg?.maxLoss ?? 0
        }
    }

    // MARK: - Physics Simulation

    private func simulationStep() {
        let pad: CGFloat = 2
        var maxVel: CGFloat = 0

        for i in bubbles.indices {
            var fx: CGFloat = 0
            var fy: CGFloat = 0

            // Collision avoidance.
            for j in bubbles.indices where j != i {
                let dx = bubbles[i].position.x - bubbles[j].position.x
                let dy = bubbles[i].position.y - bubbles[j].position.y
                let dist = hypot(dx, dy)
                let minDist = bubbles[i].radius + bubbles[j].radius + pad
                if dist < minDist && dist > 0.01 {
                    let overlap = minDist - dist
                    let strength: CGFloat = 0.4
                    fx += (dx / dist) * overlap * strength
                    fy += (dy / dist) * overlap * strength
                }
            }

            // Boundary forces.
            let r = bubbles[i].radius
            let margin: CGFloat = 2
            let bx = bubbles[i].position.x
            let by = bubbles[i].position.y
            if bx - r < margin { fx += (margin - (bx - r)) * 0.5 }
            if bx + r > viewSize.width - margin { fx -= ((bx + r) - (viewSize.width - margin)) * 0.5 }
            if by - r < margin { fy += (margin - (by - r)) * 0.5 }
            if by + r > viewSize.height - margin { fy -= ((by + r) - (viewSize.height - margin)) * 0.5 }

            // Apply forces.
            bubbles[i].velocity.x += fx
            bubbles[i].velocity.y += fy

            // Damping.
            bubbles[i].velocity.x *= 0.85
            bubbles[i].velocity.y *= 0.85

            // Clamp velocity.
            let vel = hypot(bubbles[i].velocity.x, bubbles[i].velocity.y)
            if vel > 4 {
                bubbles[i].velocity.x *= 4 / vel
                bubbles[i].velocity.y *= 4 / vel
            }
            maxVel = max(maxVel, vel)

            // Update position.
            bubbles[i].position.x += bubbles[i].velocity.x
            bubbles[i].position.y += bubbles[i].velocity.y

            // Hard clamp to bounds.
            bubbles[i].position.x = max(r, min(viewSize.width - r, bubbles[i].position.x))
            bubbles[i].position.y = max(r, min(viewSize.height - r, bubbles[i].position.y))
        }

        // Stop simulation once settled.
        if maxVel < 0.05 {
            isSettled = true
        }
    }

    // MARK: - Sync Bubbles

    private func syncBubbles(in size: CGSize) {
        let items: [(CombinedStatsJSON, String, Double)] = symbolData.compactMap { combined, tier in
            let turnover: Double
            switch sessionMode {
            case .pre, .next: turnover = combined.pre?.turnover ?? 0
            case .reg: turnover = combined.reg?.turnover ?? 0
            case .day: turnover = (combined.pre?.turnover ?? 0) + (combined.reg?.turnover ?? 0)
            }
            guard turnover > 0 else { return nil }
            return (combined, tier, turnover)
        }
        guard !items.isEmpty else { bubbles = []; return }

        // Area-proportional sizing by turnover: fill ~70% of canvas.
        let totalArea = size.width * size.height * 0.7
        let weights = items.map { sqrt($0.2) }
        let totalWeight = weights.reduce(0, +)
        let minR: CGFloat = 14
        let maxR = min(size.width, size.height) / 2.5

        let radii: [CGFloat] = weights.map { w in
            let area = totalArea * CGFloat(w / totalWeight)
            return max(minR, min(maxR, sqrt(area / .pi)))
        }

        let existing = Dictionary(uniqueKeysWithValues: bubbles.map { ($0.id, $0) })

        let count = items.count
        let cols = max(1, Int(ceil(sqrt(Double(count) * Double(size.width) / Double(size.height)))))
        let rows = max(1, Int(ceil(Double(count) / Double(cols))))
        let cellW = size.width / CGFloat(cols)
        let cellH = size.height / CGFloat(rows)

        var newBubbles: [BubbleState] = []
        for (idx, item) in items.enumerated() {
            let radius = radii[idx]

            if var old = existing[item.0.symbol] {
                old.combined = item.0
                old.tier = item.1
                old.radius = radius
                newBubbles.append(old)
            } else {
                let col = idx % cols
                let row = idx / cols
                let pos = CGPoint(
                    x: max(radius, min(size.width - radius, (CGFloat(col) + 0.5) * cellW + CGFloat.random(in: -cellW * 0.15...cellW * 0.15))),
                    y: max(radius, min(size.height - radius, (CGFloat(row) + 0.5) * cellH + CGFloat.random(in: -cellH * 0.15...cellH * 0.15)))
                )
                newBubbles.append(BubbleState(
                    id: item.0.symbol,
                    combined: item.0,
                    tier: item.1,
                    radius: radius,
                    position: pos,
                    velocity: .zero
                ))
            }
        }
        bubbles = newBubbles
        isSettled = false
    }

    // MARK: - Watchlist Change

    /// Give added symbols an upward impulse, removed symbols a downward nudge.
    private func onWatchlistChanged(added: Set<String>, removed: Set<String>) {
        for idx in bubbles.indices {
            if added.contains(bubbles[idx].id) {
                bubbles[idx].velocity.y = -6
            } else if removed.contains(bubbles[idx].id) {
                bubbles[idx].velocity.y = 3
            }
        }
        isSettled = false
    }

}

// MARK: - Bubble State

private struct BubbleState: Identifiable {
    let id: String
    var combined: CombinedStatsJSON
    var tier: String
    var radius: CGFloat
    var position: CGPoint
    var velocity: CGPoint
}
