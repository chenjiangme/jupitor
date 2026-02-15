import SwiftUI

struct BubbleChartView: View {
    @Environment(DashboardViewModel.self) private var vm
    let day: DayDataJSON
    let date: String

    @State private var bubbles: [BubbleState] = []
    @State private var viewSize: CGSize = .zero
    @State private var draggedId: String?
    @State private var wasDragged = false
    @State private var showDetail = false
    @State private var detailCombined: CombinedStatsJSON?
    @State private var isSettled = false
    @State private var showWatchlistOnly = false
    @State private var isPinching = false

    private let minInnerRatio: CGFloat = 0.15

    // Dark-to-light shades for gain (green) and loss (red), one per 100% band up to 500%.
    private static let gainShades: [Color] = [
        Color(hue: 0.33, saturation: 1.00, brightness: 0.45),
        Color(hue: 0.33, saturation: 0.85, brightness: 0.60),
        Color(hue: 0.33, saturation: 0.70, brightness: 0.75),
        Color(hue: 0.33, saturation: 0.50, brightness: 0.88),
        Color(hue: 0.33, saturation: 0.30, brightness: 1.00),
    ]
    private static let lossShades: [Color] = [
        Color(hue: 0.00, saturation: 1.00, brightness: 0.50),
        Color(hue: 0.00, saturation: 0.85, brightness: 0.65),
        Color(hue: 0.00, saturation: 0.65, brightness: 0.80),
        Color(hue: 0.00, saturation: 0.45, brightness: 0.92),
        Color(hue: 0.00, saturation: 0.25, brightness: 1.00),
    ]

    private var symbolData: [(combined: CombinedStatsJSON, tier: String)] {
        let all = day.tiers
            .filter { $0.name == "MODERATE" || $0.name == "SPORADIC" }
            .flatMap { tier in tier.symbols.map { (combined: $0, tier: tier.name) } }
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
                        ForEach(bubbles) { bubble in
                            bubbleView(bubble)
                        }
                    }
                    .coordinateSpace(name: "canvas")
                    .frame(width: geo.size.width, height: geo.size.height)
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
                .gesture(
                    MagnifyGesture()
                        .onChanged { _ in
                            isPinching = true
                        }
                        .onEnded { value in
                            isPinching = false
                            if let idx = bubbles.firstIndex(where: { $0.id == draggedId }) {
                                bubbles[idx].velocity = .zero
                            }
                            draggedId = nil
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

            }
        }
        .onChange(of: day) { _, _ in
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
    }

    // MARK: - Bubble View

    @ViewBuilder
    private func bubbleView(_ bubble: BubbleState) -> some View {
        let isWatchlist = vm.watchlistSymbols.contains(bubble.id)
        let isDragged = draggedId == bubble.id
        let diameter = bubble.radius * 2
        let ringWidth = max(4, bubble.radius * 0.18)
        let outerDia = diameter - ringWidth
        let preTurnover = bubble.combined.pre?.turnover ?? 0
        let regTurnover = bubble.combined.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0
        let innerDia = outerDia * max(minInnerRatio, preRatio)

        ZStack {
            // Subtle background.
            Circle()
                .fill(Color.white.opacity(0.04))

            // Outer ring (regular session).
            sessionRing(
                gain: bubble.combined.reg?.maxGain ?? 0,
                loss: bubble.combined.reg?.maxLoss ?? 0,
                hasData: bubble.combined.reg != nil,
                diameter: outerDia,
                lineWidth: ringWidth
            )

            // Inner ring (pre-market session).
            sessionRing(
                gain: bubble.combined.pre?.maxGain ?? 0,
                loss: bubble.combined.pre?.maxLoss ?? 0,
                hasData: bubble.combined.pre != nil,
                diameter: innerDia,
                lineWidth: ringWidth
            )

            // Symbol label.
            Text(bubble.id)
                .font(.system(size: max(9, bubble.radius * 0.38), weight: .heavy))
                .foregroundStyle((isWatchlist ? Color.watchlistColor : .white).opacity(0.5))
                .minimumScaleFactor(0.5)
                .padding(3)
        }
        .frame(width: diameter, height: diameter)
        .scaleEffect(isDragged ? 1.1 : 1.0)
        .shadow(color: isDragged ? .white.opacity(0.3) : .clear, radius: 8)
        .zIndex(isDragged ? 100 : 0)
        .position(bubble.position)
        .onTapGesture(count: 2) {
            guard !wasDragged else { return }
            Task { await vm.toggleWatchlist(symbol: bubble.id) }
        }
        .onLongPressGesture {
            detailCombined = bubble.combined
            showDetail = true
        }
        .simultaneousGesture(
            DragGesture(minimumDistance: 6, coordinateSpace: .named("canvas"))
                .onChanged { value in
                    guard !isPinching else { return }
                    wasDragged = true
                    draggedId = bubble.id
                    isSettled = false
                    if let idx = bubbles.firstIndex(where: { $0.id == bubble.id }) {
                        bubbles[idx].position = value.location
                        bubbles[idx].velocity = .zero
                    }
                }
                .onEnded { value in
                    if let idx = bubbles.firstIndex(where: { $0.id == bubble.id }) {
                        bubbles[idx].velocity = .zero
                    }
                    draggedId = nil
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
                        wasDragged = false
                    }
                }
        )
    }

    // MARK: - Session Ring

    @ViewBuilder
    private func sessionRing(gain: Double, loss: Double, hasData: Bool, diameter: CGFloat, lineWidth: CGFloat) -> some View {
        if hasData {
            ZStack {
                // Background track.
                Circle()
                    .stroke(Color.white.opacity(0.1), lineWidth: lineWidth)

                // Larger % at full width, smaller % at half width (nested inside).
                if gain >= loss {
                    gradientArcs(value: gain, shades: Self.gainShades, lineWidth: lineWidth)
                    gradientArcs(value: loss, shades: Self.lossShades, lineWidth: lineWidth * 0.5)
                } else {
                    gradientArcs(value: loss, shades: Self.lossShades, lineWidth: lineWidth)
                    gradientArcs(value: gain, shades: Self.gainShades, lineWidth: lineWidth * 0.5)
                }
            }
            .frame(width: diameter, height: diameter)
        }
    }

    /// Draw overlapping arc bands: each 100% band uses a progressively lighter shade.
    /// Later (lighter) bands draw on top of earlier (darker) ones.
    @ViewBuilder
    private func gradientArcs(value: Double, shades: [Color], lineWidth: CGFloat) -> some View {
        let capped = min(value, 5.0)
        ForEach(Array(shades.indices), id: \.self) { band in
            let frac = min(max(capped - Double(band), 0), 1.0)
            if frac > 0 {
                Circle()
                    .trim(from: 0, to: frac)
                    .stroke(shades[band], style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
                    .rotationEffect(.degrees(-90))
            }
        }
    }

    // MARK: - Physics Simulation

    private func simulationStep() {
        let pad: CGFloat = 2
        var maxVel: CGFloat = 0

        for i in bubbles.indices {
            guard bubbles[i].id != draggedId else { continue }

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
                    let strength: CGFloat = bubbles[j].id == draggedId ? 0.8 : 0.4
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
            let preTurnover = combined.pre?.turnover ?? 0
            let regTurnover = combined.reg?.turnover ?? 0
            let total = preTurnover + regTurnover
            guard total > 0 else { return nil }
            return (combined, tier, total)
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
