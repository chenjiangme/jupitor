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
    @State private var simTime: Double = 0

    private let innerRatio: CGFloat = 0.6

    private var symbolData: [(combined: CombinedStatsJSON, tier: String)] {
        day.tiers
            .filter { $0.name == "MODERATE" || $0.name == "SPORADIC" }
            .flatMap { tier in tier.symbols.map { (combined: $0, tier: tier.name) } }
    }

    var body: some View {
        VStack(spacing: 0) {
            dayHeader

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
                .task {
                    while !Task.isCancelled {
                        if viewSize.width > 0 { simulationStep() }
                        try? await Task.sleep(for: .milliseconds(16))
                    }
                }

                legend
            }
        }
        .onChange(of: day) { _, _ in
            if viewSize.width > 0 { syncBubbles(in: viewSize) }
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
        let breathScale = 1.0 + sin(simTime * 1.5 + bubble.phaseOffset) * 0.015
        let isDragged = draggedId == bubble.id
        let diameter = bubble.radius * 2
        let ringWidth = max(3, bubble.radius * 0.12)
        let borderWidth: CGFloat = isWatchlist ? 2.5 : 1
        let outerDia = diameter - ringWidth - borderWidth * 2 - 1
        let innerDia = outerDia * innerRatio

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

            // Tier / watchlist border.
            Circle()
                .strokeBorder(
                    isWatchlist ? Color.watchlistColor : Color.tierColor(for: bubble.tier).opacity(0.5),
                    lineWidth: borderWidth
                )

            // Symbol label only.
            Text(bubble.id)
                .font(bubble.radius > 40 ? .caption.bold() : bubble.radius > 24 ? .caption2.bold() : .system(size: 9, weight: .bold))
                .foregroundStyle(.white)
                .minimumScaleFactor(0.5)
                .padding(3)
        }
        .frame(width: diameter, height: diameter)
        .scaleEffect(breathScale * (isDragged ? 1.1 : 1.0))
        .shadow(color: isDragged ? .white.opacity(0.3) : .clear, radius: 8)
        .zIndex(isDragged ? 100 : 0)
        .position(bubble.position)
        .onTapGesture {
            guard !wasDragged else { return }
            detailCombined = bubble.combined
            showDetail = true
        }
        .simultaneousGesture(
            DragGesture(minimumDistance: 6, coordinateSpace: .named("canvas"))
                .onChanged { value in
                    wasDragged = true
                    draggedId = bubble.id
                    if let idx = bubbles.firstIndex(where: { $0.id == bubble.id }) {
                        bubbles[idx].position = value.location
                        bubbles[idx].velocity = .zero
                    }
                }
                .onEnded { value in
                    if let idx = bubbles.firstIndex(where: { $0.id == bubble.id }) {
                        let vx = value.predictedEndLocation.x - value.location.x
                        let vy = value.predictedEndLocation.y - value.location.y
                        bubbles[idx].velocity = CGPoint(x: vx * 0.3, y: vy * 0.3)
                    }
                    draggedId = nil
                    DispatchQueue.main.asyncAfter(deadline: .now() + 0.15) {
                        wasDragged = false
                    }
                }
        )
    }

    // MARK: - Session Ring

    private func sessionRing(gain: Double, loss: Double, hasData: Bool, diameter: CGFloat, lineWidth: CGFloat) -> some View {
        ZStack {
            // Background track.
            Circle()
                .stroke(Color.white.opacity(hasData ? 0.1 : 0.04), lineWidth: lineWidth)

            if hasData {
                // Green gain arc (clockwise from top).
                if gain > 0 {
                    Circle()
                        .trim(from: 0, to: min(gain, 0.5))
                        .stroke(Color.green, style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
                        .rotationEffect(.degrees(-90))
                }

                // Red loss arc (counter-clockwise from top).
                if loss > 0 {
                    Circle()
                        .trim(from: 1 - min(loss, 0.5), to: 1)
                        .stroke(Color.red, style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
                        .rotationEffect(.degrees(-90))
                }
            }
        }
        .frame(width: diameter, height: diameter)
    }

    // MARK: - Physics Simulation

    private func simulationStep() {
        simTime += 1.0 / 60.0
        let pad: CGFloat = 2

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
                    let strength: CGFloat = bubbles[j].id == draggedId ? 0.8 : 0.3
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

            // Brownian motion (alive feel).
            fx += CGFloat.random(in: -0.12...0.12)
            fy += CGFloat.random(in: -0.12...0.12)

            // Apply forces.
            bubbles[i].velocity.x += fx
            bubbles[i].velocity.y += fy

            // Damping.
            bubbles[i].velocity.x *= 0.9
            bubbles[i].velocity.y *= 0.9

            // Clamp velocity.
            let vel = hypot(bubbles[i].velocity.x, bubbles[i].velocity.y)
            if vel > 4 {
                bubbles[i].velocity.x *= 4 / vel
                bubbles[i].velocity.y *= 4 / vel
            }

            // Update position.
            bubbles[i].position.x += bubbles[i].velocity.x
            bubbles[i].position.y += bubbles[i].velocity.y

            // Hard clamp to bounds.
            bubbles[i].position.x = max(r, min(viewSize.width - r, bubbles[i].position.x))
            bubbles[i].position.y = max(r, min(viewSize.height - r, bubbles[i].position.y))
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
                let cx = (CGFloat(col) + 0.5) * cellW + CGFloat.random(in: -cellW * 0.15...cellW * 0.15)
                let cy = (CGFloat(row) + 0.5) * cellH + CGFloat.random(in: -cellH * 0.15...cellH * 0.15)
                let pos = CGPoint(
                    x: max(radius, min(size.width - radius, cx)),
                    y: max(radius, min(size.height - radius, cy))
                )
                newBubbles.append(BubbleState(
                    id: item.0.symbol,
                    combined: item.0,
                    tier: item.1,
                    radius: radius,
                    position: pos,
                    velocity: .zero,
                    phaseOffset: Double.random(in: 0...(2 * .pi))
                ))
            }
        }
        bubbles = newBubbles
    }

    // MARK: - Day Header

    private var dayHeader: some View {
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
    }

    // MARK: - Legend

    private var legend: some View {
        HStack(spacing: 8) {
            Text("inner=PRE  outer=REG")
                .font(.system(size: 8))
                .foregroundStyle(.secondary)

            Spacer()

            HStack(spacing: 6) {
                HStack(spacing: 2) {
                    Circle().fill(.green).frame(width: 6, height: 6)
                    Text("Gain").font(.system(size: 8)).foregroundStyle(.secondary)
                }
                HStack(spacing: 2) {
                    Circle().fill(.red).frame(width: 6, height: 6)
                    Text("Loss").font(.system(size: 8)).foregroundStyle(.secondary)
                }
            }

            Spacer()

            HStack(spacing: 8) {
                tierLegendDot("MOD", color: .tierModerate)
                tierLegendDot("SPO", color: .tierSporadic)
            }
        }
        .padding(.horizontal)
        .padding(.vertical, 6)
    }

    private func tierLegendDot(_ label: String, color: Color) -> some View {
        HStack(spacing: 3) {
            Circle().fill(color).frame(width: 6, height: 6)
            Text(label).font(.system(size: 8)).foregroundStyle(.secondary)
        }
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
    let phaseOffset: Double
}
