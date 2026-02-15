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
        .onChange(of: vm.sessionView) { _, _ in
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

        ZStack {
            Circle()
                .fill(bubble.color.opacity(0.85))

            Circle()
                .strokeBorder(
                    isWatchlist ? Color.watchlistColor : Color.tierColor(for: bubble.tier).opacity(0.5),
                    lineWidth: isWatchlist ? 2.5 : 1
                )

            VStack(spacing: 0) {
                Text(bubble.id)
                    .font(bubble.radius > 28 ? .caption2.bold() : .system(size: 9, weight: .bold))
                    .foregroundStyle(.white)
                    .minimumScaleFactor(0.6)

                if bubble.radius > 24 {
                    let s = vm.sessionView == .pre ? bubble.combined.pre : bubble.combined.reg
                    if let s, s.maxGain > 0 {
                        Text(Fmt.gain(s.maxGain))
                            .font(.system(size: bubble.radius > 32 ? 10 : 8, weight: .semibold))
                            .foregroundStyle(.white.opacity(0.85))
                    }
                }
            }
            .padding(2)
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
        let session = vm.sessionView
        let items: [(CombinedStatsJSON, String, Int, Double)] = symbolData.compactMap { combined, tier in
            let s = session == .pre ? combined.pre : combined.reg
            guard let s, s.trades > 0 else { return nil }
            return (combined, tier, s.trades, s.turnover)
        }
        guard !items.isEmpty else { bubbles = []; return }

        let maxTrades = items.map(\.2).max()!
        let count = items.count
        let maxR: CGFloat = count > 25 ? 26 : count > 15 ? 34 : 44
        let minR: CGFloat = 16

        let existing = Dictionary(uniqueKeysWithValues: bubbles.map { ($0.id, $0) })

        // Grid layout for new bubble placement (fills full space).
        let cols = max(1, Int(ceil(sqrt(Double(count) * Double(size.width) / Double(size.height)))))
        let rows = max(1, Int(ceil(Double(count) / Double(cols))))
        let cellW = size.width / CGFloat(cols)
        let cellH = size.height / CGFloat(rows)

        var newBubbles: [BubbleState] = []
        for (idx, item) in items.enumerated() {
            let norm = sqrt(Double(item.2)) / sqrt(Double(maxTrades))
            let radius = minR + CGFloat(norm) * (maxR - minR)
            let color = turnoverColor(item.3)

            if var old = existing[item.0.symbol] {
                // Preserve position and velocity, update data.
                old.combined = item.0
                old.tier = item.1
                old.radius = radius
                old.color = color
                newBubbles.append(old)
            } else {
                // New bubble — place on jittered grid.
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
                    color: color,
                    position: pos,
                    velocity: .zero,
                    phaseOffset: Double.random(in: 0...(2 * .pi))
                ))
            }
        }
        bubbles = newBubbles
    }

    // MARK: - Colors

    private func turnoverColor(_ turnover: Double) -> Color {
        let logVal = log10(max(turnover, 1e4))
        let t = max(0, min(1, (logVal - 4) / 4))
        let hue = 0.6 * (1 - t)
        return Color(hue: hue, saturation: 0.75, brightness: 0.85)
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

    private var legendGradient: some View {
        LinearGradient(
            colors: [turnoverColor(1e4), turnoverColor(1e5), turnoverColor(1e6), turnoverColor(1e7), turnoverColor(1e8)],
            startPoint: .leading,
            endPoint: .trailing
        )
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
    var color: Color
    var position: CGPoint
    var velocity: CGPoint
    let phaseOffset: Double
}
