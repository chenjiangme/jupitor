import SwiftUI

struct SymbolHistoryView: View {
    @Environment(DashboardViewModel.self) private var vm
    let symbol: String

    @State private var dates: [SymbolDateStats] = []
    @State private var rings: [RingState] = []
    @State private var contentHeight: CGFloat = 0
    @State private var viewWidth: CGFloat = 0
    @State private var isLoading = true
    @State private var hasMore = false
    @State private var isLoadingMore = false
    @State private var isSettled = false

    private let maxRadius: CGFloat = 30
    private let minRadius: CGFloat = 10
    private let minInnerRatio: CGFloat = 0.15

    var body: some View {
        Group {
            if isLoading {
                VStack { Spacer(); ProgressView(); Spacer() }
            } else if dates.isEmpty {
                VStack {
                    Spacer()
                    Text("No trading history")
                        .font(.caption).foregroundStyle(.secondary)
                    Spacer()
                }
            } else {
                GeometryReader { geo in
                    ScrollView {
                        ZStack {
                            Color.clear

                            if hasMore {
                                VStack {
                                    Color.clear.frame(height: 1)
                                        .onAppear { loadMore() }
                                    Spacer()
                                }
                            }

                            ForEach(rings) { ring in
                                ringView(ring)
                                    .position(ring.position)
                            }
                        }
                        .frame(width: geo.size.width, height: contentHeight)
                    }
                    .defaultScrollAnchor(.bottom)
                    .onAppear {
                        viewWidth = geo.size.width
                        if rings.isEmpty && !dates.isEmpty { syncRings() }
                    }
                    .onChange(of: geo.size.width) { _, w in
                        viewWidth = w
                        syncRings()
                    }
                }
                .task(id: isSettled) {
                    guard !isSettled else { return }
                    while !Task.isCancelled {
                        simulationStep()
                        try? await Task.sleep(for: .milliseconds(16))
                    }
                }
            }
        }
        .navigationTitle(symbol)
        .navigationBarTitleDisplayMode(.inline)
        .task { await loadInitial() }
    }

    // MARK: - Ring View

    @ViewBuilder
    private func ringView(_ ring: RingState) -> some View {
        let entry = ring.date
        let preTurnover = entry.pre?.turnover ?? 0
        let regTurnover = entry.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0
        let diameter = ring.radius * 2
        let lineWidth = max(3, ring.radius * 0.18)
        let outerDia = diameter - lineWidth
        let innerDia = outerDia * max(minInnerRatio, preRatio)

        ZStack {
            Circle().fill(Color.white.opacity(0.04))

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
        .frame(width: diameter, height: diameter)
    }

    // MARK: - Physics Simulation

    private func simulationStep() {
        let pad: CGFloat = 2
        var maxVel: CGFloat = 0

        for i in rings.indices {
            var fx: CGFloat = 0
            var fy: CGFloat = 0

            // Gentle gravity to pack downward.
            fy += 0.3

            // Collision avoidance.
            for j in rings.indices where j != i {
                let dx = rings[i].position.x - rings[j].position.x
                let dy = rings[i].position.y - rings[j].position.y
                let dist = hypot(dx, dy)
                let minDist = rings[i].radius + rings[j].radius + pad
                if dist < minDist && dist > 0.01 {
                    let overlap = minDist - dist
                    fx += (dx / dist) * overlap * 0.4
                    fy += (dy / dist) * overlap * 0.4
                }
            }

            // Left/right walls.
            let r = rings[i].radius
            let margin: CGFloat = 2
            if rings[i].position.x - r < margin {
                fx += (margin - (rings[i].position.x - r)) * 0.5
            }
            if rings[i].position.x + r > viewWidth - margin {
                fx -= ((rings[i].position.x + r) - (viewWidth - margin)) * 0.5
            }
            // Floor.
            if rings[i].position.y + r > contentHeight - margin {
                fy -= ((rings[i].position.y + r) - (contentHeight - margin)) * 0.5
            }
            // Ceiling (soft).
            if rings[i].position.y - r < margin {
                fy += (margin - (rings[i].position.y - r)) * 0.3
            }

            rings[i].velocity.x += fx
            rings[i].velocity.y += fy
            rings[i].velocity.x *= 0.85
            rings[i].velocity.y *= 0.85

            let vel = hypot(rings[i].velocity.x, rings[i].velocity.y)
            if vel > 4 {
                rings[i].velocity.x *= 4 / vel
                rings[i].velocity.y *= 4 / vel
            }
            maxVel = max(maxVel, vel)

            rings[i].position.x += rings[i].velocity.x
            rings[i].position.y += rings[i].velocity.y

            // Clamp.
            rings[i].position.x = max(r, min(viewWidth - r, rings[i].position.x))
            rings[i].position.y = max(r, rings[i].position.y)
            if rings[i].position.y + r > contentHeight {
                rings[i].position.y = contentHeight - r
            }
        }

        if maxVel < 0.05 {
            // Trim content height to actual extent.
            if let maxY = rings.map({ $0.position.y + $0.radius }).max() {
                contentHeight = maxY + 4
            }
            isSettled = true
        }
    }

    // MARK: - Sync Rings

    private func syncRings() {
        guard viewWidth > 0, !dates.isEmpty else { return }

        let maxTO = dates.map { ($0.pre?.turnover ?? 0) + ($0.reg?.turnover ?? 0) }.max() ?? 1

        // Compute radii and total area.
        var totalArea: CGFloat = 0
        let radii: [CGFloat] = dates.map { entry in
            let total = (entry.pre?.turnover ?? 0) + (entry.reg?.turnover ?? 0)
            let ratio = maxTO > 0 ? sqrt(CGFloat(total / maxTO)) : 0
            let r = minRadius + (maxRadius - minRadius) * ratio
            totalArea += .pi * r * r
            return r
        }

        // Estimate packed height (circles fill ~60% of area).
        let estimatedHeight = max(300, totalArea / (viewWidth * 0.55))
        contentHeight = estimatedHeight

        let existing = Dictionary(uniqueKeysWithValues: rings.map { ($0.id, $0) })

        // Grid init for new rings.
        let avgDia = (maxRadius + minRadius)
        let cols = max(1, Int(viewWidth / avgDia))
        let count = dates.count
        let rows = max(1, (count + cols - 1) / cols)
        let cellW = viewWidth / CGFloat(cols)
        let cellH = estimatedHeight / CGFloat(rows)

        var newRings: [RingState] = []
        for (idx, entry) in dates.enumerated() {
            let r = radii[idx]

            if var old = existing[entry.date] {
                old.date = entry
                old.radius = r
                newRings.append(old)
            } else {
                let col = idx % cols
                let row = idx / cols
                let x = max(r, min(viewWidth - r, (CGFloat(col) + 0.5) * cellW))
                let y = max(r, min(estimatedHeight - r, (CGFloat(row) + 0.5) * cellH))
                newRings.append(RingState(
                    id: entry.date,
                    date: entry,
                    radius: r,
                    position: CGPoint(x: x, y: y),
                    velocity: .zero
                ))
            }
        }
        rings = newRings
        isSettled = false
    }

    // MARK: - Data Loading

    private func loadInitial() async {
        guard let resp = await vm.fetchSymbolHistory(symbol: symbol) else {
            isLoading = false
            return
        }
        dates = resp.dates
        hasMore = resp.hasMore
        isLoading = false
    }

    private func loadMore() {
        guard hasMore, !isLoadingMore, let oldest = dates.first else { return }
        isLoadingMore = true
        Task {
            if let resp = await vm.fetchSymbolHistory(symbol: symbol, before: oldest.date) {
                if !resp.dates.isEmpty {
                    dates = resp.dates + dates
                    syncRings()
                }
                hasMore = resp.hasMore
            }
            isLoadingMore = false
        }
    }
}

// MARK: - Ring State

private struct RingState: Identifiable {
    let id: String
    var date: SymbolDateStats
    var radius: CGFloat
    var position: CGPoint
    var velocity: CGPoint
}
