import SwiftUI

struct BubbleChartView: View {
    @Environment(DashboardViewModel.self) private var vm
    @Environment(TradeParamsModel.self) private var tp
    let day: DayDataJSON
    let date: String
    var watchlistDate: String = ""
    var sessionMode: SessionMode = .day

    /// Date used for watchlist operations (falls back to date if not set).
    private var wlDate: String { watchlistDate.isEmpty ? date : watchlistDate }

    @State private var bubbles: [BubbleState] = []
    @State private var viewSize: CGSize = .zero
    @State private var showDetail = false
    @State private var detailCombined: CombinedStatsJSON?
    @State private var showHistory = false
    @State private var historySymbol: String = ""
    @State private var isSettled = false
    @State private var simFrame = 0
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

    private var sortedByTurnover: [CombinedStatsJSON] {
        symbolData.map(\.combined).sorted { a, b in
            totalTurnover(a) > totalTurnover(b)
        }
    }

    private func totalTurnover(_ s: CombinedStatsJSON) -> Double {
        (s.pre?.turnover ?? 0) + (s.reg?.turnover ?? 0)
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
        .contentShape(Rectangle())
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
                SymbolDetailView(symbols: sortedByTurnover, initialSymbol: combined.symbol, date: date, newsDate: sessionMode == .next ? wlDate : "", isNextMode: sessionMode == .next)
            }
        }
        .navigationDestination(isPresented: $showHistory) {
            SymbolHistoryView(symbol: historySymbol, date: date)
        }
        .onShake {
            let onScreen = Set(symbolData.map(\.combined.symbol))
            let toRemove = onScreen.intersection(vm.watchlistSymbols)
            guard !toRemove.isEmpty else { return }
            Task { await vm.removeWatchlistSymbols(toRemove, date: wlDate) }
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

            // Close gain markers (dark green line across ring).
            let darkGreen = Color(hue: 0.33, saturation: 1.0, brightness: 0.7)
            if dualRing {
                if let cg = bubble.combined.reg?.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       isSquare: isWatchlist, cornerRadius: outerDia * 0.15, color: darkGreen)
                        .frame(width: diameter, height: diameter)
                }
                if let cg = bubble.combined.pre?.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: innerDia / 2, lineWidth: ringWidth,
                                       isSquare: isWatchlist, cornerRadius: innerDia * 0.15, color: darkGreen)
                        .frame(width: diameter, height: diameter)
                }
            } else {
                if let stats = sessionStats(bubble.combined), let cg = stats.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       isSquare: isWatchlist, cornerRadius: outerDia * 0.15, color: darkGreen)
                        .frame(width: diameter, height: diameter)
                }
            }

            // Target gain markers (yellow line across ring).
            if dualRing {
                if let t = tp.targets[date]?["\(bubble.id):REG"], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       isSquare: isWatchlist, cornerRadius: outerDia * 0.15)
                        .frame(width: diameter, height: diameter)
                }
                if let t = tp.targets[date]?["\(bubble.id):PRE"], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: innerDia / 2, lineWidth: ringWidth,
                                       isSquare: isWatchlist, cornerRadius: innerDia * 0.15)
                        .frame(width: diameter, height: diameter)
                }
            } else {
                let targetKey: String = {
                    switch sessionMode {
                    case .pre, .next: return "\(bubble.id):PRE"
                    case .reg: return "\(bubble.id):REG"
                    case .day: return "\(bubble.id):PRE"
                    }
                }()
                if let t = tp.targets[date]?[targetKey], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       isSquare: isWatchlist, cornerRadius: outerDia * 0.15)
                        .frame(width: diameter, height: diameter)
                }
            }

            // Close-position needle (center → outer ring edge).
            if let stats = sessionStats(bubble.combined), stats.high > stats.low {
                CloseDialView(
                    fraction: (stats.close - stats.low) / (stats.high - stats.low),
                    needleRadius: outerDia / 2,
                    lineWidth: max(1.5, ringWidth * 0.4)
                )
                .frame(width: diameter, height: diameter)
            }

            // Symbol label + counts + price.
            VStack(spacing: 0) {
                // StockTwits + news counts.
                let counts = newsCounts(for: bubble.combined)
                if counts.st > 0 || counts.news > 0 {
                    HStack(spacing: 2) {
                        if counts.st > 0 {
                            Text("\(counts.st)")
                                .foregroundStyle(counts.stColor.opacity(0.5))
                        }
                        if counts.news > 0 {
                            Text("\(counts.news)")
                                .foregroundStyle(Color.blue.opacity(0.5))
                        }
                    }
                    .font(.system(size: max(5, bubble.radius * 0.18)))
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                }

                let closePriceBelowDollar = (sessionStats(bubble.combined)?.close ?? 1) < 1
                Text(bubble.id)
                    .font(.system(size: max(7, bubble.radius * 0.3), weight: .heavy))
                    .italic(closePriceBelowDollar)
                    .foregroundStyle((isWatchlist ? Color.watchlistColor : Color.tierColor(for: bubble.combined.tier)).opacity(0.5))
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                if isWatchlist, let stats = sessionStats(bubble.combined) {
                    Text("\(Fmt.compactPrice(stats.open)) \(Fmt.compactPrice(stats.low)) \(Fmt.compactPrice(stats.high)) \(Fmt.compactPrice(stats.close))")
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
            Task { await vm.toggleWatchlist(symbol: bubble.id, date: wlDate) }
        }
        .onTapGesture(count: 1) {
            detailCombined = bubble.combined
            showDetail = true
        }
        .onLongPressGesture {
            historySymbol = bubble.id
            showHistory = true
        }
    }

    // MARK: - Helpers

    private func sessionStats(_ c: CombinedStatsJSON) -> SymbolStatsJSON? {
        switch sessionMode {
        case .pre, .next: return c.pre
        case .reg: return c.reg
        case .day:
            guard let pre = c.pre else { return c.reg }
            guard let reg = c.reg else { return pre }
            return SymbolStatsJSON(
                symbol: pre.symbol,
                trades: pre.trades + reg.trades,
                high: max(pre.high, reg.high),
                low: min(pre.low, reg.low),
                open: pre.open,
                close: reg.close,
                size: pre.size + reg.size,
                turnover: pre.turnover + reg.turnover,
                maxGain: max(pre.maxGain, reg.maxGain),
                maxLoss: max(pre.maxLoss, reg.maxLoss),
                closeGain: max(pre.closeGain ?? 0, reg.closeGain ?? 0)
            )
        }
    }


    /// StockTwits count + color and news count for the current session.
    private func newsCounts(for c: CombinedStatsJSON) -> (st: Int, stColor: Color, news: Int) {
        let st: Int
        let color: Color
        switch sessionMode {
        case .pre:
            st = c.stPre ?? 0
            color = .indigo
        case .reg:
            st = c.stReg ?? 0
            color = .green
        case .day:
            st = (c.stPre ?? 0) + (c.stReg ?? 0)
            color = .white
        case .next:
            st = c.stPost ?? 0
            color = .orange
        }
        return (st, color, c.news ?? 0)
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
        simFrame += 1
        let pad: CGFloat = 2
        var maxVel: CGFloat = 0
        // Ramp up damping over time so bubbles converge quickly.
        let damping: CGFloat = simFrame < 60 ? 0.8 : 0.6

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

            // Vertical bias: high close fraction → top, low → bottom.
            let targetY = viewSize.height * (1 - bubbles[i].closeFraction)
            fy += (targetY - bubbles[i].position.y) * 0.03

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
            bubbles[i].velocity.x *= damping
            bubbles[i].velocity.y *= damping

            // Kill tiny velocities to prevent drift.
            let vel = hypot(bubbles[i].velocity.x, bubbles[i].velocity.y)
            if vel < 0.1 {
                bubbles[i].velocity = .zero
            } else if vel > 4 {
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

        // Stop simulation once settled or after max frames.
        if maxVel < 0.15 || simFrame > 300 {
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

            let cf: CGFloat = {
                guard let s = sessionStats(item.0), s.high > s.low else { return 0.5 }
                return CGFloat((s.close - s.low) / (s.high - s.low))
            }()

            if var old = existing[item.0.symbol] {
                old.combined = item.0
                old.tier = item.1
                old.radius = radius
                old.closeFraction = cf
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
                    velocity: .zero,
                    closeFraction: cf
                ))
            }
        }
        bubbles = newBubbles
        simFrame = 0
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

// MARK: - Close Dial

/// Needle from center to outer ring edge showing close position in [low, high].
/// Full 360°: 12 o'clock = both 0% (red) and 100% (green). Sweeps clockwise.
struct CloseDialView: View {
    let fraction: Double      // 0 = at low, 1 = at high
    let needleRadius: CGFloat // distance from center to ring edge
    var lineWidth: CGFloat = 2

    var body: some View {
        let clamped = min(max(fraction, 0), 1)
        // 12 o'clock = 270° in screen coords. Sweep 360° clockwise.
        let angle = Angle(degrees: 270 + 360 * clamped)
        // Red at 0%, green at 100%, interpolated by hue.
        let color = Color(hue: 0.33 * clamped, saturation: 0.9, brightness: 0.9)

        Canvas { context, size in
            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            let rad = angle.radians
            let tip = CGPoint(x: center.x + cos(rad) * needleRadius,
                              y: center.y + sin(rad) * needleRadius)

            var needle = Path()
            needle.move(to: center)
            needle.addLine(to: tip)
            context.stroke(needle, with: .color(color.opacity(0.5)),
                          style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
        }
    }
}

// MARK: - Target Marker (small yellow arrow on ring)

private struct TargetMarkerCanvas: View {
    let gain: Double        // 0-5 (1.0 = 100%)
    let ringRadius: CGFloat // center of the ring stroke
    let lineWidth: CGFloat  // ring stroke width
    var isSquare: Bool = false
    var cornerRadius: CGFloat = 0
    var color: Color = .yellow.opacity(0.9)

    var body: some View {
        Canvas { context, size in
            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            let frac = gain.truncatingRemainder(dividingBy: 1.0)
            let adjustedFrac = gain >= 1.0 && frac == 0 ? 1.0 : frac
            let rad = -Double.pi / 2 + 2 * Double.pi * adjustedFrac

            let p1: CGPoint
            let p2: CGPoint

            if isSquare && cornerRadius > 0 {
                let innerHalf = ringRadius - lineWidth / 2
                let outerHalf = ringRadius + lineWidth / 2
                let innerCR = max(0, cornerRadius - lineWidth / 2)
                let outerCR = cornerRadius + lineWidth / 2
                p1 = Self.pointOnRoundedRect(angle: rad, halfSize: innerHalf, cr: innerCR, center: center)
                p2 = Self.pointOnRoundedRect(angle: rad, halfSize: outerHalf, cr: outerCR, center: center)
            } else {
                let innerR = ringRadius - lineWidth / 2
                let outerR = ringRadius + lineWidth / 2
                p1 = CGPoint(x: center.x + cos(rad) * innerR, y: center.y + sin(rad) * innerR)
                p2 = CGPoint(x: center.x + cos(rad) * outerR, y: center.y + sin(rad) * outerR)
            }

            var line = Path()
            line.move(to: p1)
            line.addLine(to: p2)
            context.stroke(line, with: .color(color),
                          style: StrokeStyle(lineWidth: 2, lineCap: .round))
        }
    }

    /// Ray-rounded-rectangle intersection: returns the point on the perimeter
    /// of a rounded rectangle where a ray from center at `angle` exits.
    private static func pointOnRoundedRect(angle: Double, halfSize: CGFloat, cr: CGFloat, center: CGPoint) -> CGPoint {
        let s = Double(halfSize)
        let r = min(Double(cr), s)
        let straight = s - r // where corner arcs begin

        let dx = cos(angle)
        let dy = sin(angle)

        // Ray-box intersection.
        let tX = dx != 0 ? s / abs(dx) : Double.infinity
        let tY = dy != 0 ? s / abs(dy) : Double.infinity
        let t = min(tX, tY)
        let px = dx * t
        let py = dy * t

        // Check if ray exits through a corner region.
        if abs(px) > straight + 0.001 && abs(py) > straight + 0.001 && r > 0 {
            let ccx = (px > 0 ? 1.0 : -1.0) * straight
            let ccy = (py > 0 ? 1.0 : -1.0) * straight

            // Solve |(dx*t - ccx, dy*t - ccy)| = r
            let b = -2.0 * (dx * ccx + dy * ccy)
            let c = ccx * ccx + ccy * ccy - r * r
            let disc = b * b - 4.0 * c
            if disc >= 0 {
                let t1 = (-b + sqrt(disc)) / 2.0
                let t2 = (-b - sqrt(disc)) / 2.0
                let tc = max(t1, t2)
                return CGPoint(x: center.x + CGFloat(dx * tc), y: center.y + CGFloat(dy * tc))
            }
        }

        return CGPoint(x: center.x + CGFloat(px), y: center.y + CGFloat(py))
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
    var closeFraction: CGFloat  // 0 = at low, 1 = at high
}
