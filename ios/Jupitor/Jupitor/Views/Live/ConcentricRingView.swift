import SwiftUI

struct ConcentricRingView: View {
    @Environment(DashboardViewModel.self) private var vm
    @Environment(TradeParamsModel.self) private var tp
    let day: DayDataJSON
    let date: String
    var watchlistDate: String = ""
    var sessionMode: SessionMode = .day

    private var wlDate: String { watchlistDate.isEmpty ? date : watchlistDate }

    @State private var showDetail = false
    @State private var detailCombined: CombinedStatsJSON?
    @State private var showHistory = false
    @State private var historySymbol: String = ""
    @State private var showWatchlistOnly = false
    @AppStorage("hidePennyStocks") private var hidePennyStocks = false
    @AppStorage("gainOverLossOnly") private var gainOverLossOnly = false

    private let minInnerRatio: CGFloat = 0.15
    private let minRingRadius: CGFloat = 15

    // MARK: - Data Helpers (same as BubbleChartView)

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
            .filter { item in
                if vm.watchlistSymbols.contains(item.combined.symbol) { return true }
                if hidePennyStocks {
                    let close = sessionClose(item.combined)
                    if close > 0 && close < 1 { return false }
                }
                if gainOverLossOnly {
                    let gain = sessionGain(item.combined)
                    let loss = sessionLoss(item.combined)
                    if gain <= loss { return false }
                }
                return true
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
                gainFirst: pre.gainFirst ?? reg.gainFirst ?? true,
                closeGain: max(pre.closeGain ?? 0, reg.closeGain ?? 0),
                maxDrawdown: max(pre.maxDrawdown ?? 0, reg.maxDrawdown ?? 0),
                tradeProfile: nil
            )
        }
    }

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

    private func sessionClose(_ c: CombinedStatsJSON) -> Double {
        switch sessionMode {
        case .pre, .next: return c.pre?.close ?? 0
        case .reg: return c.reg?.close ?? 0
        case .day: return c.reg?.close ?? c.pre?.close ?? 0
        }
    }

    private func sessionGain(_ c: CombinedStatsJSON) -> Double {
        switch sessionMode {
        case .pre, .next: return c.pre?.maxGain ?? 0
        case .reg: return c.reg?.maxGain ?? 0
        case .day: return max(c.pre?.maxGain ?? 0, c.reg?.maxGain ?? 0)
        }
    }

    private func sessionLoss(_ c: CombinedStatsJSON) -> Double {
        switch sessionMode {
        case .pre, .next: return c.pre?.maxLoss ?? 0
        case .reg: return c.reg?.maxLoss ?? 0
        case .day: return max(c.pre?.maxLoss ?? 0, c.reg?.maxLoss ?? 0)
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

    private func singleRingGainFirst(_ c: CombinedStatsJSON) -> Bool {
        switch sessionMode {
        case .pre, .next: return c.pre?.gainFirst ?? true
        case .reg: return c.reg?.gainFirst ?? true
        case .day: return c.pre?.gainFirst ?? c.reg?.gainFirst ?? true
        }
    }

    // MARK: - Body

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
                    let nodes = buildRingTree(in: geo.size)
                    ZStack {
                        Color.clear.contentShape(Rectangle())
                        ForEach(nodes, id: \.symbol) { node in
                            ringNodeView(node, viewSize: geo.size)
                        }
                    }
                    .frame(width: geo.size.width, height: geo.size.height)
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
                }
        )
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

    // MARK: - Ring Tree Building

    private struct RingNode {
        let symbol: String
        let combined: CombinedStatsJSON
        let tier: String
        let center: CGPoint
        let radius: CGFloat
        let lineWidth: CGFloat
        var children: [RingNode]
    }

    /// Build a flat list of ring nodes using hierarchical circle packing.
    private func buildRingTree(in size: CGSize) -> [RingNode] {
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
        guard !items.isEmpty else { return [] }

        // Sort by turnover descending.
        let sorted = items.sorted { $0.2 > $1.2 }

        let viewCenter = CGPoint(x: size.width / 2, y: size.height / 2)
        let maxRadius = min(size.width, size.height) / 2 - 8

        // Root ring = largest symbol.
        let rootItem = sorted[0]
        let rootLineWidth = max(4, maxRadius * 0.12)
        var rootNode = RingNode(
            symbol: rootItem.0.symbol,
            combined: rootItem.0,
            tier: rootItem.1,
            center: viewCenter,
            radius: maxRadius,
            lineWidth: rootLineWidth,
            children: []
        )

        // Pack remaining symbols inside the root.
        let remaining = Array(sorted.dropFirst())
        if !remaining.isEmpty {
            let profileOverflow = rootLineWidth * 1.5
            let innerSpace = maxRadius - rootLineWidth / 2 - profileOverflow - 2
            rootNode.children = packChildren(
                remaining,
                containerRadius: innerSpace,
                containerCenter: viewCenter
            )
        }

        // Flatten tree to a list (parent first, children after for z-ordering).
        return flattenNodes(rootNode)
    }

    /// Recursively pack symbols into a circular container.
    private func packChildren(
        _ items: [(CombinedStatsJSON, String, Double)],
        containerRadius: CGFloat,
        containerCenter: CGPoint
    ) -> [RingNode] {
        guard !items.isEmpty, containerRadius >= minRingRadius else { return [] }

        // Compute child radii proportional to sqrt(turnover).
        let weights = items.map { sqrt(CGFloat($0.2)) }
        let totalWeight = weights.reduce(0, +)
        let containerArea = CGFloat.pi * containerRadius * containerRadius
        let targetArea = containerArea * 0.65

        var radii: [CGFloat] = weights.map { w in
            let area = targetArea * CGFloat(w / totalWeight)
            let r = sqrt(area / .pi)
            return min(max(r, minRingRadius), containerRadius * 0.8)
        }

        // Drop children too small.
        var validItems: [(CombinedStatsJSON, String, Double, CGFloat)] = []
        for (i, item) in items.enumerated() {
            if radii[i] >= minRingRadius {
                validItems.append((item.0, item.1, item.2, radii[i]))
            }
        }
        guard !validItems.isEmpty else { return [] }

        // Scale down if total area exceeds container.
        let totalChildArea = validItems.reduce(CGFloat(0)) { $0 + .pi * $1.3 * $1.3 }
        if totalChildArea > containerArea * 0.85 {
            let scale = sqrt(containerArea * 0.85 / totalChildArea)
            validItems = validItems.map { ($0.0, $0.1, $0.2, max(minRingRadius, $0.3 * scale)) }
        }

        // Place children geometrically.
        let positions = arrangeCircles(validItems.map { $0.3 }, in: containerRadius, center: containerCenter)

        var nodes: [RingNode] = []

        for (i, item) in validItems.enumerated() {
            guard i < positions.count else { break }
            let r = item.3
            let lw = max(4, r * 0.12)
            let node = RingNode(
                symbol: item.0.symbol,
                combined: item.0,
                tier: item.1,
                center: positions[i],
                radius: r,
                lineWidth: lw,
                children: []
            )
            nodes.append(node)
        }

        return nodes
    }

    /// Arrange circles inside a container circle. Returns centers.
    private func arrangeCircles(_ radii: [CGFloat], in containerR: CGFloat, center: CGPoint) -> [CGPoint] {
        guard !radii.isEmpty else { return [] }
        if radii.count == 1 {
            return [center]
        }
        if radii.count == 2 {
            // Side by side horizontally.
            let totalWidth = radii[0] + radii[1] + 4
            let scale = totalWidth > containerR * 2 ? containerR * 2 / totalWidth : 1.0
            let r0 = radii[0] * scale
            let r1 = radii[1] * scale
            let gap: CGFloat = 2
            return [
                CGPoint(x: center.x - (r1 + gap / 2), y: center.y),
                CGPoint(x: center.x + (r0 + gap / 2), y: center.y)
            ]
        }

        // 3+: largest at center, rest in a ring around it.
        var positions = [CGPoint](repeating: center, count: radii.count)
        positions[0] = center // largest at center

        let orbitRadius = max(radii[0] + radii[1] + 2, containerR * 0.45)
        let remaining = radii.count - 1
        for i in 1..<radii.count {
            let angle = 2 * CGFloat.pi * CGFloat(i - 1) / CGFloat(remaining) - CGFloat.pi / 2
            var cx = center.x + cos(angle) * orbitRadius
            var cy = center.y + sin(angle) * orbitRadius

            // Clamp so child stays within container.
            let dx = cx - center.x
            let dy = cy - center.y
            let dist = hypot(dx, dy)
            let maxDist = containerR - radii[i] - 2
            if dist > maxDist && dist > 0 {
                cx = center.x + dx * maxDist / dist
                cy = center.y + dy * maxDist / dist
            }
            positions[i] = CGPoint(x: cx, y: cy)
        }

        return positions
    }

    /// Flatten a ring node tree into a list (parent before children for z-order).
    private func flattenNodes(_ node: RingNode) -> [RingNode] {
        var result = [node]
        for child in node.children {
            result.append(contentsOf: flattenNodes(child))
        }
        return result
    }

    // MARK: - Ring Node View

    @ViewBuilder
    private func ringNodeView(_ node: RingNode, viewSize: CGSize) -> some View {
        let isWatchlist = vm.watchlistSymbols.contains(node.symbol)
        let diameter = node.radius * 2
        let ringWidth = node.lineWidth
        let outerDia = diameter - ringWidth
        let preTurnover = node.combined.pre?.turnover ?? 0
        let regTurnover = node.combined.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0
        let innerDia = min(outerDia - 3 * ringWidth, outerDia * max(minInnerRatio, preRatio))
        let blackFillDia = innerDia + ringWidth

        ZStack {
            let hasPre = node.combined.pre != nil
            let hasReg = node.combined.reg != nil
            let dualRing = sessionMode == .day && hasPre && hasReg

            if dualRing {
                SessionRingView(
                    gain: node.combined.reg?.maxGain ?? 0,
                    loss: node.combined.reg?.maxLoss ?? 0,
                    hasData: true,
                    diameter: outerDia,
                    lineWidth: ringWidth,
                    gainFirst: node.combined.reg?.gainFirst ?? true
                )
                Circle()
                    .fill(Color.black)
                    .frame(width: blackFillDia, height: blackFillDia)
                SessionRingView(
                    gain: node.combined.pre?.maxGain ?? 0,
                    loss: node.combined.pre?.maxLoss ?? 0,
                    hasData: true,
                    diameter: innerDia,
                    lineWidth: ringWidth,
                    gainFirst: node.combined.pre?.gainFirst ?? true
                )
            } else {
                SessionRingView(
                    gain: singleRingGain(node.combined),
                    loss: singleRingLoss(node.combined),
                    hasData: hasPre || hasReg,
                    diameter: outerDia,
                    lineWidth: ringWidth,
                    gainFirst: singleRingGainFirst(node.combined)
                )
            }

            // Volume profile.
            let profileOverflow = ringWidth * 1.5
            let profileSize = diameter + profileOverflow * 2
            if dualRing {
                if let profile = node.combined.reg?.tradeProfile, !profile.isEmpty {
                    VolumeProfileCanvas(
                        profile: profile,
                        gain: node.combined.reg?.maxGain ?? 0,
                        loss: node.combined.reg?.maxLoss ?? 0,
                        gainFirst: node.combined.reg?.gainFirst ?? true,
                        ringRadius: outerDia / 2,
                        lineWidth: ringWidth
                    )
                    .frame(width: profileSize, height: profileSize)
                }
            } else {
                if let stats = sessionStats(node.combined), let profile = stats.tradeProfile, !profile.isEmpty {
                    VolumeProfileCanvas(
                        profile: profile,
                        gain: stats.maxGain,
                        loss: stats.maxLoss,
                        gainFirst: stats.gainFirst ?? true,
                        ringRadius: outerDia / 2,
                        lineWidth: ringWidth
                    )
                    .frame(width: profileSize, height: profileSize)
                }
            }

            // Close gain markers.
            if dualRing {
                if let reg = node.combined.reg, let cg = reg.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       color: reg.dialColor)
                        .frame(width: diameter, height: diameter)
                }
                if let pre = node.combined.pre, let cg = pre.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: innerDia / 2, lineWidth: ringWidth,
                                       color: pre.dialColor)
                        .frame(width: diameter, height: diameter)
                }
            } else {
                if let stats = sessionStats(node.combined), let cg = stats.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       color: stats.dialColor)
                        .frame(width: diameter, height: diameter)
                }
            }

            // Target gain markers.
            if dualRing {
                if let t = tp.targets[date]?["\(node.symbol):REG"], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: outerDia / 2, lineWidth: ringWidth)
                        .frame(width: diameter, height: diameter)
                }
                if let t = tp.targets[date]?["\(node.symbol):PRE"], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: innerDia / 2, lineWidth: ringWidth)
                        .frame(width: diameter, height: diameter)
                }
            } else {
                let targetKey: String = {
                    switch sessionMode {
                    case .pre, .next: return "\(node.symbol):PRE"
                    case .reg: return "\(node.symbol):REG"
                    case .day: return "\(node.symbol):PRE"
                    }
                }()
                if let t = tp.targets[date]?[targetKey], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: outerDia / 2, lineWidth: ringWidth)
                        .frame(width: diameter, height: diameter)
                }
            }

            // Close-position needle (hidden but logic preserved).
            if let stats = sessionStats(node.combined), stats.high > stats.low {
                CloseDialView(
                    fraction: (stats.close - stats.low) / (stats.high - stats.low),
                    needleRadius: outerDia / 2,
                    lineWidth: max(1.5, ringWidth * 0.4)
                )
                .frame(width: diameter, height: diameter)
                .hidden()
            }

            // Symbol label at 12 o'clock.
            VStack(spacing: 0) {
                let counts = newsCounts(for: node.combined)
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
                    .font(.system(size: max(5, ringWidth * 0.8)))
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                }

                let closePriceBelowDollar = (sessionStats(node.combined)?.close ?? 1) < 1
                Text(node.symbol)
                    .font(.system(size: max(7, ringWidth * 1.4), weight: .heavy))
                    .italic(closePriceBelowDollar)
                    .foregroundStyle((isWatchlist ? Color.watchlistColor : Color.tierColor(for: node.combined.tier)).opacity(0.5))
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                if isWatchlist, let stats = sessionStats(node.combined) {
                    Text("\(Fmt.compactPrice(stats.open)) \(Fmt.compactPrice(stats.low)) \(Fmt.compactPrice(stats.high)) \(Fmt.compactPrice(stats.close))")
                        .font(.system(size: max(5, ringWidth * 0.7)))
                        .foregroundStyle(Color.watchlistPriceColor.opacity(0.4))
                        .lineLimit(1)
                        .minimumScaleFactor(0.5)
                }
            }
            .padding(.horizontal, 4)
            .padding(.vertical, 2)
            .background(Color.black.opacity(0.6).clipShape(Capsule()))
            .offset(y: -node.radius)
        }
        .frame(width: diameter + node.lineWidth * 3, height: diameter + node.lineWidth * 3)
        .position(node.center)
        .onTapGesture(count: 2) {
            guard !vm.isReplaying else { return }
            Task { await vm.toggleWatchlist(symbol: node.symbol, date: wlDate) }
        }
        .onTapGesture(count: 1) {
            guard !vm.isReplaying else { return }
            detailCombined = node.combined
            showDetail = true
        }
        .onLongPressGesture {
            guard !vm.isReplaying else { return }
            historySymbol = node.symbol
            showHistory = true
        }
    }
}
