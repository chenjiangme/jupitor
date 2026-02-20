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
                    let nodes = buildPackedRings(in: geo.size)
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

    // MARK: - Circle Packing Layout

    private struct RingNode {
        let symbol: String
        let combined: CombinedStatsJSON
        let tier: String
        let center: CGPoint
        let radius: CGFloat
        let lineWidth: CGFloat
    }

    /// Build packed ring nodes using D3-style circle packing (packSiblings).
    private func buildPackedRings(in size: CGSize) -> [RingNode] {
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

        // Radii proportional to sqrt(turnover) — preserves relative area.
        let weights = sorted.map { sqrt(CGFloat($0.2)) }

        // Pack circles (positions in arbitrary coordinate space).
        var px = [CGFloat](repeating: 0, count: sorted.count)
        var py = [CGFloat](repeating: 0, count: sorted.count)
        packSiblings(weights, px: &px, py: &py)

        // Compute bounding box including profile overflow padding.
        let pad: CGFloat = 1.2 // ~20% extra for ring stroke + volume profile bars
        var minX = CGFloat.infinity, maxX = -CGFloat.infinity
        var minY = CGFloat.infinity, maxY = -CGFloat.infinity
        for i in 0..<sorted.count {
            let r = weights[i] * pad
            minX = min(minX, px[i] - r)
            maxX = max(maxX, px[i] + r)
            minY = min(minY, py[i] - r)
            maxY = max(maxY, py[i] + r)
        }

        let packW = maxX - minX
        let packH = maxY - minY
        guard packW > 0 && packH > 0 else { return [] }

        let margin: CGFloat = 4
        let scaleX = (size.width - 2 * margin) / packW
        let scaleY = (size.height - 2 * margin) / packH
        let scale = min(scaleX, scaleY)

        let packCX = (minX + maxX) / 2
        let packCY = (minY + maxY) / 2
        let viewCX = size.width / 2
        let viewCY = size.height / 2

        var nodes: [RingNode] = []
        for (i, item) in sorted.enumerated() {
            let r = weights[i] * scale
            if r < minRingRadius { continue }
            let center = CGPoint(
                x: (px[i] - packCX) * scale + viewCX,
                y: (py[i] - packCY) * scale + viewCY
            )
            let lw = max(4, r * 0.12)
            nodes.append(RingNode(
                symbol: item.0.symbol,
                combined: item.0,
                tier: item.1,
                center: center,
                radius: r,
                lineWidth: lw
            ))
        }

        return nodes
    }

    // MARK: - D3 packSiblings Algorithm

    /// Port of D3's packSiblings: places circles with given radii into
    /// a tight non-overlapping arrangement. Outputs positions into px/py.
    private func packSiblings(_ radii: [CGFloat], px: inout [CGFloat], py: inout [CGFloat]) {
        let n = radii.count
        guard n > 0 else { return }
        if n == 1 { return } // single circle at origin

        // Place first two touching.
        px[0] = -radii[1]
        px[1] = radii[0]

        guard n > 2 else { return }

        // Place third tangent to first two.
        d3Place(px: &px, py: &py, r: radii, c: 2, b: 1, a: 0)

        guard n > 3 else { return }

        // Front chain: circular doubly-linked list indexed by circle index.
        // Chain order: 0 -> 1 -> 2 -> 0 (matching D3's a -> b -> c -> a).
        var cNext = [Int](repeating: -1, count: n)
        var cPrev = [Int](repeating: -1, count: n)
        cNext[0] = 1; cPrev[0] = 2
        cNext[1] = 2; cPrev[1] = 0
        cNext[2] = 0; cPrev[2] = 1

        var a0 = 0
        var b0 = 1

        var i = 3
        outer: while i < n {
            // Place circle i tangent to pair (a0, b0).
            d3Place(px: &px, py: &py, r: radii, c: i, b: a0, a: b0)

            // Scan front chain for intersections.
            var j = cNext[b0]
            var k = cPrev[a0]
            var sj = radii[b0]
            var sk = radii[a0]

            repeat {
                if sj <= sk {
                    if d3Intersects(px: px, py: py, r: radii, a: j, b: i) {
                        // Remove nodes between a0 and j from chain.
                        b0 = j
                        cNext[a0] = b0
                        cPrev[b0] = a0
                        continue outer // retry circle i
                    }
                    sj += radii[j]
                    j = cNext[j]
                } else {
                    if d3Intersects(px: px, py: py, r: radii, a: k, b: i) {
                        // Remove nodes between k and b0 from chain.
                        a0 = k
                        cNext[a0] = b0
                        cPrev[b0] = a0
                        continue outer // retry circle i
                    }
                    sk += radii[k]
                    k = cPrev[k]
                }
            } while j != cNext[k]

            // No intersection — insert circle i between a0 and b0.
            cPrev[i] = a0
            cNext[i] = b0
            cNext[a0] = i
            cPrev[b0] = i

            // b0 advances to the newly inserted node.
            b0 = i

            // Update a0 to the chain node whose weighted midpoint is closest to origin.
            var bestScore = d3Score(px: px, py: py, r: radii, node: a0, next: cNext[a0])
            var scan = cNext[a0]
            while scan != b0 {
                let s = d3Score(px: px, py: py, r: radii, node: scan, next: cNext[scan])
                if s < bestScore {
                    a0 = scan
                    bestScore = s
                }
                scan = cNext[scan]
            }
            b0 = cNext[a0]

            i += 1
        }
    }

    /// D3's place(b, a, c): place circle c tangent to b and a.
    private func d3Place(px: inout [CGFloat], py: inout [CGFloat], r: [CGFloat],
                         c: Int, b: Int, a: Int) {
        let dx = px[b] - px[a]
        let dy = py[b] - py[a]
        let d2 = dx * dx + dy * dy

        guard d2 > 0 else {
            px[c] = r[a] + r[c]
            py[c] = 0
            return
        }

        let a2 = (r[a] + r[c]) * (r[a] + r[c])
        let b2 = (r[b] + r[c]) * (r[b] + r[c])

        if a2 > b2 {
            let x = (d2 + b2 - a2) / (2 * d2)
            let y = sqrt(max(0, b2 / d2 - x * x))
            px[c] = px[b] - x * dx - y * dy
            py[c] = py[b] - x * dy + y * dx
        } else {
            let x = (d2 + a2 - b2) / (2 * d2)
            let y = sqrt(max(0, a2 / d2 - x * x))
            px[c] = px[a] + x * dx - y * dy
            py[c] = py[a] + x * dy + y * dx
        }
    }

    private func d3Intersects(px: [CGFloat], py: [CGFloat], r: [CGFloat],
                              a: Int, b: Int) -> Bool {
        let dr = r[a] + r[b] - 1e-6
        let dx = px[b] - px[a]
        let dy = py[b] - py[a]
        return dr > 0 && dr * dr > dx * dx + dy * dy
    }

    /// Score: squared distance of weighted midpoint between node and its successor.
    private func d3Score(px: [CGFloat], py: [CGFloat], r: [CGFloat],
                         node: Int, next: Int) -> CGFloat {
        let ab = r[node] + r[next]
        guard ab > 0 else { return 0 }
        let dx = (px[node] * r[next] + px[next] * r[node]) / ab
        let dy = (py[node] * r[next] + py[next] * r[node]) / ab
        return dx * dx + dy * dy
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

            // Symbol label (centered).
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
                    .font(.system(size: max(5, node.radius * 0.18)))
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                }

                let closePriceBelowDollar = (sessionStats(node.combined)?.close ?? 1) < 1
                Text(node.symbol)
                    .font(.system(size: max(7, node.radius * 0.3), weight: .heavy))
                    .italic(closePriceBelowDollar)
                    .foregroundStyle((isWatchlist ? Color.watchlistColor : Color.tierColor(for: node.combined.tier)).opacity(0.5))
                    .lineLimit(1)
                    .minimumScaleFactor(0.5)
                if isWatchlist, let stats = sessionStats(node.combined) {
                    Text("\(Fmt.compactPrice(stats.open)) \(Fmt.compactPrice(stats.low)) \(Fmt.compactPrice(stats.high)) \(Fmt.compactPrice(stats.close))")
                        .font(.system(size: max(5, node.radius * 0.16)))
                        .foregroundStyle(Color.watchlistPriceColor.opacity(0.4))
                        .lineLimit(1)
                        .minimumScaleFactor(0.5)
                }
            }
            .padding(2)
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
