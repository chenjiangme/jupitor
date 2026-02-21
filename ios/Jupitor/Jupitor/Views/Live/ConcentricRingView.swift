import SwiftUI

struct ConcentricRingView: View {
    @Environment(DashboardViewModel.self) private var vm
    @Environment(TradeParamsModel.self) private var tp
    let day: DayDataJSON
    let date: String
    var watchlistDate: String = ""
    var sessionMode: SessionMode = .day

    private var wlDate: String { watchlistDate.isEmpty ? date : watchlistDate }

    @State private var rings: [RingState] = []
    @State private var viewSize: CGSize = .zero
    @State private var showDetail = false
    @State private var detailCombined: CombinedStatsJSON?
    @State private var showHistory = false
    @State private var historySymbol: String = ""
    @State private var showWatchlistOnly = false
    @State private var hasInitialized = false
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
                tradeProfile: nil,
                tradeProfile30m: nil
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

    // MARK: - Ring State

    private struct RingState: Identifiable {
        let id: String          // symbol
        var combined: CombinedStatsJSON
        var tier: String
        var center: CGPoint     // viewport position (animated)
        var radius: CGFloat     // viewport radius (animated)
        var lineWidth: CGFloat
        var hasChildren: Bool
        var depth: Int          // 0 = top-level, 1 = child, 2 = grandchild...
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
                    ZStack {
                        Color.clear.contentShape(Rectangle())
                        ForEach(rings) { ring in
                            ringNodeView(ring, viewSize: geo.size)
                        }
                    }
                    .frame(width: geo.size.width, height: geo.size.height)
                    .onAppear {
                        viewSize = geo.size
                        syncRings(in: geo.size)
                    }
                    .onChange(of: geo.size) { _, new in
                        viewSize = new
                        syncRings(in: new)
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
                }
        )
        .onChange(of: day) { _, _ in if viewSize.width > 0 { syncRings(in: viewSize) } }
        .onChange(of: sessionMode) { _, _ in if viewSize.width > 0 { syncRings(in: viewSize) } }
        .onChange(of: vm.watchlistSymbols) { _, _ in if viewSize.width > 0 { syncRings(in: viewSize) } }
        .onChange(of: hidePennyStocks) { _, _ in if viewSize.width > 0 { syncRings(in: viewSize) } }
        .onChange(of: gainOverLossOnly) { _, _ in if viewSize.width > 0 { syncRings(in: viewSize) } }
        .onChange(of: showWatchlistOnly) { _, _ in if viewSize.width > 0 { syncRings(in: viewSize) } }
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

    // MARK: - Sync Rings (Global Radii + Containment Tree + Animation)

    private func syncRings(in size: CGSize) {
        // 1. Build items sorted by turnover descending.
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
        guard !items.isEmpty else {
            withAnimation(.spring(response: 0.5, dampingFraction: 0.8)) { rings = [] }
            return
        }
        let sorted = items.sorted { $0.2 > $1.2 }
        let n = sorted.count

        // 2. Compute global radii (viewport-sized, same formula as BubbleChartView).
        var radii = computeGlobalRadii(turnovers: sorted.map { $0.2 }, in: size)

        // Scale down if total effective area exceeds viewport (same as BubbleChartView).
        let canvasArea = size.width * size.height
        var totalEffArea: CGFloat = 0
        for r in radii {
            let effR = r + max(4, r * 0.12) * 1.5
            totalEffArea += .pi * effR * effR
        }
        if totalEffArea > canvasArea * 0.85 {
            let scale = sqrt(canvasArea * 0.85 / totalEffArea)
            radii = radii.map { max(minRingRadius, $0 * scale) }
        }

        // 3. Build containment tree via BFS greedy assignment.
        var childrenOf: [[Int]] = Array(repeating: [], count: n)
        var topLevel: [Int] = [0]  // largest item always top-level
        var depthOf: [Int] = Array(repeating: 0, count: n)

        for i in 1..<n {
            if let (parent, parentDepth) = tryNestBFS(item: i, radii: radii, childrenOf: childrenOf, topLevel: topLevel) {
                childrenOf[parent].append(i)
                depthOf[i] = parentDepth + 1
            } else {
                topLevel.append(i)
            }
        }

        // 4. Position top-level rings: grid initialization + force-settle.
        var positions = Array(repeating: CGPoint.zero, count: n)
        let topCount = topLevel.count
        let topRadii = topLevel.map { radii[$0] }

        // Grid that fills the viewport rectangle.
        let cols = max(1, Int(ceil(sqrt(Double(topCount) * Double(size.width) / Double(size.height)))))
        let rows = max(1, Int(ceil(Double(topCount) / Double(cols))))
        let cellW = size.width / CGFloat(cols)
        let cellH = size.height / CGFloat(rows)

        var posX = [CGFloat](repeating: 0, count: topCount)
        var posY = [CGFloat](repeating: 0, count: topCount)
        for i in 0..<topCount {
            let col = i % cols
            let row = i / cols
            let r = topRadii[i]
            let effR = r + max(4, r * 0.12) * 1.5
            posX[i] = max(effR, min(size.width - effR, (CGFloat(col) + 0.5) * cellW))
            posY[i] = max(effR, min(size.height - effR, (CGFloat(row) + 0.5) * cellH))
        }

        // Force-settle: resolve overlaps while staying within viewport.
        let pad: CGFloat = 2
        for _ in 0..<200 {
            var maxDisp: CGFloat = 0
            for i in 0..<topCount {
                var fx: CGFloat = 0, fy: CGFloat = 0
                let ri = topRadii[i]
                let effI = ri + max(4, ri * 0.12) * 1.5

                for j in 0..<topCount where j != i {
                    let rj = topRadii[j]
                    let effJ = rj + max(4, rj * 0.12) * 1.5
                    let dx = posX[i] - posX[j]
                    let dy = posY[i] - posY[j]
                    let dist = hypot(dx, dy)
                    let minDist = effI + effJ + pad
                    if dist < minDist && dist > 0.01 {
                        let push = (minDist - dist) * 0.3
                        fx += (dx / dist) * push
                        fy += (dy / dist) * push
                    }
                }

                // Boundary forces.
                if posX[i] - effI < 0 { fx += (effI - posX[i]) * 0.5 }
                if posX[i] + effI > size.width { fx -= (posX[i] + effI - size.width) * 0.5 }
                if posY[i] - effI < 0 { fy += (effI - posY[i]) * 0.5 }
                if posY[i] + effI > size.height { fy -= (posY[i] + effI - size.height) * 0.5 }

                posX[i] += fx
                posY[i] += fy
                posX[i] = max(effI, min(size.width - effI, posX[i]))
                posY[i] = max(effI, min(size.height - effI, posY[i]))
                maxDisp = max(maxDisp, hypot(fx, fy))
            }
            if maxDisp < 0.5 { break }
        }

        // 5. Assign top-level positions.
        for (i, idx) in topLevel.enumerated() {
            positions[idx] = CGPoint(x: posX[i], y: posY[i])
        }

        // Recursively position children inside their parents.
        func positionChildren(of parent: Int) {
            let children = childrenOf[parent]
            guard !children.isEmpty else { return }

            let childRadii = children.map { radii[$0] }
            var cpx = [CGFloat](repeating: 0, count: children.count)
            var cpy = [CGFloat](repeating: 0, count: children.count)
            if children.count > 1 {
                packSiblings(childRadii, px: &cpx, py: &cpy)
            }

            let cx = cpx.reduce(0, +) / CGFloat(children.count)
            let cy = cpy.reduce(0, +) / CGFloat(children.count)
            for j in 0..<children.count { cpx[j] -= cx; cpy[j] -= cy }

            for (j, childIdx) in children.enumerated() {
                positions[childIdx] = CGPoint(
                    x: positions[parent].x + cpx[j],
                    y: positions[parent].y + cpy[j]
                )
                positionChildren(of: childIdx)
            }
        }

        for idx in topLevel {
            positionChildren(of: idx)
        }

        // 6. Build RingState array and animate.
        var newRings: [RingState] = []
        for i in 0..<n {
            let r = radii[i]
            newRings.append(RingState(
                id: sorted[i].0.symbol,
                combined: sorted[i].0,
                tier: sorted[i].1,
                center: positions[i],
                radius: r,
                lineWidth: max(4, r * 0.12),
                hasChildren: !childrenOf[i].isEmpty,
                depth: depthOf[i]
            ))
        }

        if hasInitialized {
            withAnimation(.spring(response: 0.5, dampingFraction: 0.8)) {
                rings = newRings
            }
        } else {
            rings = newRings
            hasInitialized = true
        }
    }

    // MARK: - Global Radii (same formula as BubbleChartView)

    private func computeGlobalRadii(turnovers: [Double], in size: CGSize) -> [CGFloat] {
        let totalArea = size.width * size.height * 0.7
        let weights = turnovers.map { sqrt(CGFloat($0)) }
        let totalWeight = weights.reduce(0, +)
        guard totalWeight > 0 else { return turnovers.map { _ in minRingRadius } }
        let maxR = min(size.width, size.height) / 2.5
        return weights.map { w in
            let area = totalArea * w / totalWeight
            return max(minRingRadius, min(maxR, sqrt(area / .pi)))
        }
    }

    // MARK: - BFS Containment Test

    /// Try to nest `item` inside an existing ring via breadth-first search.
    /// Returns (parentIndex, parentDepth) if containment succeeds, nil otherwise.
    private func tryNestBFS(item: Int, radii: [CGFloat], childrenOf: [[Int]], topLevel: [Int]) -> (parent: Int, depth: Int)? {
        var queue: [(index: Int, depth: Int)] = topLevel.map { ($0, 0) }
        var head = 0

        while head < queue.count {
            let (parent, parentDepth) = queue[head]
            head += 1

            let innerR = radii[parent] * 0.74

            // Pack existing children + candidate item.
            let existing = childrenOf[parent]
            let testRadii = existing.map { radii[$0] } + [radii[item]]

            if testRadii.count == 1 {
                // Single child: just check radius.
                if testRadii[0] <= innerR {
                    return (parent, parentDepth)
                }
            } else {
                var tpx = [CGFloat](repeating: 0, count: testRadii.count)
                var tpy = [CGFloat](repeating: 0, count: testRadii.count)
                packSiblings(testRadii, px: &tpx, py: &tpy)

                // Center and compute enclosing radius.
                let cn = testRadii.count
                let cx = tpx.reduce(0, +) / CGFloat(cn)
                let cy = tpy.reduce(0, +) / CGFloat(cn)
                var encR: CGFloat = 0
                for j in 0..<cn {
                    let d = hypot(tpx[j] - cx, tpy[j] - cy) + testRadii[j]
                    encR = max(encR, d)
                }

                if encR <= innerR {
                    return (parent, parentDepth)
                }
            }

            // Enqueue children for deeper nesting attempts.
            for child in existing {
                queue.append((child, parentDepth + 1))
            }
        }

        return nil
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
    private func ringNodeView(_ ring: RingState, viewSize: CGSize) -> some View {
        let isWatchlist = vm.watchlistSymbols.contains(ring.id)
        let diameter = ring.radius * 2
        let ringWidth = ring.lineWidth
        let outerDia = diameter - ringWidth
        let preTurnover = ring.combined.pre?.turnover ?? 0
        let regTurnover = ring.combined.reg?.turnover ?? 0
        let total = preTurnover + regTurnover
        let preRatio = total > 0 ? sqrt(CGFloat(preTurnover / total)) : 0
        let innerDia = min(outerDia - 3 * ringWidth, outerDia * max(minInnerRatio, preRatio))
        let blackFillDia = innerDia + ringWidth

        ZStack {
            let hasPre = ring.combined.pre != nil
            let hasReg = ring.combined.reg != nil
            let dualRing = sessionMode == .day && hasPre && hasReg

            if dualRing {
                SessionRingView(
                    gain: ring.combined.reg?.maxGain ?? 0,
                    loss: ring.combined.reg?.maxLoss ?? 0,
                    hasData: true,
                    diameter: outerDia,
                    lineWidth: ringWidth,
                    gainFirst: ring.combined.reg?.gainFirst ?? true
                )
                Circle()
                    .fill(Color.black)
                    .frame(width: blackFillDia, height: blackFillDia)
                SessionRingView(
                    gain: ring.combined.pre?.maxGain ?? 0,
                    loss: ring.combined.pre?.maxLoss ?? 0,
                    hasData: true,
                    diameter: innerDia,
                    lineWidth: ringWidth,
                    gainFirst: ring.combined.pre?.gainFirst ?? true
                )
            } else {
                SessionRingView(
                    gain: singleRingGain(ring.combined),
                    loss: singleRingLoss(ring.combined),
                    hasData: hasPre || hasReg,
                    diameter: outerDia,
                    lineWidth: ringWidth,
                    gainFirst: singleRingGainFirst(ring.combined)
                )
            }

            // Volume profile.
            let profileOverflow = ringWidth * 1.5
            let profileSize = diameter + profileOverflow * 2
            if dualRing {
                if let profile = ring.combined.reg?.tradeProfile, !profile.isEmpty {
                    VolumeProfileCanvas(
                        profile: profile,
                        gain: ring.combined.reg?.maxGain ?? 0,
                        loss: ring.combined.reg?.maxLoss ?? 0,
                        gainFirst: ring.combined.reg?.gainFirst ?? true,
                        ringRadius: outerDia / 2,
                        lineWidth: ringWidth
                    )
                    .frame(width: profileSize, height: profileSize)
                }
            } else {
                if let stats = sessionStats(ring.combined), let profile = stats.tradeProfile, !profile.isEmpty {
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
                if let reg = ring.combined.reg, let cg = reg.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       color: reg.dialColor)
                        .frame(width: diameter, height: diameter)
                }
                if let pre = ring.combined.pre, let cg = pre.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: innerDia / 2, lineWidth: ringWidth,
                                       color: pre.dialColor)
                        .frame(width: diameter, height: diameter)
                }
            } else {
                if let stats = sessionStats(ring.combined), let cg = stats.closeGain, cg > 0 {
                    TargetMarkerCanvas(gain: cg, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                       color: stats.dialColor)
                        .frame(width: diameter, height: diameter)
                }
            }

            // Target gain markers.
            if dualRing {
                if let t = tp.targets[date]?["\(ring.id):REG"], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: outerDia / 2, lineWidth: ringWidth)
                        .frame(width: diameter, height: diameter)
                }
                if let t = tp.targets[date]?["\(ring.id):PRE"], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: innerDia / 2, lineWidth: ringWidth)
                        .frame(width: diameter, height: diameter)
                }
            } else {
                let targetKey: String = {
                    switch sessionMode {
                    case .pre, .next: return "\(ring.id):PRE"
                    case .reg: return "\(ring.id):REG"
                    case .day: return "\(ring.id):PRE"
                    }
                }()
                if let t = tp.targets[date]?[targetKey], t > 0 {
                    TargetMarkerCanvas(gain: t, ringRadius: outerDia / 2, lineWidth: ringWidth)
                        .frame(width: diameter, height: diameter)
                }
            }

            // Close-position needle (hidden but logic preserved).
            if let stats = sessionStats(ring.combined), stats.high > stats.low {
                CloseDialView(
                    fraction: (stats.close - stats.low) / (stats.high - stats.low),
                    needleRadius: outerDia / 2,
                    lineWidth: max(1.5, ringWidth * 0.4)
                )
                .frame(width: diameter, height: diameter)
                .hidden()
            }

            // Symbol label
            if !ring.hasChildren {
                // Leaf: centered label
                VStack(spacing: 0) {
                    let counts = newsCounts(for: ring.combined)
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
                        .font(.system(size: max(5, ring.radius * 0.18)))
                        .lineLimit(1)
                        .minimumScaleFactor(0.5)
                    }

                    let closePriceBelowDollar = (sessionStats(ring.combined)?.close ?? 1) < 1
                    Text(ring.id)
                        .font(.system(size: max(7, ring.radius * 0.3), weight: .heavy))
                        .italic(closePriceBelowDollar)
                        .foregroundStyle((isWatchlist ? Color.watchlistColor : Color.tierColor(for: ring.combined.tier)).opacity(0.5))
                        .lineLimit(1)
                        .minimumScaleFactor(0.5)
                    if isWatchlist, let stats = sessionStats(ring.combined) {
                        Text("\(Fmt.compactPrice(stats.open)) \(Fmt.compactPrice(stats.low)) \(Fmt.compactPrice(stats.high)) \(Fmt.compactPrice(stats.close))")
                            .font(.system(size: max(5, ring.radius * 0.16)))
                            .foregroundStyle(Color.watchlistPriceColor.opacity(0.4))
                            .lineLimit(1)
                            .minimumScaleFactor(0.5)
                    }
                }
                .padding(2)
            } else {
                // Parent: label at 12 o'clock with dark pill
                VStack(spacing: 0) {
                    let counts = newsCounts(for: ring.combined)
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
                        .font(.system(size: max(5, ring.radius * 0.1)))
                        .lineLimit(1)
                        .minimumScaleFactor(0.5)
                    }
                    let closePriceBelowDollar = (sessionStats(ring.combined)?.close ?? 1) < 1
                    Text(ring.id)
                        .font(.system(size: max(7, ring.radius * 0.13), weight: .heavy))
                        .italic(closePriceBelowDollar)
                        .foregroundStyle((isWatchlist ? Color.watchlistColor : Color.tierColor(for: ring.combined.tier)).opacity(0.7))
                        .lineLimit(1)
                        .minimumScaleFactor(0.5)
                }
                .padding(.horizontal, 5)
                .padding(.vertical, 2)
                .background(Color.black.opacity(0.65))
                .clipShape(Capsule())
                .offset(y: -(ring.radius - ring.lineWidth * 1.5))
            }
        }
        .frame(width: diameter + ring.lineWidth * 3, height: diameter + ring.lineWidth * 3)
        .contentShape(Circle())
        .onTapGesture(count: 2) {
            guard !vm.isReplaying else { return }
            Task { await vm.toggleWatchlist(symbol: ring.id, date: wlDate) }
        }
        .onTapGesture(count: 1) {
            guard !vm.isReplaying else { return }
            detailCombined = ring.combined
            showDetail = true
        }
        .onLongPressGesture {
            guard !vm.isReplaying else { return }
            historySymbol = ring.id
            showHistory = true
        }
        .position(ring.center)
    }
}
