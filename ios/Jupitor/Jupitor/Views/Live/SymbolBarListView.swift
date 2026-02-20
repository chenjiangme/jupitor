import SwiftUI

struct SymbolBarListView: View {
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
                ScrollView {
                    LazyVStack(spacing: 2) {
                        ForEach(symbolData.map(\.combined)) { combined in
                            symbolRow(combined)
                        }
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

    // MARK: - Symbol Row

    @ViewBuilder
    private func symbolRow(_ combined: CombinedStatsJSON) -> some View {
        let isWatchlist = vm.watchlistSymbols.contains(combined.symbol)
        let hasPre = combined.pre != nil
        let hasReg = combined.reg != nil
        let dualMode = sessionMode == .day && hasPre && hasReg

        HStack(spacing: 4) {
            // Left column: symbol name + counts
            VStack(alignment: .leading, spacing: 1) {
                let counts = newsCounts(for: combined)
                let closePriceBelowDollar = (sessionStats(combined)?.close ?? 1) < 1
                Text(combined.symbol)
                    .font(.system(size: 13, weight: .heavy))
                    .italic(closePriceBelowDollar)
                    .foregroundStyle(isWatchlist ? Color.watchlistColor : Color.tierColor(for: combined.tier))
                    .lineLimit(1)
                if counts.st > 0 || counts.news > 0 {
                    HStack(spacing: 2) {
                        if counts.st > 0 {
                            Text("\(counts.st)")
                                .foregroundStyle(counts.stColor.opacity(0.6))
                        }
                        if counts.news > 0 {
                            Text("\(counts.news)")
                                .foregroundStyle(Color.blue.opacity(0.6))
                        }
                    }
                    .font(.system(size: 9))
                    .lineLimit(1)
                }
            }
            .frame(width: 70, alignment: .leading)

            // Right column: session bar(s)
            if dualMode {
                VStack(spacing: 1) {
                    SessionBarCanvas(
                        gain: combined.reg?.maxGain ?? 0,
                        loss: combined.reg?.maxLoss ?? 0,
                        gainFirst: combined.reg?.gainFirst ?? true,
                        profile: combined.reg?.tradeProfile,
                        closeGain: combined.reg?.closeGain,
                        closeDialColor: combined.reg.map { $0.dialColor },
                        targetGain: tp.targets[date]?["\(combined.symbol):REG"]
                    )
                    SessionBarCanvas(
                        gain: combined.pre?.maxGain ?? 0,
                        loss: combined.pre?.maxLoss ?? 0,
                        gainFirst: combined.pre?.gainFirst ?? true,
                        profile: combined.pre?.tradeProfile,
                        closeGain: combined.pre?.closeGain,
                        closeDialColor: combined.pre.map { $0.dialColor },
                        targetGain: tp.targets[date]?["\(combined.symbol):PRE"]
                    )
                }
            } else {
                let stats = sessionStats(combined)
                let targetKey: String = {
                    switch sessionMode {
                    case .pre, .next: return "\(combined.symbol):PRE"
                    case .reg: return "\(combined.symbol):REG"
                    case .day: return "\(combined.symbol):PRE"
                    }
                }()
                SessionBarCanvas(
                    gain: singleRingGain(combined),
                    loss: singleRingLoss(combined),
                    gainFirst: singleRingGainFirst(combined),
                    profile: stats?.tradeProfile,
                    closeGain: stats?.closeGain,
                    closeDialColor: stats.map { $0.dialColor },
                    targetGain: tp.targets[date]?[targetKey]
                )
            }
        }
        .frame(height: dualMode ? 44 : 28)
        .padding(.horizontal, 4)
        .contentShape(Rectangle())
        .onTapGesture(count: 2) {
            guard !vm.isReplaying else { return }
            Task { await vm.toggleWatchlist(symbol: combined.symbol, date: wlDate) }
        }
        .onTapGesture(count: 1) {
            guard !vm.isReplaying else { return }
            detailCombined = combined
            showDetail = true
        }
        .onLongPressGesture {
            guard !vm.isReplaying else { return }
            historySymbol = combined.symbol
            showHistory = true
        }
    }
}

// MARK: - Session Bar Canvas

private struct SessionBarCanvas: View {
    let gain: Double
    let loss: Double
    let gainFirst: Bool
    var profile: [Int]?
    var closeGain: Double?
    var closeDialColor: Color?
    var targetGain: Double?

    var body: some View {
        Canvas { context, size in
            let halfW = size.width / 2
            let fullH = size.height
            let centerX = halfW
            let scale = halfW / max(gain, loss, 0.2)

            let gainH = gainFirst ? fullH : fullH * 0.5
            let lossH = gainFirst ? fullH * 0.5 : fullH
            let gainY = (fullH - gainH) / 2
            let lossY = (fullH - lossH) / 2

            // 1. Background bars (white 5% opacity)
            context.fill(Path(CGRect(x: 0, y: 0, width: size.width, height: fullH)),
                        with: .color(.white.opacity(0.05)))

            // 2. Gain gradient bands (right from center, 5 bands)
            let cappedGain = min(gain, 5.0)
            for band in 0..<5 {
                let bandStart = Double(band)
                let bandEnd = min(Double(band + 1), cappedGain)
                guard bandEnd > bandStart else { continue }
                let x0 = centerX + CGFloat(bandStart) * scale
                let x1 = centerX + CGFloat(bandEnd) * scale
                context.fill(Path(CGRect(x: x0, y: gainY, width: x1 - x0, height: gainH)),
                            with: .color(gainShades[band]))
            }

            // 3. Loss gradient bands (left from center, 5 bands)
            let cappedLoss = min(loss, 5.0)
            for band in 0..<5 {
                let bandStart = Double(band)
                let bandEnd = min(Double(band + 1), cappedLoss)
                guard bandEnd > bandStart else { continue }
                let x0 = centerX - CGFloat(bandEnd) * scale
                let x1 = centerX - CGFloat(bandStart) * scale
                context.fill(Path(CGRect(x: x0, y: lossY, width: x1 - x0, height: lossH)),
                            with: .color(lossShades[band]))
            }

            // 4. Center line (white 20% opacity, 0.5pt)
            var centerLine = Path()
            centerLine.move(to: CGPoint(x: centerX, y: 0))
            centerLine.addLine(to: CGPoint(x: centerX, y: fullH))
            context.stroke(centerLine, with: .color(.white.opacity(0.2)),
                          style: StrokeStyle(lineWidth: 0.5))

            // 5. Volume profile spikes (downward from top)
            if let profile = profile, !profile.isEmpty, let maxCount = profile.max(), maxCount > 0 {
                let gainSide = gain >= loss
                let maxSpikeH = fullH * 0.4
                var spikes = Path()
                for i in 0..<profile.count {
                    guard profile[i] > 0 else { continue }
                    let pct = Double(i) / 100.0
                    let x: CGFloat = gainSide
                        ? centerX + CGFloat(pct) * scale
                        : centerX - CGFloat(pct) * scale
                    let spikeH = maxSpikeH * CGFloat(profile[i]) / CGFloat(maxCount)
                    spikes.move(to: CGPoint(x: x, y: 0))
                    spikes.addLine(to: CGPoint(x: x, y: spikeH))
                }
                context.stroke(spikes, with: .color(.white.opacity(0.25)),
                              style: StrokeStyle(lineWidth: 1, lineCap: .round))
            }

            // 6. Close gain marker (colored vertical line)
            if let cg = closeGain, cg > 0 {
                let x = centerX + CGFloat(cg) * scale
                var line = Path()
                line.move(to: CGPoint(x: x, y: 0))
                line.addLine(to: CGPoint(x: x, y: fullH))
                context.stroke(line, with: .color(closeDialColor ?? .white),
                              style: StrokeStyle(lineWidth: 2, lineCap: .round))
            }

            // 7. Target marker (yellow vertical line)
            if let tg = targetGain, tg > 0 {
                let x = centerX + CGFloat(tg) * scale
                var line = Path()
                line.move(to: CGPoint(x: x, y: 0))
                line.addLine(to: CGPoint(x: x, y: fullH))
                context.stroke(line, with: .color(.yellow.opacity(0.9)),
                              style: StrokeStyle(lineWidth: 2, lineCap: .round))
            }
        }
    }
}
