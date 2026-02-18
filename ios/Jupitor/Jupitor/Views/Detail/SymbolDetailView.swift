import SwiftUI

struct SymbolDetailView: View {
    @Environment(DashboardViewModel.self) private var vm
    @Environment(TradeParamsModel.self) private var tradeParams
    let symbols: [CombinedStatsJSON]
    let initialSymbol: String
    let date: String
    var newsDate: String = ""
    var isNextMode: Bool = false

    /// The date used to fetch news. Falls back to `date` if not set.
    private var effectiveNewsDate: String { newsDate.isEmpty ? date : newsDate }

    @State private var currentIndex: Int = 0
    @State private var newsArticles: [NewsArticleJSON] = []
    @State private var isLoadingNews = false
    @State private var panOffset: CGFloat = 0
    @State private var isTransitioning = false
    @State private var isAdjustingTarget = false
    @State private var isSwiping = false
    @State private var targetResetToken = 0

    private var currentSymbol: String { symbols[currentIndex].symbol }

    /// For live dates, look up fresh stats from vm (updated every 5s).
    /// In NEXT mode, skip the fresh lookup so we use the NEXT session data as passed.
    private var combined: CombinedStatsJSON {
        if !isNextMode, date == vm.date, let today = vm.today {
            if let fresh = today.tiers.flatMap(\.symbols).first(where: { $0.symbol == currentSymbol }) {
                return fresh
            }
        }
        return symbols[currentIndex]
    }

    private var news: [NewsArticleJSON] {
        let cal = Calendar.current
        return newsArticles.filter { a in
            guard a.source != "stocktwits" else { return false }
            if isNextMode {
                // NEXT mode: only after-4PM articles.
                let c = cal.dateComponents(in: Self.et, from: a.date)
                let minutes = (c.hour ?? 0) * 60 + (c.minute ?? 0)
                return minutes >= 960
            }
            return true
        }
    }
    private static let et = TimeZone(identifier: "America/New_York")!

    /// StockTwits messages split by ET time of day.
    /// In NEXT mode, counts only after-4PM messages.
    /// Otherwise counts only messages from the display date.
    private var stocktwitsBuckets: (overnight: Int, pre: Int, regular: Int, after: Int) {
        var overnight = 0, pre = 0, regular = 0, after = 0
        let cal = Calendar.current
        for a in newsArticles where a.source == "stocktwits" {
            let c = cal.dateComponents(in: Self.et, from: a.date)
            let msgDate = String(format: "%04d-%02d-%02d", c.year ?? 0, c.month ?? 0, c.day ?? 0)
            let minutes = (c.hour ?? 0) * 60 + (c.minute ?? 0)
            if isNextMode {
                // NEXT mode: only after-4PM messages from the news date.
                guard msgDate == effectiveNewsDate, minutes >= 960 else { continue }
                after += 1
            } else {
                guard msgDate == date else { continue }
                if minutes < 240 {         // before 4:00 AM
                    overnight += 1
                } else if minutes < 570 {  // 4:00 AM – 9:30 AM
                    pre += 1
                } else if minutes < 960 {  // 9:30 AM – 4:00 PM
                    regular += 1
                } else {                   // 4:00 PM+
                    after += 1
                }
            }
        }
        return (overnight, pre, regular, after)
    }

    private var stocktwitsTotal: Int {
        let b = stocktwitsBuckets
        return b.overnight + b.pre + b.regular + b.after
    }

    private var canGoBack: Bool { currentIndex > 0 }
    private var canGoForward: Bool { currentIndex < symbols.count - 1 }

    private func commitSwipe(offset: CGFloat) {
        let threshold: CGFloat = 80
        let w = UIScreen.main.bounds.width

        if offset < -threshold && canGoForward {
            isTransitioning = true
            withAnimation(.easeOut(duration: 0.15)) { panOffset = -w }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                currentIndex += 1
                panOffset = w
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { panOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isTransitioning = false
            }
        } else if offset > threshold && canGoBack {
            isTransitioning = true
            withAnimation(.easeOut(duration: 0.15)) { panOffset = w }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                currentIndex -= 1
                panOffset = -w
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { panOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isTransitioning = false
            }
        } else {
            withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) { panOffset = 0 }
        }
    }

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 16) {
                // Header.
                HStack {
                    VStack(alignment: .leading, spacing: 2) {
                        Text(combined.symbol)
                            .font(.title2.bold())
                        Text(combined.tier)
                            .font(.caption)
                            .foregroundStyle(Color.tierColor(for: combined.tier))
                    }
                    Spacer()
                    Button {
                        Task { await vm.toggleWatchlist(symbol: combined.symbol, date: date) }
                    } label: {
                        Image(systemName: vm.watchlistSymbols.contains(combined.symbol) ? "star.fill" : "star")
                            .font(.title3)
                            .foregroundStyle(
                                vm.watchlistSymbols.contains(combined.symbol) ? Color.watchlistColor : .secondary
                            )
                    }
                }
                .padding(.horizontal)

                // Ring visualization.
                DetailRingView(combined: combined, date: date, isAdjustingTarget: $isAdjustingTarget, resetToken: targetResetToken)
                    .frame(maxWidth: .infinity)
                    .padding(.vertical, 8)

                // Session cards.
                if let pre = combined.pre {
                    SessionCard(label: "Pre-Market", stats: pre)
                }
                if let reg = combined.reg {
                    SessionCard(label: "Regular", stats: reg)
                }

                // StockTwits counts by session.
                if !isLoadingNews, stocktwitsTotal > 0 {
                    let b = stocktwitsBuckets
                    HStack {
                        Text("StockTwits")
                            .font(.headline)
                        Spacer()
                        HStack(spacing: 8) {
                            if b.overnight > 0 {
                                Label("\(b.overnight)", systemImage: "moon.fill")
                                    .foregroundStyle(.secondary)
                            }
                            if b.pre > 0 {
                                Label("\(b.pre)", systemImage: "sunrise.fill")
                                    .foregroundStyle(.indigo)
                            }
                            if b.regular > 0 {
                                Label("\(b.regular)", systemImage: "sun.max.fill")
                                    .foregroundStyle(.green.opacity(0.7))
                            }
                            if b.after > 0 {
                                Label("\(b.after)", systemImage: "sunset.fill")
                                    .foregroundStyle(.orange.opacity(0.7))
                            }
                        }
                        .font(.caption2.monospacedDigit())
                        .fixedSize()
                    }
                    .lineLimit(1)
                    .padding(.horizontal)
                }

                // News articles.
                if isLoadingNews {
                    ProgressView()
                        .frame(maxWidth: .infinity)
                        .padding()
                } else if !news.isEmpty {
                    VStack(alignment: .leading, spacing: 8) {
                        Text("News")
                            .font(.headline)
                            .padding(.horizontal)

                        ForEach(news) { article in
                            VStack(alignment: .leading, spacing: 4) {
                                HStack {
                                    Text(article.source)
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                    Spacer()
                                    Text(article.date, style: .time)
                                        .font(.caption2)
                                        .foregroundStyle(.secondary)
                                }
                                Text(article.headline)
                                    .font(.subheadline.bold())
                                    .lineLimit(3)
                                if let content = article.content, !content.isEmpty {
                                    Text(content)
                                        .font(.caption)
                                        .foregroundStyle(.secondary)
                                        .lineLimit(4)
                                }
                            }
                            .padding(.horizontal)
                            .padding(.vertical, 4)
                        }
                    }
                }
            }
            .padding(.vertical)
        }
        .offset(x: panOffset)
        .background(Color.black)
        .navigationTitle(combined.symbol)
        .navigationBarTitleDisplayMode(.inline)
        .scrollDisabled(isSwiping)
        .simultaneousGesture(
            DragGesture(minimumDistance: 30)
                .onChanged { value in
                    guard !isTransitioning, !isAdjustingTarget else { return }
                    let t = value.translation
                    guard abs(t.width) > abs(t.height) else { return }
                    isSwiping = true
                    if (t.width < 0 && canGoForward) || (t.width > 0 && canGoBack) {
                        panOffset = t.width
                    }
                }
                .onEnded { value in
                    isSwiping = false
                    guard !isTransitioning, !isAdjustingTarget else {
                        panOffset = 0
                        return
                    }
                    let t = value.translation
                    guard abs(t.width) > abs(t.height) else {
                        withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) { panOffset = 0 }
                        return
                    }
                    commitSwipe(offset: t.width)
                }
        )
        .onAppear {
            if let idx = symbols.firstIndex(where: { $0.symbol == initialSymbol }) {
                currentIndex = idx
            }
        }
        .task(id: combined.symbol) {
            isLoadingNews = true
            newsArticles = await vm.fetchNewsArticles(symbol: combined.symbol, date: effectiveNewsDate)
            isLoadingNews = false
        }
        .onShake {
            Task { await tradeParams.deleteAllTargets(symbol: combined.symbol, date: date) }
            targetResetToken += 1
        }
    }
}

// MARK: - Detail Ring

private struct DetailRingView: View {
    let combined: CombinedStatsJSON
    let date: String
    @Binding var isAdjustingTarget: Bool
    let resetToken: Int

    private let maxDiameter: CGFloat = 140
    private let minDiameter: CGFloat = 50
    private let ringWidth: CGFloat = 12

    private var preTurnover: Double { combined.pre?.turnover ?? 0 }
    private var regTurnover: Double { combined.reg?.turnover ?? 0 }

    private func diameter(for turnover: Double, maxTurnover: Double) -> CGFloat {
        guard maxTurnover > 0 else { return minDiameter }
        let ratio = sqrt(CGFloat(turnover / maxTurnover))
        return max(minDiameter, maxDiameter * ratio)
    }

    var body: some View {
        let maxT = max(preTurnover, regTurnover)
        let preDia = diameter(for: preTurnover, maxTurnover: maxT)
        let regDia = diameter(for: regTurnover, maxTurnover: maxT)

        HStack(spacing: 24) {
            if let pre = combined.pre {
                TargetRingView(
                    label: "PRE",
                    stats: pre,
                    dia: preDia,
                    ringWidth: ringWidth,
                    targetKey: "\(combined.symbol):PRE",
                    date: date,
                    isAdjusting: $isAdjustingTarget,
                    resetToken: resetToken
                )
            }
            if let reg = combined.reg {
                TargetRingView(
                    label: "REG",
                    stats: reg,
                    dia: regDia,
                    ringWidth: ringWidth,
                    targetKey: "\(combined.symbol):REG",
                    date: date,
                    isAdjusting: $isAdjustingTarget,
                    resetToken: resetToken
                )
            }
        }
    }
}

// MARK: - Target Ring (interactive gain target)

private struct TargetRingView: View {
    @Environment(TradeParamsModel.self) private var tp
    let label: String
    let stats: SymbolStatsJSON
    let dia: CGFloat
    let ringWidth: CGFloat
    let targetKey: String
    let date: String
    @Binding var isAdjusting: Bool
    let resetToken: Int

    @State private var target: Double? = nil
    @State private var prevAngle: Double = 0
    @State private var isDragging = false
    @State private var isLocked = false

    private var outerDia: CGFloat { dia - ringWidth }
    private var viewSize: CGFloat { dia + 20 }

    var body: some View {
        VStack(spacing: 6) {
            ZStack {
                SessionRingView(
                    gain: stats.maxGain,
                    loss: stats.maxLoss,
                    hasData: true,
                    diameter: outerDia,
                    lineWidth: ringWidth
                )

                // Close gain marker (green line across ring).
                if let cg = stats.closeGain, cg > 0 {
                    CloseGainMarkerCanvas(gain: cg, ringRadius: outerDia / 2, lineWidth: ringWidth)
                }

                // Max drawdown marker (cyan line — where price dropped to after peak).
                if let dd = stats.maxDrawdown, dd > 0 {
                    CloseGainMarkerCanvas(gain: stats.maxGain - dd, ringRadius: outerDia / 2, lineWidth: ringWidth,
                                          color: .cyan.opacity(0.9))
                }

                if stats.high > stats.low {
                    CloseDialView(
                        fraction: (stats.close - stats.low) / (stats.high - stats.low),
                        needleRadius: outerDia / 2,
                        lineWidth: max(1.5, ringWidth * 0.4)
                    )
                    .frame(width: dia, height: dia)
                }

                // Target line overlay.
                if let t = target, t > 0 {
                    TargetArrowCanvas(
                        gain: t,
                        ringRadius: outerDia / 2,
                        lineWidth: ringWidth
                    )
                }

                // Lock indicator.
                if isLocked, target != nil {
                    Image(systemName: "lock.fill")
                        .font(.system(size: 10))
                        .foregroundStyle(.yellow.opacity(0.6))
                }
            }
            .frame(width: viewSize, height: viewSize)
            .contentShape(Circle())
            .onTapGesture(count: 2) {
                guard target != nil else { return }
                isLocked.toggle()
            }
            .gesture(
                LongPressGesture(minimumDuration: 0.3)
                    .sequenced(before: DragGesture(minimumDistance: 5))
                    .onChanged { value in
                        guard !isLocked else { return }
                        switch value {
                        case .second(true, let drag):
                            guard let drag else { return }
                            let center = CGPoint(x: viewSize / 2, y: viewSize / 2)
                            let dx = drag.location.x - center.x
                            let dy = drag.location.y - center.y
                            var angle = atan2(Double(dx), -Double(dy))
                            if angle < 0 { angle += 2 * .pi }

                            if !isDragging {
                                isDragging = true
                                isAdjusting = true
                                prevAngle = angle
                                if target == nil {
                                    target = angle / (2 * .pi)
                                }
                                return
                            }

                            var delta = angle - prevAngle
                            if delta > .pi { delta -= 2 * .pi }
                            if delta < -.pi { delta += 2 * .pi }

                            let current = target ?? 0
                            target = max(0, min(5.0, current + delta / (2 * .pi)))
                            prevAngle = angle
                        default: break
                        }
                    }
                    .onEnded { _ in
                        guard !isLocked else { return }
                        isDragging = false
                        isAdjusting = false
                        if let t = target, t < 0.02 {
                            target = nil
                            Task { await tp.deleteTarget(key: targetKey, date: date) }
                        } else if let t = target {
                            Task { await tp.setTarget(key: targetKey, value: t, date: date) }
                        }
                    }
            )

            // Label + target %.
            HStack(spacing: 4) {
                Text(label)
                    .font(.caption2.bold())
                    .foregroundStyle(.secondary)
                if let t = target, t > 0 {
                    Text(String(format: "%.0f%%", t * 100))
                        .font(.caption2.bold())
                        .foregroundStyle(isLocked ? .yellow.opacity(0.5) : .yellow)
                }
                if isLocked {
                    Image(systemName: "lock.fill")
                        .font(.system(size: 8))
                        .foregroundStyle(.yellow.opacity(0.5))
                }
            }
        }
        .onAppear {
            target = tp.targets[date]?[targetKey]
        }
        .onChange(of: targetKey) { _, newKey in
            target = tp.targets[date]?[newKey]
        }
        .onChange(of: tp.targets[date]?[targetKey]) { _, newValue in
            if !isDragging {
                target = newValue
            }
        }
        .onChange(of: resetToken) { _, _ in
            target = nil
        }
    }
}

// MARK: - Target Line Canvas

private struct TargetArrowCanvas: View {
    let gain: Double    // 0-5 (1.0 = 100%)
    let ringRadius: CGFloat
    let lineWidth: CGFloat

    var body: some View {
        Canvas { context, size in
            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            let frac = gain.truncatingRemainder(dividingBy: 1.0)
            let adjustedFrac = gain >= 1.0 && frac == 0 ? 1.0 : frac
            let rad = -Double.pi / 2 + 2 * Double.pi * adjustedFrac

            let innerR = ringRadius - lineWidth / 2
            let outerR = ringRadius + lineWidth / 2

            let p1 = CGPoint(x: center.x + cos(rad) * innerR,
                             y: center.y + sin(rad) * innerR)
            let p2 = CGPoint(x: center.x + cos(rad) * outerR,
                             y: center.y + sin(rad) * outerR)

            var line = Path()
            line.move(to: p1)
            line.addLine(to: p2)
            context.stroke(line, with: .color(.yellow.opacity(0.9)),
                          style: StrokeStyle(lineWidth: 2.5, lineCap: .round))
        }
    }
}

// MARK: - Close Gain Marker

private struct CloseGainMarkerCanvas: View {
    let gain: Double
    let ringRadius: CGFloat
    let lineWidth: CGFloat
    var color: Color = Color(hue: 0.33, saturation: 1.0, brightness: 0.7)

    var body: some View {
        Canvas { context, size in
            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            let frac = gain.truncatingRemainder(dividingBy: 1.0)
            let adjustedFrac = gain >= 1.0 && frac == 0 ? 1.0 : frac
            let rad = -Double.pi / 2 + 2 * Double.pi * adjustedFrac

            let innerR = ringRadius - lineWidth / 2
            let outerR = ringRadius + lineWidth / 2

            let p1 = CGPoint(x: center.x + cos(rad) * innerR,
                             y: center.y + sin(rad) * innerR)
            let p2 = CGPoint(x: center.x + cos(rad) * outerR,
                             y: center.y + sin(rad) * outerR)

            var line = Path()
            line.move(to: p1)
            line.addLine(to: p2)
            context.stroke(line, with: .color(color),
                          style: StrokeStyle(lineWidth: 2.5, lineCap: .round))
        }
    }
}

