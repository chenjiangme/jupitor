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
    @State private var targetResetToken = 0

    private var currentSymbol: String { symbols[currentIndex].symbol }

    /// For live dates, look up fresh stats from vm (updated every 5s).
    private var combined: CombinedStatsJSON {
        if date == vm.date, let today = vm.today {
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
        .simultaneousGesture(
            DragGesture(minimumDistance: 30)
                .onChanged { value in
                    guard !isTransitioning, !isAdjustingTarget else { return }
                    let t = value.translation
                    guard abs(t.width) > abs(t.height) else { return }
                    if (t.width < 0 && canGoForward) || (t.width > 0 && canGoBack) {
                        panOffset = t.width
                    }
                }
                .onEnded { value in
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

    private var outerDia: CGFloat { dia - ringWidth }
    // Extra padding for the arrow to sit outside the ring.
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

                if stats.high > stats.low {
                    CloseDialView(
                        fraction: (stats.close - stats.low) / (stats.high - stats.low),
                        needleRadius: outerDia / 2,
                        lineWidth: max(1.5, ringWidth * 0.4)
                    )
                    .frame(width: dia, height: dia)
                }

                // Target arrow overlay.
                if let t = target, t > 0 {
                    TargetArrowCanvas(
                        gain: t,
                        ringRadius: outerDia / 2,
                        lineWidth: ringWidth
                    )
                }
            }
            .frame(width: viewSize, height: viewSize)
            .contentShape(Circle())
            .highPriorityGesture(
                DragGesture(minimumDistance: 5)
                    .onChanged { value in
                        let center = CGPoint(x: viewSize / 2, y: viewSize / 2)
                        let dx = value.location.x - center.x
                        let dy = value.location.y - center.y
                        // Angle from 12 o'clock, clockwise (0 to 2π).
                        var angle = atan2(Double(dx), -Double(dy))
                        if angle < 0 { angle += 2 * .pi }

                        if !isDragging {
                            isDragging = true
                            isAdjusting = true
                            prevAngle = angle
                            if target == nil {
                                // First touch: place arrow at finger angle.
                                target = angle / (2 * .pi)
                            }
                            return
                        }

                        var delta = angle - prevAngle
                        // Handle wrap-around at 12 o'clock.
                        if delta > .pi { delta -= 2 * .pi }
                        if delta < -.pi { delta += 2 * .pi }

                        let current = target ?? 0
                        target = max(0, min(5.0, current + delta / (2 * .pi)))
                        prevAngle = angle
                    }
                    .onEnded { _ in
                        isDragging = false
                        isAdjusting = false
                        // Clear if dragged below 2%.
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
                        .foregroundStyle(.yellow)
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

// MARK: - Target Arrow Canvas

private struct TargetArrowCanvas: View {
    let gain: Double    // 0-5 (1.0 = 100%)
    let ringRadius: CGFloat
    let lineWidth: CGFloat

    var body: some View {
        Canvas { context, size in
            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            // Angle: 12 o'clock = -π/2, clockwise. Wraps every 100%.
            let frac = gain.truncatingRemainder(dividingBy: 1.0)
            let adjustedFrac = gain >= 1.0 && frac == 0 ? 1.0 : frac
            let rad = -Double.pi / 2 + 2 * Double.pi * adjustedFrac

            let arrowR = ringRadius + lineWidth / 2 + 6
            let tipR = ringRadius + lineWidth / 2 - 2
            let spread: Double = 0.12

            let tip = CGPoint(
                x: center.x + cos(rad) * tipR,
                y: center.y + sin(rad) * tipR
            )
            let base1 = CGPoint(
                x: center.x + cos(rad - spread) * arrowR,
                y: center.y + sin(rad - spread) * arrowR
            )
            let base2 = CGPoint(
                x: center.x + cos(rad + spread) * arrowR,
                y: center.y + sin(rad + spread) * arrowR
            )

            var triangle = Path()
            triangle.move(to: tip)
            triangle.addLine(to: base1)
            triangle.addLine(to: base2)
            triangle.closeSubpath()
            context.fill(triangle, with: .color(.yellow.opacity(0.9)))

            // Band dots for >100% (show filled dots outside ring).
            let bands = Int(gain)
            if bands > 0 {
                let dotR = arrowR + 6
                for i in 0..<min(bands, 4) {
                    let dotAngle = rad + Double(i - bands / 2) * 0.2
                    let pos = CGPoint(
                        x: center.x + cos(dotAngle) * dotR,
                        y: center.y + sin(dotAngle) * dotR
                    )
                    context.fill(Circle().path(in: CGRect(x: pos.x - 2, y: pos.y - 2, width: 4, height: 4)),
                                 with: .color(.yellow.opacity(0.6)))
                }
            }
        }
    }
}

