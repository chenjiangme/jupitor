import SwiftUI

struct CNSymbolHistoryView: View {
    @Environment(CNHeatmapViewModel.self) private var vm

    @State private var currentStock: CNHeatmapStock
    @State private var historyData: CNSymbolHistoryResponse?
    @State private var isLoading = true
    @State private var errorMessage: String?
    @State private var layout: [(rect: CGRect, day: CNSymbolDay)] = []
    @State private var lastSize: CGSize = .zero
    @State private var dragOffsetX: CGFloat = 0
    @State private var dragOffsetY: CGFloat = 0
    private let stockIndex: String  // "csi300" or "csi500"
    private let onDismiss: () -> Void

    init(stock: CNHeatmapStock, onDismiss: @escaping () -> Void) {
        _currentStock = State(initialValue: stock)
        self.stockIndex = stock.index
        self.onDismiss = onDismiss
    }

    /// Stocks filtered to same index group (CSI 300 or CSI 500) as the initially tapped stock.
    private var stocks: [CNHeatmapStock] {
        (vm.filteredStocks ?? []).filter { $0.index == stockIndex }
    }

    private var currentIndex: Int {
        stocks.firstIndex(where: { $0.symbol == currentStock.symbol }) ?? 0
    }

    var body: some View {
        GeometryReader { geo in
            let size = geo.size
            ZStack {
                Color(red: 0.06, green: 0.06, blue: 0.08).ignoresSafeArea()

                if isLoading {
                    ProgressView()
                } else if let error = errorMessage {
                    Text(error)
                        .foregroundStyle(.red)
                        .font(.caption)
                } else if let data = historyData, !data.days.isEmpty {
                    Canvas { context, canvasSize in
                        drawTreemap(context: context, size: canvasSize, days: data.days)
                    }
                } else {
                    Text("No data")
                        .foregroundStyle(.secondary)
                }

                // Watermark: symbol name centered.
                Text(currentStock.name)
                    .font(.system(size: 40, weight: .bold))
                    .foregroundStyle(.white.opacity(0.25))
                    .allowsHitTesting(false)
            }
            .offset(x: dragOffsetX, y: max(dragOffsetY, 0))
            .gesture(
                DragGesture(minimumDistance: 30)
                    .onChanged { value in
                        let h = value.translation.width
                        let v = value.translation.height
                        // Decide axis: if mostly vertical-down, track Y; otherwise track X.
                        if v > 40 && v > abs(h) {
                            dragOffsetY = v
                            dragOffsetX = 0
                        } else {
                            dragOffsetX = h
                            dragOffsetY = 0
                        }
                    }
                    .onEnded { value in
                        let h = value.translation.width
                        let v = value.translation.height

                        if v > 100 && v > abs(h) {
                            // Swipe down → dismiss.
                            withAnimation(.easeInOut(duration: 0.25)) { onDismiss() }
                            return
                        }

                        let threshold: CGFloat = 60
                        if h < -threshold {
                            navigate(by: 1, size: size)
                        } else if h > threshold {
                            navigate(by: -1, size: size)
                        }
                        withAnimation(.easeOut(duration: 0.2)) {
                            dragOffsetX = 0
                            dragOffsetY = 0
                        }
                    }
            )
            .onAppear {
                lastSize = size
            }
            .onChange(of: size) { _, newSize in
                guard newSize != lastSize else { return }
                lastSize = newSize
                if let data = historyData {
                    recomputeLayout(size: newSize, days: data.days)
                }
            }
        }
        .ignoresSafeArea(edges: .bottom)
        .task {
            await loadHistory(symbol: currentStock.symbol)
        }
    }

    // MARK: - Navigation

    private func navigate(by delta: Int, size: CGSize) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < stocks.count else { return }
        let newStock = stocks[newIndex]
        currentStock = newStock
        Task {
            await loadHistory(symbol: newStock.symbol)
        }
    }

    // MARK: - Data Loading

    private func loadHistory(symbol: String) async {
        isLoading = true
        errorMessage = nil
        layout = []

        do {
            let resp = try await vm.api.fetchSymbolHistory(symbol: symbol, end: vm.currentDate)
            historyData = resp
            isLoading = false
            if lastSize.width > 0 {
                recomputeLayout(size: lastSize, days: resp.days)
            }
        } catch {
            isLoading = false
            errorMessage = error.localizedDescription
            print("CN symbol history error: \(error)")
        }
    }

    // MARK: - Layout

    private func recomputeLayout(size: CGSize, days: [CNSymbolDay]) {
        guard !days.isEmpty else {
            layout = []
            return
        }
        layout = squarify(days: days, in: CGRect(origin: .zero, size: size))
    }

    private func squarify(days: [CNSymbolDay], in rect: CGRect) -> [(rect: CGRect, day: CNSymbolDay)] {
        guard !days.isEmpty else { return [] }

        // Chronological order: oldest first (top-left → bottom-right).
        let ordered = days.sorted { $0.date < $1.date }
        let totalTurn = ordered.reduce(0.0) { $0 + max($1.turn, 0.001) }
        let areas = ordered.map { max($0.turn, 0.001) / totalTurn * Double(rect.width * rect.height) }

        return layoutSquarified(items: ordered, areas: areas, in: rect)
    }

    private func layoutSquarified(items: [CNSymbolDay], areas: [Double], in rect: CGRect) -> [(rect: CGRect, day: CNSymbolDay)] {
        guard !items.isEmpty else { return [] }

        var result: [(rect: CGRect, day: CNSymbolDay)] = []
        var remaining = Array(zip(items, areas))
        var currentRect = rect

        while !remaining.isEmpty {
            let isWide = currentRect.width >= currentRect.height
            let sideLength = isWide ? Double(currentRect.height) : Double(currentRect.width)

            guard sideLength > 0 else { break }

            var row: [(day: CNSymbolDay, area: Double)] = []
            var rowArea = 0.0

            for item in remaining {
                let testRow = row + [(day: item.0, area: item.1)]
                let testArea = rowArea + item.1

                if row.isEmpty || worstAspect(row: testRow, totalArea: testArea, sideLength: sideLength) <=
                    worstAspect(row: row, totalArea: rowArea, sideLength: sideLength) {
                    row = testRow
                    rowArea = testArea
                } else {
                    break
                }
            }

            remaining.removeFirst(row.count)

            let rowThickness = rowArea / sideLength

            var offset: CGFloat = 0
            for item in row {
                let itemLength = item.area / rowThickness

                let itemRect: CGRect
                if isWide {
                    itemRect = CGRect(
                        x: currentRect.minX,
                        y: currentRect.minY + offset,
                        width: rowThickness,
                        height: itemLength
                    )
                } else {
                    itemRect = CGRect(
                        x: currentRect.minX + offset,
                        y: currentRect.minY,
                        width: itemLength,
                        height: rowThickness
                    )
                }

                result.append((rect: itemRect, day: item.day))
                offset += itemLength
            }

            if isWide {
                currentRect = CGRect(
                    x: currentRect.minX + rowThickness,
                    y: currentRect.minY,
                    width: currentRect.width - rowThickness,
                    height: currentRect.height
                )
            } else {
                currentRect = CGRect(
                    x: currentRect.minX,
                    y: currentRect.minY + rowThickness,
                    width: currentRect.width,
                    height: currentRect.height - rowThickness
                )
            }
        }

        return result
    }

    private func worstAspect(row: [(day: CNSymbolDay, area: Double)], totalArea: Double, sideLength: Double) -> Double {
        guard sideLength > 0, totalArea > 0 else { return .infinity }
        let thickness = totalArea / sideLength

        var worst = 0.0
        for item in row {
            let length = item.area / thickness
            guard length > 0, thickness > 0 else { continue }
            let ratio = max(length / thickness, thickness / length)
            worst = max(worst, ratio)
        }
        return worst
    }

    // MARK: - Drawing

    private func drawTreemap(context: GraphicsContext, size: CGSize, days: [CNSymbolDay]) {
        for item in layout {
            let rect = item.rect
            let day = item.day

            let inset = rect.insetBy(dx: 0.5, dy: 0.5)
            guard inset.width > 0, inset.height > 0 else { continue }

            let color = pctChgColor(day.pctChg)
            context.fill(Path(inset), with: .color(color))

            guard inset.width > 24, inset.height > 16 else { continue }

            let priceText = day.close >= 100 ? String(format: "%.0f", day.close) : String(format: "%.2f", day.close)
            let dateStr = String(day.date.suffix(5))
            let fontSize: CGFloat = min(inset.width / 5, inset.height / 3.5, 14)
            guard fontSize >= 5 else { continue }

            let showDate = inset.height > 28
            let priceY = showDate ? inset.midY - fontSize * 0.55 : inset.midY

            let priceLabel = Text(priceText)
                .font(.system(size: fontSize, weight: .semibold))
                .foregroundColor(.white.opacity(0.9))
            context.draw(
                context.resolve(priceLabel),
                at: CGPoint(x: inset.midX, y: priceY),
                anchor: .center
            )

            if showDate {
                let dateLabel = Text(dateStr)
                    .font(.system(size: fontSize * 0.75, weight: .regular))
                    .foregroundColor(.white.opacity(0.5))
                context.draw(
                    context.resolve(dateLabel),
                    at: CGPoint(x: inset.midX, y: inset.midY + fontSize * 0.55),
                    anchor: .center
                )
            }
        }
    }

    // MARK: - Color

    private func pctChgColor(_ pctChg: Double) -> Color {
        if abs(pctChg) < 0.05 {
            return Color(red: 0.2, green: 0.2, blue: 0.2)
        }

        let t = min(abs(pctChg) / 10.0, 1.0)

        if pctChg > 0 {
            return Color(red: 0.05 * (1 - t), green: 0.25 + 0.55 * t, blue: 0.08 * (1 - t))
        } else {
            return Color(red: 0.3 + 0.7 * t, green: 0.08 * (1 - t), blue: 0.05 * (1 - t))
        }
    }
}
