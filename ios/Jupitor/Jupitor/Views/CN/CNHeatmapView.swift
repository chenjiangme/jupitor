import SwiftUI

struct CNHeatmapView: View {
    @Environment(CNHeatmapViewModel.self) private var vm

    @State private var selectedStock: CNHeatmapStock?
    @State private var layout: [(rect: CGRect, stock: CNHeatmapStock)] = []
    @State private var lastSize: CGSize = .zero

    // Zoom / pan state.
    @State private var scale: CGFloat = 1.0
    @State private var lastScale: CGFloat = 1.0
    @State private var offset: CGSize = .zero
    @State private var lastOffset: CGSize = .zero

    private func resetZoom() {
        withAnimation(.easeOut(duration: 0.2)) {
            scale = 1.0
            lastScale = 1.0
            offset = .zero
            lastOffset = .zero
        }
        vm.isZoomed = false
    }

    private func clampOffset(_ off: CGSize, scale: CGFloat, viewSize: CGSize) -> CGSize {
        guard scale > 1 else { return .zero }
        let maxX = (viewSize.width * scale - viewSize.width) / 2
        let maxY = (viewSize.height * scale - viewSize.height) / 2
        return CGSize(
            width: min(max(off.width, -maxX), maxX),
            height: min(max(off.height, -maxY), maxY)
        )
    }

    var body: some View {
        GeometryReader { geo in
            let size = geo.size
            ZStack {
                Color(red: 0.06, green: 0.06, blue: 0.08).ignoresSafeArea()

                if vm.currentDate.isEmpty || (vm.isLoading && vm.heatmapData == nil) {
                    ProgressView()
                        .foregroundStyle(.secondary)
                } else if let stocks = vm.filteredStocks, !stocks.isEmpty, let stats = vm.heatmapData?.stats {
                    Canvas { context, canvasSize in
                        drawTreemap(context: context, size: canvasSize, stocks: stocks, stats: stats)
                    } symbols: {
                        // Empty — we draw everything directly.
                    }
                    .scaleEffect(scale)
                    .offset(offset)
                    .contentShape(Rectangle())
                    .onTapGesture { location in
                        // Inverse transform for hit testing.
                        let center = CGPoint(x: size.width / 2, y: size.height / 2)
                        let adjusted = CGPoint(
                            x: (location.x - center.x - offset.width) / scale + center.x,
                            y: (location.y - center.y - offset.height) / scale + center.y
                        )
                        if let hit = layout.first(where: { $0.rect.contains(adjusted) }) {
                            selectedStock = hit.stock
                        }
                    }
                    .gesture(
                        TapGesture(count: 2).onEnded { resetZoom() }
                    )
                    .gesture(
                        MagnifyGesture()
                            .onChanged { value in
                                let newScale = min(max(lastScale * value.magnification, 1.0), 5.0)
                                scale = newScale
                                offset = clampOffset(lastOffset, scale: newScale, viewSize: size)
                                vm.isZoomed = newScale > 1.05
                            }
                            .onEnded { value in
                                let newScale = min(max(lastScale * value.magnification, 1.0), 5.0)
                                if newScale < 1.05 {
                                    resetZoom()
                                } else {
                                    scale = newScale
                                    lastScale = newScale
                                    offset = clampOffset(offset, scale: newScale, viewSize: size)
                                    lastOffset = offset
                                }
                            }
                    )
                    .simultaneousGesture(
                        scale > 1.05
                        ? DragGesture()
                            .onChanged { value in
                                let newOffset = CGSize(
                                    width: lastOffset.width + value.translation.width,
                                    height: lastOffset.height + value.translation.height
                                )
                                offset = clampOffset(newOffset, scale: scale, viewSize: size)
                            }
                            .onEnded { value in
                                let newOffset = CGSize(
                                    width: lastOffset.width + value.translation.width,
                                    height: lastOffset.height + value.translation.height
                                )
                                offset = clampOffset(newOffset, scale: scale, viewSize: size)
                                lastOffset = offset
                            }
                        : nil
                    )
                } else {
                    Text("No data")
                        .foregroundStyle(.secondary)
                }
            }
            .onChange(of: size) { _, newSize in
                lastSize = newSize
                recomputeLayout(size: newSize)
            }
            .task(id: "\(vm.currentDate)-\(vm.heatmapData?.date ?? "")-\(vm.indexFilter.rawValue)-\(vm.selectedIndustries.count)-\(vm.excludedIndustries.count)") {
                if lastSize.width > 0 {
                    recomputeLayout(size: lastSize)
                }
                // Reset zoom when data changes.
                if scale != 1.0 { resetZoom() }
            }
            .onAppear {
                lastSize = size
                recomputeLayout(size: size)
            }
        }
        .sheet(item: $selectedStock) { stock in
            CNSymbolHistoryView(stock: stock)
                .environment(vm)
        }
    }

    // MARK: - Layout Computation

    private func recomputeLayout(size: CGSize) {
        guard let stocks = vm.filteredStocks, !stocks.isEmpty else {
            layout = []
            return
        }

        let csi300 = stocks.filter { $0.index == "csi300" }
        let csi500 = stocks.filter { $0.index == "csi500" }

        // Single index filter: use full area, no split.
        if csi300.isEmpty {
            layout = squarify(stocks: csi500, in: CGRect(origin: .zero, size: size))
            return
        }
        if csi500.isEmpty {
            layout = squarify(stocks: csi300, in: CGRect(origin: .zero, size: size))
            return
        }

        // Both: split vertically by turnover proportion.
        let total300 = csi300.reduce(0.0) { $0 + max($1.turn, 0) }
        let total500 = csi500.reduce(0.0) { $0 + max($1.turn, 0) }
        let totalTurn = total300 + total500

        guard totalTurn > 0 else {
            layout = []
            return
        }

        let fraction300 = total300 / totalTurn
        let split300Height = size.height * fraction300

        var result: [(rect: CGRect, stock: CNHeatmapStock)] = []

        let rect300 = CGRect(x: 0, y: 0, width: size.width, height: split300Height)
        result.append(contentsOf: squarify(stocks: csi300, in: rect300))

        let rect500 = CGRect(x: 0, y: split300Height, width: size.width, height: size.height - split300Height)
        result.append(contentsOf: squarify(stocks: csi500, in: rect500))

        layout = result
    }

    // MARK: - Squarified Treemap

    private func squarify(stocks: [CNHeatmapStock], in rect: CGRect) -> [(rect: CGRect, stock: CNHeatmapStock)] {
        guard !stocks.isEmpty else { return [] }

        let totalTurn = stocks.reduce(0.0) { $0 + max($1.turn, 0.001) }
        let areas = stocks.map { max($0.turn, 0.001) / totalTurn * Double(rect.width * rect.height) }

        // Sort by area descending for better squarification.
        let sorted = zip(stocks, areas).sorted { $0.1 > $1.1 }
        let sortedStocks = sorted.map(\.0)
        let sortedAreas = sorted.map(\.1)

        return layoutSquarified(items: sortedStocks, areas: sortedAreas, in: rect)
    }

    private func layoutSquarified(items: [CNHeatmapStock], areas: [Double], in rect: CGRect) -> [(rect: CGRect, stock: CNHeatmapStock)] {
        guard !items.isEmpty else { return [] }

        var result: [(rect: CGRect, stock: CNHeatmapStock)] = []
        var remaining = Array(zip(items, areas))
        var currentRect = rect

        while !remaining.isEmpty {
            let isWide = currentRect.width >= currentRect.height
            let sideLength = isWide ? Double(currentRect.height) : Double(currentRect.width)

            guard sideLength > 0 else { break }

            // Greedily add items to the current row to minimize worst aspect ratio.
            var row: [(stock: CNHeatmapStock, area: Double)] = []
            var rowArea = 0.0

            for item in remaining {
                let testRow = row + [(stock: item.0, area: item.1)]
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

            // Lay out the row.
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

                result.append((rect: itemRect, stock: item.stock))
                offset += itemLength
            }

            // Shrink remaining rect.
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

    private func worstAspect(row: [(stock: CNHeatmapStock, area: Double)], totalArea: Double, sideLength: Double) -> Double {
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

    private func drawTreemap(context: GraphicsContext, size: CGSize, stocks: [CNHeatmapStock], stats: CNHeatmapStats) {
        for item in layout {
            let rect = item.rect
            let stock = item.stock

            // Gap between cells.
            let inset = rect.insetBy(dx: 0.5, dy: 0.5)
            guard inset.width > 0, inset.height > 0 else { continue }

            // Color by pctChg (CN: red=up, green=down).
            let color = pctChgColor(stock.pctChg)
            context.fill(Path(inset), with: .color(color))

            // Text labels on cells large enough.
            guard inset.width > 24, inset.height > 16 else { continue }

            let pctText = String(format: "%+.1f%%", stock.pctChg)

            let pctColor: Color = stock.pctChg > 0 ? Color(red: 0.1, green: 0.9, blue: 0.3) : (stock.pctChg < 0 ? .red : .white)

            let fontSize: CGFloat = min(inset.width / 5, inset.height / 3.5, 12)
            guard fontSize >= 5 else { continue }

            // Name label (primary).
            let nameLabel = Text(stock.name)
                .font(.system(size: fontSize, weight: .medium))
                .foregroundColor(.white.opacity(0.9))
            context.draw(
                context.resolve(nameLabel),
                at: CGPoint(x: inset.midX, y: inset.midY - fontSize * 0.55),
                anchor: .center
            )

            // PctChg label.
            if inset.height > 28 {
                let pctLabel = Text(pctText)
                    .font(.system(size: fontSize * 0.85, weight: .semibold))
                    .foregroundColor(pctColor)
                context.draw(
                    context.resolve(pctLabel),
                    at: CGPoint(x: inset.midX, y: inset.midY + fontSize * 0.55),
                    anchor: .center
                )
            }
        }

        // Draw divider between CSI 300 and CSI 500.
        if let firstCSI500 = layout.first(where: { $0.stock.index == "csi500" }) {
            let y = firstCSI500.rect.minY
            var path = Path()
            path.move(to: CGPoint(x: 0, y: y))
            path.addLine(to: CGPoint(x: size.width, y: y))
            context.stroke(path, with: .color(.white.opacity(0.3)), lineWidth: 1.5)
        }
    }

    // MARK: - Color Mapping

    /// Green = up, red = down (same as US). Intensity scales with magnitude.
    private func pctChgColor(_ pctChg: Double) -> Color {
        if abs(pctChg) < 0.05 {
            return Color(red: 0.2, green: 0.2, blue: 0.2)
        }

        // Clamp to ±10% for color scaling (limit up/down).
        let t = min(abs(pctChg) / 10.0, 1.0)

        if pctChg > 0 {
            // Up: dark green → bright green.
            return Color(red: 0.05 * (1 - t), green: 0.25 + 0.55 * t, blue: 0.08 * (1 - t))
        } else {
            // Down: dark red → bright red.
            return Color(red: 0.3 + 0.7 * t, green: 0.08 * (1 - t), blue: 0.05 * (1 - t))
        }
    }
}

// MARK: - Detail Sheet

struct CNStockDetailSheet: View {
    let stock: CNHeatmapStock
    let stats: CNHeatmapStats?
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            List {
                Section("Basic") {
                    row("Symbol", stock.symbol)
                    row("Name", stock.name)
                    row("Index", stock.index == "csi300" ? "CSI 300" : "CSI 500")
                    if stock.isST {
                        row("Status", "ST")
                    }
                }

                Section("Trading") {
                    row("Close", String(format: "%.2f", stock.close))
                    row("Change", String(format: "%+.2f%%", stock.pctChg))
                    row("Turnover Rate", String(format: "%.2f%%", stock.turn))
                    row("Amount", formatAmount(stock.amount))
                }

                Section("Valuation") {
                    row("PE (TTM)", stock.peTTM == 0 ? "N/A" : String(format: "%.1f", stock.peTTM))
                }

                if let stats {
                    Section("Market Context") {
                        row("Median Turnover", String(format: "%.2f%%", stats.turnP50))
                        row("P90 Turnover", String(format: "%.2f%%", stats.turnP90))
                        row("Max Turnover", String(format: "%.2f%%", stats.turnMax))
                    }
                }
            }
            .navigationTitle(stock.name)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }

    private func row(_ label: String, _ value: String) -> some View {
        HStack {
            Text(label).foregroundStyle(.secondary)
            Spacer()
            Text(value)
        }
    }

    private func formatAmount(_ amount: Double) -> String {
        if amount >= 1e8 {
            return String(format: "%.1f\u{4ebf}", amount / 1e8) // 亿
        } else if amount >= 1e4 {
            return String(format: "%.0f\u{4e07}", amount / 1e4) // 万
        }
        return String(format: "%.0f", amount)
    }
}
