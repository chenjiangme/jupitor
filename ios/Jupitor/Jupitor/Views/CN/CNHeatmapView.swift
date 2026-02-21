import SwiftUI

struct CNHeatmapView: View {
    @Environment(CNHeatmapViewModel.self) private var vm

    @State private var selectedStock: CNHeatmapStock?
    @State private var layout: [(rect: CGRect, stock: CNHeatmapStock)] = []

    var body: some View {
        GeometryReader { geo in
            let size = geo.size
            ZStack {
                Color(red: 0.06, green: 0.06, blue: 0.08).ignoresSafeArea()

                if vm.currentDate.isEmpty || (vm.isLoading && vm.heatmapData == nil) {
                    ProgressView()
                        .foregroundStyle(.secondary)
                } else if let data = vm.heatmapData, let stocks = data.stocks, !stocks.isEmpty {
                    Canvas { context, canvasSize in
                        drawTreemap(context: context, size: canvasSize, stocks: stocks, stats: data.stats)
                    } symbols: {
                        // Empty — we draw everything directly.
                    }
                    .contentShape(Rectangle())
                    .onTapGesture { location in
                        if let hit = layout.first(where: { $0.rect.contains(location) }) {
                            selectedStock = hit.stock
                        }
                    }
                    .onChange(of: vm.heatmapData?.date) { _, _ in
                        recomputeLayout(size: size)
                    }
                    .onAppear { recomputeLayout(size: size) }
                    .onChange(of: size) { _, newSize in
                        recomputeLayout(size: newSize)
                    }
                } else {
                    Text("No data")
                        .foregroundStyle(.secondary)
                }
            }
        }
        .sheet(item: $selectedStock) { stock in
            CNStockDetailSheet(stock: stock, stats: vm.heatmapData?.stats)
        }
    }

    // MARK: - Layout Computation

    private func recomputeLayout(size: CGSize) {
        guard let data = vm.heatmapData, let stocks = data.stocks, !stocks.isEmpty else {
            layout = []
            return
        }

        // Split into CSI 300 and CSI 500 groups.
        let csi300 = stocks.filter { $0.index == "csi300" }
        let csi500 = stocks.filter { $0.index == "csi500" }

        let total300 = csi300.reduce(0.0) { $0 + max($1.amount, 0) }
        let total500 = csi500.reduce(0.0) { $0 + max($1.amount, 0) }
        let totalAmount = total300 + total500

        guard totalAmount > 0 else {
            layout = []
            return
        }

        let fraction300 = total300 / totalAmount
        let split300Height = size.height * fraction300

        var result: [(rect: CGRect, stock: CNHeatmapStock)] = []

        // CSI 300 on top.
        let rect300 = CGRect(x: 0, y: 0, width: size.width, height: split300Height)
        result.append(contentsOf: squarify(stocks: csi300, in: rect300))

        // CSI 500 on bottom.
        let rect500 = CGRect(x: 0, y: split300Height, width: size.width, height: size.height - split300Height)
        result.append(contentsOf: squarify(stocks: csi500, in: rect500))

        layout = result
    }

    // MARK: - Squarified Treemap

    private func squarify(stocks: [CNHeatmapStock], in rect: CGRect) -> [(rect: CGRect, stock: CNHeatmapStock)] {
        guard !stocks.isEmpty else { return [] }

        let totalAmount = stocks.reduce(0.0) { $0 + max($1.amount, 1) }
        let areas = stocks.map { max($0.amount, 1) / totalAmount * Double(rect.width * rect.height) }

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

            // Color by turnover rate.
            let color = turnoverColor(turn: stock.turn, stats: stats)
            context.fill(Path(inset), with: .color(color))

            // Text labels on cells large enough.
            guard inset.width > 30, inset.height > 20 else { continue }

            // Stock code (last part after dot).
            let code = stock.symbol.split(separator: ".").last.map(String.init) ?? stock.symbol
            let pctText = String(format: "%+.1f%%", stock.pctChg)

            // CN convention: red = up, green = down.
            let pctColor: Color = stock.pctChg > 0 ? .red : (stock.pctChg < 0 ? Color(red: 0.1, green: 0.8, blue: 0.2) : .white)

            let fontSize: CGFloat = min(inset.width / 5.5, inset.height / 3.5, 12)
            guard fontSize >= 5 else { continue }

            // Code label.
            let codeText = Text(code)
                .font(.system(size: fontSize, weight: .medium, design: .monospaced))
                .foregroundColor(.white.opacity(0.9))
            context.draw(
                context.resolve(codeText),
                at: CGPoint(x: inset.midX, y: inset.midY - fontSize * 0.6),
                anchor: .center
            )

            // PctChg label.
            if inset.height > 30 {
                let pctLabel = Text(pctText)
                    .font(.system(size: fontSize * 0.85, weight: .semibold))
                    .foregroundColor(pctColor)
                context.draw(
                    context.resolve(pctLabel),
                    at: CGPoint(x: inset.midX, y: inset.midY + fontSize * 0.6),
                    anchor: .center
                )
            }

            // Name on larger cells.
            if inset.width > 55, inset.height > 45 {
                let nameLabel = Text(stock.name)
                    .font(.system(size: fontSize * 0.75))
                    .foregroundColor(.white.opacity(0.6))
                context.draw(
                    context.resolve(nameLabel),
                    at: CGPoint(x: inset.midX, y: inset.midY + fontSize * 1.8),
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

    private func turnoverColor(turn: Double, stats: CNHeatmapStats) -> Color {
        guard stats.turnMax > 0 else { return Color(red: 0.15, green: 0.15, blue: 0.25) }

        // Log-scale normalization using percentile stats.
        let logTurn = log(max(turn, 0.01) + 1)
        let logP50 = log(max(stats.turnP50, 0.01) + 1)
        let logP90 = log(max(stats.turnP90, 0.01) + 1)
        let logMax = log(stats.turnMax + 1)

        if logTurn <= logP50 {
            // Low: dark blue → cyan
            let t = logP50 > 0 ? logTurn / logP50 : 0
            return Color(
                red: 0.05 + 0.0 * t,
                green: 0.08 + 0.35 * t,
                blue: 0.25 + 0.35 * t
            )
        } else if logTurn <= logP90 {
            // Mid: cyan → yellow → orange
            let t = (logP90 > logP50) ? (logTurn - logP50) / (logP90 - logP50) : 0
            if t < 0.5 {
                let u = t * 2
                return Color(
                    red: 0.05 + 0.85 * u,
                    green: 0.43 + 0.37 * u,
                    blue: 0.60 - 0.50 * u
                )
            } else {
                let u = (t - 0.5) * 2
                return Color(
                    red: 0.90 + 0.10 * u,
                    green: 0.80 - 0.25 * u,
                    blue: 0.10 - 0.05 * u
                )
            }
        } else {
            // High: orange → bright red
            let t = min((logMax > logP90) ? (logTurn - logP90) / (logMax - logP90) : 1, 1)
            return Color(
                red: 1.0,
                green: 0.55 - 0.45 * t,
                blue: 0.05 - 0.05 * t
            )
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
