import SwiftUI

struct CNDatePickerSheet: View {
    @Environment(CNHeatmapViewModel.self) private var vm
    @Environment(\.dismiss) private var dismiss

    @State private var displayYear: Int = 0
    @State private var displayMonth: Int = 0
    @State private var mode: PickerMode = .calendar

    private enum PickerMode {
        case calendar, monthYear
    }

    // Pre-parsed set of available dates for O(1) lookup.
    private var availableDates: Set<String> {
        Set(vm.dates)
    }

    // All unique year-months from available dates, sorted.
    private var availableMonths: [(year: Int, month: Int)] {
        var seen = Set<String>()
        var result: [(year: Int, month: Int)] = []
        for date in vm.dates {
            let ym = String(date.prefix(7)) // "YYYY-MM"
            if seen.insert(ym).inserted, let (y, m) = parseYM(ym) {
                result.append((year: y, month: m))
            }
        }
        return result
    }

    var body: some View {
        NavigationStack {
            Group {
                if displayMonth == 0 {
                    Color.clear
                } else {
                    VStack(spacing: 0) {
                        switch mode {
                        case .calendar:
                            calendarView
                        case .monthYear:
                            monthYearPicker
                        }
                    }
                }
            }
            .navigationTitle("Select Date")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
            .onAppear {
                initDisplay()
            }
        }
    }

    // MARK: - Calendar View

    private var calendarView: some View {
        VStack(spacing: 12) {
            // Month/year header with navigation.
            HStack {
                Button { navigateMonth(by: -1) } label: {
                    Image(systemName: "chevron.left")
                        .font(.title3.weight(.medium))
                }
                .disabled(!canNavigateMonth(by: -1))

                Spacer()

                Button {
                    withAnimation(.easeInOut(duration: 0.2)) { mode = .monthYear }
                } label: {
                    Text(monthYearLabel(year: displayYear, month: displayMonth))
                        .font(.headline)
                        .foregroundStyle(.primary)
                }

                Spacer()

                Button { navigateMonth(by: 1) } label: {
                    Image(systemName: "chevron.right")
                        .font(.title3.weight(.medium))
                }
                .disabled(!canNavigateMonth(by: 1))
            }
            .padding(.horizontal, 20)
            .padding(.top, 16)

            // Weekday headers.
            HStack(spacing: 0) {
                ForEach(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"], id: \.self) { day in
                    Text(day)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity)
                }
            }
            .padding(.horizontal, 12)

            // Day grid.
            let cells = calendarCells(year: displayYear, month: displayMonth)
            let rows = cells.chunked(into: 7)

            LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 0), count: 7), spacing: 6) {
                ForEach(Array(cells.enumerated()), id: \.offset) { _, cell in
                    dayCell(cell)
                }
            }
            .padding(.horizontal, 12)

            Spacer()
        }
    }

    @ViewBuilder
    private func dayCell(_ cell: CalendarCell) -> some View {
        if let day = cell.day {
            let dateStr = String(format: "%04d-%02d-%02d", displayYear, displayMonth, day)
            let isAvailable = availableDates.contains(dateStr)
            let isCurrent = dateStr == vm.currentDate

            Button {
                if isAvailable {
                    vm.navigateTo(dateStr)
                    dismiss()
                }
            } label: {
                Text("\(day)")
                    .font(.body.monospacedDigit())
                    .frame(width: 38, height: 38)
                    .background(
                        isCurrent
                            ? AnyShapeStyle(.orange)
                            : isAvailable
                                ? AnyShapeStyle(Color.white.opacity(0.1))
                                : AnyShapeStyle(.clear)
                    )
                    .clipShape(Circle())
                    .foregroundStyle(
                        isCurrent ? .white
                            : isAvailable ? .primary
                            : Color.secondary.opacity(0.3)
                    )
            }
            .disabled(!isAvailable)
        } else {
            Color.clear
                .frame(width: 38, height: 38)
        }
    }

    // MARK: - Month/Year Picker

    private var monthYearPicker: some View {
        let months = availableMonths
        let years = Array(Set(months.map(\.year))).sorted()

        return ScrollViewReader { proxy in
            List {
                ForEach(years.reversed(), id: \.self) { year in
                    Section {
                        let yearMonths = months.filter { $0.year == year }.sorted { $0.month > $1.month }
                        ForEach(yearMonths, id: \.month) { ym in
                            let isSelected = ym.year == displayYear && ym.month == displayMonth
                            Button {
                                displayYear = ym.year
                                displayMonth = ym.month
                                withAnimation(.easeInOut(duration: 0.2)) { mode = .calendar }
                            } label: {
                                HStack {
                                    Text(monthYearLabel(year: ym.year, month: ym.month))
                                        .foregroundStyle(isSelected ? .orange : .primary)
                                    Spacer()
                                    // Count of trading days in that month.
                                    let count = tradingDayCount(year: ym.year, month: ym.month)
                                    Text("\(count) days")
                                        .foregroundStyle(.secondary)
                                        .font(.caption)
                                    if isSelected {
                                        Image(systemName: "checkmark")
                                            .foregroundStyle(.orange)
                                    }
                                }
                            }
                            .id("\(ym.year)-\(ym.month)")
                        }
                    } header: {
                        Text(String(year))
                            .font(.title3.weight(.semibold))
                    }
                }
            }
            .listStyle(.insetGrouped)
            .onAppear {
                proxy.scrollTo("\(displayYear)-\(displayMonth)", anchor: .center)
            }
        }
    }

    // MARK: - Helpers

    private func initDisplay() {
        guard !vm.currentDate.isEmpty else { return }
        if let (y, m) = parseYM(String(vm.currentDate.prefix(7))) {
            displayYear = y
            displayMonth = m
        }
    }

    private func parseYM(_ ym: String) -> (Int, Int)? {
        let parts = ym.split(separator: "-")
        guard parts.count >= 2, let y = Int(parts[0]), let m = Int(parts[1]) else { return nil }
        return (y, m)
    }

    private func monthYearLabel(year: Int, month: Int) -> String {
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "en_US")
        let monthName = formatter.monthSymbols[month - 1]
        return "\(monthName) \(year)"
    }

    private func navigateMonth(by delta: Int) {
        var m = displayMonth + delta
        var y = displayYear
        if m > 12 { m = 1; y += 1 }
        if m < 1 { m = 12; y -= 1 }
        displayYear = y
        displayMonth = m
    }

    private func canNavigateMonth(by delta: Int) -> Bool {
        var m = displayMonth + delta
        var y = displayYear
        if m > 12 { m = 1; y += 1 }
        if m < 1 { m = 12; y -= 1 }
        return availableMonths.contains { $0.year == y && $0.month == m }
    }

    private func tradingDayCount(year: Int, month: Int) -> Int {
        let prefix = String(format: "%04d-%02d", year, month)
        return vm.dates.filter { $0.hasPrefix(prefix) }.count
    }

    // MARK: - Calendar Grid

    private struct CalendarCell {
        let day: Int? // nil = empty padding cell
    }

    private func calendarCells(year: Int, month: Int) -> [CalendarCell] {
        var cal = Calendar(identifier: .gregorian)
        cal.firstWeekday = 2 // Monday

        guard let firstOfMonth = cal.date(from: DateComponents(year: year, month: month, day: 1)),
              let range = cal.range(of: .day, in: .month, for: firstOfMonth) else {
            return []
        }

        // Weekday of first day (1=Sun..7=Sat → convert to Mon=0..Sun=6).
        let weekday = cal.component(.weekday, from: firstOfMonth)
        let mondayOffset = (weekday + 5) % 7 // Mon=0, Tue=1, ..., Sun=6

        var cells: [CalendarCell] = []

        // Leading empty cells.
        for _ in 0..<mondayOffset {
            cells.append(CalendarCell(day: nil))
        }

        // Day cells.
        for day in range {
            cells.append(CalendarCell(day: day))
        }

        return cells
    }
}

private extension Array {
    func chunked(into size: Int) -> [[Element]] {
        stride(from: 0, to: count, by: size).map {
            Array(self[$0..<Swift.min($0 + size, count)])
        }
    }
}
