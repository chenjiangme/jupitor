import SwiftUI

struct HistoryDateListView: View {
    @Environment(DashboardViewModel.self) private var vm

    var body: some View {
        List(vm.historyDates.reversed(), id: \.self) { date in
            NavigationLink {
                HistoryDayView(date: date)
            } label: {
                Text(date)
                    .font(.body.monospacedDigit())
            }
        }
        .navigationTitle("History")
        .navigationBarTitleDisplayMode(.inline)
        .overlay {
            if vm.historyDates.isEmpty {
                ContentUnavailableView(
                    "No History",
                    systemImage: "calendar.badge.exclamationmark"
                )
            }
        }
    }
}

// MARK: - History Day View

struct HistoryDayView: View {
    @Environment(DashboardViewModel.self) private var vm
    let date: String
    @State private var selectedDay: DaySelection = .today

    enum DaySelection: String, CaseIterable {
        case today = "TODAY"
        case next = "NEXT DAY"
    }

    var body: some View {
        @Bindable var vm = vm

        ZStack {
            Color.black.ignoresSafeArea()

            if vm.isLoadingHistory {
                ProgressView()
            } else if let day = vm.historyDay {
                VStack(spacing: 0) {
                    if vm.historyNext != nil {
                        Picker("Day", selection: $selectedDay) {
                            ForEach(DaySelection.allCases, id: \.self) { d in
                                Text(d.rawValue).tag(d)
                            }
                        }
                        .pickerStyle(.segmented)
                        .padding(.horizontal)
                        .padding(.top, 8)
                    }

                    Picker("Session", selection: $vm.sessionView) {
                        ForEach(SessionView.allCases, id: \.self) { s in
                            Text(s.rawValue).tag(s)
                        }
                    }
                    .pickerStyle(.segmented)
                    .padding(.horizontal)
                    .padding(.vertical, 8)

                    let displayDay = selectedDay == .next ? (vm.historyNext ?? day) : day
                    BubbleChartView(day: displayDay, date: date)
                }
            } else {
                Text("Failed to load data")
                    .foregroundStyle(.secondary)
            }
        }
        .navigationTitle(date)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                sortMenu
            }
        }
        .task {
            await vm.loadHistory(date: date)
        }
    }

    private var sortMenu: some View {
        Menu {
            Section("Pre-Market") {
                sortButton(.preTrades)
                sortButton(.preGain)
                sortButton(.preTurnover)
            }
            Section("Regular") {
                sortButton(.regTrades)
                sortButton(.regGain)
                sortButton(.regTurnover)
            }
            Section {
                sortButton(.news)
            }
        } label: {
            Text(vm.sortLabel)
                .font(.caption.bold())
        }
    }

    private func sortButton(_ mode: SortMode) -> some View {
        Button {
            Task { await vm.setSortMode(mode) }
        } label: {
            if vm.sortMode == mode {
                Label(mode.label, systemImage: "checkmark")
            } else {
                Text(mode.label)
            }
        }
    }
}
