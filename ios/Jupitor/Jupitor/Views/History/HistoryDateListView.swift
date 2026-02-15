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
    @State private var currentDate: String
    @State private var selectedDay: DaySelection = .today

    init(date: String) {
        _currentDate = State(initialValue: date)
    }

    enum DaySelection: String, CaseIterable {
        case today = "TODAY"
        case next = "NEXT DAY"
    }

    private var currentIndex: Int {
        vm.historyDates.firstIndex(of: currentDate) ?? 0
    }
    private var canGoBack: Bool { currentIndex > 0 }
    private var canGoForward: Bool { currentIndex < vm.historyDates.count - 1 }

    private func navigate(by delta: Int) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < vm.historyDates.count else { return }
        currentDate = vm.historyDates[newIndex]
        selectedDay = .today
    }

    var body: some View {
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

                    let displayDay = selectedDay == .next ? (vm.historyNext ?? day) : day
                    BubbleChartView(day: displayDay, date: currentDate)
                }
            } else {
                Text("Failed to load data")
                    .foregroundStyle(.secondary)
            }
        }
        .navigationTitle(currentDate)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarTrailing) {
                HStack(spacing: 16) {
                    Button { navigate(by: -1) } label: {
                        Image(systemName: "chevron.left")
                    }
                    .disabled(!canGoBack)

                    Button { navigate(by: 1) } label: {
                        Image(systemName: "chevron.right")
                    }
                    .disabled(!canGoForward)
                }
            }
        }
        .gesture(
            DragGesture(minimumDistance: 60)
                .onEnded { value in
                    let dx = value.translation.width
                    let dy = value.translation.height
                    guard abs(dx) > abs(dy) * 1.5 else { return }
                    if dx > 60 { navigate(by: -1) }
                    else if dx < -60 { navigate(by: 1) }
                }
        )
        .task(id: currentDate) {
            await vm.loadHistory(date: currentDate)
        }
    }
}
