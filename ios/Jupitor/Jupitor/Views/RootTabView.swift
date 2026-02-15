import SwiftUI

struct RootTabView: View {
    @Environment(DashboardViewModel.self) private var vm
    @State private var currentDate: String = ""
    @State private var selectedDay: DaySelection = .today
    @State private var showingSettings = false

    enum DaySelection: String, CaseIterable {
        case today = "TODAY"
        case next = "NEXT DAY"
    }

    // All navigable dates: history + live (ascending).
    private var allDates: [String] {
        var dates = vm.historyDates
        if !vm.date.isEmpty && (dates.isEmpty || dates.last! < vm.date) {
            dates.append(vm.date)
        }
        return dates
    }

    private var currentIndex: Int {
        allDates.firstIndex(of: currentDate) ?? max(0, allDates.count - 1)
    }
    private var canGoBack: Bool { currentIndex > 0 }
    private var canGoForward: Bool { currentIndex < allDates.count - 1 }
    private var isLive: Bool { !vm.date.isEmpty && currentDate == vm.date }

    // Data for current view.
    private var dayData: DayDataJSON? {
        isLive ? vm.today : vm.historyDay
    }
    private var nextData: DayDataJSON? {
        isLive ? vm.next : vm.historyNext
    }
    private var isDataLoading: Bool {
        isLive ? (vm.isLoading && vm.today == nil) : vm.isLoadingHistory
    }

    private func navigate(by delta: Int) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < allDates.count else { return }
        currentDate = allDates[newIndex]
        selectedDay = .today
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Color.black.ignoresSafeArea()

                if currentDate.isEmpty {
                    ProgressView("Connecting...")
                        .foregroundStyle(.secondary)
                } else if isDataLoading {
                    ProgressView()
                        .foregroundStyle(.secondary)
                } else if let day = dayData {
                    VStack(spacing: 0) {
                        if nextData != nil {
                            Picker("Day", selection: $selectedDay) {
                                ForEach(DaySelection.allCases, id: \.self) { d in
                                    Text(d.rawValue).tag(d)
                                }
                            }
                            .pickerStyle(.segmented)
                            .padding(.horizontal)
                            .padding(.top, 8)
                        }

                        let displayDay = selectedDay == .next ? (nextData ?? day) : day
                        BubbleChartView(day: displayDay, date: currentDate)
                    }
                } else if isLive, let error = vm.error {
                    VStack(spacing: 12) {
                        Image(systemName: "wifi.slash")
                            .font(.largeTitle)
                            .foregroundStyle(.secondary)
                        Text(error)
                            .foregroundStyle(.secondary)
                            .multilineTextAlignment(.center)
                        Button("Retry") { vm.start() }
                            .buttonStyle(.bordered)
                    }
                    .padding()
                } else {
                    Text("No data")
                        .foregroundStyle(.secondary)
                }
            }
            .navigationTitle(currentDate)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    if isLive {
                        HStack(spacing: 4) {
                            Circle()
                                .fill(.green)
                                .frame(width: 8, height: 8)
                                .pulseAnimation()
                            Text("LIVE")
                                .font(.caption2.bold())
                                .foregroundStyle(.green)
                        }
                    } else {
                        Button {
                            currentDate = vm.date
                            selectedDay = .today
                        } label: {
                            Text("LIVE")
                                .font(.caption2.bold())
                                .foregroundStyle(.secondary)
                        }
                    }
                }

                ToolbarItem(placement: .topBarTrailing) {
                    HStack(spacing: 12) {
                        Button { navigate(by: -1) } label: {
                            Image(systemName: "chevron.left")
                        }
                        .disabled(!canGoBack)

                        Button { navigate(by: 1) } label: {
                            Image(systemName: "chevron.right")
                        }
                        .disabled(!canGoForward)

                        Button { showingSettings = true } label: {
                            Image(systemName: "gear")
                        }
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
            .sheet(isPresented: $showingSettings) {
                SettingsView()
            }
            .onChange(of: vm.date) { oldDate, newDate in
                if !newDate.isEmpty && (currentDate.isEmpty || currentDate == oldDate) {
                    currentDate = newDate
                }
            }
            .task(id: currentDate) {
                if !currentDate.isEmpty && !isLive {
                    await vm.loadHistory(date: currentDate)
                }
            }
        }
    }
}
