import SwiftUI

struct RootTabView: View {
    @Environment(DashboardViewModel.self) private var vm
    @State private var currentDate: String = ""
    @State private var selectedDay: DaySelection = .today
    @State private var showingSettings = false
    @State private var panOffset: CGFloat = 0
    @State private var isTransitioning = false
    @State private var dragLocked: Bool? // nil=undetermined, true=horizontal, false=vertical

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
    private var displayDate: String {
        if selectedDay == .next, currentIndex + 1 < allDates.count {
            return allDates[currentIndex + 1]
        }
        return currentDate
    }

    private func navigate(by delta: Int) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < allDates.count else { return }
        currentDate = allDates[newIndex]
        selectedDay = .today
    }

    private func commitSwipe(offset: CGFloat) {
        let threshold: CGFloat = 80
        let w = UIScreen.main.bounds.width

        if offset < -threshold && canGoForward {
            isTransitioning = true
            // Slide current content off-screen left.
            withAnimation(.easeOut(duration: 0.15)) { panOffset = -w }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                navigate(by: 1)
                panOffset = w // Place new content off-screen right (instant).
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { panOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isTransitioning = false
            }
        } else if offset > threshold && canGoBack {
            isTransitioning = true
            // Slide current content off-screen right.
            withAnimation(.easeOut(duration: 0.15)) { panOffset = w }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                navigate(by: -1)
                panOffset = -w // Place new content off-screen left (instant).
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { panOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isTransitioning = false
            }
        } else {
            // Below threshold — snap back.
            withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) { panOffset = 0 }
        }
    }

    var body: some View {
        NavigationStack {
            ZStack {
                Color.black.ignoresSafeArea()

                Group {
                    if currentDate.isEmpty {
                        ProgressView("Connecting...")
                            .foregroundStyle(.secondary)
                    } else if isDataLoading {
                        ProgressView()
                            .foregroundStyle(.secondary)
                    } else if let day = dayData {
                        let displayDay = selectedDay == .next ? (nextData ?? day) : day
                        BubbleChartView(day: displayDay, date: displayDate)
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
                .offset(x: panOffset)
            }
            .navigationTitle(currentDate)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    HStack(spacing: 8) {
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

                        if nextData != nil {
                            HStack(spacing: 4) {
                                Button {
                                    selectedDay = .today
                                } label: {
                                    Text("T")
                                        .font(.caption2.bold())
                                        .foregroundStyle(selectedDay == .today ? .white : .secondary)
                                }
                                Button {
                                    selectedDay = .next
                                } label: {
                                    Text("N")
                                        .font(.caption2.bold())
                                        .foregroundStyle(selectedDay == .next ? .white : .secondary)
                                }
                            }
                        }
                    }
                    .fixedSize()
                }

                ToolbarItem(placement: .topBarTrailing) {
                    Button { showingSettings = true } label: {
                        Image(systemName: "gear")
                    }
                }
            }
            .simultaneousGesture(
                DragGesture(minimumDistance: 30)
                    .onChanged { value in
                        guard !isTransitioning else { return }
                        let t = value.translation
                        if dragLocked == nil {
                            dragLocked = abs(t.width) > abs(t.height)
                        }
                        guard dragLocked == true else { return }
                        if (t.width < 0 && canGoForward) || (t.width > 0 && canGoBack) {
                            panOffset = t.width
                        } else {
                            panOffset = 0
                        }
                    }
                    .onEnded { value in
                        let wasHorizontal = dragLocked == true
                        dragLocked = nil
                        guard !isTransitioning, wasHorizontal else {
                            panOffset = 0
                            return
                        }
                        commitSwipe(offset: value.translation.width)
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
