import SwiftUI

struct RootTabView: View {
    @Environment(DashboardViewModel.self) private var vm
    @State private var currentDate: String = ""
    @State private var sessionMode: SessionMode = .day
    @State private var showingSettings = false
    @State private var panOffset: CGFloat = 0
    @State private var isTransitioning = false
    @State private var dragLocked: Bool? // nil=undetermined, true=horizontal, false=vertical
    @State private var verticalOffset: CGFloat = 0
    @State private var isVerticalTransitioning = false

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
        if sessionMode == .next, currentIndex + 1 < allDates.count {
            return allDates[currentIndex + 1]
        }
        return currentDate
    }

    /// Available session modes for current state (excludes .next when no next data).
    private var availableModes: [SessionMode] {
        nextData != nil ? SessionMode.allCases : [.pre, .reg, .day]
    }

    private func navigate(by delta: Int) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < allDates.count else { return }
        currentDate = allDates[newIndex]
        sessionMode = .day
    }

    private func cycleSessionMode(direction: Int) {
        let modes = availableModes
        guard let idx = modes.firstIndex(of: sessionMode) else {
            sessionMode = .day
            return
        }
        let newIdx = idx + direction
        guard newIdx >= 0, newIdx < modes.count else { return }
        sessionMode = modes[newIdx]
    }

    private func commitSwipe(offset: CGFloat) {
        let threshold: CGFloat = 80
        let w = UIScreen.main.bounds.width

        if offset < -threshold && canGoForward {
            isTransitioning = true
            withAnimation(.easeOut(duration: 0.15)) { panOffset = -w }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                navigate(by: 1)
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
                navigate(by: -1)
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

    private func commitVerticalSwipe(offset: CGFloat) {
        let threshold: CGFloat = 60
        let h = UIScreen.main.bounds.height
        let modes = availableModes
        guard let idx = modes.firstIndex(of: sessionMode) else {
            withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) { verticalOffset = 0 }
            return
        }

        // Swipe up (negative offset) → next mode, swipe down → previous mode.
        if offset < -threshold && idx + 1 < modes.count {
            isVerticalTransitioning = true
            withAnimation(.easeOut(duration: 0.15)) { verticalOffset = -h }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                sessionMode = modes[idx + 1]
                verticalOffset = h
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { verticalOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isVerticalTransitioning = false
            }
        } else if offset > threshold && idx > 0 {
            isVerticalTransitioning = true
            withAnimation(.easeOut(duration: 0.15)) { verticalOffset = h }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.18) {
                sessionMode = modes[idx - 1]
                verticalOffset = -h
                DispatchQueue.main.async {
                    withAnimation(.easeOut(duration: 0.2)) { verticalOffset = 0 }
                }
            }
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.45) {
                isVerticalTransitioning = false
            }
        } else {
            withAnimation(.spring(response: 0.3, dampingFraction: 0.8)) { verticalOffset = 0 }
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
                        let displayDay = sessionMode == .next ? (nextData ?? day) : day
                        BubbleChartView(day: displayDay, date: displayDate, sessionMode: sessionMode)
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
                .offset(x: panOffset, y: verticalOffset)
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
                                sessionMode = .day
                            } label: {
                                Text("LIVE")
                                    .font(.caption2.bold())
                                    .foregroundStyle(.secondary)
                            }
                        }

                        Text(sessionMode.label)
                            .font(.caption2.bold())
                            .foregroundStyle(sessionMode == .day ? .secondary : .white)
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
                        guard !isTransitioning, !isVerticalTransitioning else { return }
                        let t = value.translation
                        if dragLocked == nil {
                            dragLocked = abs(t.width) > abs(t.height)
                        }
                        if dragLocked == true {
                            if (t.width < 0 && canGoForward) || (t.width > 0 && canGoBack) {
                                panOffset = t.width
                            } else {
                                panOffset = 0
                            }
                        } else {
                            verticalOffset = t.height
                        }
                    }
                    .onEnded { value in
                        let locked = dragLocked
                        dragLocked = nil
                        guard !isTransitioning, !isVerticalTransitioning else {
                            panOffset = 0
                            verticalOffset = 0
                            return
                        }
                        if locked == true {
                            commitSwipe(offset: value.translation.width)
                        } else {
                            commitVerticalSwipe(offset: value.translation.height)
                        }
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
            .onChange(of: nextData == nil) { _, noNext in
                if noNext && sessionMode == .next { sessionMode = .day }
            }
            .task(id: currentDate) {
                if !currentDate.isEmpty && !isLive {
                    await vm.loadHistory(date: currentDate)
                }
            }
        }
    }
}
