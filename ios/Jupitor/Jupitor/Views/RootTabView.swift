import SwiftUI

struct RootTabView: View {
    @Environment(DashboardViewModel.self) private var vm
    @AppStorage("showDayMode") private var showDayMode = false
    @AppStorage("useConcentricView") private var useConcentricView = false
    @State private var currentDate: String = ""
    @State private var sessionMode: SessionMode = .pre
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
        if vm.isReplaying, let replay = vm.replayDayData {
            return replay
        }
        return isLive ? vm.today : vm.historyDay
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

    /// Available session modes for current state.
    private var availableModes: [SessionMode] {
        var modes: [SessionMode] = [.pre, .reg]
        if showDayMode { modes.append(.day) }
        if nextData != nil { modes.append(.next) }
        return modes
    }

    private var replayTimeLabel: String {
        guard let ts = vm.replayTime else { return "" }
        let date = Date(timeIntervalSince1970: Double(ts) / 1000.0)
        let fmt = DateFormatter()
        fmt.timeZone = TimeZone(identifier: "America/New_York")
        fmt.dateFormat = "h:mm:ss a"
        return fmt.string(from: date)
    }

    private func navigate(by delta: Int) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < allDates.count else { return }
        currentDate = allDates[newIndex]
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
                sessionMode.backgroundColor.ignoresSafeArea()

                Group {
                    if currentDate.isEmpty {
                        ProgressView("Connecting...")
                            .foregroundStyle(.secondary)
                    } else if isDataLoading {
                        ProgressView()
                            .foregroundStyle(.secondary)
                    } else if let day = dayData {
                        let displayDay = vm.isReplaying ? day : (sessionMode == .next ? (nextData ?? day) : day)
                        if useConcentricView {
                            ConcentricRingView(day: displayDay, date: displayDate, watchlistDate: currentDate, sessionMode: sessionMode)
                                .transition(.opacity)
                        } else {
                            BubbleChartView(day: displayDay, date: displayDate, watchlistDate: currentDate, sessionMode: sessionMode)
                                .transition(.opacity)
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
                .offset(x: panOffset, y: verticalOffset)
            }
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    HStack(spacing: 6) {
                        if isLive && !vm.isReplaying {
                            HStack(spacing: 4) {
                                Circle()
                                    .fill(.green)
                                    .frame(width: 8, height: 8)
                                    .pulseAnimation()
                                Text("LIVE")
                                    .font(.caption2.bold())
                                    .foregroundStyle(.green)
                            }
                            .fixedSize()
                        } else if !vm.isReplaying {
                            Button {
                                currentDate = vm.date
                                sessionMode = .reg
                            } label: {
                                Text("LIVE")
                                    .font(.caption2.bold())
                                    .foregroundStyle(.secondary)
                            }
                        }
                        Button {
                            if vm.isReplaying {
                                vm.toggleReplay()
                            } else {
                                Task { await vm.startReplay(date: currentDate, sessionMode: sessionMode) }
                            }
                        } label: {
                            Image(systemName: vm.isReplaying ? "stop.circle.fill" : "play.circle")
                                .foregroundStyle(vm.isReplaying ? .orange : .secondary)
                        }
                    }
                }

                ToolbarItem(placement: .principal) {
                    VStack(spacing: vm.isReplaying ? 2 : 0) {
                        HStack(spacing: 6) {
                            Text(currentDate)
                                .font(.headline)
                            Text(sessionMode.label)
                                .font(.caption2.bold())
                                .foregroundStyle(sessionMode == .day ? Color.secondary : Color.white)
                            if !vm.isReplaying && !vm.watchlistSymbols.isEmpty {
                                Text("\(vm.watchlistSymbols.count)")
                                    .font(.caption2.bold())
                                    .foregroundStyle(Color.watchlistColor)
                            }
                        }
                        if vm.isReplaying {
                            Text(replayTimeLabel)
                                .font(.caption.monospacedDigit())
                                .foregroundStyle(.orange)
                        }
                    }
                }

                ToolbarItem(placement: .topBarTrailing) {
                    HStack(spacing: 12) {
                        Button {
                            withAnimation(.easeInOut(duration: 0.3)) { useConcentricView.toggle() }
                        } label: {
                            Image(systemName: useConcentricView ? "circle.circle" : "bubbles.and.sparkles")
                                .foregroundStyle(.secondary)
                        }
                        Button { showingSettings = true } label: {
                            Image(systemName: "gear")
                        }
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
                        if vm.isReplaying {
                            if dragLocked == true {
                                let width = UIScreen.main.bounds.width
                                let fraction = Double(value.location.x / width)
                                vm.scrubTo(fraction: fraction, date: currentDate, sessionMode: sessionMode)
                            } else {
                                verticalOffset = t.height
                            }
                        } else {
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
                    }
                    .onEnded { value in
                        let locked = dragLocked
                        dragLocked = nil
                        guard !isTransitioning, !isVerticalTransitioning else {
                            panOffset = 0
                            verticalOffset = 0
                            return
                        }
                        if vm.isReplaying {
                            if locked == true {
                                vm.scrubEnded()
                            } else {
                                commitVerticalSwipe(offset: value.translation.height)
                            }
                        } else {
                            if locked == true {
                                commitSwipe(offset: value.translation.width)
                            } else {
                                commitVerticalSwipe(offset: value.translation.height)
                            }
                        }
                    }
            )
            .sheet(isPresented: $showingSettings) {
                SettingsView()
            }
            .onChange(of: vm.date) { oldDate, newDate in
                let isFirst = currentDate.isEmpty
                if !newDate.isEmpty && (isFirst || currentDate == oldDate) {
                    currentDate = newDate
                }
                // Auto-detect session on first load.
                if isFirst && !newDate.isEmpty {
                    if let next = vm.next, !next.tiers.isEmpty {
                        sessionMode = .next
                    } else if let today = vm.today, today.regCount > 0 {
                        sessionMode = .reg
                    } else {
                        sessionMode = .pre
                    }
                }
            }
            .onChange(of: nextData == nil) { _, noNext in
                if noNext && sessionMode == .next { sessionMode = .reg }
            }
            .onChange(of: showDayMode) { _, show in
                if !show && sessionMode == .day { sessionMode = .reg }
            }
            .onChange(of: sessionMode) { _, newMode in
                if vm.isReplaying {
                    vm.replaySessionChanged(date: currentDate, sessionMode: newMode)
                }
            }
            .task(id: currentDate) {
                if vm.isReplaying { vm.toggleReplay() }
                if !currentDate.isEmpty && !isLive {
                    await vm.loadHistory(date: currentDate)
                }
                if !currentDate.isEmpty {
                    await vm.updateDisplayDate(currentDate)
                }
            }
        }
    }
}
