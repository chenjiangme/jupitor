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
    @State private var sessionMode: SessionMode = .day
    @State private var panOffset: CGFloat = 0
    @State private var verticalOffset: CGFloat = 0
    @State private var isTransitioning = false
    @State private var isVerticalTransitioning = false
    @State private var dragLocked: Bool?

    init(date: String) {
        _currentDate = State(initialValue: date)
    }

    private var currentIndex: Int {
        vm.historyDates.firstIndex(of: currentDate) ?? 0
    }
    private var canGoBack: Bool { currentIndex > 0 }
    private var canGoForward: Bool { currentIndex < vm.historyDates.count - 1 }
    private var displayDate: String {
        if sessionMode == .next, currentIndex + 1 < vm.historyDates.count {
            return vm.historyDates[currentIndex + 1]
        }
        return currentDate
    }

    private var availableModes: [SessionMode] {
        vm.historyNext != nil ? SessionMode.allCases : [.pre, .reg, .day]
    }

    private func navigate(by delta: Int) {
        let newIndex = currentIndex + delta
        guard newIndex >= 0, newIndex < vm.historyDates.count else { return }
        currentDate = vm.historyDates[newIndex]
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
        ZStack {
            Color.black.ignoresSafeArea()

            Group {
                if vm.isLoadingHistory {
                    ProgressView()
                } else if let day = vm.historyDay {
                    let displayDay = sessionMode == .next ? (vm.historyNext ?? day) : day
                    BubbleChartView(day: displayDay, date: displayDate, sessionMode: sessionMode)
                } else {
                    Text("Failed to load data")
                        .foregroundStyle(.secondary)
                }
            }
            .offset(x: panOffset, y: verticalOffset)
        }
        .navigationTitle(currentDate)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarLeading) {
                Text(sessionMode.label)
                    .font(.caption2.bold())
                    .foregroundStyle(sessionMode == .day ? Color.secondary : Color.white)
            }
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
        .onChange(of: vm.historyNext == nil) { _, noNext in
            if noNext && sessionMode == .next { sessionMode = .day }
        }
        .task(id: currentDate) {
            await vm.loadHistory(date: currentDate)
        }
    }
}
