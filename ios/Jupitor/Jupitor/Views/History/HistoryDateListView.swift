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
    @State private var panOffset: CGFloat = 0
    @State private var isTransitioning = false
    @State private var dragLocked: Bool?

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

    var body: some View {
        ZStack {
            Color.black.ignoresSafeArea()

            Group {
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
            .offset(x: panOffset)
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
        .task(id: currentDate) {
            await vm.loadHistory(date: currentDate)
        }
    }
}
