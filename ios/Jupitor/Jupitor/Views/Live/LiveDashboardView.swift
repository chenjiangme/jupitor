import SwiftUI

struct LiveDashboardView: View {
    @Environment(DashboardViewModel.self) private var vm
    @State private var showingSettings = false
    @State private var selectedDay: DaySelection = .today

    enum DaySelection: String, CaseIterable {
        case today = "TODAY"
        case next = "NEXT DAY"
    }

    var body: some View {
        @Bindable var vm = vm

        ZStack {
            Color.black.ignoresSafeArea()

            if vm.isLoading && vm.today == nil {
                ProgressView("Connecting...")
                    .foregroundStyle(.secondary)
            } else if let today = vm.today {
                VStack(spacing: 0) {
                    // Day picker (only when next exists).
                    if vm.next != nil {
                        Picker("Day", selection: $selectedDay) {
                            ForEach(DaySelection.allCases, id: \.self) { d in
                                Text(d.rawValue).tag(d)
                            }
                        }
                        .pickerStyle(.segmented)
                        .padding(.horizontal)
                        .padding(.top, 8)
                    }

                    // Session toggle.
                    Picker("Session", selection: $vm.sessionView) {
                        ForEach(SessionView.allCases, id: \.self) { s in
                            Text(s.rawValue).tag(s)
                        }
                    }
                    .pickerStyle(.segmented)
                    .padding(.horizontal)
                    .padding(.vertical, 8)

                    let day = selectedDay == .next ? (vm.next ?? today) : today
                    SymbolListView(day: day, date: vm.date)
                }
            } else if let error = vm.error {
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
            }
        }
        .navigationTitle(vm.date)
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarLeading) {
                HStack(spacing: 4) {
                    Circle()
                        .fill(.green)
                        .frame(width: 8, height: 8)
                        .pulseAnimation()
                    Text("LIVE")
                        .font(.caption2.bold())
                        .foregroundStyle(.green)
                }
            }

            ToolbarItem(placement: .topBarTrailing) {
                HStack(spacing: 12) {
                    sortMenu
                    Button { showingSettings = true } label: {
                        Image(systemName: "gear")
                    }
                }
            }
        }
        .sheet(isPresented: $showingSettings) {
            SettingsView()
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
