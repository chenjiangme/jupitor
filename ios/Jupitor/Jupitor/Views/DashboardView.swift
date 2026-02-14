import SwiftUI

struct DashboardView: View {
    @State private var vm = DashboardViewModel(
        baseURL: URL(string: UserDefaults.standard.string(forKey: "serverURL") ?? "http://localhost:8080")!
    )
    @State private var showingSettings = false

    var body: some View {
        NavigationStack {
            ZStack {
                Color.black.ignoresSafeArea()

                if vm.isLoading && vm.today == nil {
                    ProgressView("Connecting...")
                        .foregroundStyle(.secondary)
                } else if let today = vm.today {
                    ScrollView {
                        VStack(alignment: .leading, spacing: 0) {
                            // Session toggle.
                            Picker("Session", selection: $vm.sessionView) {
                                ForEach(SessionView.allCases, id: \.self) { s in
                                    Text(s.rawValue).tag(s)
                                }
                            }
                            .pickerStyle(.segmented)
                            .padding(.horizontal)
                            .padding(.vertical, 8)

                            // Today section.
                            DaySectionView(
                                day: today,
                                session: vm.sessionView,
                                watchlist: vm.watchlistSymbols,
                                onSelect: { sym in
                                    Task { await vm.loadNews(symbol: sym) }
                                },
                                onToggleWatchlist: { sym in
                                    Task { await vm.toggleWatchlist(symbol: sym) }
                                }
                            )

                            // Next day section.
                            if let next = vm.next {
                                Divider()
                                    .background(.gray)
                                    .padding(.vertical, 8)

                                DaySectionView(
                                    day: next,
                                    session: vm.sessionView,
                                    watchlist: vm.watchlistSymbols,
                                    onSelect: { sym in
                                        Task { await vm.loadNews(symbol: sym) }
                                    },
                                    onToggleWatchlist: { sym in
                                        Task { await vm.toggleWatchlist(symbol: sym) }
                                    }
                                )
                            }
                        }
                        .padding(.bottom, 20)
                    }
                    .gesture(
                        DragGesture(minimumDistance: 50)
                            .onEnded { value in
                                if value.translation.width > 50 {
                                    Task { await vm.navigateHistory(delta: -1) }
                                } else if value.translation.width < -50 {
                                    Task { await vm.navigateHistory(delta: 1) }
                                }
                            }
                    )
                } else if let error = vm.error {
                    VStack(spacing: 12) {
                        Image(systemName: "wifi.slash")
                            .font(.largeTitle)
                            .foregroundStyle(.secondary)
                        Text(error)
                            .foregroundStyle(.secondary)
                            .multilineTextAlignment(.center)
                        Button("Retry") {
                            vm.start()
                        }
                        .buttonStyle(.bordered)
                    }
                    .padding()
                }
            }
            .navigationTitle(headerTitle)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button {
                        if vm.isHistoryMode {
                            Task { await vm.goToLive() }
                        }
                    } label: {
                        Image(systemName: vm.isHistoryMode ? "antenna.radiowaves.left.and.right" : "circle.fill")
                            .foregroundStyle(vm.isHistoryMode ? .blue : .green)
                            .font(.caption)
                    }
                }

                ToolbarItem(placement: .topBarTrailing) {
                    HStack(spacing: 12) {
                        Button(vm.sortLabel) {
                            Task { await vm.cycleSortMode() }
                        }
                        .font(.caption.monospaced())

                        Button {
                            showingSettings = true
                        } label: {
                            Image(systemName: "gear")
                        }
                    }
                }
            }
        }
        .sheet(isPresented: $vm.showingNews) {
            NewsSheetView(
                symbol: vm.selectedSymbol ?? "",
                date: vm.date,
                articles: vm.newsArticles
            )
        }
        .sheet(isPresented: $showingSettings) {
            SettingsView()
        }
        .onAppear { vm.start() }
        .onDisappear { vm.stop() }
    }

    private var headerTitle: String {
        if vm.isHistoryMode {
            let pos = "\(vm.historyIndex + 1)/\(vm.historyDates.count)"
            return "\(vm.date) [\(pos)]"
        }
        return vm.date
    }
}

// MARK: - Settings

struct SettingsView: View {
    @AppStorage("serverURL") private var serverURL = "http://localhost:8080"
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            Form {
                Section("Server") {
                    TextField("Base URL", text: $serverURL)
                        .textInputAutocapitalization(.never)
                        .autocorrectionDisabled()
                        .keyboardType(.URL)
                }

                Section {
                    Text("Changes take effect after restarting the app.")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            .navigationTitle("Settings")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}

// MARK: - Day Section

struct DaySectionView: View {
    let day: DayDataJSON
    let session: SessionView
    let watchlist: Set<String>
    let onSelect: (String) -> Void
    let onToggleWatchlist: (String) -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Day header.
            HStack {
                Text(day.label)
                    .font(.caption.bold())
                    .foregroundStyle(.white)
                Spacer()
                if day.preCount > 0 {
                    Text("pre: \(Fmt.intWithCommas(day.preCount))")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                if day.regCount > 0 {
                    Text("reg: \(Fmt.intWithCommas(day.regCount))")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
            }
            .padding(.horizontal)
            .padding(.vertical, 6)
            .background(Color.cyan.opacity(0.3))

            if day.tiers.isEmpty {
                Text("(no matching symbols)")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .padding()
            } else {
                ForEach(day.tiers) { tier in
                    TierSectionView(
                        tier: tier,
                        session: session,
                        watchlist: watchlist,
                        onSelect: onSelect,
                        onToggleWatchlist: onToggleWatchlist
                    )
                }
            }
        }
    }
}
