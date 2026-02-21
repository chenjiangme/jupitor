import SwiftUI

struct SettingsView: View {
    @AppStorage("serverURL") private var serverURL = "http://mbpro:8080"
    @AppStorage("showDayMode") private var showDayMode = false
    @AppStorage("chartViewMode") private var chartViewMode = 0
    @AppStorage("hidePennyStocks") private var hidePennyStocks = false
    @AppStorage("gainOverLossOnly") private var gainOverLossOnly = false
    @AppStorage("topStocksOnly") private var topStocksOnly = false
    @AppStorage("replaySpeed") private var replaySpeed = 60000
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

                Section("Display") {
                    Toggle("Show Day Mode", isOn: $showDayMode)
                    Picker("Chart View", selection: $chartViewMode) {
                        Text("Bubbles").tag(0)
                        Text("List").tag(1)
                    }
                }

                Section("Filters") {
                    Toggle("Hide Below $1", isOn: $hidePennyStocks)
                    Toggle("Gain > Loss Only", isOn: $gainOverLossOnly)
                    Toggle("Top Stocks Only", isOn: $topStocksOnly)
                }

                Section("Replay") {
                    Picker("1 second =", selection: $replaySpeed) {
                        Text("5 seconds").tag(5000)
                        Text("15 seconds").tag(15000)
                        Text("1 minute").tag(60000)
                    }
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
