import SwiftUI

struct SettingsView: View {
    @AppStorage("serverURL") private var serverURL = "http://mbpro:8080"
    @AppStorage("showDayMode") private var showDayMode = false
    @AppStorage("hidePennyStocks") private var hidePennyStocks = false
    @AppStorage("gainOverLossOnly") private var gainOverLossOnly = false
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
                }

                Section("Filters") {
                    Toggle("Hide Below $1", isOn: $hidePennyStocks)
                    Toggle("Gain > Loss Only", isOn: $gainOverLossOnly)
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
