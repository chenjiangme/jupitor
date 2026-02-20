import AppIntents

struct ToggleViewIntent: AppIntent {
    static var title: LocalizedStringResource = "Toggle Chart View"
    static var description = IntentDescription("Switch between bubble chart and concentric ring views")
    static var openAppWhenRun: Bool = false

    func perform() async throws -> some IntentResult {
        let key = "useConcentricView"
        let current = UserDefaults.standard.bool(forKey: key)
        UserDefaults.standard.set(!current, forKey: key)
        return .result()
    }
}

struct JupitorShortcuts: AppShortcutsProvider {
    static var appShortcuts: [AppShortcut] {
        AppShortcut(
            intent: ToggleViewIntent(),
            phrases: ["Toggle chart view in \(.applicationName)"],
            shortTitle: "Toggle View",
            systemImageName: "circle.circle"
        )
    }
}
