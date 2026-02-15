import SwiftUI

@main
struct JupitorApp: App {
    @State private var vm = DashboardViewModel(
        baseURL: URL(string: UserDefaults.standard.string(forKey: "serverURL") ?? "http://mbpro:8080")!
    )

    var body: some Scene {
        WindowGroup {
            RootTabView()
                .environment(vm)
                .preferredColorScheme(.dark)
                .onAppear { vm.start() }
                .onDisappear { vm.stop() }
        }
    }
}
