import SwiftUI

@main
struct JupitorApp: App {
    @State private var vm = DashboardViewModel(
        baseURL: URL(string: UserDefaults.standard.string(forKey: "serverURL") ?? "http://mbpro:8080")!
    )
    @State private var tradeParams = TradeParamsModel(
        baseURL: URL(string: UserDefaults.standard.string(forKey: "serverURL") ?? "http://mbpro:8080")!
    )

    var body: some Scene {
        WindowGroup {
            RootTabView()
                .environment(vm)
                .environment(tradeParams)
                .preferredColorScheme(.dark)
                .onAppear { vm.start(); tradeParams.start() }
                .onDisappear { vm.stop(); tradeParams.stop() }
        }
    }
}
