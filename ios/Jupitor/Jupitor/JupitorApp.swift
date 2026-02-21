import SwiftUI

@main
struct JupitorApp: App {
    @State private var vm = DashboardViewModel(
        baseURL: URL(string: UserDefaults.standard.string(forKey: "serverURL") ?? "http://mbpro:8080")!
    )
    @State private var tradeParams = TradeParamsModel(
        baseURL: URL(string: UserDefaults.standard.string(forKey: "serverURL") ?? "http://mbpro:8080")!
    )
    @State private var cnVM = CNHeatmapViewModel(
        baseURL: URL(string: UserDefaults.standard.string(forKey: "cnServerURL") ?? "http://mbpro:8081")!
    )

    var body: some Scene {
        WindowGroup {
            RootTabView()
                .environment(vm)
                .environment(tradeParams)
                .environment(cnVM)
                .preferredColorScheme(.dark)
                .onAppear { vm.start(); tradeParams.start(); cnVM.start() }
                .onDisappear { vm.stop(); tradeParams.stop() }
        }
    }
}
