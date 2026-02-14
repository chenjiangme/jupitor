import SwiftUI

struct RootTabView: View {
    var body: some View {
        TabView {
            NavigationStack {
                LiveDashboardView()
            }
            .tabItem {
                Label("Live", systemImage: "chart.line.uptrend.xyaxis")
            }

            NavigationStack {
                HistoryDateListView()
            }
            .tabItem {
                Label("History", systemImage: "calendar")
            }

            NavigationStack {
                WatchlistView()
            }
            .tabItem {
                Label("Watchlist", systemImage: "star.fill")
            }
        }
    }
}
