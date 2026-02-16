import SwiftUI

extension Color {
    // Tier colors.
    static let tierActive = Color.green
    static let tierModerate = Color.yellow
    static let tierSporadic = Color.red

    // Data colors.
    static let gainColor = Color.green
    static let lossColor = Color.red
    static let watchlistColor = Color.orange

    static func tierColor(for name: String) -> Color {
        switch name {
        case "ACTIVE": return .tierActive
        case "MODERATE": return .tierModerate
        case "SPORADIC": return .tierSporadic
        default: return .primary
        }
    }
}

// MARK: - Pulse Animation

extension View {
    func pulseAnimation() -> some View {
        modifier(PulseModifier())
    }
}

struct PulseModifier: ViewModifier {
    @State private var isPulsing = false

    func body(content: Content) -> some View {
        content
            .opacity(isPulsing ? 0.4 : 1.0)
            .animation(.easeInOut(duration: 1.5).repeatForever(autoreverses: true), value: isPulsing)
            .onAppear { isPulsing = true }
    }
}
