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

// MARK: - Two-Finger Swipe

extension View {
    func onTwoFingerSwipe(left: @escaping () -> Void, right: @escaping () -> Void) -> some View {
        overlay(TwoFingerSwipeView(onLeft: left, onRight: right))
    }
}

private struct TwoFingerSwipeView: UIViewRepresentable {
    let onLeft: () -> Void
    let onRight: () -> Void

    func makeUIView(context: Context) -> UIView {
        let view = UIView()
        view.backgroundColor = .clear

        let leftSwipe = UISwipeGestureRecognizer(target: context.coordinator, action: #selector(Coordinator.handleSwipe(_:)))
        leftSwipe.direction = .left
        leftSwipe.numberOfTouchesRequired = 2
        view.addGestureRecognizer(leftSwipe)

        let rightSwipe = UISwipeGestureRecognizer(target: context.coordinator, action: #selector(Coordinator.handleSwipe(_:)))
        rightSwipe.direction = .right
        rightSwipe.numberOfTouchesRequired = 2
        view.addGestureRecognizer(rightSwipe)

        return view
    }

    func updateUIView(_ uiView: UIView, context: Context) {
        context.coordinator.onLeft = onLeft
        context.coordinator.onRight = onRight
    }

    func makeCoordinator() -> Coordinator {
        Coordinator(onLeft: onLeft, onRight: onRight)
    }

    class Coordinator: NSObject {
        var onLeft: () -> Void
        var onRight: () -> Void

        init(onLeft: @escaping () -> Void, onRight: @escaping () -> Void) {
            self.onLeft = onLeft
            self.onRight = onRight
        }

        @objc func handleSwipe(_ gesture: UISwipeGestureRecognizer) {
            if gesture.direction == .left { onLeft() }
            else if gesture.direction == .right { onRight() }
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
