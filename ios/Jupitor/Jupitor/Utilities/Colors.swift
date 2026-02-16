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
        background(TwoFingerSwipeHelper(onLeft: left, onRight: right))
    }
}

private struct TwoFingerSwipeHelper: UIViewRepresentable {
    let onLeft: () -> Void
    let onRight: () -> Void

    func makeUIView(context: Context) -> GestureHostView {
        GestureHostView()
    }

    func updateUIView(_ uiView: GestureHostView, context: Context) {
        uiView.onLeft = onLeft
        uiView.onRight = onRight
    }
}

private class GestureHostView: UIView {
    var onLeft: (() -> Void)?
    var onRight: (() -> Void)?
    private var gestures: [UIGestureRecognizer] = []

    override func didMoveToWindow() {
        super.didMoveToWindow()
        if let window {
            attachGestures(to: window)
        } else {
            detachGestures()
        }
    }

    private func attachGestures(to window: UIWindow) {
        guard gestures.isEmpty else { return }

        // Remove any stale two-finger nav gestures from other instances.
        window.gestureRecognizers?
            .filter { $0.name?.hasPrefix("twoFingerNav") == true }
            .forEach { window.removeGestureRecognizer($0) }

        let left = UISwipeGestureRecognizer(target: self, action: #selector(handleSwipe(_:)))
        left.direction = .left
        left.numberOfTouchesRequired = 2
        left.cancelsTouchesInView = false
        left.name = "twoFingerNavLeft"
        window.addGestureRecognizer(left)
        gestures.append(left)

        let right = UISwipeGestureRecognizer(target: self, action: #selector(handleSwipe(_:)))
        right.direction = .right
        right.numberOfTouchesRequired = 2
        right.cancelsTouchesInView = false
        right.name = "twoFingerNavRight"
        window.addGestureRecognizer(right)
        gestures.append(right)
    }

    private func detachGestures() {
        for g in gestures {
            g.view?.removeGestureRecognizer(g)
        }
        gestures.removeAll()
    }

    @objc private func handleSwipe(_ gesture: UISwipeGestureRecognizer) {
        if gesture.direction == .left { onLeft?() }
        else if gesture.direction == .right { onRight?() }
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
