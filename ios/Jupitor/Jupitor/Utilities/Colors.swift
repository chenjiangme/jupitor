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
    private var panGesture: UIPanGestureRecognizer?
    private var fired = false

    override func didMoveToWindow() {
        super.didMoveToWindow()
        if let window {
            attachGesture(to: window)
        } else {
            detachGesture()
        }
    }

    private func attachGesture(to window: UIWindow) {
        guard panGesture == nil else { return }

        // Remove stale gesture from other instances.
        window.gestureRecognizers?
            .filter { $0.name == "twoFingerNav" }
            .forEach { window.removeGestureRecognizer($0) }

        let pan = UIPanGestureRecognizer(target: self, action: #selector(handlePan(_:)))
        pan.minimumNumberOfTouches = 2
        pan.maximumNumberOfTouches = 2
        pan.cancelsTouchesInView = false
        pan.delaysTouchesBegan = false
        pan.name = "twoFingerNav"
        window.addGestureRecognizer(pan)
        panGesture = pan
    }

    private func detachGesture() {
        if let g = panGesture {
            g.view?.removeGestureRecognizer(g)
            panGesture = nil
        }
    }

    @objc private func handlePan(_ gesture: UIPanGestureRecognizer) {
        switch gesture.state {
        case .began:
            fired = false
        case .changed:
            guard !fired else { return }
            let t = gesture.translation(in: gesture.view)
            guard abs(t.x) > 80, abs(t.x) > abs(t.y) * 1.5 else { return }
            fired = true
            if t.x < 0 { onLeft?() }
            else { onRight?() }
        default:
            break
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
