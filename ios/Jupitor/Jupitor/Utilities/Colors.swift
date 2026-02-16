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
        if window != nil {
            attachGestures()
        } else {
            detachGestures()
        }
    }

    private func attachGestures() {
        guard gestures.isEmpty else { return }

        // Walk up responder chain to find the VC's view (ancestor of all content).
        var responder: UIResponder? = self
        var targetView: UIView?
        while let next = responder?.next {
            if let vc = next as? UIViewController {
                targetView = vc.view
                break
            }
            responder = next
        }
        guard let targetView else { return }

        let left = UISwipeGestureRecognizer(target: self, action: #selector(handleSwipe(_:)))
        left.direction = .left
        left.numberOfTouchesRequired = 2
        left.cancelsTouchesInView = false
        targetView.addGestureRecognizer(left)
        gestures.append(left)

        let right = UISwipeGestureRecognizer(target: self, action: #selector(handleSwipe(_:)))
        right.direction = .right
        right.numberOfTouchesRequired = 2
        right.cancelsTouchesInView = false
        targetView.addGestureRecognizer(right)
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
