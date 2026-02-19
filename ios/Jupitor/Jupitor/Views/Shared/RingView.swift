import SwiftUI

// Shared gain/loss shade arrays and ring rendering, used by BubbleChartView and SymbolHistoryView.

let gainShades: [Color] = [
    Color(hue: 0.33, saturation: 1.00, brightness: 0.45),
    Color(hue: 0.33, saturation: 0.85, brightness: 0.60),
    Color(hue: 0.33, saturation: 0.70, brightness: 0.75),
    Color(hue: 0.33, saturation: 0.50, brightness: 0.88),
    Color(hue: 0.33, saturation: 0.30, brightness: 1.00),
]

let lossShades: [Color] = [
    Color(hue: 0.00, saturation: 1.00, brightness: 0.50),
    Color(hue: 0.00, saturation: 0.85, brightness: 0.65),
    Color(hue: 0.00, saturation: 0.65, brightness: 0.80),
    Color(hue: 0.00, saturation: 0.45, brightness: 0.92),
    Color(hue: 0.00, saturation: 0.25, brightness: 1.00),
]

struct GradientArcsView: View {
    let value: Double
    let shades: [Color]
    let lineWidth: CGFloat
    var cornerRadius: CGFloat = 0

    var body: some View {
        let capped = min(value, 5.0)
        let style = StrokeStyle(lineWidth: lineWidth, lineCap: .round)
        ForEach(Array(shades.indices), id: \.self) { band in
            let frac = min(max(capped - Double(band), 0), 1.0)
            if frac > 0 {
                if cornerRadius > 0 {
                    RoundedRectangle(cornerRadius: cornerRadius)
                        .trim(from: 0, to: frac)
                        .stroke(shades[band], style: style)
                        .rotationEffect(.degrees(-90))
                } else {
                    Circle()
                        .trim(from: 0, to: frac)
                        .stroke(shades[band], style: style)
                        .rotationEffect(.degrees(-90))
                }
            }
        }
    }
}

struct SessionRingView: View {
    let gain: Double
    let loss: Double
    let hasData: Bool
    let diameter: CGFloat
    let lineWidth: CGFloat
    var isSquare: Bool = false

    private var cornerRadius: CGFloat {
        isSquare ? diameter * 0.15 : 0
    }

    var body: some View {
        if hasData {
            ZStack {
                if isSquare {
                    RoundedRectangle(cornerRadius: cornerRadius)
                        .stroke(Color.white.opacity(0.1), lineWidth: lineWidth)
                } else {
                    Circle()
                        .stroke(Color.white.opacity(0.1), lineWidth: lineWidth)
                }

                if gain >= loss {
                    GradientArcsView(value: gain, shades: gainShades, lineWidth: lineWidth, cornerRadius: cornerRadius)
                    GradientArcsView(value: loss, shades: lossShades, lineWidth: lineWidth * 0.5, cornerRadius: cornerRadius)
                        .scaleEffect(x: -1, y: 1)
                } else {
                    GradientArcsView(value: loss, shades: lossShades, lineWidth: lineWidth, cornerRadius: cornerRadius)
                        .scaleEffect(x: -1, y: 1)
                    GradientArcsView(value: gain, shades: gainShades, lineWidth: lineWidth * 0.5, cornerRadius: cornerRadius)
                }
            }
            .frame(width: diameter, height: diameter)
        }
    }
}
