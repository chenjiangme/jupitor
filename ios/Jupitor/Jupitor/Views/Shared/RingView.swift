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

    var body: some View {
        let capped = min(value, 5.0)
        let style = StrokeStyle(lineWidth: lineWidth, lineCap: .round)
        ForEach(Array(shades.indices), id: \.self) { band in
            let frac = min(max(capped - Double(band), 0), 1.0)
            if frac > 0 {
                Circle()
                    .trim(from: 0, to: frac)
                    .stroke(shades[band], style: style)
                    .rotationEffect(.degrees(-90))
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
    var gainFirst: Bool = true

    var body: some View {
        if hasData {
            ZStack {
                Circle()
                    .stroke(Color.white.opacity(0.1), lineWidth: lineWidth)

                // The arc that happened first starts at 12 o'clock.
                // Gain goes clockwise, loss goes counter-clockwise.
                // The second arc starts where the first ends, going back.
                // Whichever arc is longer at any point gets the wide stroke.
                if gainFirst {
                    let gainEnd = min(gain, 5.0).truncatingRemainder(dividingBy: 1.0)
                    let gainWide = gain >= loss
                    GradientArcsView(value: gain, shades: gainShades,
                                     lineWidth: gainWide ? lineWidth : lineWidth * 0.5)
                    GradientArcsView(value: loss, shades: lossShades,
                                     lineWidth: gainWide ? lineWidth * 0.5 : lineWidth)
                        .scaleEffect(x: -1, y: 1)
                        .rotationEffect(.degrees(gainEnd * 360))
                } else {
                    let lossEnd = min(loss, 5.0).truncatingRemainder(dividingBy: 1.0)
                    let lossWide = loss >= gain
                    GradientArcsView(value: loss, shades: lossShades,
                                     lineWidth: lossWide ? lineWidth : lineWidth * 0.5)
                        .scaleEffect(x: -1, y: 1)
                    GradientArcsView(value: gain, shades: gainShades,
                                     lineWidth: lossWide ? lineWidth * 0.5 : lineWidth)
                        .rotationEffect(.degrees(-lossEnd * 360))
                }
            }
            .frame(width: diameter, height: diameter)
        }
    }
}
