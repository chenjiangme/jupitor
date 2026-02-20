import SwiftUI

// Shared canvas components used by BubbleChartView and ConcentricRingView.

// MARK: - Dial Color

extension SymbolStatsJSON {
    /// Dial-style color: red (hue 0) at low, green (hue 0.33) at high.
    var dialColor: Color {
        let cf = high > low ? (close - low) / (high - low) : 0.5
        let clamped = min(max(cf, 0), 1)
        return Color(hue: 0.33 * clamped, saturation: 0.9, brightness: 0.9)
    }
}

// MARK: - Close Dial

/// Needle from center to outer ring edge showing close position in [low, high].
/// Full 360: 12 o'clock = both 0% (red) and 100% (green). Sweeps clockwise.
struct CloseDialView: View {
    let fraction: Double      // 0 = at low, 1 = at high
    let needleRadius: CGFloat // distance from center to ring edge
    var lineWidth: CGFloat = 2

    var body: some View {
        let clamped = min(max(fraction, 0), 1)
        // 12 o'clock = 270 in screen coords. Sweep 360 clockwise.
        let angle = Angle(degrees: 270 + 360 * clamped)
        // Red at 0%, green at 100%, interpolated by hue.
        let color = Color(hue: 0.33 * clamped, saturation: 0.9, brightness: 0.9)

        Canvas { context, size in
            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            let rad = angle.radians
            let tip = CGPoint(x: center.x + cos(rad) * needleRadius,
                              y: center.y + sin(rad) * needleRadius)

            var needle = Path()
            needle.move(to: center)
            needle.addLine(to: tip)
            context.stroke(needle, with: .color(color.opacity(0.5)),
                          style: StrokeStyle(lineWidth: lineWidth, lineCap: .round))
        }
    }
}

// MARK: - Target Marker (small colored line on ring)

struct TargetMarkerCanvas: View {
    let gain: Double        // 0-5 (1.0 = 100%)
    let ringRadius: CGFloat // center of the ring stroke
    let lineWidth: CGFloat  // ring stroke width
    var color: Color = .yellow.opacity(0.9)

    var body: some View {
        Canvas { context, size in
            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            let frac = gain.truncatingRemainder(dividingBy: 1.0)
            let adjustedFrac = gain >= 1.0 && frac == 0 ? 1.0 : frac
            let rad = -Double.pi / 2 + 2 * Double.pi * adjustedFrac

            let innerR = ringRadius - lineWidth / 2
            let outerR = ringRadius + lineWidth / 2
            let p1 = CGPoint(x: center.x + cos(rad) * innerR, y: center.y + sin(rad) * innerR)
            let p2 = CGPoint(x: center.x + cos(rad) * outerR, y: center.y + sin(rad) * outerR)

            var line = Path()
            line.move(to: p1)
            line.addLine(to: p2)
            context.stroke(line, with: .color(color),
                          style: StrokeStyle(lineWidth: 4, lineCap: .round))
        }
    }
}

// MARK: - Volume Profile (trade count histogram on outer ring)

struct VolumeProfileCanvas: View {
    let profile: [Int]      // trade count per 1% VWAP bucket (low->high)
    let gain: Double         // maxGain for this session
    let loss: Double         // maxLoss for this session
    let gainFirst: Bool
    let ringRadius: CGFloat  // center of the ring stroke
    let lineWidth: CGFloat   // ring stroke width

    var body: some View {
        Canvas { context, size in
            guard !profile.isEmpty else { return }
            let maxCount = profile.max() ?? 1
            guard maxCount > 0 else { return }

            let center = CGPoint(x: size.width / 2, y: size.height / 2)
            let outerEdge = ringRadius + lineWidth / 2
            let maxBarLen = lineWidth * 1.5

            let gainSide = gain >= loss
            let startAngle = -Double.pi / 2

            // Build angle+radius pairs for the filled mountain shape.
            func bucketAngle(_ i: Int) -> Double {
                let pct = Double(i) / 100.0
                if gainSide {
                    return startAngle + 2 * Double.pi * pct
                } else {
                    let reversePct = Double(profile.count - 1 - i) / 100.0
                    if gainFirst {
                        let gainEnd = gain.truncatingRemainder(dividingBy: 1.0)
                        return startAngle + Double(gainEnd) * 2 * Double.pi - 2 * Double.pi * reversePct
                    } else {
                        return startAngle - 2 * Double.pi * reversePct
                    }
                }
            }

            var bars = Path()
            for i in 0..<profile.count {
                guard profile[i] > 0 else { continue }
                let angle = bucketAngle(i)
                let barLen = maxBarLen * CGFloat(profile[i]) / CGFloat(maxCount)
                bars.move(to: CGPoint(
                    x: center.x + cos(angle) * outerEdge,
                    y: center.y + sin(angle) * outerEdge
                ))
                bars.addLine(to: CGPoint(
                    x: center.x + cos(angle) * (outerEdge + barLen),
                    y: center.y + sin(angle) * (outerEdge + barLen)
                ))
            }
            context.stroke(bars, with: .color(.white.opacity(0.25)),
                          style: StrokeStyle(lineWidth: 1.5, lineCap: .round))
        }
    }
}
