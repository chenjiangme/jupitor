import Foundation

enum Fmt {
    static func count(_ n: Int) -> String {
        let v = Double(n)
        switch v {
        case 1e9...: return String(format: "%.1fB", v / 1e9)
        case 1e6...: return String(format: "%.1fM", v / 1e6)
        case 100_000...: return String(format: "%.0fK", v / 1e3)
        default: return intWithCommas(n)
        }
    }

    static func turnover(_ v: Double) -> String {
        switch v {
        case 1e9...: return String(format: "$%.1fB", v / 1e9)
        case 1e6...: return String(format: "$%.1fM", v / 1e6)
        case 1e3...: return String(format: "$%.1fK", v / 1e3)
        default: return String(format: "$%.0f", v)
        }
    }

    static func price(_ p: Double) -> String {
        if p == 0 { return "-" }
        return String(format: "%.2f", p)
    }

    /// Compact price: no leading zero for < $1 (e.g. ".45"), trailing zeros stripped.
    static func compactPrice(_ p: Double) -> String {
        if p == 0 { return "-" }
        var s = String(format: "%.2f", p)
        // Strip trailing zeros: "1.20" → "1.2", "1.00" → "1"
        if s.contains(".") {
            while s.hasSuffix("0") { s = String(s.dropLast()) }
            if s.hasSuffix(".") { s = String(s.dropLast()) }
        }
        // Drop leading zero only if formatted value starts with "0." (not rounded up to 1+)
        if s.hasPrefix("0.") { s = String(s.dropFirst()) }
        return s
    }

    static func gain(_ g: Double) -> String {
        if g <= 0 { return "" }
        return String(format: "+%.0f%%", g * 100)
    }

    static func loss(_ l: Double) -> String {
        if l <= 0 { return "" }
        return String(format: "-%.0f%%", l * 100)
    }

    static func intWithCommas(_ n: Int) -> String {
        let formatter = NumberFormatter()
        formatter.numberStyle = .decimal
        formatter.groupingSeparator = ","
        return formatter.string(from: NSNumber(value: n)) ?? "\(n)"
    }
}
