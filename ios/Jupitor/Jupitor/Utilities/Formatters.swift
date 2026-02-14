import Foundation

enum Fmt {
    static func count(_ n: Int) -> String {
        if n >= 100_000 {
            return String(format: "%.0fK", Double(n) / 1000)
        }
        return intWithCommas(n)
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
