import SwiftUI

struct SymbolListView: View {
    let day: DayDataJSON
    let date: String

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 0) {
                // Day header.
                HStack {
                    Text(day.label)
                        .font(.caption.bold())
                        .foregroundStyle(.white)
                    Spacer()
                    if day.preCount > 0 {
                        Text("pre: \(Fmt.intWithCommas(day.preCount))")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                    if day.regCount > 0 {
                        Text("reg: \(Fmt.intWithCommas(day.regCount))")
                            .font(.caption2)
                            .foregroundStyle(.secondary)
                    }
                }
                .padding(.horizontal)
                .padding(.vertical, 6)
                .background(Color.cyan.opacity(0.3))

                if day.tiers.isEmpty {
                    Text("(no matching symbols)")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                        .padding()
                } else {
                    ForEach(day.tiers) { tier in
                        TierSectionView(tier: tier, date: date)
                    }
                }
            }
            .padding(.bottom, 20)
        }
    }
}
