import SwiftUI

struct CNIndustryFilterView: View {
    @Environment(CNHeatmapViewModel.self) private var vm
    @Environment(\.dismiss) private var dismiss

    private let columns = [
        GridItem(.adaptive(minimum: 100), spacing: 8)
    ]

    var body: some View {
        NavigationStack {
            ScrollView {
                LazyVGrid(columns: columns, spacing: 8) {
                    ForEach(vm.industries, id: \.self) { industry in
                        let count = vm.industryCounts[industry] ?? 0
                        let state = filterState(for: industry)
                        Button {
                            withAnimation(.easeInOut(duration: 0.15)) {
                                vm.toggleIndustry(industry)
                            }
                        } label: {
                            VStack(spacing: 2) {
                                Text(industry)
                                    .font(.caption)
                                    .lineLimit(1)
                                Text("\(count)")
                                    .font(.caption2)
                                    .foregroundStyle(.secondary)
                            }
                            .frame(maxWidth: .infinity)
                            .padding(.vertical, 8)
                            .padding(.horizontal, 4)
                            .background(state.background, in: RoundedRectangle(cornerRadius: 8))
                            .foregroundStyle(state.foreground)
                        }
                    }
                }
                .padding()
            }
            .background(Color(uiColor: .systemGroupedBackground))
            .navigationTitle("Industry Filter")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    Button("Reset") {
                        withAnimation { vm.resetIndustryFilter() }
                    }
                    .disabled(!vm.hasIndustryFilter)
                }
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }

    private enum FilterState {
        case normal, selected, excluded

        var background: Color {
            switch self {
            case .normal: return .gray.opacity(0.2)
            case .selected: return .green.opacity(0.3)
            case .excluded: return .red.opacity(0.3)
            }
        }

        var foreground: Color {
            switch self {
            case .normal: return .primary
            case .selected: return .green
            case .excluded: return .red
            }
        }
    }

    private func filterState(for industry: String) -> FilterState {
        if vm.selectedIndustries.contains(industry) { return .selected }
        if vm.excludedIndustries.contains(industry) { return .excluded }
        return .normal
    }
}
