import SwiftUI

struct CNIndustryFilterView: View {
    @Environment(CNHeatmapViewModel.self) private var vm
    @Environment(\.dismiss) private var dismiss

    @State private var showSaveAlert = false
    @State private var presetName = ""

    private let columns = [
        GridItem(.adaptive(minimum: 100), spacing: 8)
    ]

    var body: some View {
        NavigationStack {
            ScrollView {
                // Presets section.
                if !vm.presets.isEmpty {
                    ScrollView(.horizontal, showsIndicators: false) {
                        HStack(spacing: 8) {
                            ForEach(vm.presets) { preset in
                                let active = vm.isPresetActive(preset)
                                Button {
                                    withAnimation(.easeInOut(duration: 0.15)) {
                                        vm.applyPreset(preset)
                                    }
                                } label: {
                                    Text(preset.name)
                                        .font(.caption)
                                        .fontWeight(active ? .bold : .regular)
                                        .padding(.horizontal, 12)
                                        .padding(.vertical, 6)
                                        .background(active ? Color.blue.opacity(0.3) : Color.gray.opacity(0.2), in: Capsule())
                                        .foregroundStyle(active ? .blue : .primary)
                                }
                                .contextMenu {
                                    Button(role: .destructive) {
                                        vm.deletePreset(name: preset.name)
                                    } label: {
                                        Label("Delete", systemImage: "trash")
                                    }
                                }
                            }
                        }
                        .padding(.horizontal)
                    }
                    .padding(.top, 8)
                }

                LazyVGrid(columns: columns, spacing: 8) {
                    ForEach(sortedIndustries, id: \.self) { industry in
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
            .onAppear { sortedIndustries = computeSortedIndustries() }
            .navigationTitle("Industry Filter")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarLeading) {
                    HStack(spacing: 12) {
                        Button("Reset") {
                            withAnimation { vm.resetIndustryFilter() }
                        }
                        .disabled(!vm.hasIndustryFilter)

                        Button {
                            presetName = ""
                            showSaveAlert = true
                        } label: {
                            Image(systemName: "square.and.arrow.down")
                        }
                        .disabled(!vm.hasIndustryFilter)
                    }
                }
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
            .alert("Save Preset", isPresented: $showSaveAlert) {
                TextField("Name", text: $presetName)
                Button("Save") {
                    let name = presetName.trimmingCharacters(in: .whitespaces)
                    guard !name.isEmpty else { return }
                    vm.savePreset(name: name)
                }
                Button("Cancel", role: .cancel) {}
            } message: {
                Text("Enter a name for this filter preset")
            }
        }
    }

    @State private var sortedIndustries: [String] = []

    private func computeSortedIndustries() -> [String] {
        vm.industries.sorted { a, b in
            let sa = sortOrder(filterState(for: a))
            let sb = sortOrder(filterState(for: b))
            return sa < sb
        }
    }

    private func sortOrder(_ state: FilterState) -> Int {
        switch state {
        case .selected: return 0
        case .normal: return 1
        case .excluded: return 2
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
