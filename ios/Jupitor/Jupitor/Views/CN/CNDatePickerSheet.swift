import SwiftUI

struct CNDatePickerSheet: View {
    @Environment(CNHeatmapViewModel.self) private var vm
    @Environment(\.dismiss) private var dismiss

    var body: some View {
        NavigationStack {
            List(vm.dates.reversed(), id: \.self) { date in
                Button {
                    vm.navigateTo(date)
                    dismiss()
                } label: {
                    HStack {
                        Text(date)
                            .foregroundStyle(date == vm.currentDate ? .orange : .primary)
                        Spacer()
                        if date == vm.currentDate {
                            Image(systemName: "checkmark")
                                .foregroundStyle(.orange)
                        }
                    }
                }
            }
            .navigationTitle("Select Date")
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .topBarTrailing) {
                    Button("Done") { dismiss() }
                }
            }
        }
    }
}
