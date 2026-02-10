package broker

import "testing"

func TestAlpacaBrokerName(t *testing.T) {
	b := NewAlpacaBroker("key", "secret", "https://paper-api.alpaca.markets")
	if got := b.Name(); got != "alpaca" {
		t.Errorf("AlpacaBroker.Name() = %q, want %q", got, "alpaca")
	}
}

func TestSimulatorBrokerName(t *testing.T) {
	b := NewSimulatorBroker()
	if got := b.Name(); got != "simulator" {
		t.Errorf("SimulatorBroker.Name() = %q, want %q", got, "simulator")
	}
}
