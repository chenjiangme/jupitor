package cn

import "testing"

func TestBaoStockClientNew(t *testing.T) {
	c := NewBaoStockClient("10.0.0.1", 10086)
	if c.host != "10.0.0.1" {
		t.Errorf("BaoStockClient.host = %q, want %q", c.host, "10.0.0.1")
	}
	if c.port != 10086 {
		t.Errorf("BaoStockClient.port = %d, want %d", c.port, 10086)
	}
}

func TestDailyBarGathererName(t *testing.T) {
	client := NewBaoStockClient("localhost", 10086)
	g := NewDailyBarGatherer(client, nil, "2020-01-01")
	if got := g.Name(); got != "cn-daily" {
		t.Errorf("DailyBarGatherer.Name() = %q, want %q", got, "cn-daily")
	}
}
