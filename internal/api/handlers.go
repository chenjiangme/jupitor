package api

import (
	"net/http"
)

// HandleGetBars returns historical bar data for a symbol.
func HandleGetBars(w http.ResponseWriter, _ *http.Request) {
	// TODO: parse symbol, market, start, end from query params
	// TODO: read bars from BarStore and serialize as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"TODO","data":[]}`))
}

// HandleGetTrades returns historical trade data for a symbol.
func HandleGetTrades(w http.ResponseWriter, _ *http.Request) {
	// TODO: parse symbol, start, end from query params
	// TODO: read trades from TradeStore and serialize as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"TODO","data":[]}`))
}

// HandleGetOrders returns orders matching the given status filter.
func HandleGetOrders(w http.ResponseWriter, _ *http.Request) {
	// TODO: parse status filter from query params
	// TODO: read orders from OrderStore and serialize as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"TODO","data":[]}`))
}

// HandleGetPositions returns all currently open positions.
func HandleGetPositions(w http.ResponseWriter, _ *http.Request) {
	// TODO: read positions from Engine or PositionStore
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"TODO","data":[]}`))
}

// HandleGetAccount returns the current account information.
func HandleGetAccount(w http.ResponseWriter, _ *http.Request) {
	// TODO: fetch account info from Broker and serialize as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"TODO","data":{}}`))
}

// HandleSubmitOrder accepts and processes a new order submission.
func HandleSubmitOrder(w http.ResponseWriter, _ *http.Request) {
	// TODO: parse order from request body JSON
	// TODO: submit via Engine.SubmitOrder
	// TODO: return created order as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte(`{"status":"TODO","order":{}}`))
}

// HandleCancelOrder requests cancellation of an open order.
func HandleCancelOrder(w http.ResponseWriter, _ *http.Request) {
	// TODO: parse order ID from URL path
	// TODO: cancel via Engine.CancelOrder
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"TODO"}`))
}
