package api

import (
	"net/http"
)

// Client represents a single WebSocket connection managed by a Hub.
type Client struct {
	hub  *Hub
	send chan []byte
}

// Hub manages a set of WebSocket clients and broadcasts messages to all
// connected clients.
type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
}

// NewHub creates a new Hub with initialised channels and client map.
func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
}

// Run starts the Hub's main event loop. It should be launched as a goroutine.
func (h *Hub) Run() {
	// TODO: implement full WebSocket hub event loop
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

// HandleWebSocket upgrades an HTTP connection to a WebSocket and registers
// the client with the Hub.
func HandleWebSocket(_ http.ResponseWriter, _ *http.Request) {
	// TODO: upgrade HTTP connection to WebSocket using gorilla/websocket or nhooyr.io/websocket
	// TODO: create Client, register with hub, start read/write pumps
}
