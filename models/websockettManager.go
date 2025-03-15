package models

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

// WebSocketManager handles WebSocket connections and broadcasts
type WebSocketManager struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mu         sync.Mutex
}

// NewWebSocketManager creates a new WebSocket manager
func NewWebSocketManager() *WebSocketManager {
	return &WebSocketManager{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

// Start begins the WebSocket manager
func (wsm *WebSocketManager) Start() {
	go func() {
		for {
			select {
			case client := <-wsm.register:
				wsm.mu.Lock()
				wsm.clients[client] = true
				wsm.mu.Unlock()
				log.Printf("New WebSocket client connected. Total clients: %d", len(wsm.clients))
			case client := <-wsm.unregister:
				wsm.mu.Lock()
				if _, ok := wsm.clients[client]; ok {
					delete(wsm.clients, client)
					client.Close()
				}
				wsm.mu.Unlock()
				log.Printf("WebSocket client disconnected. Remaining clients: %d", len(wsm.clients))
			case message := <-wsm.broadcast:
				wsm.mu.Lock()
				for client := range wsm.clients {
					err := client.WriteMessage(websocket.TextMessage, message)
					if err != nil {
						log.Printf("Error sending message to client: %v", err)
						client.Close()
						delete(wsm.clients, client)
					}
				}
				wsm.mu.Unlock()
			}
		}
	}()
}

// BroadcastJobUpdate sends a job update to all connected clients
func (wsm *WebSocketManager) BroadcastJobUpdate(job *PDFJob) {
	update := map[string]interface{}{
		"type":      "job_update",
		"job_id":    job.ID,
		"status":    job.Status,
		"timestamp": job.UpdatedAt,
	}

	if job.Status == StatusFailed && job.ErrorMessage != "" {
		update["error"] = job.ErrorMessage
	}

	jsonData, err := json.Marshal(update)
	if err != nil {
		log.Printf("Failed to marshal job update: %v", err)
		return
	}

	wsm.broadcast <- jsonData
}

// RegisterClient registers a new WebSocket client
func (wsm *WebSocketManager) RegisterClient(conn *websocket.Conn) {
	wsm.register <- conn
}

// UnregisterClient unregisters a WebSocket client
func (wsm *WebSocketManager) UnregisterClient(conn *websocket.Conn) {
	wsm.unregister <- conn
}
