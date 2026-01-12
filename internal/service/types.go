package service

import "time"

type Indicators struct {
	Population       string
	Health           string
	EconomyGDP       string
	EconomyGov       string
	GrowthGDP        string
	GrowthPopulation string
}

type MessageType int

const (
	FAILED MessageType = iota
	SUCCESS
)

type NotifyMessage struct {
	Message     string      `json:"message"`
	Type        MessageType `json:"messageType"`
	CreatedAt   time.Time   `json:"created_at"`
	CompletedAt time.Time   `json:"completed_at"`
}
