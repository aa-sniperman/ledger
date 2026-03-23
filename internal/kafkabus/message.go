package kafkabus

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/sniperman/ledger/internal/command"
)

type AcceptedCommandMessage struct {
	Envelope    command.Envelope `json:"envelope"`
	PublishedAt time.Time        `json:"published_at"`
}

func MessageFromEnvelope(envelope command.Envelope, publishedAt time.Time) (AcceptedCommandMessage, error) {
	message := AcceptedCommandMessage{
		Envelope:    envelope,
		PublishedAt: publishedAt,
	}
	if err := message.Validate(); err != nil {
		return AcceptedCommandMessage{}, err
	}
	return message, nil
}

func (m AcceptedCommandMessage) Validate() error {
	if err := m.Envelope.Validate(); err != nil {
		return err
	}
	if m.PublishedAt.IsZero() {
		return fmt.Errorf("published_at is required")
	}
	return nil
}

func EncodeMessage(message AcceptedCommandMessage) ([]byte, error) {
	if err := message.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(message)
}

func DecodeMessage(payload []byte) (AcceptedCommandMessage, error) {
	var message AcceptedCommandMessage
	if err := json.Unmarshal(payload, &message); err != nil {
		return AcceptedCommandMessage{}, fmt.Errorf("decode accepted command message: %w", err)
	}
	if err := message.Validate(); err != nil {
		return AcceptedCommandMessage{}, err
	}
	return message, nil
}
