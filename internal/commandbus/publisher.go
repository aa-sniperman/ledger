package commandbus

import (
	"context"

	"github.com/sniperman/ledger/internal/command"
)

type Publisher interface {
	PublishAccepted(ctx context.Context, envelope command.Envelope) error
}

type NoopPublisher struct{}

func (NoopPublisher) PublishAccepted(context.Context, command.Envelope) error {
	return nil
}
