package command

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

func NewID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate command id: %w", err)
	}

	return "cmd_" + hex.EncodeToString(bytes[:]), nil
}
