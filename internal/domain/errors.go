package domain

import "errors"

var (
	ErrInvalidAccount          = errors.New("invalid account")
	ErrInvalidMoney            = errors.New("invalid money")
	ErrInvalidEntry            = errors.New("invalid entry")
	ErrInvalidTransaction      = errors.New("invalid transaction")
	ErrInvalidBalanceBuckets   = errors.New("invalid balance buckets")
	ErrInvalidTransition       = errors.New("invalid transaction status transition")
	ErrMissingAccount          = errors.New("missing account")
	ErrAccountCurrencyMismatch = errors.New("account currency mismatch")
	ErrAccountVersionMismatch  = errors.New("account version mismatch")
	ErrBalanceLockFailed       = errors.New("balance lock failed")
)
