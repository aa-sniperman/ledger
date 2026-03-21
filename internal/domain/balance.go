package domain

import "fmt"

type BalanceBuckets struct {
	PostedDebits   int64
	PostedCredits  int64
	PendingDebits  int64
	PendingCredits int64
}

func (b BalanceBuckets) Validate() error {
	if b.PostedDebits < 0 || b.PostedCredits < 0 || b.PendingDebits < 0 || b.PendingCredits < 0 {
		return fmt.Errorf("%w: buckets must be non-negative", ErrInvalidBalanceBuckets)
	}

	if b.PendingDebits < b.PostedDebits {
		return fmt.Errorf("%w: pending debits must include posted debits", ErrInvalidBalanceBuckets)
	}

	if b.PendingCredits < b.PostedCredits {
		return fmt.Errorf("%w: pending credits must include posted credits", ErrInvalidBalanceBuckets)
	}

	return nil
}

func (b BalanceBuckets) Apply(delta BalanceBuckets) (BalanceBuckets, error) {
	next := BalanceBuckets{
		PostedDebits:   b.PostedDebits + delta.PostedDebits,
		PostedCredits:  b.PostedCredits + delta.PostedCredits,
		PendingDebits:  b.PendingDebits + delta.PendingDebits,
		PendingCredits: b.PendingCredits + delta.PendingCredits,
	}

	if err := next.Validate(); err != nil {
		return BalanceBuckets{}, err
	}

	return next, nil
}

func (b BalanceBuckets) Posted(normalBalance NormalBalance) (int64, error) {
	if err := b.Validate(); err != nil {
		return 0, err
	}

	return applyNormalBalance(normalBalance, b.PostedDebits, b.PostedCredits)
}

func (b BalanceBuckets) Pending(normalBalance NormalBalance) (int64, error) {
	if err := b.Validate(); err != nil {
		return 0, err
	}

	return applyNormalBalance(normalBalance, b.PendingDebits, b.PendingCredits)
}

func (b BalanceBuckets) Available(normalBalance NormalBalance) (int64, error) {
	if err := b.Validate(); err != nil {
		return 0, err
	}

	switch normalBalance {
	case NormalBalanceCredit:
		return b.PostedCredits - b.PendingDebits, nil
	case NormalBalanceDebit:
		return b.PostedDebits - b.PendingCredits, nil
	default:
		return 0, fmt.Errorf("%w: unsupported normal balance %q", ErrInvalidBalanceBuckets, normalBalance)
	}
}

func applyNormalBalance(normalBalance NormalBalance, debits, credits int64) (int64, error) {
	switch normalBalance {
	case NormalBalanceCredit:
		return credits - debits, nil
	case NormalBalanceDebit:
		return debits - credits, nil
	default:
		return 0, fmt.Errorf("%w: unsupported normal balance %q", ErrInvalidBalanceBuckets, normalBalance)
	}
}
