package sharding

import (
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var shardIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

type ShardID string

type SystemAccountRole string

const (
	UserWalletAccountPrefix                       = "user_wallet"
	SystemAccountRoleSettlement SystemAccountRole = "system_settlement"
	SystemAccountRolePayoutHold SystemAccountRole = "payout_holding"
	SystemAccountRoleCashIn     SystemAccountRole = "cash_in_clearing"
)

type Router struct {
	shardIDs         []ShardID
	rolePoolSizes    map[SystemAccountRole]int
	knownSystemRoles map[SystemAccountRole]struct{}
}

func NewRouter(shardIDs []ShardID, rolePoolSizes map[SystemAccountRole]int) (Router, error) {
	if len(shardIDs) == 0 {
		return Router{}, fmt.Errorf("at least one shard is required")
	}

	seen := make(map[ShardID]struct{}, len(shardIDs))
	normalizedShardIDs := make([]ShardID, 0, len(shardIDs))
	for _, shardID := range shardIDs {
		if err := shardID.Validate(); err != nil {
			return Router{}, err
		}
		if _, ok := seen[shardID]; ok {
			return Router{}, fmt.Errorf("duplicate shard id %q", shardID)
		}

		seen[shardID] = struct{}{}
		normalizedShardIDs = append(normalizedShardIDs, shardID)
	}
	slices.Sort(normalizedShardIDs)

	normalizedPoolSizes := make(map[SystemAccountRole]int, len(rolePoolSizes))
	knownRoles := map[SystemAccountRole]struct{}{
		SystemAccountRoleSettlement: {},
		SystemAccountRolePayoutHold: {},
		SystemAccountRoleCashIn:     {},
	}

	for role, size := range rolePoolSizes {
		if err := role.Validate(); err != nil {
			return Router{}, err
		}
		if size <= 0 {
			return Router{}, fmt.Errorf("system account pool size for role %q must be positive", role)
		}
		normalizedPoolSizes[role] = size
		knownRoles[role] = struct{}{}
	}

	return Router{
		shardIDs:         normalizedShardIDs,
		rolePoolSizes:    normalizedPoolSizes,
		knownSystemRoles: knownRoles,
	}, nil
}

func (id ShardID) Validate() error {
	if !shardIDPattern.MatchString(string(id)) {
		return fmt.Errorf("invalid shard id %q", id)
	}
	return nil
}

func (r Router) ShardForUser(userID string) (ShardID, error) {
	if strings.TrimSpace(userID) == "" {
		return "", fmt.Errorf("user id is required")
	}

	return rendezvousPickShard(userID, r.shardIDs), nil
}

func (r Router) ShardForAccount(accountID string) (ShardID, error) {
	prefix, remainder, ok := strings.Cut(accountID, ":")
	if !ok || remainder == "" {
		return "", fmt.Errorf("invalid account id %q", accountID)
	}

	if prefix == UserWalletAccountPrefix {
		parts := strings.Split(remainder, ":")
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return "", fmt.Errorf("invalid user account id %q", accountID)
		}
		return r.ShardForUser(parts[0])
	}

	role := SystemAccountRole(prefix)
	if err := role.Validate(); err != nil {
		return "", fmt.Errorf("invalid system account id %q: %w", accountID, err)
	}

	parts := strings.Split(remainder, ":")
	if len(parts) != 2 && len(parts) != 3 {
		return "", fmt.Errorf("invalid system account id %q", accountID)
	}

	shardID := ShardID(parts[0])
	if err := shardID.Validate(); err != nil {
		return "", fmt.Errorf("invalid system account id %q: %w", accountID, err)
	}

	if !slices.Contains(r.shardIDs, shardID) {
		return "", fmt.Errorf("unknown shard id %q in account %q", shardID, accountID)
	}

	if strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("invalid system account currency in %q", accountID)
	}

	if len(parts) == 3 {
		if _, err := strconv.Atoi(parts[2]); err != nil {
			return "", fmt.Errorf("invalid system account slot in %q", accountID)
		}
	}

	return shardID, nil
}

func UserAccountID(userID, currency string) string {
	return fmt.Sprintf("%s:%s:%s", UserWalletAccountPrefix, userID, currency)
}

func (r Router) SystemAccountForUser(userID, currency string, role SystemAccountRole) (string, ShardID, error) {
	shardID, err := r.ShardForUser(userID)
	if err != nil {
		return "", "", err
	}

	accountID, err := r.SystemAccountIDForShard(userID, shardID, currency, role)
	if err != nil {
		return "", "", err
	}

	return accountID, shardID, nil
}

func (r Router) SystemAccountIDForShard(routingKey string, shardID ShardID, currency string, role SystemAccountRole) (string, error) {
	if err := role.Validate(); err != nil {
		return "", err
	}
	if err := shardID.Validate(); err != nil {
		return "", err
	}
	if strings.TrimSpace(currency) == "" {
		return "", fmt.Errorf("currency is required")
	}
	if !slices.Contains(r.shardIDs, shardID) {
		return "", fmt.Errorf("unknown shard id %q", shardID)
	}

	poolSize := r.poolSizeForRole(role)
	if poolSize == 1 {
		return fmt.Sprintf("%s:%s:%s", role, shardID, currency), nil
	}

	slotRoutingKey := fmt.Sprintf("%s:%s:%s:%s", routingKey, shardID, currency, role)
	slot := deterministicSlotIndex(slotRoutingKey, poolSize)
	return fmt.Sprintf("%s:%s:%s:%d", role, shardID, currency, slot), nil
}

func (r Router) poolSizeForRole(role SystemAccountRole) int {
	if size, ok := r.rolePoolSizes[role]; ok {
		return size
	}
	return 1
}

func (role SystemAccountRole) Validate() error {
	if strings.TrimSpace(string(role)) == "" {
		return fmt.Errorf("system account role is required")
	}
	return nil
}

func (r Router) ShardIDs() []ShardID {
	shardIDs := make([]ShardID, len(r.shardIDs))
	copy(shardIDs, r.shardIDs)
	return shardIDs
}

func rendezvousPickShard(key string, shardIDs []ShardID) ShardID {
	bestShardID := shardIDs[0]
	bestScore := rendezvousScore(key, string(bestShardID))

	for _, shardID := range shardIDs[1:] {
		score := rendezvousScore(key, string(shardID))
		if score > bestScore {
			bestShardID = shardID
			bestScore = score
		}
	}

	return bestShardID
}

func deterministicSlotIndex(key string, poolSize int) int {
	return int(crc32.ChecksumIEEE([]byte(key)) % uint32(poolSize))
}

func rendezvousScore(key, candidate string) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	_, _ = hasher.Write([]byte{0})
	_, _ = hasher.Write([]byte(candidate))
	return hasher.Sum64()
}
