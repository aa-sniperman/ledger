package sharding

import (
	"fmt"
	"strings"
	"testing"
)

func TestShardForUserIsDeterministic(t *testing.T) {
	t.Parallel()

	router, err := NewRouter([]ShardID{"shard-a", "shard-b", "shard-c"}, map[SystemAccountRole]int{
		SystemAccountRolePayoutHold: 4,
	})
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	first, err := router.ShardForUser("user_123")
	if err != nil {
		t.Fatalf("route user first time: %v", err)
	}

	second, err := router.ShardForUser("user_123")
	if err != nil {
		t.Fatalf("route user second time: %v", err)
	}

	if first != second {
		t.Fatalf("expected deterministic shard routing, got %q then %q", first, second)
	}
}

func TestShardForUserIsIndependentOfShardInputOrder(t *testing.T) {
	t.Parallel()

	firstRouter, err := NewRouter([]ShardID{"shard-a", "shard-b", "shard-c"}, nil)
	if err != nil {
		t.Fatalf("build first router: %v", err)
	}

	secondRouter, err := NewRouter([]ShardID{"shard-c", "shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build second router: %v", err)
	}

	firstShard, err := firstRouter.ShardForUser("user_123")
	if err != nil {
		t.Fatalf("route user with first router: %v", err)
	}

	secondShard, err := secondRouter.ShardForUser("user_123")
	if err != nil {
		t.Fatalf("route user with second router: %v", err)
	}

	if firstShard != secondShard {
		t.Fatalf("expected shard routing to be independent of shard input order, got %q then %q", firstShard, secondShard)
	}
}

func TestSystemAccountForUserStaysInUserShard(t *testing.T) {
	t.Parallel()

	router, err := NewRouter([]ShardID{"shard-a", "shard-b"}, map[SystemAccountRole]int{
		SystemAccountRolePayoutHold: 4,
	})
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	accountID, shardID, err := router.SystemAccountForUser("user_123", SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("route system account: %v", err)
	}

	accountShard, err := router.ShardForAccount(accountID)
	if err != nil {
		t.Fatalf("route system account by id: %v", err)
	}

	if accountShard != shardID {
		t.Fatalf("expected system account shard %q to match routed user shard %q", accountShard, shardID)
	}
}

func TestSystemAccountForUserUsesDeterministicPoolSlot(t *testing.T) {
	t.Parallel()

	router, err := NewRouter([]ShardID{"shard-a", "shard-b"}, map[SystemAccountRole]int{
		SystemAccountRolePayoutHold: 8,
	})
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	first, _, err := router.SystemAccountForUser("user_123", SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("first account selection: %v", err)
	}

	second, _, err := router.SystemAccountForUser("user_123", SystemAccountRolePayoutHold)
	if err != nil {
		t.Fatalf("second account selection: %v", err)
	}

	if first != second {
		t.Fatalf("expected deterministic pool selection, got %q then %q", first, second)
	}
}

func TestShardForAccountRoutesUserWalletByUserID(t *testing.T) {
	t.Parallel()

	router, err := NewRouter([]ShardID{"shard-a", "shard-b"}, nil)
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	expected, err := router.ShardForUser("user_123")
	if err != nil {
		t.Fatalf("route user: %v", err)
	}

	actual, err := router.ShardForAccount(UserAccountID("user_123"))
	if err != nil {
		t.Fatalf("route user account: %v", err)
	}

	if actual != expected {
		t.Fatalf("expected user account shard %q, got %q", expected, actual)
	}
}

func TestNewRouterRejectsInvalidShardIDs(t *testing.T) {
	t.Parallel()

	_, err := NewRouter([]ShardID{"Shard A"}, nil)
	if err == nil {
		t.Fatal("expected invalid shard id error")
	}
}

func TestSystemAccountPoolSpreadsUsersAcrossShardLocalSlots(t *testing.T) {
	t.Parallel()

	router, err := NewRouter([]ShardID{"shard-a", "shard-b"}, map[SystemAccountRole]int{
		SystemAccountRolePayoutHold: 4,
	})
	if err != nil {
		t.Fatalf("build router: %v", err)
	}

	shardID := ShardID("shard-a")
	slotCounts := map[string]int{
		"0": 0,
		"1": 0,
		"2": 0,
		"3": 0,
	}

	seenUsers := 0
	for i := 1; i <= 5000 && seenUsers < 200; i++ {
		userID := fmt.Sprintf("user_%d", i)
		userShardID, err := router.ShardForUser(userID)
		if err != nil {
			t.Fatalf("route user %s: %v", userID, err)
		}
		if userShardID != shardID {
			continue
		}

		accountID, systemShardID, err := router.SystemAccountForUser(userID, SystemAccountRolePayoutHold)
		if err != nil {
			t.Fatalf("pick system account for %s: %v", userID, err)
		}
		if systemShardID != shardID {
			t.Fatalf("expected system shard %s, got %s", shardID, systemShardID)
		}

		parts := strings.Split(accountID, ":")
		if len(parts) != 3 {
			t.Fatalf("expected pooled system account id, got %q", accountID)
		}

		slotCounts[parts[2]]++
		seenUsers++
	}

	if seenUsers < 200 {
		t.Fatalf("expected at least 200 users in shard %s, got %d", shardID, seenUsers)
	}

	for slot, count := range slotCounts {
		if count == 0 {
			t.Fatalf("expected slot %s to receive routed users, got counts %+v", slot, slotCounts)
		}
	}
}
