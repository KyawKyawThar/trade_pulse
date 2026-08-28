package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

// fakeAdmin stands in for *kadm.Client so lag can be exercised without a
// broker.
type fakeAdmin struct {
	lags     kadm.DescribedGroupLags
	err      error
	askedFor []string
}

func (f *fakeAdmin) Lag(_ context.Context, groups ...string) (kadm.DescribedGroupLags, error) {
	f.askedFor = append(f.askedFor, groups...)
	return f.lags, f.err
}

// describedLag builds a kadm result for one group over one topic.
func describedLag(group, topic, state string, members int, perPartition map[int32]kadm.GroupMemberLag) kadm.DescribedGroupLags {
	described := kadm.DescribedGroupLag{
		Group:   group,
		State:   state,
		Lag:     kadm.GroupLag{topic: perPartition},
		Members: make([]kadm.DescribedGroupMember, members),
	}

	return kadm.DescribedGroupLags{group: described}
}

func TestReadLagSumsPartitionLag(t *testing.T) {
	admin := &fakeAdmin{lags: describedLag("processor-service", "trades.raw", "Stable", 2, map[int32]kadm.GroupMemberLag{
		0: {Partition: 0, Lag: 12},
		1: {Partition: 1, Lag: 30},
	})}

	lag, err := NewLagReaderWithAdmin(admin, "processor-service", "trades.raw").ReadLag(context.Background())

	if err != nil {
		t.Fatalf("ReadLag() error = %v", err)
	}

	if lag.Total != 42 {
		t.Fatalf("total lag = %d, want 42", lag.Total)
	}

	if lag.State != "Stable" || lag.Members != 2 {
		t.Fatalf("lag = %+v, want Stable group with 2 members", lag)
	}

	if len(lag.Partitions) != 2 || lag.Partitions[0].Partition != 0 || lag.Partitions[1].Partition != 1 {
		t.Fatalf("partitions = %+v, want sorted partitions 0,1", lag.Partitions)
	}

	if len(admin.askedFor) != 1 || admin.askedFor[0] != "processor-service" {
		t.Fatalf("asked for groups %v, want [processor-service]", admin.askedFor)
	}
}

// A partition kadm could not compute reports Lag -1; counting that as zero
// would make an unknown lag read as a healthy one.
func TestReadLagExcludesUncomputablePartitionsFromTotal(t *testing.T) {
	admin := &fakeAdmin{lags: describedLag("g", "t", "Stable", 1, map[int32]kadm.GroupMemberLag{
		0: {Partition: 0, Lag: 7},
		1: {Partition: 1, Lag: -1, Err: errors.New("list offsets failed")},
	})}

	lag, err := NewLagReaderWithAdmin(admin, "g", "t").ReadLag(context.Background())

	if err != nil {
		t.Fatalf("ReadLag() error = %v", err)
	}

	if lag.Total != 7 {
		t.Fatalf("total lag = %d, want 7 (broken partition excluded)", lag.Total)
	}

	if lag.Partitions[1].Error == "" {
		t.Fatalf("partition 1 = %+v, want the per-partition error reported", lag.Partitions[1])
	}
}

// An Empty group with lag is the consumer-is-down signal.
func TestReadLagReportsEmptyGroupAsStalled(t *testing.T) {
	admin := &fakeAdmin{lags: describedLag("g", "t", "Empty", 0, map[int32]kadm.GroupMemberLag{
		0: {Partition: 0, Lag: 9001},
	})}

	lag, err := NewLagReaderWithAdmin(admin, "g", "t").ReadLag(context.Background())

	if err != nil {
		t.Fatalf("ReadLag() error = %v", err)
	}

	if lag.State != "Empty" || lag.Members != 0 || lag.Total != 9001 {
		t.Fatalf("lag = %+v, want Empty group with 0 members and lag 9001", lag)
	}

	if !lag.Stalled() {
		t.Fatal("Stalled() = false, want true for lag with no members")
	}
}

// A caught-up group with no members is idle, not stalled — there is nothing to
// drain, so it must not page anyone.
func TestZeroLagWithoutMembersIsNotStalled(t *testing.T) {
	lag := ConsumerLag{Members: 0, Total: 0}

	if lag.Stalled() {
		t.Fatal("Stalled() = true, want false for a caught-up idle group")
	}
}

func TestReadLagErrorsWhenGroupMissing(t *testing.T) {
	if _, err := NewLagReaderWithAdmin(&fakeAdmin{lags: kadm.DescribedGroupLags{}}, "g", "t").ReadLag(context.Background()); err == nil {
		t.Fatal("ReadLag() error = nil, want an error for a group the broker does not know")
	}
}

func TestReadLagPropagatesAdminError(t *testing.T) {
	admin := &fakeAdmin{err: errors.New("broker unreachable")}

	_, err := NewLagReaderWithAdmin(admin, "g", "t").ReadLag(context.Background())

	if err == nil || !errors.Is(err, admin.err) {
		t.Fatalf("ReadLag() error = %v, want it to wrap %v", err, admin.err)
	}
}

// A nil reader must degrade, not panic: nothing stops a caller boxing one into
// an interface.
func TestReadLagOnNilReaderReturnsError(t *testing.T) {
	var reader *LagReader

	if _, err := reader.ReadLag(context.Background()); err == nil {
		t.Fatal("ReadLag() error = nil, want an error from a nil reader")
	}
}

func TestNewLagReaderRejectsIncompleteConfig(t *testing.T) {
	for name, tc := range map[string]struct {
		brokers      []string
		group, topic string
	}{
		"no brokers": {nil, "g", "t"},
		"no group":   {[]string{"localhost:9092"}, "", "t"},
		"no topic":   {[]string{"localhost:9092"}, "g", ""},
	} {
		if _, err := NewLagReader(tc.brokers, tc.group, tc.topic); err == nil {
			t.Fatalf("%s: NewLagReader() error = nil, want an error", name)
		}
	}
}
