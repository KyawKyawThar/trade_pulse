// Package kafka holds the broker-side helpers shared by every service that
// talks to Kafka: today, consumer-group lag observation for health reports and
// (from Sprint 4) Prometheus metrics.
//
// Lag is read through the broker's admin API rather than by joining the group.
// A service that only wants to *observe* a group must never become a member of
// it: joining triggers a rebalance and takes partitions away from the real
// consumer. Asking the coordinator directly also keeps reporting the truth when
// the consumer is dead — its committed offsets freeze while end offsets climb,
// so lag visibly grows and the group state reads Empty, instead of the report
// going silent exactly when it matters.
package kafka

import (
	"context"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// groupLagReader is the subset of *kadm.Client that LagReader depends on.
// Defined on this side (accept interfaces, return structs) so ReadLag can be
// exercised against a fake instead of a live broker.
type groupLagReader interface {
	Lag(ctx context.Context, groups ...string) (kadm.DescribedGroupLags, error)
}

// PartitionLag is how far behind a group is on one partition.
type PartitionLag struct {
	Partition int32  `json:"partition"`
	Lag       int64  `json:"lag"`
	Error     string `json:"error,omitempty"` // set when this partition's lag could not be computed
}

// ConsumerLag is a point-in-time view of one consumer group's progress on one
// topic.
//
// State is the group's Kafka state: "Stable" with members is the healthy case;
// "Empty" means the group exists but nothing is consuming it right now — the
// signal that the consuming service is down, which is exactly when Total keeps
// climbing.
type ConsumerLag struct {
	Group      string         `json:"group"`
	Topic      string         `json:"topic"`
	State      string         `json:"state"`
	Members    int            `json:"members"`
	Total      int64          `json:"total"`
	Partitions []PartitionLag `json:"partitions,omitempty"`
}

// Stalled reports whether the group has lag but nobody consuming it. This is
// the failure worth paging on: a merely large Total may just be a cold start
// catching up, but lag with no members means nothing is draining it.
func (l ConsumerLag) Stalled() bool { return l.Members == 0 && l.Total > 0 }

// LagReader observes one consumer group's lag on one topic. Safe for
// concurrent use; kadm's client is.
type LagReader struct {
	admin groupLagReader
	group string
	topic string
	close func()
}

// NewLagReader dials brokers with a plain, group-less client used only for
// admin requests. The caller owns Close.
func NewLagReader(brokers []string, group, topic string) (*LagReader, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("kafka lag: no broker configured")
	}

	if group == "" || topic == "" {
		return nil, fmt.Errorf("kafka lag: group and topic are required")
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafka lag: new admin client: %w", err)
	}

	admin := kadm.NewClient(client)

	return &LagReader{admin: admin, group: group, topic: topic, close: admin.Close}, nil
}

// NewLagReaderWithAdmin builds a reader around an existing admin client,
// primarily for tests.
func NewLagReaderWithAdmin(admin groupLagReader, group, topic string) *LagReader {
	return &LagReader{admin: admin, group: group, topic: topic}
}

// ReadLag queries the group coordinator. A partition whose lag could not be
// computed (kadm reports -1 with an error) is reported individually and left
// out of Total rather than silently counted as zero — an unknown lag must not
// read as a healthy one.
func (l *LagReader) ReadLag(ctx context.Context) (ConsumerLag, error) {
	if l == nil || l.admin == nil {
		return ConsumerLag{}, fmt.Errorf("kafka lag: reader not configured")
	}

	lags, err := l.admin.Lag(ctx, l.group)
	if err != nil {
		return ConsumerLag{}, fmt.Errorf("kafka lag: %w", err)
	}

	described, ok := lags[l.group]
	if !ok {
		return ConsumerLag{}, fmt.Errorf("kafka lag: group %q not found", l.group)
	}

	if err := described.Error(); err != nil {
		return ConsumerLag{}, fmt.Errorf("kafka lag: describe group %q: %w", l.group, err)
	}

	lag := ConsumerLag{
		Group:   l.group,
		Topic:   l.topic,
		State:   described.State,
		Members: len(described.Members),
	}

	for partition, member := range described.Lag[l.topic] {
		entry := PartitionLag{Partition: partition, Lag: member.Lag}

		if member.Err != nil {
			entry.Error = member.Err.Error()
		} else {
			lag.Total += member.Lag
		}

		lag.Partitions = append(lag.Partitions, entry)
	}

	sort.Slice(lag.Partitions, func(i, j int) bool {
		return lag.Partitions[i].Partition < lag.Partitions[j].Partition
	})

	return lag, nil
}

// Close releases the admin client's broker connections.
func (l *LagReader) Close() {
	if l != nil && l.close != nil {
		l.close()
	}
}
