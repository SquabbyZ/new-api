package logkafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// PollInterval is how often the broker is asked for the series that cannot be
// read from memory. It is a constant rather than a setting on purpose: the 30 s
// warning threshold is judged within about one interval of it being crossed,
// and a five minute rate window holds twenty samples.
const PollInterval = 15 * time.Second

// GroupStateAbsent is the group state reported when there is nothing to be
// behind: either the broker does not know the group, or it knows it and the
// group has never committed an offset for this topic.
//
// Both cases have to land here, and the second is the one that matters. Measured
// against Kafka 3.7.0, a group that has just been created but has committed
// nothing is reported as Empty, and a group whose only member is joining is
// reported as PreparingRebalance. Empty is a state operators alert on, so a
// first start would raise an alarm about a consumer that is working perfectly.
const GroupStateAbsent = "Absent"

// Kafka's sentinel for the log-end offset. It cannot be paired with a request
// for the log-start offset: the timestamp is a per-partition field from v1 on.
const listOffsetsLatest = -1

// listOffsetsOldestTimestamp is one millisecond past the epoch, used instead of
// Kafka's -2 sentinel to find the oldest retained record.
//
// The reason is measured, not stylistic. Against Kafka 3.7.0, a ListOffsets
// query with -1 or -2 answers with the offset but always sets the response
// Timestamp to -1: the log-end offset carries no time, so the log-end record's
// timestamp cannot be read from it, and every record of a real topic is after
// one millisecond past the epoch, so this form returns the oldest record *and*
// its timestamp.
const listOffsetsOldestTimestamp = 1

// BrokerStatus is one poll's worth of broker-derived state. Every field here is
// something no counter in this process can answer: how far behind the group is
// in wall-clock terms, whether it is still a live group, and how much of the
// retention window has been used.
type BrokerStatus struct {
	// Partitions is one entry per partition of the log topic, in partition
	// order. An empty slice is a real answer for an empty topic.
	Partitions []PartitionStatus
	// GroupState is one of Kafka's group states, or GroupStateAbsent.
	GroupState string
	// RetentionUsedRatio is nil when the topic holds no records at all: with
	// nothing retained there is no age to report, and neither 0 nor 1 would be
	// true.
	RetentionUsedRatio *float64
}

// PartitionStatus is one partition's offsets and its time lag.
type PartitionStatus struct {
	Partition int32
	// LagRecords is the log-end offset minus the group's committed offset, and
	// it is always known: a group with no commit at all starts at the beginning
	// of the topic, so its lag is the log-end offset.
	LagRecords int64
	// LagSeconds is the age of the oldest record this consumer still has to
	// process: the first record at or after the consumer's position, measured
	// against now. It is zero when the group has committed everything the topic
	// holds, and nil when the answer is unknown -- either because this process
	// has never consumed the partition, or because the broker could not answer
	// for it. Absent and zero are different claims and only one of them is true
	// in each case; a zero would draw a healthy line for a partition nothing
	// has drained.
	LagSeconds *float64
}

// PollBroker asks the broker for the whole derived picture, and returns an
// error if any part of it is unknown. A partial answer is deliberately not
// returned: a partition dropped from the result reads as a partition with no
// backlog, which is the one misreading this whole path exists to prevent. The
// caller turns the error into "the derived series are absent" plus a failed
// liveness gate, never into stale values.
func (c *Consumer) PollBroker(ctx context.Context) (BrokerStatus, error) {
	pollCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()

	partitions, err := c.topicPartitions(pollCtx)
	if err != nil {
		return BrokerStatus{}, err
	}

	// The log-end offset of every partition, which is what the records lag and
	// the "is this partition empty" question are both read from.
	latest := make(map[int32]int64, len(partitions))
	oldestProbe := make(map[int32]int64, len(partitions))
	for _, partition := range partitions {
		latest[partition] = listOffsetsLatest
		oldestProbe[partition] = listOffsetsOldestTimestamp
	}
	atLatest, err := c.listOffsets(pollCtx, latest)
	if err != nil {
		return BrokerStatus{}, err
	}
	atOldest, err := c.listOffsets(pollCtx, oldestProbe)
	if err != nil {
		return BrokerStatus{}, err
	}

	// The lag probe: from the consumer's own position, the first record it
	// still has to process. It is asked for only the partitions this process
	// has consumed, because for the others the answer would be "the beginning
	// of the topic", which is a fact about the topic rather than about the
	// consumer, and would be reported as a lag that has nothing to do with it.
	consumed := c.lastConsumed()
	lagProbe := make(map[int32]int64, len(consumed))
	for partition, last := range consumed {
		if _, ok := latest[partition]; !ok {
			continue
		}
		lagProbe[partition] = last.UnixMilli() + 1
	}
	behind := map[int32]kmsg.ListOffsetsResponseTopicPartition{}
	if len(lagProbe) > 0 {
		behind, err = c.listOffsets(pollCtx, lagProbe)
		if err != nil {
			return BrokerStatus{}, err
		}
	}

	groupState, err := c.describeGroup(pollCtx)
	if err != nil {
		return BrokerStatus{}, err
	}
	committed := c.client.CommittedOffsets()[c.topic]

	now := time.Now()
	status := BrokerStatus{GroupState: groupState, Partitions: make([]PartitionStatus, 0, len(partitions))}
	var oldestAt time.Time
	for _, partition := range partitions {
		end := atLatest[partition]
		committedOffset := committed[partition].Offset
		_, consumedPartition := consumed[partition]

		partitionStatus := PartitionStatus{
			Partition:  partition,
			LagRecords: max(end.Offset-committedOffset, 0),
		}
		switch {
		case !consumedPartition:
			// This process has never consumed the partition, so it cannot say
			// how far behind in time it is. Reporting zero here would claim an
			// observation nobody made.
		case end.Offset <= committedOffset:
			// The group has committed everything the topic holds, so it is
			// caught up by definition and there is no lag to measure.
			zero := 0.0
			partitionStatus.LagSeconds = &zero
		case behind[partition].Offset >= 0 && behind[partition].Timestamp >= 0:
			seconds := max(now.Sub(time.UnixMilli(behind[partition].Timestamp)).Seconds(), 0)
			partitionStatus.LagSeconds = &seconds
		}
		status.Partitions = append(status.Partitions, partitionStatus)

		// The oldest retained record of any partition is the topic's, and it is
		// what the retention window is measured from.
		if record := atOldest[partition]; record.Offset < end.Offset && record.Timestamp >= 0 {
			if at := time.UnixMilli(record.Timestamp); oldestAt.IsZero() || at.Before(oldestAt) {
				oldestAt = at
			}
		}
	}

	// A group that holds no committed offset for this topic has nothing to be
	// behind yet. Its state would otherwise be Empty (a group that exists and
	// has done nothing), which is one of the states operators alert on, so a
	// deployment that has just started would page about itself.
	if len(committed) == 0 {
		status.GroupState = GroupStateAbsent
	}

	if !oldestAt.IsZero() && c.retentionHours > 0 {
		window := float64(c.retentionHours) * 3600
		ratio := min(max(now.Sub(oldestAt).Seconds()/window, 0), 1)
		status.RetentionUsedRatio = &ratio
	}
	return status, nil
}

// topicPartitions asks the broker for the log topic's partition set. It is
// cached for one interval, so the discovery costs one request per interval at
// most, and the partition count is an operator's decision that does not change
// under a running process.
func (c *Consumer) topicPartitions(ctx context.Context) ([]int32, error) {
	topic := c.topic
	request := kmsg.NewPtrMetadataRequest()
	request.Topics = []kmsg.MetadataRequestTopic{{Topic: &topic}}

	response, err := c.client.RequestCachedMetadata(ctx, request, PollInterval)
	if err != nil {
		return nil, fmt.Errorf("the kafka log metrics poller could not read the metadata of topic %q: %w", c.topic, err)
	}
	for _, described := range response.Topics {
		if described.Topic == nil || *described.Topic != c.topic {
			continue
		}
		if err := kerr.ErrorForCode(described.ErrorCode); err != nil {
			return nil, fmt.Errorf("the kafka log metrics poller could not describe topic %q: %w", c.topic, err)
		}
		partitions := make([]int32, 0, len(described.Partitions))
		for _, partition := range described.Partitions {
			partitions = append(partitions, partition.Partition)
		}
		slices.Sort(partitions)
		return partitions, nil
	}
	return nil, fmt.Errorf("the kafka log metrics poller found no metadata for topic %q, and this application does not create topics", c.topic)
}

// listOffsets reads one offset per partition, each for its own timestamp. The
// timestamp is what makes the seconds possible at all: a query with a real
// timestamp answers with the offset of the first record at or after it and with
// that record's own timestamp, which is the only form of this request that
// carries any time back.
func (c *Consumer) listOffsets(ctx context.Context, timestamps map[int32]int64) (map[int32]kmsg.ListOffsetsResponseTopicPartition, error) {
	request := kmsg.NewPtrListOffsetsRequest()
	// -1 is the consumer replica id: ask the leader, not a follower.
	request.ReplicaID = -1
	request.Topics = []kmsg.ListOffsetsRequestTopic{{
		Topic:      c.topic,
		Partitions: make([]kmsg.ListOffsetsRequestTopicPartition, 0, len(timestamps)),
	}}
	for partition, timestamp := range timestamps {
		request.Topics[0].Partitions = append(request.Topics[0].Partitions, kmsg.ListOffsetsRequestTopicPartition{
			Partition: partition,
			Timestamp: timestamp,
		})
	}

	response, err := c.client.Request(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("the kafka log metrics poller could not list offsets of topic %q: %w", c.topic, err)
	}
	listOffsets, ok := response.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("the kafka log metrics poller got %T instead of a list offsets response", response)
	}

	offsets := make(map[int32]kmsg.ListOffsetsResponseTopicPartition, len(timestamps))
	for _, described := range listOffsets.Topics {
		if described.Topic != c.topic {
			continue
		}
		for _, partition := range described.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				return nil, fmt.Errorf("the kafka log metrics poller could not list the offset of topic %q partition %d: %w", c.topic, partition.Partition, err)
			}
			offsets[partition.Partition] = partition
		}
	}
	if len(offsets) != len(timestamps) {
		return nil, fmt.Errorf("the kafka log metrics poller asked for %d partition(s) of topic %q and got %d", len(timestamps), c.topic, len(offsets))
	}
	return offsets, nil
}

// describeGroup reads the group's state. A group the broker does not know is
// reported as absent rather than as dead, which is the difference between "this
// deployment has not committed yet" and "the consumer has stopped".
func (c *Consumer) describeGroup(ctx context.Context) (string, error) {
	request := kmsg.NewPtrDescribeGroupsRequest()
	request.Groups = []string{c.groupID}

	response, err := c.client.Request(ctx, request)
	if err != nil {
		return "", fmt.Errorf("the kafka log metrics poller could not describe group %q: %w", c.groupID, err)
	}
	described, ok := response.(*kmsg.DescribeGroupsResponse)
	if !ok {
		return "", fmt.Errorf("the kafka log metrics poller got %T instead of a describe groups response", response)
	}
	for _, group := range described.Groups {
		if group.Group != c.groupID {
			continue
		}
		if errors.Is(kerr.ErrorForCode(group.ErrorCode), kerr.GroupIDNotFound) {
			return GroupStateAbsent, nil
		}
		if err := kerr.ErrorForCode(group.ErrorCode); err != nil {
			return "", fmt.Errorf("the kafka log metrics poller could not describe group %q: %w", c.groupID, err)
		}
		return group.State, nil
	}
	return GroupStateAbsent, nil
}
