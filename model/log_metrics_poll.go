package model

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/pkg/logkafka"
)

// The broker poll behind the derived series.
//
// The lag cannot be derived from anything this process already holds. The
// consumer sees a partition's high water mark while it is fetching, and reading
// lag from it would cost no requests at all, but it is the wrong number: when a
// batch cannot be written and the batch is full, the consumer stops polling and
// retries instead, so the high water mark freezes for as long as the log
// database is refusing writes. That is precisely the incident the lag is meant
// to describe. The poll below asks the broker directly, so its answer keeps
// moving while the consumer is stuck.
//
// It runs only while the metrics endpoint is up. With METRICS_ADDR unset there
// is nobody to read the derived series, so the requests are not spent.
const (
	// pollReportEvery rate-limits the poller's own error line. A broker that is
	// down fails every interval; the first failure is always reported, and the
	// rest are, because four lines a minute would drown the log this endpoint
	// complements.
	pollReportEvery = 20
)

var (
	logMetricsPollStartOnce sync.Once
	logMetricsPollStopOnce  sync.Once
	logMetricsPollCancel    context.CancelFunc
	logMetricsPollDone      chan struct{}

	// logMetricsPollStatus holds the last successful poll, or nil when the
	// most recent attempt failed. Failure stores nil rather than leaving the
	// previous value in place, which is what keeps a stale answer out of the
	// exposition.
	logMetricsPollStatus atomic.Pointer[logkafka.BrokerStatus]
	// logMetricsPollSuccess is whether the most recent attempt succeeded. It is
	// false before the first attempt, so a process that has just started never
	// reports itself healthy for the interval before its first poll.
	logMetricsPollSuccess atomic.Bool
	logMetricsPollErrors  atomic.Int64
)

// startLogMetricsPoll begins polling the broker for the derived series. It must
// run before the consumer that owns the client is closed.
func startLogMetricsPoll(consumer *logkafka.Consumer) {
	logMetricsPollStartOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		logMetricsPollCancel = cancel
		logMetricsPollDone = done
		go func() {
			defer close(done)
			pollLogBroker(ctx, consumer)
		}()
	})
}

// stopLogMetricsPoll stops the poller and waits for it. The poller shares the
// consumer's client, so it has to be off that client before the client is
// closed; closing it first would end the poll in flight and print a "client
// closed" error during a normal shutdown, which reads like a fault.
func stopLogMetricsPoll() {
	logMetricsPollStopOnce.Do(func() {
		if logMetricsPollCancel == nil {
			return
		}
		logMetricsPollCancel()
		<-logMetricsPollDone
	})
}

// pollLogBroker asks the broker once immediately and then once per interval.
// The immediate first poll is what keeps the endpoint from answering with
// "no data" for a whole interval after a restart.
func pollLogBroker(ctx context.Context, consumer *logkafka.Consumer) {
	ticker := time.NewTicker(logkafka.PollInterval)
	defer ticker.Stop()

	for {
		status, err := consumer.PollBroker(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logMetricsPollStatus.Store(nil)
			logMetricsPollSuccess.Store(false)
			if failures := logMetricsPollErrors.Add(1); failures == 1 || failures%pollReportEvery == 0 {
				common.SysError(fmt.Sprintf("the log metrics poller could not read the broker's state (%d failure(s) so far), so the lag, retention and group state series are absent rather than stale: %s", failures, err.Error()))
			}
		} else {
			logMetricsPollStatus.Store(&status)
			logMetricsPollSuccess.Store(true)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// logMetricsStatusSnapshot returns the last successful poll. The second result
// is false when the most recent attempt failed, which is what removes the
// derived series from the exposition.
func logMetricsStatusSnapshot() (logkafka.BrokerStatus, bool) {
	status := logMetricsPollStatus.Load()
	if status == nil {
		return logkafka.BrokerStatus{}, false
	}
	return *status, true
}
