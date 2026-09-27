package logkafka

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/QuantumNous/new-api/common"
)

// replayReadBuffer sizes the reader the replay uses per segment. A row can be
// large, so the buffer only has to be big enough that an ordinary row is read in
// one syscall.
const replayReadBuffer = 1 << 20

// replayLoop drains sealed segments back to the topic, oldest first, one segment
// at a time and one row at a time inside it.
//
// That is not a throughput decision. It is what makes "the whole segment was
// acknowledged, or none of it was removed" true, which is in turn the only thing
// that bounds the duplicates this path can create: everything else follows from
// the removal being the acknowledgement.
//
// Replay runs once as soon as it starts rather than waiting for the first tick,
// so a process that starts after an outage begins draining immediately.
func (s *Spool) replayLoop(ctx context.Context, deliver func([]byte, func(error)), healthy func() bool) {
	defer close(s.replayStopped)

	var notBefore time.Time
	for {
		if ctx.Err() != nil {
			return
		}
		if !time.Now().Before(notBefore) {
			s.replayOnce(ctx, deliver, healthy, &notBefore)
		}
		// sleepUntil returns on either the interval or the context ending, and
		// the check after it is what tells the two apart.
		sleepUntil(ctx, s.replayInterval)
	}
}

func (s *Spool) replayOnce(ctx context.Context, deliver func([]byte, func(error)), healthy func() bool, notBefore *time.Time) {
	if !healthy() {
		// The replay health criterion is the lower edge of the same hysteresis
		// band the request path branches on, over the same measurement. Both
		// edges are needed: replaying into a producer that is still behind would
		// refill the buffer that is being drained, and the request path would go
		// on writing to the disk.
		return
	}
	path, ok := s.oldestSealedSegment()
	if !ok {
		return
	}
	if !s.replaySegment(ctx, path, deliver) && ctx.Err() == nil {
		// A failed replay keeps the whole segment. It does not retry before the
		// delivery timeout -- the same budget a single produce request gets --
		// because retrying a broker that is down immediately only burns CPU.
		*notBefore = time.Now().Add(s.retryInterval)
	}
}

// oldestSealedSegment returns the segment that has been waiting longest. It is
// derived from the directory every time rather than from a registry shared with
// the writer: the filesystem is the state, so the two goroutines cannot disagree
// about what exists, and an unlink either of them performs is atomic.
func (s *Spool) oldestSealedSegment() (string, bool) {
	segments := s.sealedSegmentsOldestFirst()
	if len(segments) == 0 {
		return "", false
	}
	return segments[0], true
}

// replaySegment delivers every row of one segment and removes it once every row
// has been acknowledged. It reports whether the segment is finished with.
//
// A failure leaves the file exactly as it was. Nothing is removed, nothing is
// rewritten, and in particular nothing is written back to the spool: rows that
// failed to replay must never be offered to Stash again.
func (s *Spool) replaySegment(ctx context.Context, path string, deliver func([]byte, func(error))) bool {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// Evicted, expired, or already replayed: nothing is left to do with
			// this name, and it is not a loss.
			return true
		}
		s.replayFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not open the segment %s for replay: %s", filepath.Base(path), err))
		return false
	}
	defer file.Close()

	s.replaying.Store(true)
	defer s.replaying.Store(false)
	common.SysLog(fmt.Sprintf("kafka log spool: replaying the sealed segment %s", filepath.Base(path)))

	reader := bufio.NewReaderSize(file, replayReadBuffer)
	rows := int64(0)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if line[len(line)-1] != '\n' {
				// A sealed segment ends with a newline by construction; this is
				// the second net, for a segment an earlier process left behind.
				s.tornRows.Add(1)
				s.tornBytes.Add(int64(len(line)))
				s.report(fmt.Sprintf("the kafka log spool found a partial line at the end of the sealed segment %s, so it was skipped", filepath.Base(path)))
			} else if !deliverAndWait(ctx, deliver, line[:len(line)-1]) {
				if ctx.Err() != nil {
					return false
				}
				s.replayFailed.Add(1)
				s.report(fmt.Sprintf("the kafka log spool could not replay the segment %s after %d row(s); the whole segment is kept and will be replayed again", filepath.Base(path), rows))
				return false
			} else {
				rows++
			}
		}
		if err != nil {
			break
		}
	}

	// Removal is the acknowledgement, and it is the only one. A crash before it
	// means the whole segment is delivered again -- which is where the
	// at-least-once duplicates come from, and why a segment is never removed
	// after a partial delivery.
	//
	// The read handle is closed first, and on Windows that is not tidiness: a
	// file handle there is opened without FILE_SHARE_DELETE, so a process cannot
	// unlink a file it is holding open -- not even one it just finished reading
	// itself. Leaving the handle to the deferred close made every replay report a
	// removal failure and re-deliver the segment forever.
	if err := file.Close(); err != nil {
		s.replayFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool could not close the segment %s after replaying %d row(s): %s", filepath.Base(path), rows, err))
		return false
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		s.replayFailed.Add(1)
		s.report(fmt.Sprintf("the kafka log spool replayed %d row(s) of %s but could not remove it, so they will be delivered again: %s", rows, filepath.Base(path), err))
		return false
	}
	s.replayedSegments.Add(1)
	s.replayedRows.Add(rows)
	return true
}

// deliverAndWait hands one row to the broker and waits for its acknowledgement.
// The callback channel is buffered because TryProduce reports a full buffer by
// calling its promise inline, which would otherwise block on a channel that
// nobody is reading yet.
func deliverAndWait(ctx context.Context, deliver func([]byte, func(error)), payload []byte) bool {
	done := make(chan error, 1)
	deliver(payload, func(err error) { done <- err })
	select {
	case err := <-done:
		return err == nil
	case <-ctx.Done():
		return false
	}
}
