package resource

import (
	"fmt"
	"path"
	"scow-slurm-adapter/caller"
	"time"

	"github.com/fsnotify/fsnotify"
)

type EventType string

const (
	IntervalBased EventType = "intervalBased"
	FSUpdate      EventType = "fsUpdate"
)

type Notifier struct {
	sleepInterval time.Duration
	// destination where notifications are sent
	dest    chan<- Info
	fsEvent <-chan fsnotify.Event
}

type Info struct {
	Event EventType
}

func newNotifier(sleepInterval time.Duration, dest chan<- Info, slurmConfigPath string) (*Notifier, error) {
	ch, err := createFSWatcherEvent([]string{slurmConfigPath})
	if err != nil {
		return nil, err
	}

	return &Notifier{
		sleepInterval: sleepInterval,
		dest:          dest,
		fsEvent:       ch,
	}, nil
}

func createFSWatcherEvent(fsWatchPaths []string) (chan fsnotify.Event, error) {
	fsWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	for _, path := range fsWatchPaths {
		if err = fsWatcher.Add(path); err != nil {
			return nil, fmt.Errorf("failed to watch: %q; %w", path, err)
		}
	}
	return fsWatcher.Events, nil
}

func (n *Notifier) Run() {
	timeEvents := make(<-chan time.Time)
	if n.sleepInterval > 0 {
		ticker := time.NewTicker(n.sleepInterval)
		defer ticker.Stop()
		timeEvents = ticker.C
	}

	for {
		select {
		case <-timeEvents:
			caller.Logger.Tracef("timer update received")
			i := Info{Event: IntervalBased}
			n.dest <- i

		case e := <-n.fsEvent:
			basename := path.Base(e.Name)
			caller.Logger.Tracef("fsnotify event received filename %s, op: %v", basename, e.Op)
			if basename == "slurm.conf" {
				i := Info{Event: FSUpdate}
				n.dest <- i
			}
		}
	}
}
