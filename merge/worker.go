package merge

import (
	"ashishkujoy/bitcask/config"
	"ashishkujoy/bitcask/kv"
	log "ashishkujoy/bitcask/kv/log"
	"time"
)

// Worker encapsulates KVStore and MergeConfig. Worker is an abstraction inside merge package that performs merge of inactive segment files every fixed duration
type Worker[Key config.BitcaskKey] struct {
	kvStore *kv.KVStore[Key]
	config  *config.MergeConfig[Key]
	quit    chan struct{}
}

// NewWorker creates an instance of Worker and starts the Worker
func NewWorker[Key config.BitcaskKey](kvStore *kv.KVStore[Key], config *config.MergeConfig[Key]) *Worker[Key] {
	worker := &Worker[Key]{
		kvStore: kvStore,
		config:  config,
		quit:    make(chan struct{}),
	}
	worker.start()
	return worker
}

// start is invoked from the NewWorker function. It spins a goroutine that runs every fixed duration defined in `runMergeEvery` field of MergeConfig
func (worker *Worker[Key]) start() {
	ticker := time.NewTicker(worker.config.RunMergeEvery())
	go func() {
		for {
			select {
			case <-ticker.C:
				worker.beginMerge()
			case <-worker.quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func (worker *Worker[Key]) beginMerge() {
	var fileIds []uint64
	var entries [][]*log.MappedStoredEntry[Key]
	var err error

	if worker.config.ShouldReadAllSegments() {
		fileIds, entries, err = worker.kvStore.ReadAllInactiveSegments(worker.config.KeyMapper())
	} else {
		fileIds, entries, err = worker.kvStore.ReadInactiveSegments(
			worker.config.TotalSegmentsToRead(),
			worker.config.KeyMapper(),
		)
	}

	if err == nil && len(entries) > 2 {
		mergedState := NewMergedState[Key]()
		mergedState.takeAll(entries[0])

		for index := 1; index < len(entries); index++ {
			mergedState.mergeWith(entries[index])
		}

		worker.kvStore.WriteBack(fileIds, mergedState.valueByKey)
	}
}

// Stop closes the quit channel which is used to signal the merge goroutine to stop
func (worker *Worker[Key]) Stop() {
	close(worker.quit)
}
