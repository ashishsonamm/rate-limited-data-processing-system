package storage

import (
	"fmt"
	"github.com/ashishsonamm/rate-limited-data-processing-system/client/pkg/constants"
	"go.uber.org/zap"
	"math/rand"
	"sync"
)

type StorageService interface {
	Set(key, value string)
	Get(key string) (string, bool)
	SimulateRecords(count int)
}

type StorageServiceImpl struct {
	logger *zap.SugaredLogger
	data   map[string]string
	mu     sync.RWMutex
}

func NewStorageService(logger *zap.SugaredLogger) *StorageServiceImpl {
	return &StorageServiceImpl{
		logger: logger,
		data:   make(map[string]string),
		mu:     sync.RWMutex{},
	}
}

func (impl *StorageServiceImpl) Set(key, value string) {
	impl.mu.Lock()
	defer impl.mu.Unlock()
	impl.data[key] = value
}

func (impl *StorageServiceImpl) Get(key string) (string, bool) {
	impl.mu.RLock()
	defer impl.mu.RUnlock()
	value, ok := impl.data[key]
	return value, ok
}

func (impl *StorageServiceImpl) SimulateRecords(count int) {
	impl.logger.Infow("simulating records", "count", count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("record_%d", i)
		data := fmt.Sprintf("data_%d", rand.Intn(constants.TotalRecords))
		impl.Set(id, data)
	}
}
