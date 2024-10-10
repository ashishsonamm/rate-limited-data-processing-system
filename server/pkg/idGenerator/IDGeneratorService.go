package idGenerator

import "sync/atomic"

type IDGeneratorService interface {
	GenerateIDAtomically() uint64
}

type IDGeneratorServiceImpl struct {
	counter uint64
}

func NewIDGeneratorService() *IDGeneratorServiceImpl {
	return &IDGeneratorServiceImpl{
		counter: 0,
	}
}

func (impl *IDGeneratorServiceImpl) GenerateIDAtomically() uint64 {
	return atomic.AddUint64(&impl.counter, 1)
}
