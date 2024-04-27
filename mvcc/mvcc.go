package mvcc

import (
	"sync"

	"github.com/letterbeezps/miniKV/engine"
	"github.com/letterbeezps/miniKV/engine/memory"
	"github.com/pkg/errors"
)

type MVCC struct {
	Lock   *sync.Mutex
	Engine engine.Engine
}

func NewMVCC() *MVCC {
	return &MVCC{
		Lock:   &sync.Mutex{},
		Engine: memory.NewMemory(),
	}
}

func (m *MVCC) Begin(readOnly bool) (*TX, error) {
	tx := &TX{
		Lock:   m.Lock,
		Engine: m.Engine,
	}
	err := tx.Begin(readOnly)
	if err != nil {
		return nil, errors.Wrap(err, "tx begin")
	}
	return tx, nil
}
