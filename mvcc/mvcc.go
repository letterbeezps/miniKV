package mvcc

import (
	"sync"

	"github.com/letterbeezps/miniKV/engine"
	"github.com/pkg/errors"
)

type MVCC struct {
	Lock   *sync.Mutex
	Engine engine.Engine
}

func (m *MVCC) Begin() (*TX, error) {
	tx := &TX{
		Lock:   m.Lock,
		Engine: m.Engine,
	}
	err := tx.Begin()
	if err != nil {
		return nil, errors.Wrap(err, "tx begin")
	}
	return tx, nil
}
