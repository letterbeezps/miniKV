package mvcc

import (
	"sync"
	"testing"

	"github.com/letterbeezps/miniKV/engine/memory"
	"github.com/stretchr/testify/assert"
)

func Test_getPrefixEnd(t *testing.T) {
	ret := getPrefixEnd(TxActivePrefix)
	assert.Equal(t, TxActivePrefixEnd, ret)
}

func Test_TxBegin(t *testing.T) {
	mvcc := MVCC{
		Lock:   &sync.Mutex{},
		Engine: memory.NewMemory(),
	}

	tx1, err := mvcc.Begin()
	assert.Nil(t, err)
	assert.Equal(t, TXID(0), tx1.State.TxID)
	assert.Equal(t, map[TXID]struct{}{}, tx1.State.ActiveTx)

	tx2, err := mvcc.Begin()
	assert.Nil(t, err)
	assert.Equal(t, TXID(1), tx2.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(0): {},
	}, tx2.State.ActiveTx)

	tx3, err := mvcc.Begin()
	assert.Nil(t, err)
	assert.Equal(t, TXID(2), tx3.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(0): {},
		TXID(1): {},
	}, tx3.State.ActiveTx)
}
