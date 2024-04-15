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
	assert.Equal(t, TXID(1), tx1.State.TxID)
	assert.Equal(t, map[TXID]struct{}{}, tx1.State.ActiveTx)

	tx2, err := mvcc.Begin()
	assert.Nil(t, err)
	assert.Equal(t, TXID(2), tx2.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(1): {},
	}, tx2.State.ActiveTx)

	tx3, err := mvcc.Begin()
	assert.Nil(t, err)
	assert.Equal(t, TXID(3), tx3.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(1): {},
		TXID(2): {},
	}, tx3.State.ActiveTx)

	err = tx2.Commit()
	assert.Nil(t, err)

	tx4, err := mvcc.Begin()
	assert.Nil(t, err)
	assert.Equal(t, TXID(4), tx4.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(1): {},
		TXID(3): {},
	}, tx4.State.ActiveTx)
}

func Test_GetSet(t *testing.T) {
	mvcc := MVCC{
		Lock:   &sync.Mutex{},
		Engine: memory.NewMemory(),
	}

	tx1, err := mvcc.Begin()
	assert.Nil(t, err)
	assert.Nil(t, tx1.Set("a", []byte{1}))
	assert.Nil(t, tx1.Set("b", []byte{1}))
	assert.Nil(t, tx1.Commit())

	tx2, err := mvcc.Begin()
	assert.Nil(t, err)
	tx3, err := mvcc.Begin()
	assert.Nil(t, err)
	tx4, err := mvcc.Begin()
	assert.Nil(t, err)

	assert.Nil(t, tx2.Set("a", []byte{2}))
	assert.Nil(t, tx3.Set("b", []byte{3}))
	assert.Nil(t, tx4.Set("c", []byte{4}))
	assert.Nil(t, tx2.Commit())

	assert.Nil(t, tx4.Commit())

	ret, err := tx3.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret) // tx2 is in tx3's active set

	ret, err = tx3.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{3}, ret) // tx2 can see it's self

	tx5, err := mvcc.Begin()
	assert.Nil(t, err)
	ret, err = tx5.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, ret)

	ret, err = tx5.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret) // tx3 is in tx5's active sets

	ret, err = tx5.Get("c")
	assert.Nil(t, err)
	assert.Equal(t, []byte{4}, ret)

	assert.Nil(t, tx3.Commit())

	// all txs are commited, so tx6 can see the latest data version
	tx6, err := mvcc.Begin()
	assert.Nil(t, err)
	ret, err = tx6.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{2}, ret)

	ret, err = tx6.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{3}, ret)

	ret, err = tx4.Get("c")
	assert.Nil(t, err)
	assert.Equal(t, []byte{4}, ret)
}
