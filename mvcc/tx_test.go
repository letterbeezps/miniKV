package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getPrefixEnd(t *testing.T) {
	ret := getPrefixEnd(TxActivePrefix)
	assert.Equal(t, TxActivePrefixEnd, ret)
}

func Test_TxBegin(t *testing.T) {
	mvcc := NewMVCC()

	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Equal(t, TXID(1), tx1.State.TxID)
	assert.Equal(t, map[TXID]struct{}{}, tx1.State.ActiveTx)

	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Equal(t, TXID(2), tx2.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(1): {},
	}, tx2.State.ActiveTx)

	tx3, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Equal(t, TXID(3), tx3.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(1): {},
		TXID(2): {},
	}, tx3.State.ActiveTx)

	err = tx2.Commit()
	assert.Nil(t, err)

	tx4, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Equal(t, TXID(4), tx4.State.TxID)
	assert.Equal(t, map[TXID]struct{}{
		TXID(1): {},
		TXID(3): {},
	}, tx4.State.ActiveTx)
}

func Test_GetSet(t *testing.T) {
	mvcc := NewMVCC()

	tx1, err := mvcc.Begin(false)

	assert.Nil(t, err)
	assert.Nil(t, tx1.Set("a", []byte{1}))
	assert.Nil(t, tx1.Set("b", []byte{1}))
	assert.Nil(t, tx1.Commit())

	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx3, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx4, err := mvcc.Begin(false)
	assert.Nil(t, err)

	assert.Nil(t, tx2.Set("a", []byte{2}))
	assert.Nil(t, tx3.Set("b", []byte{3}))
	assert.Nil(t, tx4.Set("c", []byte{4}))
	assert.Nil(t, tx2.Commit())

	assert.Nil(t, tx4.Commit())

	ret, err := tx3.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret) // tx2 is in tx3's active set

	ret, err = tx3.Get("None")
	assert.Nil(t, err)
	assert.Equal(t, []byte{}, ret)

	ret, err = tx3.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{3}, ret) // tx2 can see it's self

	tx5, err := mvcc.Begin(false)
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
	assert.Nil(t, tx5.RollBack())

	assert.Nil(t, tx3.Commit())

	// all txs are commited, so tx6 can see the latest data version
	tx6, err := mvcc.Begin(true)
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

func Test_delete_conflict(t *testing.T) {
	mvcc := NewMVCC()

	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx1.Set("a", []byte{1}))

	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)

	tx3, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx3.Set("c", []byte{3}))

	tx4, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx4.Set("d", []byte{4}))
	assert.Nil(t, tx4.Commit())

	assert.ErrorIs(t, tx2.Delete("a"), ErrorSerialization)
	assert.ErrorIs(t, tx2.Delete("c"), ErrorSerialization)
	assert.ErrorIs(t, tx2.Delete("d"), ErrorSerialization)

}

func Test_get_isolation(t *testing.T) {
	mvcc := NewMVCC()

	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx1.Set("a", []byte{1}))
	assert.Nil(t, tx1.Set("b", []byte{1}))
	assert.Nil(t, tx1.Set("d", []byte{1}))
	assert.Nil(t, tx1.Set("e", []byte{1}))
	assert.Nil(t, tx1.Commit())

	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx2.Set("a", []byte{2}))
	assert.Nil(t, tx2.Delete("b"))
	assert.Nil(t, tx2.Set("c", []byte{2}))

	tx3, err := mvcc.Begin(true)
	assert.Nil(t, err)

	tx4, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx4.Set("d", []byte{4}))
	assert.Nil(t, tx2.Delete("e"))
	assert.Nil(t, tx2.Set("f", []byte{4}))
	assert.Nil(t, tx4.Commit())

	ret, err := tx3.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret)
	ret, err = tx3.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret)
	ret, err = tx3.Get("c")
	assert.Nil(t, err)
	assert.Equal(t, []byte{}, ret)
	ret, err = tx3.Get("d")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret)
	ret, err = tx3.Get("e")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret)
	ret, err = tx3.Get("f")
	assert.Nil(t, err)
	assert.Equal(t, []byte{}, ret)

	assert.ErrorIs(t, tx3.RollBack(), ErrorReadOnly)
}

func Test_set(t *testing.T) {
	mvcc := NewMVCC()
	tx0, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx0.Set("a", []byte{0}))
	assert.Nil(t, tx0.Set("a", []byte{0}))
	assert.Nil(t, tx0.RollBack())
}

func Test_set_conflict(t *testing.T) {
	mvcc := NewMVCC()
	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx3, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx4, err := mvcc.Begin(false)
	assert.Nil(t, err)

	assert.Nil(t, tx1.Set("a", []byte{1}))
	assert.Nil(t, tx3.Set("c", []byte{3}))
	assert.Nil(t, tx4.Set("d", []byte{4}))
	assert.Nil(t, tx4.Commit())

	assert.ErrorIs(t, tx2.Set("a", []byte{2}), ErrorSerialization)
	assert.ErrorIs(t, tx2.Set("c", []byte{2}), ErrorSerialization)
	assert.ErrorIs(t, tx2.Set("d", []byte{2}), ErrorSerialization)
}

func Test_rollback(t *testing.T) {
	mvcc := NewMVCC()
	tx0, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx0.Set("a", []byte{0}))
	assert.Nil(t, tx0.Set("b", []byte{0}))
	assert.Nil(t, tx0.Set("c", []byte{0}))
	assert.Nil(t, tx0.Set("d", []byte{0}))
	assert.Nil(t, tx0.Commit())

	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx3, err := mvcc.Begin(false)
	assert.Nil(t, err)

	assert.Nil(t, tx1.Set("a", []byte{1}))
	assert.Nil(t, tx2.Set("b", []byte{2}))
	assert.Nil(t, tx2.Delete("c"))
	assert.Nil(t, tx3.Set("d", []byte{3}))

	assert.ErrorIs(t, tx1.Set("b", []byte{1}), ErrorSerialization)
	assert.ErrorIs(t, tx3.Set("c", []byte{3}), ErrorSerialization)

	assert.Nil(t, tx2.RollBack())

	tx4, err := mvcc.Begin(true)
	assert.Nil(t, err)
	ret, err := tx4.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)
	ret, err = tx4.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)
	ret, err = tx4.Get("c")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)
	ret, err = tx4.Get("d")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)

	assert.Nil(t, tx2.Set("b", []byte{1}))
	assert.Nil(t, tx2.Set("c", []byte{3}))
	assert.Nil(t, tx1.Commit())
	assert.Nil(t, tx3.Commit())

	tx5, err := mvcc.Begin(true)
	assert.Nil(t, err)
	ret, err = tx5.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret)
	ret, err = tx5.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{1}, ret)
	ret, err = tx5.Get("c")
	assert.Nil(t, err)
	assert.Equal(t, []byte{3}, ret)
	ret, err = tx5.Get("d")
	assert.Nil(t, err)
	assert.Equal(t, []byte{3}, ret)
}

func Test_dirty_write(t *testing.T) {
	mvcc := NewMVCC()
	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx1.Set("a", []byte{0}))

	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.ErrorIs(t, tx2.Set("a", []byte{1}), ErrorSerialization)
}

func Test_dirty_read(t *testing.T) {
	mvcc := NewMVCC()
	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx1.Set("a", []byte{0}))

	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)
	ret, err := tx2.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{}, ret)
}

func Test_lost_update(t *testing.T) {
	mvcc := NewMVCC()
	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)

	_, err = tx1.Get("a")
	assert.Nil(t, err)
	_, err = tx2.Get("a")
	assert.Nil(t, err)

	assert.Nil(t, tx1.Set("a", []byte{0}))
	assert.ErrorIs(t, tx2.Set("a", []byte{1}), ErrorSerialization)
	assert.Nil(t, tx1.Commit())
	assert.Nil(t, tx2.RollBack())
}

func Test_fuzzy_read(t *testing.T) {
	mvcc := NewMVCC()
	tx0, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx0.Set("a", []byte{0}))
	assert.Nil(t, tx0.Commit())

	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)

	ret, err := tx2.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)

	assert.Nil(t, tx1.Set("a", []byte{1}))
	assert.Nil(t, tx1.Commit())

	ret, err = tx2.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)

}

func Test_read_skew(t *testing.T) {
	mvcc := NewMVCC()
	tx0, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx0.Set("a", []byte{0}))
	assert.Nil(t, tx0.Set("b", []byte{0}))
	assert.Nil(t, tx0.Commit())

	tx1, err := mvcc.Begin(true)
	assert.Nil(t, err)
	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)

	ret, err := tx1.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)

	assert.Nil(t, tx2.Set("a", []byte{2}))
	assert.Nil(t, tx2.Set("b", []byte{2}))
	assert.Nil(t, tx2.Commit())

	ret, err = tx1.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)

}

func Test_write_skew(t *testing.T) {
	mvcc := NewMVCC()
	tx0, err := mvcc.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx0.Set("a", []byte{0}))
	assert.Nil(t, tx0.Set("b", []byte{0}))
	assert.Nil(t, tx0.Commit())

	tx1, err := mvcc.Begin(false)
	assert.Nil(t, err)
	tx2, err := mvcc.Begin(false)
	assert.Nil(t, err)

	ret, err := tx1.Get("a")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)
	ret, err = tx2.Get("b")
	assert.Nil(t, err)
	assert.Equal(t, []byte{0}, ret)

	assert.Nil(t, tx2.Set("a", []byte{2}))
	assert.Nil(t, tx1.Set("b", []byte{1}))
	assert.Nil(t, tx1.Commit())
	assert.Nil(t, tx2.Commit())

}
