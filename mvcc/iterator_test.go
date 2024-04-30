package mvcc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

/*

		a_1,            e_1,
    a_2,            b_2,        f_2
a_3, 			b_3,
            b_4,
			            	(f_5),
*/

func Test_Tx_Iteraotr(t *testing.T) {
	db := NewMVCC()

	tx1, err := db.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx1.Write("a", []byte("a_1")))
	assert.Nil(t, tx1.Write("e", []byte("e_1")))
	assert.Nil(t, tx1.Commit())

	t.Log("/////////////////////////////  tx2 /////////////////////")
	tx2, err := db.Begin(false)
	assert.Nil(t, err)
	iter, err := tx2.Iter("a", "f")
	assert.Nil(t, err)
	datas := []struct {
		key string
		val []byte
	}{
		{"a", []byte("a_1")},
		{"e", []byte("e_1")},
	}
	i := 0
	for iter.IsValid() {
		assert.Equal(t, datas[i].key, iter.Key())
		assert.Equal(t, datas[i].val, iter.Value())
		t.Log(iter.Key())
		t.Log(string(iter.Value()))
		assert.Nil(t, iter.Next())
		i++
	}
	assert.Nil(t, tx2.Write("a", []byte("a_2")))
	assert.Nil(t, tx2.Write("b", []byte("b_2")))
	assert.Nil(t, tx2.Write("f", []byte("f_2")))
	assert.Nil(t, tx2.Commit())

	t.Log("/////////////////////////////  tx3 /////////////////////")
	tx3, err := db.Begin(false)
	assert.Nil(t, err)
	iter, err = tx3.Iter("a", "f")
	assert.Nil(t, err)
	datas = []struct {
		key string
		val []byte
	}{
		{"a", []byte("a_2")},
		{"b", []byte("b_2")},
		{"e", []byte("e_1")},
		{"f", []byte("f_2")},
	}
	i = 0
	for iter.IsValid() {
		assert.Equal(t, datas[i].key, iter.Key())
		assert.Equal(t, datas[i].val, iter.Value())
		t.Log(iter.Key())
		t.Log(string(iter.Value()))
		assert.Nil(t, iter.Next())
		i++
	}
	assert.Nil(t, tx3.Write("a", []byte("a_3")))
	assert.Nil(t, tx3.Write("b", []byte("b_3")))
	assert.Nil(t, tx3.Commit())

	t.Log("/////////////////////////////  tx4 /////////////////////")
	tx4, err := db.Begin(false)
	assert.Nil(t, err)
	iter, err = tx4.Iter("a", "f")
	assert.Nil(t, err)
	datas = []struct {
		key string
		val []byte
	}{
		{"a", []byte("a_3")},
		{"b", []byte("b_3")},
		{"e", []byte("e_1")},
		{"f", []byte("f_2")},
	}
	i = 0
	for iter.IsValid() {
		assert.Equal(t, datas[i].key, iter.Key())
		assert.Equal(t, datas[i].val, iter.Value())
		t.Log(iter.Key())
		t.Log(string(iter.Value()))
		assert.Nil(t, iter.Next())
		i++
	}
	assert.Nil(t, tx4.Write("b", []byte("b_4")))
	assert.Nil(t, tx4.Commit())

	tx5, err := db.Begin(false)
	assert.Nil(t, err)
	assert.Nil(t, tx5.Write("f", []byte("f_5")))

	t.Log("/////////////////////////////  tx6 /////////////////////")
	tx6, err := db.Begin(true)
	assert.Nil(t, err)
	iter, err = tx6.Iter("a", "f")
	assert.Nil(t, err)
	datas = []struct {
		key string
		val []byte
	}{
		{"a", []byte("a_3")},
		{"b", []byte("b_4")},
		{"e", []byte("e_1")},
		{"f", []byte("f_2")},
	}
	i = 0
	for iter.IsValid() {
		assert.Equal(t, datas[i].key, iter.Key())
		assert.Equal(t, datas[i].val, iter.Value())
		t.Log(iter.Key())
		t.Log(string(iter.Value()))
		assert.Nil(t, iter.Next())
		i++
	}
}
