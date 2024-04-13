package mvcc

import (
	"testing"

	"github.com/google/orderedcode"
	"github.com/stretchr/testify/assert"
)

func Test_keys(t *testing.T) {
	buf1, err := orderedcode.Append(nil, "aaa", uint64(12))
	assert.Nil(t, err)
	key1 := string(buf1)

	buf2, err := orderedcode.Append(nil, "aaa", uint64(1))
	assert.Nil(t, err)
	key2 := string(buf2)

	t.Log(key1, key2)
	assert.Greater(t, key1, key2)

	buf1, err = orderedcode.Append(nil, "aaa", orderedcode.Decr(uint64(12)))
	assert.Nil(t, err)
	key1 = string(buf1)

	buf2, err = orderedcode.Append(nil, "aaa", orderedcode.Decr(uint64(1)))
	assert.Nil(t, err)
	key2 = string(buf2)

	assert.Less(t, key1, key2)
	t.Log(key1, key2)

	a, b := "", uint64(0)
	_, err = orderedcode.Parse(key1, &a, orderedcode.Decr(&b))
	assert.Nil(t, err)
	t.Log(a, b)

	buf11, err := orderedcode.Append(nil, "aaa")
	assert.Nil(t, err)
	buf1, err = orderedcode.Append(buf11, orderedcode.Decr(uint64(12)))
	assert.Nil(t, err)
	key1 = string(buf1)
	t.Log(key1)

	buf1, err = orderedcode.Append([]byte("aaa"), orderedcode.Decr(uint64(1234567890)))
	assert.Nil(t, err)
	key1 = string(buf1)
	t.Log(key1)
	_, err = orderedcode.Parse(key1[3:], orderedcode.Decr(&b))
	assert.Nil(t, err)
	t.Log(b)

	t.Log(len(key1))
	buf1 = buf1[3:]
	key1 = string(buf1)
	t.Log(key1)
	t.Log(len(key1))

	_, err = orderedcode.Parse(key1, orderedcode.Decr(&b))
	assert.Nil(t, err)
	t.Log(b)
}

func Test_activeKey(t *testing.T) {
	k, err := encodeTxActiveKey(12)
	assert.Nil(t, err)
	t.Log(k)

	id, err := decodeTxActiveKey(k)
	assert.Nil(t, err)
	assert.Equal(t, uint64(12), id)
}

func Test_txWritKey(t *testing.T) {
	k, err := encodeTxWriteKey(12, "aa_bb")
	assert.Nil(t, err)
	t.Log(k)

	id, origin_key, err := decodeTxWriteKey(k)
	assert.Nil(t, err)
	assert.Equal(t, uint64(12), id)
	assert.Equal(t, "aa_bb", origin_key)
}

func Test_txKey(t *testing.T) {
	k, err := encodeTxKey(12, "aa_bb_cc")
	assert.Nil(t, err)
	t.Log(k)

	id, origin_key, err := decodeTxKey(k)
	assert.Nil(t, err)
	assert.Equal(t, uint64(12), id)
	assert.Equal(t, "aa_bb_cc", origin_key)
}
