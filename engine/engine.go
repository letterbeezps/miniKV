package engine

import (
	"fmt"
	"testing"

	internal "github.com/letterbeezps/miniKV/internal"
	"github.com/stretchr/testify/assert"
)

type Engine interface {
	Get(key string) ([]byte, bool)

	Set(key string, value []byte)

	Delete(key string)

	Scan(start, end internal.Bound, iter func(key string, value []byte) bool)

	Reverse(start, end internal.Bound, iter func(key string, value []byte) bool)

	Iter(start, end internal.Bound) Iterator
}

type Iterator interface {
	Value() []byte

	Key() string

	IsValid() bool

	Next()
}

//////// test ////////////

func EngineTestGetSet(engine Engine, t *testing.T) {
	ret, ok := engine.Get("a")
	assert.False(t, ok)
	assert.Equal(t, []byte{}, ret)

	engine.Set("a", []byte("aaa"))
	ret, ok = engine.Get("a")
	assert.True(t, ok)
	assert.Equal(t, []byte("aaa"), ret)

	engine.Set("b", []byte("bbb"))
	ret, ok = engine.Get("a")
	assert.True(t, ok)
	assert.Equal(t, []byte("aaa"), ret)
	ret, ok = engine.Get("b")
	assert.True(t, ok)
	assert.Equal(t, []byte("bbb"), ret)

	ret, ok = engine.Get("A")
	assert.False(t, ok)
	assert.Equal(t, []byte{}, ret)
	ret, ok = engine.Get("B")
	assert.False(t, ok)
	assert.Equal(t, []byte{}, ret)

	engine.Delete("a")
	ret, ok = engine.Get("a")
	assert.False(t, ok)
	assert.Equal(t, []byte{}, ret)
	ret, ok = engine.Get("b")
	assert.True(t, ok)
	assert.Equal(t, []byte("bbb"), ret)

	engine.Set("b", []byte{})
	ret, ok = engine.Get("b")
	assert.True(t, ok)
	assert.Equal(t, []byte{}, ret)
}

func ScanTest(engine Engine, t *testing.T) {
	data := map[string][]byte{
		"a":    []byte("a"),
		"aa":   []byte("aa"),
		"aaa":  []byte("aaa"),
		"aaaa": []byte("aaaaa"),
		"b":    []byte("b"),
		"ba":   []byte("ba"),
		"bb":   []byte("bb"),
		"bc":   []byte("bc"),
		"c":    []byte("c"),
	}
	for k, v := range data {
		engine.Set(k, v)
	}

	iterator := engine.Iter(
		internal.NewBound("", internal.NoBound),
		internal.NewBound("", internal.NoBound),
	)
	for iterator.IsValid() {
		fmt.Println("key", iterator.Key(), "value", string(iterator.Value()))
		iterator.Next()
	}
}
