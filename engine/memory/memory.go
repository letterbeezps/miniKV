package memory

import (
	"github.com/letterbeezps/miniKV/engine"
	internal "github.com/letterbeezps/miniKV/internal"
	"github.com/tidwall/btree"
)

var _ engine.Engine = NewMemory()

type Memory struct {
	Data btree.Map[string, []byte]
}

func NewMemory() *Memory {
	ret := Memory{
		Data: btree.Map[string, []byte]{},
	}
	return &ret
}

func (m *Memory) Get(key string) ([]byte, bool) {
	ret, ok := m.Data.Get(key)
	if len(ret) == 0 {
		ret = []byte{}
	}
	return ret, ok
}

func (m *Memory) Set(key string, value []byte) {
	m.Data.Set(key, value)
}

func (m *Memory) Delete(key string) {
	m.Data.Delete(key)
}

// iter items in asc order, (start <= end)
func (m *Memory) Scan(start, end internal.Bound, iter func(key string, value []byte) bool) {
	if start.BoundType == internal.NoBound {
		m.Data.Scan(func(key string, value []byte) bool {
			if end.BoundType == internal.Include && key > end.Key {
				return false
			} else if end.BoundType == internal.Exclude && key >= end.Key {
				return false
			}
			return iter(key, value)
		})
	} else {
		m.Data.Ascend(start.Key, func(key string, value []byte) bool {
			if start.BoundType == internal.Exclude && key == start.Key { // skip first key if Exclude it
				return true
			} else if end.BoundType == internal.Include && key > end.Key {
				return false
			} else if end.BoundType == internal.Exclude && key >= end.Key {
				return false
			}
			return iter(key, value)
		})
	}
}

// iter items in desc order, (start >= end)
func (m *Memory) Reverse(start, end internal.Bound, iter func(key string, value []byte) bool) {
	if start.BoundType == internal.NoBound {
		m.Data.Reverse(func(key string, value []byte) bool {
			if end.BoundType == internal.Include && key < end.Key {
				return false
			} else if end.BoundType == internal.Exclude && key <= end.Key {
				return false
			}
			return iter(key, value)
		})
	} else {
		m.Data.Descend(start.Key, func(key string, value []byte) bool {
			if start.BoundType == internal.Exclude && key == start.Key {
				return true
			} else if end.BoundType == internal.Include && key < end.Key {
				return false
			} else if end.BoundType == internal.Exclude && key <= end.Key {
				return false
			}
			return iter(key, value)
		})
	}
}

func (m *Memory) Iter(start, end internal.Bound) engine.Iterator {
	iter := m.Data.Iter()
	key, value := "", []byte{}
	valid := true
	switch start.BoundType {
	case internal.NoBound:
		valid = iter.First()
	case internal.Include, internal.Exclude:
		valid = iter.Seek(start.Key)
		if valid && start.BoundType == internal.Exclude && iter.Key() == start.Key {
			valid = iter.Next()
		}
	}
	if valid {
		key = iter.Key()
		value = iter.Value()
	}
	return &MemoryIterator{
		Start: start,
		End:   end,
		K:     key,
		V:     value,
		Iter:  &iter,
	}
}

func (m *Memory) ReverseIter(start, end internal.Bound) engine.Iterator {
	iter := m.Data.Iter()
	key, value := "", []byte{}
	valid := true
	switch end.BoundType {
	case internal.NoBound:
		valid = iter.Last()
	case internal.Include, internal.Exclude:
		valid = iter.Seek(end.Key)
		if !valid {
			valid = iter.Last()
		}
		if valid && iter.Key() > end.Key {
			valid = iter.Prev()
		}
		if valid && end.BoundType == internal.Exclude && iter.Key() == end.Key {
			valid = iter.Prev()
		}
	}
	if valid {
		key = iter.Key()
		value = iter.Value()
	}
	return &MemoryIterator{
		Start:     start,
		End:       end,
		K:         key,
		V:         value,
		Iter:      &iter,
		IsReverse: true,
	}
}
