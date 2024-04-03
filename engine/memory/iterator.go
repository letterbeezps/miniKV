package memory

import (
	internal "github.com/letterbeezps/miniKV/internal"
	"github.com/tidwall/btree"
)

type MemoryIterator struct {
	Start     internal.Bound
	End       internal.Bound
	Iter      *btree.MapIter[string, []byte]
	K         string
	V         []byte
	IsReverse bool
}

func (iter *MemoryIterator) Value() []byte {
	return iter.V
}

func (iter *MemoryIterator) Key() string {
	return iter.K
}

func (iter *MemoryIterator) IsValid() bool {
	return iter.K != ""
}

func (iter *MemoryIterator) next() {
	valid := iter.Iter.Next()
	if valid && iter.End.BoundType == internal.Include {
		valid = iter.Iter.Key() <= iter.End.Key
	}
	if valid && iter.End.BoundType == internal.Exclude {
		valid = iter.Iter.Key() < iter.End.Key
	}

	if valid {
		iter.K = iter.Iter.Key()
		iter.V = iter.Iter.Value()
	} else {
		iter.K = ""
		iter.V = []byte{}
	}
}

func (iter *MemoryIterator) prev() {
	valid := iter.Iter.Prev()
	if valid && iter.Start.BoundType == internal.Include {
		valid = iter.Iter.Key() >= iter.Start.Key
	}
	if valid && iter.Start.BoundType == internal.Exclude {
		valid = iter.Iter.Key() > iter.Start.Key
	}

	if valid {
		iter.K = iter.Iter.Key()
		iter.V = iter.Iter.Value()
	} else {
		iter.K = ""
		iter.V = []byte{}
	}
}

func (iter *MemoryIterator) Next() {
	if !iter.IsReverse {
		iter.next()
	} else {
		iter.prev()
	}
}
