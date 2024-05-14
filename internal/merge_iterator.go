package keytype

import (
	"github.com/letterbeezps/miniKV/internal/iface"
	"github.com/pkg/errors"
)

type TwoMergeIterator struct {
	First       iface.Iterator
	Second      iface.Iterator
	ChooseFirst bool
	Less        func(f, s string) bool
	Equal       func(f, s string) bool
}

func NewTwoMergeIterstor(f, s iface.Iterator, less, equal func(f, s string) bool) (*TwoMergeIterator, error) {
	ret := &TwoMergeIterator{
		First:  f,
		Second: s,
		Less:   less,
		Equal:  equal,
	}
	if err := ret.skipSecond(); err != nil {
		return nil, errors.Wrap(err, "skipSecond")
	}
	ret.ChooseFirst = ret.chooseFirst()
	return ret, nil
}

func (iter *TwoMergeIterator) chooseFirst() bool {
	if !iter.First.IsValid() {
		return false
	}
	if !iter.Second.IsValid() {
		return true
	}
	return iter.Less(iter.First.Key(), iter.Second.Key())
}

func (iter *TwoMergeIterator) skipSecond() error {
	if iter.First.IsValid() && iter.Second.IsValid() && iter.Equal(iter.First.Key(), iter.Second.Key()) {
		return iter.Second.Next()
	}
	return nil
}

func (iter *TwoMergeIterator) Value() []byte {
	if iter.chooseFirst() {
		return iter.First.Value()
	} else {
		return iter.Second.Value()
	}
}

func (iter *TwoMergeIterator) Key() string {
	if iter.chooseFirst() {
		return iter.First.Key()
	} else {
		return iter.Second.Key()
	}
}

func (iter *TwoMergeIterator) IsValid() bool {
	if iter.chooseFirst() {
		return iter.First.IsValid()
	} else {
		return iter.Second.IsValid()
	}
}

func (iter *TwoMergeIterator) Next() error {
	if iter.ChooseFirst {
		if err := iter.First.Next(); err != nil {
			return errors.Wrap(err, "First Next")
		}
	} else {
		if err := iter.Second.Next(); err != nil {
			return errors.Wrap(err, "Second Next")
		}
	}
	if err := iter.skipSecond(); err != nil {
		return errors.Wrap(err, "skipSecond")
	}
	iter.ChooseFirst = iter.chooseFirst()
	return nil
}
