package mvcc

import (
	"fmt"

	"github.com/letterbeezps/miniKV/internal/iface"
	"github.com/pkg/errors"
)

type TXIterator struct {
	State          *TxState
	Start          string
	End            string
	LastK          string
	K              string
	V              []byte
	EngineIterator iface.Iterator
}

func (iter *TXIterator) Value() []byte {
	if len(iter.V) == 0 {
		return []byte{}
	}
	return iter.V
}

func (iter *TXIterator) Key() string {
	return iter.K
}

func (iter *TXIterator) IsValid() bool {
	return iter.K != ""
}

func (iter *TXIterator) Next() error {
	var nextK string
	var nextV []byte
	for iter.EngineIterator.IsValid() {
		check_id, origin_key, err := decodeTxKey(iter.EngineIterator.Key())
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("got bad txKey: %s", iter.Key()))
		}
		if (!iter.State.IsVisible(check_id)) || (iter.LastK != "" && iter.LastK == origin_key) {
			err := iter.EngineIterator.Next()
			if err != nil {
				return errors.Wrap(err, "EngineIterator.Next()")
			}
			continue
		}
		if origin_key > iter.End {
			break
		}
		iter.LastK = origin_key
		nextK = origin_key
		nextV = iter.EngineIterator.Value()
		break
	}
	iter.K = nextK
	iter.V = nextV
	return nil
}
