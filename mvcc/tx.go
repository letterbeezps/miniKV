package mvcc

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/letterbeezps/miniKV/engine"
	internal "github.com/letterbeezps/miniKV/internal"
	"github.com/letterbeezps/miniKV/internal/iface"
	"github.com/pkg/errors"
)

type TX struct {
	Lock   *sync.Mutex
	Engine engine.Engine
	State  *TxState
}

func (tx *TX) Begin(readOnly bool) error {
	tx.Lock.Lock()
	defer tx.Lock.Unlock()

	txId := uint64(1)
	txIdBytes, ok := tx.Engine.Get(NextTxID)
	if ok {
		txId = binary.BigEndian.Uint64(txIdBytes)
	}
	nextIdBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nextIdBytes, txId+1)
	tx.Engine.Set(NextTxID, nextIdBytes)
	active, err := tx.scanActive()
	if err != nil {
		return errors.Wrap(err, "scanActive")
	}
	tx.State = &TxState{
		TxID:     txId,
		ReadOnly: readOnly,
		ActiveTx: active,
	}
	if !readOnly {
		activeKey, err := encodeTxActiveKey(txId)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("getTxActiveKey with %d", txId))
		}
		tx.Engine.Set(activeKey, []byte{})
	}
	return nil
}

func (tx *TX) scanActive() (map[TXID]struct{}, error) {
	start := internal.NewBound(TxActivePrefix, internal.Include)
	end := internal.NewBound(TxActivePrefixEnd, internal.Exclude)
	iter := tx.Engine.Iter(start, end)
	ret := map[TXID]struct{}{}
	for iter.IsValid() {
		key := iter.Key()
		txId, err := decodeTxActiveKey(key)
		if err != nil {
			return nil, errors.Wrap(err, "decodeTxActiveKey")
		}
		ret[txId] = struct{}{}
		iter.Next()
	}
	return ret, nil
}

func (tx *TX) Write(key string, value []byte) error {
	if tx.State.ReadOnly {
		return errors.Wrap(ErrorReadOnly, fmt.Sprintf("tx with id %d is read only", tx.State.TxID))
	}
	miniTxID := tx.State.TxID + 1
	for id := range tx.State.ActiveTx {
		if id < miniTxID {
			miniTxID = id
		}
	}
	endTxKey, err := encodeTxKey(miniTxID, key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", key))
	}
	startTxKey, err := encodeTxKey(uint64(math.MaxUint64), key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", key))
	}
	start := internal.NewBound(startTxKey, internal.Include)
	end := internal.NewBound(endTxKey, internal.Include)
	iter := tx.Engine.Iter(start, end)
	for iter.IsValid() {
		check_id, _, err := decodeTxKey(iter.Key())
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("got bad txKey: %s", iter.Key()))
		}
		if !tx.State.IsVisible(check_id) {
			return errors.Wrap(ErrorSerialization, fmt.Sprintf("cur tx: %d, exist: %d", tx.State.TxID, check_id))
		}
		iter.Next()
	}

	tnWriteKey, err := encodeTxWriteKey(tx.State.TxID, key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("encodeTxWriteKey with key: %s", key))
	}
	tx.Engine.Set(tnWriteKey, []byte{})
	txKey, err := encodeTxKey(tx.State.TxID, key)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", key))
	}
	tx.Engine.Set(txKey, value)
	return nil
}

func (tx *TX) Set(key string, value []byte) error {
	return tx.Write(key, value)
}

func (tx *TX) Delete(key string) error {
	return tx.Write(key, []byte{})
}

func (tx *TX) Commit() error {
	if tx.State.ReadOnly {
		return errors.Wrap(ErrorReadOnly, fmt.Sprintf("tx with id %d is read only, not need commit", tx.State.TxID))
	}
	startKey, err := encodeTxWriteKey(tx.State.TxID, "")
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("encodeTxWriteKey with id: %d", tx.State.TxID))
	}
	endKey := getPrefixEnd(startKey)
	start := internal.NewBound(startKey, internal.Include)
	end := internal.NewBound(endKey, internal.Exclude)
	iter := tx.Engine.Iter(start, end)
	removeKeys := []string{}
	for iter.IsValid() {
		removeKeys = append(removeKeys, iter.Key())
		iter.Next()
	}
	for _, key := range removeKeys {
		tx.Engine.DeleteReal(key)
	}

	activeKey, err := encodeTxActiveKey(tx.State.TxID)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("getTxActiveKey with %d", tx.State.TxID))
	}
	tx.Engine.DeleteReal(activeKey)
	return nil
}

func (tx *TX) RollBack() error {
	if tx.State.ReadOnly {
		return errors.Wrap(ErrorReadOnly, fmt.Sprintf("tx with id %d is read only, not need rollback", tx.State.TxID))
	}
	startKey, err := encodeTxWriteKey(tx.State.TxID, "")
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("encodeTxWriteKey with id: %d", tx.State.TxID))
	}
	endKey := getPrefixEnd(startKey)
	start := internal.NewBound(startKey, internal.Include)
	end := internal.NewBound(endKey, internal.Exclude)
	iter := tx.Engine.Iter(start, end)
	removeKeys := []string{}
	for iter.IsValid() {
		removeKeys = append(removeKeys, iter.Key())
		_, origin_key, err := decodeTxWriteKey(iter.Key())
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("decodeTxWriteKey with key: %s", iter.Key()))
		}
		txKey, err := encodeTxKey(tx.State.TxID, origin_key)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", txKey))
		}
		removeKeys = append(removeKeys, txKey)
		iter.Next()
	}

	for _, key := range removeKeys {
		tx.Engine.DeleteReal(key)
	}

	activeKey, err := encodeTxActiveKey(tx.State.TxID)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("getTxActiveKey with %d", tx.State.TxID))
	}
	tx.Engine.DeleteReal(activeKey)
	return nil
}

func (tx *TX) Get(key string) ([]byte, error) {
	endTxKey, err := encodeTxKey(0, key)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", key))
	}
	startTxKey, err := encodeTxKey(tx.State.TxID, key)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", key))
	}
	start := internal.NewBound(startTxKey, internal.Include)
	end := internal.NewBound(endTxKey, internal.Include)
	iter := tx.Engine.Iter(start, end)
	for iter.IsValid() {
		check_id, _, err := decodeTxKey(iter.Key())
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("got bad txKey: %s", iter.Key()))
		}
		if tx.State.IsVisible(check_id) {
			return iter.Value(), nil
		}
		err = iter.Next()
		if err != nil {
			return nil, errors.Wrap(err, "iter.Next()")
		}
	}
	return []byte{}, nil
}

func (tx *TX) Iter(start, end string) (iface.Iterator, error) {
	endTxKey, err := encodeTxKey(0, end)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", end))
	}
	startTxKey, err := encodeTxKey(tx.State.TxID, start)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("encodeTxKey with key: %s", start))
	}
	startEngineKey := internal.NewBound(startTxKey, internal.Include)

	endEngineKey := internal.NewBound(endTxKey, internal.Include)

	engineIter := tx.Engine.Iter(startEngineKey, endEngineKey)

	ret := &TXIterator{
		State:          tx.State,
		Start:          start,
		End:            end,
		EngineIterator: engineIter,
	}
	err = ret.Next()
	if err != nil {
		return nil, errors.Wrap(err, "Next")
	}
	return ret, nil
}
