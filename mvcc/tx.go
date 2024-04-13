package mvcc

import (
	"encoding/binary"
	"fmt"
	"math"
	"sync"

	"github.com/letterbeezps/miniKV/engine"
	internal "github.com/letterbeezps/miniKV/internal"
	"github.com/pkg/errors"
)

type TX struct {
	Lock   *sync.Mutex
	Engine engine.Engine
	State  *TxState
}

func (tx *TX) Begin() error {
	tx.Lock.Lock()
	defer tx.Lock.Unlock()

	txId := uint64(0)
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
		ReadOnly: false,
		ActiveTx: active,
	}
	activeKey, err := encodeTxActiveKey(txId)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("getTxActiveKey with %d", txId))
	}
	tx.Engine.Set(activeKey, []byte{})
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
			return errors.Wrap(err, fmt.Sprintf("serialization, cur tx: %d, exist: %d", tx.State.TxID, check_id))
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
