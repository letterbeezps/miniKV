package mvcc

import (
	"fmt"
	"strings"

	"github.com/google/orderedcode"
	"github.com/pkg/errors"
)

const (
	NextTxID = "NextTxId"

	TxActivePrefix    = "TxActive"
	TxActivePrefixEnd = "TxActivf"

	TxWritePrefix = "TxWrite" // store the operations of currenct tx, which will be delete when commit/rollback this tx

	TxKeyPrefix = "TxKey"
)

func getPrefixEnd(key string) string {
	if key == "" {
		return ""
	}
	b := []byte(key)
	b[len(b)-1] += 1
	return string(b)
}

// ////// txActive ///////////
func encodeTxActiveKey(txId TXID) (string, error) {
	k, err := orderedcode.Append(nil, orderedcode.Decr(uint64(txId)))
	if err != nil {
		return "", errors.Wrap(err, "getTxActiveKey")
	}
	return fmt.Sprintf("%s_%s", TxActivePrefix, string(k)), nil
}

func decodeTxActiveKey(key string) (TXID, error) {
	keys := strings.Split(key, "_")
	if len(keys) != 2 {
		return 0, errors.New(fmt.Sprintf("bad format of TxActiveKey: %s", key))
	}
	var id uint64
	_, err := orderedcode.Parse(keys[1], orderedcode.Decr(&id))
	if err != nil {
		return 0, errors.Wrap(err, fmt.Sprintf("parse failed: %s", key))
	}
	return id, nil
}

//////// txWrite ////////////

func encodeTxWriteKey(txId TXID, key string) (string, error) {
	k, err := orderedcode.Append(nil, orderedcode.Decr(uint64(txId)))
	if err != nil {
		return "", errors.Wrap(err, "getTxWriteKey")
	}
	ret := fmt.Sprintf("%s_%s", TxWritePrefix, string(k))
	if key != "" {
		ret = fmt.Sprintf("%s_%s", ret, key)
	}
	return ret, nil
}

func decodeTxWriteKey(key string) (TXID, string, error) {
	keys := strings.Split(key, "_")
	if len(keys) < 3 {
		return 0, "", errors.New(fmt.Sprintf("bad format of TxWriteKey: %s", key))
	}
	var id uint64
	_, err := orderedcode.Parse(keys[1], orderedcode.Decr(&id))
	if err != nil {
		return 0, "", errors.Wrap(err, fmt.Sprintf("parse failed: %s", key))
	}
	return id, strings.Join(keys[2:], "_"), nil
}

//////// txKey ////////////

func encodeTxKey(txId TXID, key string) (string, error) {
	k, err := orderedcode.Append(nil, orderedcode.Decr(uint64(txId)))
	if err != nil {
		return "", errors.Wrap(err, "getTxKey")
	}
	ret := fmt.Sprintf("%s_%s_%s", TxKeyPrefix, key, string(k))
	return ret, nil
}

func decodeTxKey(key string) (TXID, string, error) {
	keys := strings.Split(key, "_")
	if len(keys) < 3 {
		return 0, "", errors.New(fmt.Sprintf("bad format of TxKey: %s", key))
	}
	var id uint64
	_, err := orderedcode.Parse(keys[len(keys)-1], orderedcode.Decr(&id))
	if err != nil {
		return 0, "", errors.Wrap(err, fmt.Sprintf("parse failed: %s", key))
	}
	return id, strings.Join(keys[1:len(keys)-1], "_"), nil
}
