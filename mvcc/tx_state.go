package mvcc

type TXID = uint64

type TxState struct {
	TxID     TXID
	ReadOnly bool
	ActiveTx map[TXID]struct{}
}

// check whether the given txId is visible to currenct tx
func (ts TxState) IsVisible(txId TXID) bool {
	if _, ok := ts.ActiveTx[txId]; ok { // active tx is invisible to current tx
		return false
	}
	if ts.ReadOnly {
		return txId < ts.TxID
	} else {
		return txId <= ts.TxID
	}
}
