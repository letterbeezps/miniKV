package mvcc

import "errors"

var (
	ErrorSerialization     = errors.New("serialization")
	ErrorReadOnly          = errors.New("can't update with read only tx")
	ErrorNotSupportNoBound = errors.New("tx iterator not support NoBound")
)
