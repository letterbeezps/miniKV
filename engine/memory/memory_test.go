package memory

import (
	"testing"

	"github.com/letterbeezps/miniKV/engine"
)

func Test_GetSet(t *testing.T) {
	m := NewMemory()
	engine.EngineTestGetSet(m, t)
}

func Test_Iterator(t *testing.T) {
	m := NewMemory()
	engine.ScanTest(m, t)
}
