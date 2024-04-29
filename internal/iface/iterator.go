package iface

type Iterator interface {
	Value() []byte

	Key() string

	IsValid() bool

	Next()
}
