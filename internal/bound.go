package keytype

type BoundType = int

const (
	Include BoundType = 0
	Exclude BoundType = 1
	NoBound BoundType = 3
)

type Bound struct {
	Key       string
	BoundType BoundType
}

func NewBound(key string, boundType BoundType) Bound {
	return Bound{
		Key:       key,
		BoundType: boundType,
	}
}
