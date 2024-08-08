package protocol

type LockDBState struct {
	Padding0         [14]uint32
	LockCount        uint64
	Padding1         [14]uint32
	UnLockCount      uint64
	Padding2         [15]uint32
	LockedCount      uint32
	Padding3         [15]uint32
	KeyCount         uint32
	Padding4         [15]uint32
	WaitCount        uint32
	Padding5         [15]uint32
	TimeoutedCount   uint32
	Padding6         [15]uint32
	ExpiredCount     uint32
	Padding7         [15]uint32
	UnlockErrorCount uint32
	Padding8         [15]uint32
}
