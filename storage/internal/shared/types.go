package shared

type Role string

const (
	RoleHead   Role = "HEAD"
	RoleMiddle Role = "MIDDLE"
	RoleTail   Role = "TAIL"
	RoleOrphan Role = "ORPHAN"
)

type ObjectData struct {
	Key  string `json:"objectKey"`
	Data []byte `json:"data"`
}

type WriteRequest struct {
	Epoch          uint64 `json:"epoch"`
	SequenceNumber uint64 `json:"sequenceNumber"`
	ObjectID       string `json:"objectId"`
	Data           []byte `json:"data"`
	ChunkId        uint64 `json:"chunkId"`
}

type AckRequest struct {
	Epoch          uint64 `json:"epoch"`
	SequenceNumber uint64 `json:"sequenceNumber"`
}

type ReConfigCommand struct {
	NewEpoch     uint64 `json:"newEpoch"`
	AssignedRole Role   `json:"assignedRole"`
	PrevAddress  string `json:"prevAddress"`
	NextAddress  string `json:"nextAddress"`
}

type NodeMetaDataDto struct {
	NodeId  string `json:"nodeId"`
	Address string `json:"address"`
}
