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
	Epoch          int    `json:"epoch"`
	SequenceNumber uint64 `json:"sequenceNumber"`
	RequestID      string `json:"requestId"`
	ObjectID       string `json:"objectId"`
	Data           []byte `json:"data"`
	OpType         string `json:"opType"`
}

type AckRequest struct {
	Epoch          int    `json:"epoch"`
	SequenceNumber uint64 `json:"sequenceNumber"`
}

type ReConfigCommand struct {
	NewEpoch      int    `json:"newEpoch"`
	AssignedRole  Role   `json:"assignedRole"`
	PrevAddress   string `json:"prevAddress"`
	NextAddress   string `json:"nextAddress"`
	MasterAddress string `json:"masterAddress"`
}
