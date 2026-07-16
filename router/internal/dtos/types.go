package dtos

type ChainRegistrationRequest struct {
	ChainId     string `json:"chainId"`
	HeadAddress string `json:"headAddress"`
	TailAddress string `json:"tailAddress"`
}
