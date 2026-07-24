package registry

import (
	"log"
	"sync"
)

type ChainRegistrationRequest struct {
	ChainId       string `json:"chainId"`
	HeadAddress   string `json:"headAddress"`
	TailAddress   string `json:"tailAddress"`
	MasterAddress string `json:"masterAddress"`
}

type Topology struct {
	Chains []*Chain `json:"chains"`
}

type Chain struct {
	HeadAddress   string `json:"headAddress"`
	TailAddress   string `json:"tailAddress"`
	ChainId       string `json:"chainId"`
	MasterAddress string `json:"masterAddress"`
}

type Registry struct {
	registryMutex sync.RWMutex
	chainRegistry []*Chain
}

func (r *Registry) InitializeRegistry() {
	r.chainRegistry = make([]*Chain, 0)
}

func (r *Registry) RegisterChain(req ChainRegistrationRequest) {
	r.registryMutex.Lock()
	defer r.registryMutex.Unlock()
	for _, chain := range r.chainRegistry {
		if chain.ChainId == req.ChainId {
			return
		}
	}
	r.chainRegistry = append(r.chainRegistry, &Chain{
		ChainId:       req.ChainId,
		HeadAddress:   req.HeadAddress,
		TailAddress:   req.TailAddress,
		MasterAddress: req.MasterAddress,
	})
	log.Printf("New Chain registered successfully. Current chain count: %d", len(r.chainRegistry))
}

func (r *Registry) CopyTopology() *Topology {
	r.registryMutex.RLock()
	defer r.registryMutex.RUnlock()
	chains := make([]*Chain, len(r.chainRegistry))
	copy(chains, r.chainRegistry)
	return &Topology{
		Chains: chains,
	}
}
