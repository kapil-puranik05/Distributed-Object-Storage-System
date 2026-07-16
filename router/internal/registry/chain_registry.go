package registry

import (
	"router/internal/dtos"
	"sync"
)

type Chain struct {
	HeadAddress string `json:"headAddress"`
	TailAddress string `json:"tailAddress"`
	ChainId     string `json:"chainId"`
}

type Registry struct {
	registryMutex sync.RWMutex
	chainRegistry []*Chain
	nextChain     int
}

func (r *Registry) registerChain(req dtos.ChainRegistrationRequest) {
	r.registryMutex.Lock()
	defer r.registryMutex.Unlock()
	for _, chain := range r.chainRegistry {
		if chain.ChainId == req.ChainId {
			return
		}
	}
	r.chainRegistry = append(r.chainRegistry, &Chain{
		ChainId:     req.ChainId,
		HeadAddress: req.HeadAddress,
		TailAddress: req.TailAddress,
	})
}

func (r *Registry) getNextChain() *Chain {
	r.registryMutex.Lock()
	if len(r.chainRegistry) == 0 {
		r.registryMutex.Unlock()
		return nil
	}
	chain := r.chainRegistry[r.nextChain]
	r.nextChain = (r.nextChain + 1) % len(r.chainRegistry)
	r.registryMutex.Unlock()
	return chain
}
