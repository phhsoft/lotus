package storage

import (
	"context"
	"time"

	"github.com/filecoin-project/go-state-types/dline"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/specs-storage/storage"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/store"
	"github.com/filecoin-project/lotus/chain/types"
	sectorstorage "github.com/filecoin-project/lotus/extern/sector-storage"
	"github.com/filecoin-project/lotus/node/config"

	"go.opencensus.io/trace"
)

type WindowPoStScheduler struct {
	api              storageMinerApi
	feeCfg           config.MinerFeeConfig
	prover           storage.Prover
	faultTracker     sectorstorage.FaultTracker
	proofType        abi.RegisteredPoStProof
	partitionSectors uint64
	state            *stateMachine

	actor  address.Address
	worker address.Address

	cur *types.TipSet

	// if a post is in progress, this indicates for which ElectionPeriodStart
	activeDeadline *dline.Info
	abort          context.CancelFunc

	//failed abi.ChainEpoch // eps
	//failLk sync.Mutex
}

func NewWindowedPoStScheduler(api storageMinerApi, fc config.MinerFeeConfig, sb storage.Prover, ft sectorstorage.FaultTracker, actor address.Address, worker address.Address) (*WindowPoStScheduler, error) {
	mi, err := api.StateMinerInfo(context.TODO(), actor, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("getting sector size: %w", err)
	}

	rt, err := mi.SealProofType.RegisteredWindowPoStProof()
	if err != nil {
		return nil, err
	}

	return &WindowPoStScheduler{
		api:              api,
		feeCfg:           fc,
		prover:           sb,
		faultTracker:     ft,
		proofType:        rt,
		partitionSectors: mi.WindowPoStPartitionSectors,

		actor:  actor,
		worker: worker,
	}, nil
}

type stateMachineAPIImpl struct {
	storageMinerApi
	*WindowPoStScheduler
}

func (s *WindowPoStScheduler) Run(ctx context.Context) {
	// Initialize state machine
	smImpl := &stateMachineAPIImpl{storageMinerApi: s.api, WindowPoStScheduler: s}
	s.state = newStateMachine(smImpl, s.actor)
	defer s.state.abort(nil)

	var notifs <-chan []*api.HeadChange
	var err error
	var gotCur bool

	// not fine to panic after this point
	for {
		if notifs == nil {
			notifs, err = s.api.ChainNotify(ctx)
			if err != nil {
				log.Errorf("ChainNotify error: %+v", err)

				build.Clock.Sleep(10 * time.Second)
				continue
			}

			gotCur = false
		}

		select {
		case changes, ok := <-notifs:
			if !ok {
				log.Warn("WindowPoStScheduler notifs channel closed")
				notifs = nil
				continue
			}

			if !gotCur {
				if len(changes) != 1 {
					log.Errorf("expected first notif to have len = 1")
					continue
				}
				if changes[0].Type != store.HCCurrent {
					log.Errorf("expected first notif to tell current ts")
					continue
				}

				if err := s.update(ctx, changes[0].Val); err != nil {
					log.Errorf("%+v", err)
				}

				gotCur = true
				continue
			}

			ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.headChange")

			var highest *types.TipSet

			for _, change := range changes {
				if change.Val == nil {
					log.Errorf("change.Val was nil")
				}
				if change.Type == store.HCApply {
					highest = change.Val
				}
			}

			if err := s.update(ctx, highest); err != nil {
				log.Error("handling head updates in windowPost sched: %+v", err)
			}

			span.End()
		case <-ctx.Done():
			return
		}
	}
}

func (s *WindowPoStScheduler) update(ctx context.Context, newTS *types.TipSet) error {
	if newTS == nil {
		return xerrors.Errorf("no new tipset in WindowPoStScheduler.update")
	}

	return s.state.headChange(ctx, newTS)
}
