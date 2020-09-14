package storage

import (
	"bytes"
	"context"
	"time"

	"github.com/filecoin-project/go-state-types/dline"

	"github.com/filecoin-project/specs-actors/actors/runtime/proof"

	"github.com/filecoin-project/go-bitfield"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/specs-actors/actors/builtin"
	"github.com/filecoin-project/specs-actors/actors/builtin/miner"
	"go.opencensus.io/trace"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors"
	"github.com/filecoin-project/lotus/chain/types"
)

func (s *WindowPoStScheduler) failPost(deadline *dline.Info) {
	log.Errorf("TODO")
	/*s.failLk.Lock()
	if eps > s.failed {
		s.failed = eps
	}
	s.failLk.Unlock()*/

	s.state.abort(deadline)
}

func (s *WindowPoStScheduler) generatePoST(ctx context.Context, deadline *dline.Info, ts *types.TipSet) {
	ctx, abort := context.WithCancel(ctx)

	s.state.startGenerateProof(abort, deadline)

	go func() {
		defer abort()

		ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.generatePoST")
		defer span.End()

		posts, err := s.runPost(ctx, *deadline, ts)
		if err != nil {
			log.Errorf("runPost failed: %+v", err)
			s.failPost(deadline)
			return
		}

		err = s.state.completeGenerateProof(posts)
		if err != nil {
			log.Errorf("completeGenerateProof failed: %+v", err)
			s.failPost(deadline)
		}
	}()
}

func (s *WindowPoStScheduler) submitPoST(ctx context.Context, deadline *dline.Info, ts *types.TipSet) {
	ctx, abort := context.WithCancel(ctx)

	posts := s.state.startSubmitPoST(abort)

	go func() {
		defer abort()

		ctx, span := trace.StartSpan(ctx, "WindowPoStScheduler.submitPoST")
		defer span.End()

		commEpoch := deadline.Open
		commRand, err := s.api.ChainGetRandomnessFromTickets(ctx, ts.Key(), crypto.DomainSeparationTag_PoStChainCommit, commEpoch, nil)
		if err != nil {
			err = xerrors.Errorf("failed to get chain randomness for windowPost (ts=%d; deadline=%d): %w", ts.Height(), commEpoch, err)
			log.Errorf("submitPost failed: %+v", err)
			s.failPost(deadline)
			return
		}

		var submitErr error
		for i := range posts {
			post := &posts[i]
			post.ChainCommitEpoch = commEpoch
			post.ChainCommitRand = commRand

			if submitErr = s.submitPost(ctx, post); submitErr != nil {
				log.Errorf("submitPost failed: %+v", submitErr)
			}
		}

		if submitErr != nil {
			s.failPost(deadline)
			return
		}

		s.state.completeSubmitPost()
	}()
}

func (s *WindowPoStScheduler) checkSectors(ctx context.Context, check bitfield.BitField) (bitfield.BitField, error) {
	spt, err := s.proofType.RegisteredSealProof()
	if err != nil {
		return bitfield.BitField{}, xerrors.Errorf("getting seal proof type: %w", err)
	}

	mid, err := address.IDFromAddress(s.actor)
	if err != nil {
		return bitfield.BitField{}, err
	}

	sectors := make(map[abi.SectorID]struct{})
	var tocheck []abi.SectorID
	err = check.ForEach(func(snum uint64) error {
		s := abi.SectorID{
			Miner:  abi.ActorID(mid),
			Number: abi.SectorNumber(snum),
		}

		tocheck = append(tocheck, s)
		sectors[s] = struct{}{}
		return nil
	})
	if err != nil {
		return bitfield.BitField{}, xerrors.Errorf("iterating over bitfield: %w", err)
	}

	bad, err := s.faultTracker.CheckProvable(ctx, spt, tocheck)
	if err != nil {
		return bitfield.BitField{}, xerrors.Errorf("checking provable sectors: %w", err)
	}
	for _, id := range bad {
		delete(sectors, id)
	}

	log.Warnw("Checked sectors", "checked", len(tocheck), "good", len(sectors))

	sbf := bitfield.New()
	for s := range sectors {
		sbf.Set(uint64(s.Number))
	}

	return sbf, nil
}

func (s *WindowPoStScheduler) checkNextRecoveries(ctx context.Context, dlIdx uint64, partitions []*miner.Partition) error {
	ctx, span := trace.StartSpan(ctx, "storage.checkNextRecoveries")
	defer span.End()

	params := &miner.DeclareFaultsRecoveredParams{
		Recoveries: []miner.RecoveryDeclaration{},
	}

	faulty := uint64(0)

	for partIdx, partition := range partitions {
		unrecovered, err := bitfield.SubtractBitField(partition.Faults, partition.Recoveries)
		if err != nil {
			return xerrors.Errorf("subtracting recovered set from fault set: %w", err)
		}

		uc, err := unrecovered.Count()
		if err != nil {
			return xerrors.Errorf("counting unrecovered sectors: %w", err)
		}

		if uc == 0 {
			continue
		}

		faulty += uc

		recovered, err := s.checkSectors(ctx, unrecovered)
		if err != nil {
			return xerrors.Errorf("checking unrecovered sectors: %w", err)
		}

		// if all sectors failed to recover, don't declare recoveries
		recoveredCount, err := recovered.Count()
		if err != nil {
			return xerrors.Errorf("counting recovered sectors: %w", err)
		}

		if recoveredCount == 0 {
			continue
		}

		params.Recoveries = append(params.Recoveries, miner.RecoveryDeclaration{
			Deadline:  dlIdx,
			Partition: uint64(partIdx),
			Sectors:   recovered,
		})
	}

	if len(params.Recoveries) == 0 {
		if faulty != 0 {
			log.Warnw("No recoveries to declare", "deadline", dlIdx, "faulty", faulty)
		}

		return nil
	}

	enc, aerr := actors.SerializeParams(params)
	if aerr != nil {
		return xerrors.Errorf("could not serialize declare recoveries parameters: %w", aerr)
	}

	msg := &types.Message{
		To:     s.actor,
		From:   s.worker,
		Method: builtin.MethodsMiner.DeclareFaultsRecovered,
		Params: enc,
		Value:  types.NewInt(0),
	}
	spec := &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)}
	s.setSender(ctx, msg, spec)

	sm, err := s.api.MpoolPushMessage(ctx, msg, &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)})
	if err != nil {
		return xerrors.Errorf("pushing message to mpool: %w", err)
	}

	log.Warnw("declare faults recovered Message CID", "cid", sm.Cid())

	rec, err := s.api.StateWaitMsg(context.TODO(), sm.Cid(), build.MessageConfidence)
	if err != nil {
		return xerrors.Errorf("declare faults recovered wait error: %w", err)
	}

	if rec.Receipt.ExitCode != 0 {
		return xerrors.Errorf("declare faults recovered wait non-0 exit code: %d", rec.Receipt.ExitCode)
	}

	return nil
}

func (s *WindowPoStScheduler) checkNextFaults(ctx context.Context, dlIdx uint64, partitions []*miner.Partition) error {
	ctx, span := trace.StartSpan(ctx, "storage.checkNextFaults")
	defer span.End()

	params := &miner.DeclareFaultsParams{
		Faults: []miner.FaultDeclaration{},
	}

	bad := uint64(0)

	for partIdx, partition := range partitions {
		toCheck, err := partition.ActiveSectors()
		if err != nil {
			return xerrors.Errorf("getting active sectors: %w", err)
		}

		good, err := s.checkSectors(ctx, toCheck)
		if err != nil {
			return xerrors.Errorf("checking sectors: %w", err)
		}

		faulty, err := bitfield.SubtractBitField(toCheck, good)
		if err != nil {
			return xerrors.Errorf("calculating faulty sector set: %w", err)
		}

		c, err := faulty.Count()
		if err != nil {
			return xerrors.Errorf("counting faulty sectors: %w", err)
		}

		if c == 0 {
			continue
		}

		bad += c

		params.Faults = append(params.Faults, miner.FaultDeclaration{
			Deadline:  dlIdx,
			Partition: uint64(partIdx),
			Sectors:   faulty,
		})
	}

	if len(params.Faults) == 0 {
		return nil
	}

	log.Errorw("DETECTED FAULTY SECTORS, declaring faults", "count", bad)

	enc, aerr := actors.SerializeParams(params)
	if aerr != nil {
		return xerrors.Errorf("could not serialize declare faults parameters: %w", aerr)
	}

	msg := &types.Message{
		To:     s.actor,
		From:   s.worker,
		Method: builtin.MethodsMiner.DeclareFaults,
		Params: enc,
		Value:  types.NewInt(0), // TODO: Is there a fee?
	}
	spec := &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)}
	s.setSender(ctx, msg, spec)

	sm, err := s.api.MpoolPushMessage(ctx, msg, spec)
	if err != nil {
		return xerrors.Errorf("pushing message to mpool: %w", err)
	}

	log.Warnw("declare faults Message CID", "cid", sm.Cid())

	rec, err := s.api.StateWaitMsg(context.TODO(), sm.Cid(), build.MessageConfidence)
	if err != nil {
		return xerrors.Errorf("declare faults wait error: %w", err)
	}

	if rec.Receipt.ExitCode != 0 {
		return xerrors.Errorf("declare faults wait non-0 exit code: %d", rec.Receipt.ExitCode)
	}

	return nil
}

func (s *WindowPoStScheduler) runPost(ctx context.Context, di dline.Info, ts *types.TipSet) ([]miner.SubmitWindowedPoStParams, error) {
	ctx, span := trace.StartSpan(ctx, "storage.runPost")
	defer span.End()

	go func() {
		// TODO: extract from runPost, run on fault cutoff boundaries

		// check faults / recoveries for the *next* deadline. It's already too
		// late to declare them for this deadline
		declDeadline := (di.Index + 2) % miner.WPoStPeriodDeadlines

		partitions, err := s.api.StateMinerPartitions(context.TODO(), s.actor, declDeadline, ts.Key())
		if err != nil {
			log.Errorf("getting partitions: %v", err)
			return
		}

		if err := s.checkNextRecoveries(context.TODO(), declDeadline, partitions); err != nil {
			// TODO: This is potentially quite bad, but not even trying to post when this fails is objectively worse
			log.Errorf("checking sector recoveries: %v", err)
		}

		if err := s.checkNextFaults(context.TODO(), declDeadline, partitions); err != nil {
			// TODO: This is also potentially really bad, but we try to post anyways
			log.Errorf("checking sector faults: %v", err)
		}
	}()

	buf := new(bytes.Buffer)
	if err := s.actor.MarshalCBOR(buf); err != nil {
		return nil, xerrors.Errorf("failed to marshal address to cbor: %w", err)
	}

	rand, err := s.api.ChainGetRandomnessFromBeacon(ctx, ts.Key(), crypto.DomainSeparationTag_WindowedPoStChallengeSeed, di.Challenge, buf.Bytes())
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain randomness for windowPost (ts=%d; deadline=%d): %w", ts.Height(), di, err)
	}

	// Get the partitions for the given deadline
	partitions, err := s.api.StateMinerPartitions(ctx, s.actor, di.Index, ts.Key())
	if err != nil {
		return nil, xerrors.Errorf("getting partitions: %w", err)
	}

	// Split partitions into batches, so as not to exceed the number of sectors
	// allowed in a single message
	partitionBatches, err := s.batchPartitions(partitions)
	if err != nil {
		return nil, err
	}

	// Generate proofs in batches
	posts := make([]miner.SubmitWindowedPoStParams, 0, len(partitionBatches))
	for batchIdx, batch := range partitionBatches {
		batchPartitionStartIdx := 0
		for _, batch := range partitionBatches[:batchIdx] {
			batchPartitionStartIdx += len(batch)
		}

		params := miner.SubmitWindowedPoStParams{
			Deadline:   di.Index,
			Partitions: make([]miner.PoStPartition, 0, len(batch)),
			Proofs:     nil,
		}

		skipCount := uint64(0)
		postSkipped := bitfield.New()
		var postOut []proof.PoStProof
		somethingToProve := true
		for retries := 0; retries < 5; retries++ {
			var sinfos []proof.SectorInfo
			for partIdx, partition := range batch {
				// TODO: Can do this in parallel
				toProve, err := partition.ActiveSectors()
				if err != nil {
					return nil, xerrors.Errorf("getting active sectors: %w", err)
				}

				toProve, err = bitfield.MergeBitFields(toProve, partition.Recoveries)
				if err != nil {
					return nil, xerrors.Errorf("adding recoveries to set of sectors to prove: %w", err)
				}

				toProve, err = bitfield.SubtractBitField(toProve, postSkipped)
				if err != nil {
					return nil, xerrors.Errorf("toProve - postSkipped: %w", err)
				}

				good, err := s.checkSectors(ctx, toProve)
				if err != nil {
					return nil, xerrors.Errorf("checking sectors to skip: %w", err)
				}

				skipped, err := bitfield.SubtractBitField(toProve, good)
				if err != nil {
					return nil, xerrors.Errorf("toProve - good: %w", err)
				}

				sc, err := skipped.Count()
				if err != nil {
					return nil, xerrors.Errorf("getting skipped sector count: %w", err)
				}

				skipCount += sc

				ssi, err := s.sectorsForProof(ctx, good, partition.Sectors, ts)
				if err != nil {
					return nil, xerrors.Errorf("getting sorted sector info: %w", err)
				}

				if len(ssi) == 0 {
					continue
				}

				sinfos = append(sinfos, ssi...)
				params.Partitions = append(params.Partitions, miner.PoStPartition{
					Index:   uint64(batchPartitionStartIdx + partIdx),
					Skipped: skipped,
				})
			}

			if len(sinfos) == 0 {
				// nothing to prove for this batch
				somethingToProve = false
				break
			}

			// Generate proof
			log.Infow("running windowPost",
				"chain-random", rand,
				"deadline", di,
				"height", ts.Height(),
				"skipped", skipCount)

			tsStart := build.Clock.Now()

			mid, err := address.IDFromAddress(s.actor)
			if err != nil {
				return nil, err
			}

			var ps []abi.SectorID
			postOut, ps, err = s.prover.GenerateWindowPoSt(ctx, abi.ActorID(mid), sinfos, abi.PoStRandomness(rand))
			elapsed := time.Since(tsStart)

			log.Infow("computing window PoSt", "batch", batchIdx, "elapsed", elapsed)

			if err == nil {
				// Proof generation successful, stop retrying
				break
			}

			// Proof generation failed, so retry

			if len(ps) == 0 {
				return nil, xerrors.Errorf("running post failed: %w", err)
			}

			log.Warnw("generate window PoSt skipped sectors", "sectors", ps, "error", err, "try", retries)

			skipCount += uint64(len(ps))
			for _, sector := range ps {
				postSkipped.Set(uint64(sector.Number))
			}
		}

		// Nothing to prove for this batch, try the next batch
		if !somethingToProve {
			continue
		}

		if len(postOut) == 0 {
			return nil, xerrors.Errorf("received no proofs back from generate window post")
		}

		params.Proofs = postOut

		posts = append(posts, params)
	}

	// Compute randomness after generating proofs so as to reduce the impact
	// of chain reorgs (which change randomness)
	commEpoch := di.Open
	commRand, err := s.api.ChainGetRandomnessFromTickets(ctx, ts.Key(), crypto.DomainSeparationTag_PoStChainCommit, commEpoch, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to get chain randomness for windowPost (ts=%d; deadline=%d): %w", ts.Height(), commEpoch, err)
	}

	for i := range posts {
		posts[i].ChainCommitEpoch = commEpoch
		posts[i].ChainCommitRand = commRand
	}

	return posts, nil
}

func (s *WindowPoStScheduler) batchPartitions(partitions []*miner.Partition) ([][]*miner.Partition, error) {
	// Get the number of sectors allowed in a partition, for this proof size
	sectorsPerPartition, err := builtin.PoStProofWindowPoStPartitionSectors(s.proofType)
	if err != nil {
		return nil, xerrors.Errorf("getting sectors per partition: %w", err)
	}

	// We don't want to exceed the number of sectors allowed in a message.
	// So given the number of sectors in a partition, work out the number of
	// partitions that can be in a message without exceeding sectors per
	// message:
	// floor(number of sectors allowed in a message / sectors per partition)
	// eg:
	// max sectors per message  7:  ooooooo
	// sectors per partition    3:  ooo
	// partitions per message   2:  oooOOO
	//                              <1><2> (3rd doesn't fit)
	partitionsPerMsg := int(miner.AddressedSectorsMax / sectorsPerPartition)

	// The number of messages will be:
	// ceiling(number of partitions / partitions per message)
	batchCount := len(partitions) / partitionsPerMsg
	if len(partitions)%partitionsPerMsg != 0 {
		batchCount++
	}

	// Split the partitions into batches
	batches := make([][]*miner.Partition, 0, batchCount)
	for i := 0; i < len(partitions); i += partitionsPerMsg {
		end := i + partitionsPerMsg
		if end > len(partitions) {
			end = len(partitions)
		}
		batches = append(batches, partitions[i:end])
	}

	return batches, nil
}

func (s *WindowPoStScheduler) sectorsForProof(ctx context.Context, goodSectors, allSectors bitfield.BitField, ts *types.TipSet) ([]proof.SectorInfo, error) {
	sset, err := s.api.StateMinerSectors(ctx, s.actor, &goodSectors, false, ts.Key())
	if err != nil {
		return nil, err
	}

	if len(sset) == 0 {
		return nil, nil
	}

	substitute := proof.SectorInfo{
		SectorNumber: sset[0].ID,
		SealedCID:    sset[0].Info.SealedCID,
		SealProof:    sset[0].Info.SealProof,
	}

	sectorByID := make(map[uint64]proof.SectorInfo, len(sset))
	for _, sector := range sset {
		sectorByID[uint64(sector.ID)] = proof.SectorInfo{
			SectorNumber: sector.ID,
			SealedCID:    sector.Info.SealedCID,
			SealProof:    sector.Info.SealProof,
		}
	}

	proofSectors := make([]proof.SectorInfo, 0, len(sset))
	if err := allSectors.ForEach(func(sectorNo uint64) error {
		if info, found := sectorByID[sectorNo]; found {
			proofSectors = append(proofSectors, info)
		} else {
			proofSectors = append(proofSectors, substitute)
		}
		return nil
	}); err != nil {
		return nil, xerrors.Errorf("iterating partition sector bitmap: %w", err)
	}

	return proofSectors, nil
}

func (s *WindowPoStScheduler) submitPost(ctx context.Context, proof *miner.SubmitWindowedPoStParams) error {
	ctx, span := trace.StartSpan(ctx, "storage.commitPost")
	defer span.End()

	enc, aerr := actors.SerializeParams(proof)
	if aerr != nil {
		return xerrors.Errorf("could not serialize submit post parameters: %w", aerr)
	}

	msg := &types.Message{
		To:     s.actor,
		From:   s.worker,
		Method: builtin.MethodsMiner.SubmitWindowedPoSt,
		Params: enc,
		Value:  types.NewInt(0),
	}
	spec := &api.MessageSendSpec{MaxFee: abi.TokenAmount(s.feeCfg.MaxWindowPoStGasFee)}
	s.setSender(ctx, msg, spec)

	// TODO: consider maybe caring about the output
	sm, err := s.api.MpoolPushMessage(ctx, msg, spec)
	if err != nil {
		return xerrors.Errorf("pushing message to mpool: %w", err)
	}

	log.Infof("Submitted window post: %s", sm.Cid())

	go func() {
		rec, err := s.api.StateWaitMsg(context.TODO(), sm.Cid(), build.MessageConfidence)
		if err != nil {
			log.Error(err)
			return
		}

		if rec.Receipt.ExitCode == 0 {
			return
		}

		log.Errorf("Submitting window post %s failed: exit %d", sm.Cid(), rec.Receipt.ExitCode)
	}()

	return nil
}

func (s *WindowPoStScheduler) setSender(ctx context.Context, msg *types.Message, spec *api.MessageSendSpec) {
	mi, err := s.api.StateMinerInfo(ctx, s.actor, types.EmptyTSK)
	if err != nil {
		log.Errorw("error getting miner info", "error", err)

		// better than just failing
		msg.From = s.worker
		return
	}

	gm, err := s.api.GasEstimateMessageGas(ctx, msg, spec, types.EmptyTSK)
	if err != nil {
		log.Errorw("estimating gas", "error", err)
		msg.From = s.worker
		return
	}
	*msg = *gm

	minFunds := big.Add(msg.RequiredFunds(), msg.Value)

	pa, err := AddressFor(ctx, s.api, mi, PoStAddr, minFunds)
	if err != nil {
		log.Errorw("error selecting address for post", "error", err)
		msg.From = s.worker
		return
	}

	msg.From = pa
}
