package storage

import (
	"context"
	"sync"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/dline"
	"github.com/filecoin-project/lotus/chain/types"

	"github.com/filecoin-project/specs-actors/actors/builtin/miner"

	"github.com/filecoin-project/go-state-types/abi"
)

type stateMachineAPI interface {
	StateMinerProvingDeadline(context.Context, address.Address, types.TipSetKey) (*dline.Info, error)
	generatePoST(ctx context.Context, deadline *dline.Info, ts *types.TipSet)
	submitPoST(ctx context.Context, deadline *dline.Info, ts *types.TipSet)
}

const SubmitConfidence = 4 // TODO: config

type PoSTStatus string

const (
	PoSTStatusNew             PoSTStatus = "PoSTStatusNew"
	PoSTStatusProving         PoSTStatus = "PoSTStatusProving"
	PoSTStatusProvingComplete PoSTStatus = "PoSTStatusProvingComplete"
	PoSTStatusSubmitting      PoSTStatus = "PoSTStatusSubmitting"
	PoSTStatusComplete        PoSTStatus = "PoSTStatusComplete"
)

type stateMachine struct {
	api   stateMachineAPI
	actor address.Address

	lk          sync.RWMutex
	state       PoSTStatus
	deadline    *dline.Info
	posts       []miner.SubmitWindowedPoStParams
	abortProof  context.CancelFunc
	abortSubmit context.CancelFunc
	currentTS   *types.TipSet
	currentCtx  context.Context
}

func newStateMachine(api stateMachineAPI, actor address.Address) *stateMachine {
	return &stateMachine{
		api:   api,
		actor: actor,
		state: PoSTStatusNew,
	}
}

func (a *stateMachine) startGenerateProof(abort context.CancelFunc, deadline *dline.Info) {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.abortProof = abort
	a.deadline = deadline
	a.state = PoSTStatusProving
}

func (a *stateMachine) completeGenerateProof(posts []miner.SubmitWindowedPoStParams) error {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.posts = posts
	a.state = PoSTStatusProvingComplete

	// Generating the proof has completed, so move onto the next state
	return a.stateChange()
}

func (a *stateMachine) startSubmitPoST(abort context.CancelFunc) []miner.SubmitWindowedPoStParams {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.abortSubmit = abort
	a.state = PoSTStatusSubmitting
	return a.posts
}

func (a *stateMachine) completeSubmitPost() {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.state = PoSTStatusComplete
}

func (a *stateMachine) expired(at abi.ChainEpoch) bool {
	return a.state != PoSTStatusNew && a.state != PoSTStatusComplete && at >= a.deadline.Close
}

func (a *stateMachine) started() bool {
	return a.state != PoSTStatusNew
}

func (a *stateMachine) readyToSubmitProof(height abi.ChainEpoch) bool {
	return a.state == PoSTStatusProvingComplete && height > a.deadline.PeriodStart+SubmitConfidence
}

func (a *stateMachine) proofSubmitted() bool {
	return a.state == PoSTStatusComplete
}

func (a *stateMachine) headChange(ctx context.Context, newTS *types.TipSet) error {
	a.lk.Lock()
	defer a.lk.Unlock()

	a.currentCtx = ctx
	a.currentTS = newTS

	return a.stateChange()
}

func (a *stateMachine) stateChange() error {
	ctx := a.currentCtx
	newTS := a.currentTS

	// Get the current proving deadline info
	diNew, err := a.api.StateMinerProvingDeadline(ctx, a.actor, newTS.Key())
	if err != nil {
		return err
	}

	if !diNew.PeriodStarted() {
		return nil // not proving anything yet
	}

	// If the active PoST has expired, cancel it
	newHeight := newTS.Height()
	if a.expired(newHeight) {
		log.Warnf("aborting window PoST for deadline %d, expired at %d, current height is %d", diNew.Index, a.deadline.Close, newTS.Height())
		a.abort(diNew)
		return nil
	}

	// If the current deadline proof generation has not started, start it
	if !a.started() {
		a.api.generatePoST(ctx, diNew, newTS)
		return nil
	}

	// If the proof has been generated, and it's not already being submitted,
	// submit it
	if a.readyToSubmitProof(newHeight) {
		a.api.submitPoST(ctx, diNew, newTS)
		return nil
	}

	// If the proof has been submitted
	if a.proofSubmitted() {
		// Check if the current height is within the challenge period for the
		// next deadline
		diNext, err := a.nextDeadline(diNew)
		if err != nil {
			return err
		}

		if newHeight < diNext.Challenge {
			return nil
		}

		// Start generating the proof for the next deadline
		a.deadline = diNext
		a.api.generatePoST(ctx, diNext, newTS)

		return nil
	}

	return nil
}

func (a *stateMachine) abort(deadline *dline.Info) {
	a.lk.Lock()
	defer a.lk.Unlock()

	aborted := false
	switch a.state {
	case PoSTStatusProving:
		a.abortProof()
		aborted = true
	case PoSTStatusSubmitting:
		a.abortSubmit()
		aborted = true
	}
	a.state = PoSTStatusNew

	if aborted {
		if deadline == nil {
			deadline = a.deadline
		}
		log.Warnf("Aborting Window PoSt (Deadline: %+v)", deadline)
	}
}

func (a *stateMachine) nextDeadline(currentDeadline *dline.Info) (*dline.Info, error) {
	// TODO: return the *next* deadline
	return currentDeadline, nil
}
