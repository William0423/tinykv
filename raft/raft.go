// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"go.etcd.io/etcd/raft/quorum"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	Voters quorum.JointConfig

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	logger log.Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	r := &Raft{
		id:               c.ID,
		Lead:             None,
		Prs:              make(map[uint64]*Progress),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	peers := c.peers
	for _, p := range peers {
		r.Prs[p] = &Progress{Next: 1}
	}

	r.becomeFollower(r.Term, None)

	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	r.tickElection()
}

// tickElection is run by followers and candidates after r.electionTimeout.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.isElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
	//if r.promotable() && r.pastElectionTimeout() {
	//	r.electionElapsed = 0
	//	r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	//}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	log.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
	log.Infof("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader
	log.Infof("%x became leader at term %d", r.id, r.Term)
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	// TODO
	//for i := range r.Prs {
	//r.Prs[i] = &Progress{Next: r.RaftLog.LastIndex() + 1, ins: newInflights(r.maxInflight)}
	//if i == r.id {
	//	r.prs[i].Match = r.raftLog.lastIndex()
	//}
	//}
	//r.pendingConf = false
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
		// local message
	case m.Term > r.Term:
		//lead := m.From
		// 消息的Term比当前节点的Term大，当前节点需要变成Follower节点
		//if m.MsgType == pb.MessageType_MsgRequestVote {
		//	lead = None
		//}
		log.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		//r.becomeFollower(m.Term, lead)
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term < r.Term: // 最新版本在这个分支会更详细的处理
		// ignore
		log.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
			r.id, r.Term, m.MsgType, m.From, m.Term)
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgHup:
		log.Infof("%x is starting a new election at term %d", r.id, r.Term)
		// 竞选
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		canVote := r.Vote == m.From || (r.Vote == None && r.Lead == None)
		if canVote && r.RaftLog.isUpToDate(m.Index, m.LogTerm) {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: voteRespMsgType(m.MsgType)})
			if m.MsgType == pb.MessageType_MsgRequestVote {
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.MsgType, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: voteRespMsgType(m.MsgType), Reject: true})
		}
	default:
		// 下面是代替了stepFunc的执行：
		switch r.State {
		case StateFollower:
			r.stepFollower(m)
		case StateCandidate:
			r.stepCandidate(m)
		case StateLeader:
			r.stepLeader(m)
		}
	}
	return nil
}

func (r *Raft) campaign() {
	r.becomeCandidate()

	var term uint64
	var voteMsg pb.MessageType
	voteMsg = pb.MessageType_MsgRequestVote
	term = r.Term

	if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == quorum.VoteWon {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
		r.becomeLeader()
		return
	}
	var ids []uint64
	{
		idMap := r.Voters.IDs()
		ids = make([]uint64, 0, len(idMap))
		for id := range idMap {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}
	for _, id := range ids {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), voteMsg, id, r.Term)
		r.send(pb.Message{Term: term, To: id, MsgType: voteMsg, Index: r.RaftLog.LastIndex(), LogTerm: r.RaftLog.LastTerm()})
	}

	//q := r.quorumSize()
	//pollSize := r.poll(r.id, true)
	//if q == pollSize {
	//	r.becomeLeader()
	//}
	//
	//for i := range r.Prs {
	//	if i == r.id {
	//		continue
	//	}
	//	log.Infof("%x [logterm: %d, index: %d] sent vote request to %x at term %d",
	//		r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), i, r.Term)
	//}
}

// voteResponseType maps vote and prevote message types to their corresponding responses.
func voteRespMsgType(msgt pb.MessageType) pb.MessageType {
	switch msgt {
	case pb.MessageType_MsgRequestVote:
		return pb.MessageType_MsgRequestVoteResponse
	default:
		panic(fmt.Sprintf("not a vote message: %s", msgt))
	}
}

func (r *Raft) poll(id uint64, t pb.MessageType, v bool) (granted int, rejected int, result quorum.VoteResult) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	r.RecordVote(id, v)
	return r.TallyVotes()
}

func (r *Raft) RecordVote(id uint64, v bool) {
	_, ok := r.votes[id]
	if !ok {
		r.votes[id] = v
	}
}

func (r *Raft) TallyVotes() (granted int, rejected int, _ quorum.VoteResult) {
	for _, vv := range r.votes {
		if vv {
			granted++
		} else {
			rejected++
		}
	}
	result := r.Voters.VoteResult(r.votes)
	return granted, rejected, result
}

//func (r *Raft) poll(id uint64, v bool) (granted int)  {
//	if v {
//		log.Infof("%x received vote from %x at term %d", r.id, id, r.Term)
//	} else {
//		log.Infof("%x received vote rejection from %x at term %d", r.id, id, r.Term)
//	}
//	if _, ok := r.votes[id]; !ok {
//		r.votes[id] = v
//	}
//	for _, vv := range r.votes {
//		if vv {
//			granted++
//		}
//	}
//	return granted
//}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.handleSnapshot(m)
	case pb.MessageType_MsgTimeoutNow:
		r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)

		//case pb.MessageType_MsgRequestVote:
		//	// TODO
		//	// if (r.Vote == None || r.Vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
		//	if r.Vote == None || r.Vote == m.From {
		//		log.Infof("%x [logterm: %d, index: %d, vote: %x] voted for %x [logterm: %d, index: %d] at term %d",
		//			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
		//		// r.send(pb.Message{To: m.From, Type: pb.MsgVoteResp})
		//	} else {
		//		log.Infof("%x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %d",
		//			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
		//		// r.send(pb.Message{To: m.From, Type: pb.MsgVoteResp, Reject: true})
		//	}
	}

}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		log.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return
	case pb.MessageType_MsgAppend:
		r.becomeFollower(r.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(r.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		//log.Infof("%x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %x",
		//	r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
		// r.send(pb.Message{To: m.From, Type: pb.MsgVoteResp, Reject: true})
	case pb.MessageType_MsgRequestVoteResponse:
		gr, rj, res := r.poll(m.From, m.MsgType, !m.Reject)
		//log.Infof("%x [q:%d] has received %d votes and %d vote rejections", r.id, r.quorumSize(), gr, len(r.votes)-gr)
		r.logger.Infof("%x has received %d %s votes and %d vote rejections", r.id, gr, m.MsgType, rj)
		switch res {
		case quorum.VoteWon:
			r.becomeLeader()
			// TODO
			//r.bcastAppend()
		//case len(r.votes) - gr:
		case quorum.VoteLost:
			// TODO: 这段什么意思？
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:
		r.logger.Debugf("%x [term %d state %v] ignored MsgTimeoutNow from %x", r.id, r.Term, r.State, m.From)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	//pr := r.Prs[m.From]
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		// TODO
		//r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		if len(m.Entries) == 0 {
			log.Panicf("%x stepped empty MsgProp", r.id)
		}
		if r.Prs[r.id] == nil {
			r.logger.Errorf("raft proposal dropped")
		}
		// TODO
		//for i, e := range m.Entries {
		//	if e.EntryType == pb.EntryType_EntryConfChange {
		//		if r.pendingConf {
		//			m.Entries[i] = &pb.Entry{EntryType: pb.EntryType_EntryNormal}
		//		}
		//		r.pendingConf = true
		//	}
		//}
		//r.appendEntry(m.Entries...)
		//r.bcastAppend()
	case pb.MessageType_MsgAppend:
		if m.Reject {
			//log.Debugf("%x received msgApp rejection(lastindex: %d) from %x for index %d",
			//	r.id, "m.RejectHint", m.From, m.Index)
			log.Debugf("%x received msgApp rejection(lastindex: ) from %x for index %d", r.id, m.From, m.Index)
			// TODO
			//if pr.maybeDecrTo(m.Index, m.RejectHint) {
			//	log.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
			//	if pr.State == ProgressStateReplicate {
			//		pr.becomeProbe()
			//	}
			//	r.sendAppend(m.From)
			//}
		} else {
			// TODO
			//oldPaused := pr.isPaused()
		}
	case pb.MessageType_MsgRequestVote:
		log.Infof("%x [logterm: %d, index: %d, vote: %x] rejected vote from %x [logterm: %d, index: %d] at term %d",
			r.id, r.RaftLog.LastTerm(), r.RaftLog.LastIndex(), r.Vote, m.From, m.LogTerm, m.Index, r.Term)
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	}

	pr := r.Prs[m.From]
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
	}
	switch m.MsgType {
	case pb.MessageType_MsgAppendResponse:

	case pb.MessageType_MsgHeartbeatResponse:
		// TODO
		//if pr.State == ProgressStateReplicate && pr.ins.full() {
		//	pr.ins.freeFirstOne()
		//}
		//if pr.Match < r.raftLog.lastIndex() {
		//	r.sendAppend(m.From)
		//}
	}

}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) quorumSize() int { return len(r.Prs)/2 + 1 }

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

// isElectionTimeout returns true if r.elapsed is greater than the
// randomized election timeout in (electiontimeout, 2 * electiontimeout - 1).
// Otherwise, it returns false.
func (r *Raft) isElectionTimeout() bool {
	d := r.electionElapsed - r.electionTimeout
	if d < 0 {
		return false
	}
	// 	return d > r.rand.Int()%r.electionTimeout
	return true
}
