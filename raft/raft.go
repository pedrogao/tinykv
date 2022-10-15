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
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
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

	// log replication progress of each peer
	// nextIndex, matchIndex
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

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
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
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
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err)
	}

	// Your Code Here (2A).
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	prs := map[uint64]*Progress{}
	for _, peer := range c.peers {
		prs[peer] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	return &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		Prs:              prs, // next, match log index
		State:            StateFollower,
		votes:            map[uint64]bool{}, // votes
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		heartbeatElapsed: 0,
		electionElapsed:  0,
	}
}

// sendAppend sends an `append` RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	// 只是加入msgs，实际上由node来处理
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// 分布式环境中使用物理时钟有很多坑，因此使用逻辑时钟

	// 在Raft.tick函数中，根据Raft.State推动election timeout或是 heartbeat timeout。
	// 这是通过控制electionElapsed和heartbeatElapsed进行自增操作实现的，
	// 当electionElapsed >= randomizeElectionTimeout时触发一次选举，
	// 当heartbeatElapsed >= heartbeatTimeout时触发一次心跳。
	// 触发之后对应地要清零Elapsed。
	switch r.State {
	case StateFollower:
		r.followerTick()
	case StateCandidate:
		r.candidateTick()
	case StateLeader:
		r.leaderTick()
	}
}

func (r *Raft) leaderTick() {
	r.heartbeatElapsed += 1 // leader需要发送心跳
	// 触发心跳
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// 心跳属于节点内部消息
		m := pb.Message{
			MsgType: pb.MessageType_MsgBeat, // 内部消息，告知leader(自己)发送 heart beat
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		}
		err := r.Step(m)
		if err != nil {
			log.Errorf("trigger heart beat err: %s", err)
		}
		r.heartbeatElapsed = 0
	}
}

func (r *Raft) candidateTick() {
	r.electionElapsed += 1 // follower、candidate发出选举
	// 触发选举
	if r.electionElapsed > r.electionTimeout {
		r.becomeCandidate()
		m := pb.Message{
			MsgType: pb.MessageType_MsgHup, // 选举
			To:      r.id,
			From:    r.id, // 内部消息
		}
		err := r.Step(m)
		if err != nil {
			log.Errorf("trigger election err: %s", err)
		}
		r.electionElapsed = 0
	}
}

func (r *Raft) followerTick() {
	r.electionElapsed += 1 // follower、candidate发出选举
	// 触发选举
	if r.electionElapsed > r.electionTimeout {
		r.becomeCandidate()
		m := pb.Message{
			MsgType: pb.MessageType_MsgHup, // 选举
			To:      r.id,
			From:    r.id, // 内部消息
		}
		err := r.Step(m)
		if err != nil {
			log.Errorf("trigger election err: %s", err)
		}
		r.electionElapsed = 0
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = 0 // follower投票重置
	r.electionElapsed = 0
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Vote = r.id // 给自己投票
	r.votes = map[uint64]bool{
		r.id: true,
	}
	r.Term += 1
	r.electionElapsed = 0
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	for _, progress := range r.Prs {
		progress.Match = 0                        // 一个没有匹配上，就是0
		progress.Next = r.RaftLog.LastIndex() + 1 // 初始化为 lastIndex+1
	}
	// 发送一次空日志给自己
	//m := pb.Message{
	//	MsgType: pb.MessageType_MsgPropose,
	//	To:      r.id,
	//	From:    r.id,
	//	Entries: nil,
	//}
	//err := r.Step(m)
	//if err != nil {
	//	log.Errorf("leader step propose nop err: %s", err)
	//}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 处理 message
	// 每次逻辑时钟tick，调用step
	switch r.State {
	case StateFollower: // follower
		r.stepFollower(m)
	case StateCandidate: // candidate
		r.stepCandidate(m)
	case StateLeader: // leader
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if m.Term > r.Term { // 你的任期大，那么我变成追随者
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgRequestVote:
		r.sendVote(m)
	case pb.MessageType_MsgHeartbeat: // 处理心跳
		r.handleHeartbeat(m)
	case pb.MessageType_MsgBeat: // 发送心跳
		for id := range r.Prs {
			if id == r.id { // 跳过自己
				continue
			}
			r.sendHeartbeat(id)
		}
		r.heartbeatElapsed = 0
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend:
		if m.Term >= r.Term { // 你的任期大，那么我变成追随者
			r.becomeFollower(m.Term, r.Term)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgHup: // start a new election
		r.startElection()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVote(m) // 处理选票响应
	case pb.MessageType_MsgRequestVote: // 发送投票
		r.sendVote(m)
	case pb.MessageType_MsgHeartbeat: // 处理心跳
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgAppend: // 追加日志
		if m.Term >= r.Term { // 你的任期大，那么我变成追随者
			r.becomeFollower(m.Term, r.Term)
			r.handleAppendEntries(m)
		}
	case pb.MessageType_MsgHup: // start a new election
		r.startElection()
	case pb.MessageType_MsgRequestVote:
		r.sendVote(m)
	case pb.MessageType_MsgHeartbeat: // 处理心跳
		r.handleHeartbeat(m)
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// TODO
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	// TODO
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

// startElection start a new election when heartbeat timeout
func (r *Raft) startElection() {
	// 2A 发起选举
	r.becomeCandidate()    // 切换为candidate
	r.heartbeatElapsed = 0 // 心跳清0
	r.electionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		log.Panicf("raft log term err: %s", err)
	}

	for id := range r.Prs {
		if id == r.id { // 跳过自己
			continue
		}
		r.sendVote(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote, // 请求投票
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: lastLogTerm,
			Index:   lastIndex,
		})
	}
}

// sendVote send vote RPC
func (r *Raft) sendVote(m pb.Message) {
	// 2A 发送投票
	r.msgs = append(r.msgs, m)
}

// handleVote handle vote RPC
func (r *Raft) handleVote(m pb.Message) {
	if r.State != StateCandidate {
		log.Debugf("peer: %d state changed, and refuse %d vote response: %v", r.id, m.From, m)
		return
	}
	// 任期小的选票不需要
	if m.Term != None && m.Term < r.Term {
		log.Debugf("peer: %d term changed, and refuse %d vote response: %v", r.id, m.From, m)
		return
	}
	r.votes[m.From] = !m.Reject // 是否投票
	num := 0
	for _, b := range r.votes {
		if b {
			num++
		}
	}
	// 得到一半以上的选票，直接成为leader
	if num > len(r.Prs)/2 {
		log.Debugf("peer: %d win election and become leader", r.id)
		r.becomeLeader()
	}
}

func (r *Raft) softState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
