package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"studygo/6.824/src/labrpc"
	"sync"
	"time"
)

//import "labrpc"

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// 自定义属性
	isLeader         bool          // 查看当前节点是否是一个leader
	resetTimer       chan struct{} // 重置选举超时
	electionTimer    *time.Timer   // 选举超时实例
	electionTimeout  time.Duration // 选举超时时间
	heartBeatTimeout time.Duration // 心跳超时时间

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	CurrentTerm  int          // 当前的任期
	Votedfor     int          // 投票给谁
	Logs         []LogEntry   // 日志结构
	commitCond   *sync.Cond   // 主要用于提交日志索引更新通知
	newEntryCond []*sync.Cond // 唤醒每个节点一致性检查

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed
	// (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine
	// (initialized to 0, increases monotonically)

	// Volatile state on leaders:
	//(Reinitialized after election)
	nextIndex []int // for each server, index of the next log entry
	// to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry
	// known to be replicated on server (initialized to 0, increases monotonically)

	applyCh  chan ApplyMsg // 日志提交成功后 向服务发起的应用消息
	shutdown chan struct{} // 中断
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	isleader = rf.isLeader
	return term, isleader
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := 0
	isLeader := false

	select {
	case <-rf.shutdown:
		return index, term, isLeader
	default:

	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
	// 判断当前节点是否是leader 如果是leader 从client中添加日志
	if rf.isLeader {
		log := LogEntry{
			Term:    rf.CurrentTerm,
			Command: command,
		}
		rf.Logs = append(rf.Logs, log)
		// 获取当前最后的日志索引
		index = len(rf.Logs) - 1
		term = rf.CurrentTerm
		isLeader = true

		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index

		// client日志添加到leader成功
		rf.wakeupConsistencyCheck()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// 所有节点初始化都是follwer
	rf.isLeader = false
	rf.Votedfor = -1

	// 所有节点数量
	peersLen := len(peers)

	rf.nextIndex = make([]int, peersLen)
	rf.matchIndex = make([]int, peersLen)

	rf.Logs = make([]LogEntry, 1)
	// commit cond
	rf.commitCond = sync.NewCond(&rf.mu)

	rf.newEntryCond = make([]*sync.Cond, peersLen)
	for i := 0; i < peersLen; i++ {
		rf.newEntryCond[i] = sync.NewCond(&rf.mu)
	}
	rf.shutdown = make(chan struct{})

	// 给一个空的
	rf.Logs[0] = LogEntry{
		Term:    0,
		Command: nil,
	}

	for i := 0; i < len(peers); i++ {
		// 初始化为leader最后索引值加1
		// 也就是leader的日志长度
		rf.nextIndex[i] = len(rf.Logs)
	}

	// 初始化重置超时选举
	rf.resetTimer = make(chan struct{})

	// 设置超时选举时间 (注意rand的作用)
	rf.electionTimeout = time.Millisecond *
		(400 + time.Duration(rand.Int63()&400))
	rf.electionTimer = time.NewTimer(rf.electionTimeout)

	// 设置心跳超时时间
	rf.heartBeatTimeout = time.Millisecond * 200
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 启动选举
	go rf.electionDaemon()

	// 日志应用进程
	go rf.applyEntryDaemon()

	return rf
}
