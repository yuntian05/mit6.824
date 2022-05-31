package raft

import (
	"sync"
	"time"
)

// raft 选举leader

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// 投票请求参数
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// 投票响应参数
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

//
// example RequestVote RPC handler.
// 接收到投票请求后 进行处理
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 当前节点最后一个日志的索引
	lastLogIndex := len(rf.Logs) - 1
	// 当前节点最后一个日志的任期
	lastLogTerm := rf.Logs[lastLogIndex].Term

	// 判断leader的任期号与自己的谁大
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.CurrentTerm {
			// 将当前节点变更为follwer
			rf.CurrentTerm = args.Term
			rf.isLeader = false
			rf.Votedfor = -1
		}

		// If 当前节点 votedFor is null or candidateId, and candidate’s log
		// is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
		if rf.Votedfor == -1 || rf.Votedfor == args.CandidateId {
			// 候选人日志是否和自己的最后一个日志一样新
			if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex ||
				args.LastLogTerm > lastLogTerm {
				rf.resetTimer <- struct{}{}
				// 更改自己这个节点的状态
				rf.isLeader = false
				rf.Votedfor = args.CandidateId
				// 投票
				//reply.Term = rf.CurrentTerm
				reply.VoteGranted = true
			}
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 启动选举进程
func (rf *Raft) electionDaemon() {
	for {
		select {
		// 接收到重置请求后的处理
		case <-rf.resetTimer:
			if !rf.electionTimer.Stop() {
				// 发送超时
				<-rf.electionTimer.C
			}

			// 重置超时选举
			rf.electionTimer.Reset(rf.electionTimeout)
		case <-rf.electionTimer.C:
			// 超时, 也就是说follwer在指定时间内没有
			// 收到来自leader的信息, 就自己变成candidate
			// 向其他节点发起投票请求
			go rf.canVotes()
			// 重置超时选举
			rf.electionTimer.Reset(rf.electionTimeout)
		}
	}
}

func (rf *Raft) fillRequestVoteArgs(args *RequestVoteArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 任期号+1
	rf.CurrentTerm += 1
	// 投票给自己
	rf.Votedfor = rf.me

	args.Term = rf.CurrentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.Logs) - 1
	args.LastLogTerm = rf.Logs[args.LastLogIndex].Term
}

// 发起投票请求
func (rf *Raft) canVotes() {
	// 请求参数
	var voteArgs RequestVoteArgs
	rf.fillRequestVoteArgs(&voteArgs)

	peersLen := len(rf.peers)
	// 设置缓存channel，大小为peersLen
	replyCh := make(chan RequestVoteReply, peersLen)

	// 正式发起投票
	var wg sync.WaitGroup
	for i := 0; i < peersLen; i++ {
		if i == rf.me {
			rf.resetTimer <- struct{}{}
			continue
		}
		wg.Add(1)
		// 对每个节点发起投票
		go func(n int) {
			defer wg.Done()
			var reply RequestVoteReply

			// 投票RPC结果
			doneCh := make(chan bool)
			go func() {
				ok := rf.sendRequestVote(n, &voteArgs, &reply)
				// 将请求结果放入doneCh中
				doneCh <- ok
			}()

			select {
			case ok := <-doneCh:
				if !ok {
					return
				}
				//响应的结果存入replyCh
				replyCh <- reply
			}
		}(i)
	}

	// 另起一个协程关闭结果通道
	go func() {
		wg.Wait()
		close(replyCh)
	}()

	// 统计票数结果，自己会给自己投一票，所以初始值为1
	votes := 1
	// 遍历缓存通道，获取每一个响应的投票结果
	for reply := range replyCh {
		if reply.VoteGranted == true {
			// 得到了当前节点的投票
			votes++
			// 得到了半数以上的票
			if votes > peersLen/2 {
				rf.mu.Lock()
				// 成功当选leader
				rf.isLeader = true
				rf.mu.Unlock()

				// 重置相关状态
				rf.resetOnElection()

				// 发起心跳，防止其他的follower变为candidate
				go rf.heartbeatDaemon()

				// 当选leader后 发起日志复制操作
				go rf.logEntryAgreeDaemon()

				return
			}
		} else if reply.Term > rf.CurrentTerm {
			// Reply false if term < currentTerm (§5.1)
			// reply.Term:follwer 的任期
			// rf.CurrentTerm: 竞选leader的candidate的任期
			// 改变状态 重新变为follower
			rf.mu.Lock()
			rf.CurrentTerm = reply.Term
			rf.isLeader = false
			rf.Votedfor = -1
			rf.mu.Unlock()
			rf.resetTimer <- struct{}{}
			return
		}
	}
}

func (rf *Raft) heartbeatDaemon() {
	for {
		if _, isleader := rf.GetState(); isleader {
			// 只要是leader，就可以不断重置选举超时
			rf.resetTimer <- struct{}{}
		} else {
			break
		}

		// 设置心跳超时
		time.Sleep(rf.heartBeatTimeout)
	}
}

// When a leader ﬁrst comes to power, it initializes
//all nextIndex values to the index just after the
//last one in its log (11 in Figure 7)
func (rf *Raft) resetOnElection()  {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 节点数量
	peersLen := len(rf.peers)
	// 日志长度 恰好是最后一个日志的index加1
	logLen := len(rf.Logs)
	for i := 0; i < peersLen; i++ {
		rf.nextIndex[i] = logLen
		rf.matchIndex[i] = 0
		if i == rf.me {
			rf.matchIndex[i] = logLen - 1
		}
	}
}
