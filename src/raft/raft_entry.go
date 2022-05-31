package raft

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// raft log管理

// log结构
type LogEntry struct {
	Term    int         // 任期
	Command interface{} // client发送的命令
}

// 日志请求结构
type AppendEntriesArgs struct {
	Term int //leader’s term
	// 在raft中，有可能client直接连follower，此时follower
	// 需要给client发送leaderId，方便follower重定向
	LeaderId     int        // so follower can redirect clients
	PreLogIndex  int        // index of log entry immediately preceding new ones
	PreLogTerm   int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// 日志复制响应结构
type AppendEntriesReply struct {
	CurrentTerm int  // currentTerm, for leader to update itself
	Success     bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// 自定义相关冲突变量
	ConflictTerm int // 冲突日志的任期号
	FirstIndex   int // 存储第一个冲突编号的日志索引
}

// 日志应用进程
func (rf *Raft) applyEntryDaemon() {
	// 日志提交完成后
	// 将日志进行应用,返回
	for {
		rf.mu.Lock()
		var logs []LogEntry
		// 判断，如果节点最后的应用日志索引与已提交的日志索引相等
		// 说明已提交的日志都已经被应用
		for rf.lastApplied == rf.commitIndex {
			rf.commitCond.Wait()
			select {
			case <-rf.shutdown:
				// 检查是否有中断指令
				close(rf.applyCh)
				rf.mu.Unlock()
				return
			default:

			}
		}

		// 获取最后的应用日志索引与最新提交的日志索引
		last, cur := rf.lastApplied, rf.commitIndex
		if last < cur {
			// 满足条件说明还有一部分已经被提交的日志还没应用
			rf.lastApplied = cur
			// 找到已提交但还没应用的部分日志
			logs = make([]LogEntry, cur - last)
			copy(logs, rf.Logs[last + 1:cur + 1])
		}
		rf.mu.Unlock()

		// 对还没被应用的日志进行应用
		for i := 0; i < cur-last; i++ {
			reply := ApplyMsg{
				Index:       last + i + 1,
				Command:     logs[i].Command,
			}
			// 传回响应
			rf.applyCh <- reply
		}
	}
}

// 唤醒一致性检查
func (rf *Raft) wakeupConsistencyCheck() {
	peersLen := len(rf.peers)
	for i := 0; i < peersLen; i++ {
		if i == rf.me {
			continue
		}
		rf.newEntryCond[i].Broadcast()
	}
}

// 启动日志复制进程
func (rf *Raft) logEntryAgreeDaemon() {
	peersLen := len(rf.peers)
	// 遍历节点 向其他节点发起日志复制操作
	for i := 0; i < peersLen; i++ {
		if i == rf.me {
			continue
		}
		go rf.consistencyCheckDaemon(i)
	}
}

func (rf *Raft) consistencyCheckDaemon(n int) {
	for {
		rf.mu.Lock()
		// 每个节点都在等待client提交命令到leader上去
		rf.newEntryCond[n].Wait()

		select {
		case <-rf.shutdown:
			rf.mu.Unlock()
			return
		default:

		}
		if !rf.isLeader {
			// 当前节点不是leader 直接返回
			rf.mu.Unlock()
			return
		}
		// 判断节点角色，只有leader才能发起日志复制
		var args AppendEntriesArgs
		args.Term = rf.CurrentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PreLogIndex = rf.nextIndex[n] - 1
		args.PreLogTerm = rf.Logs[args.PreLogIndex].Term

		// 判断是否有新的日志进来
		// len(rf.logs): leader当前的日志总数
		// rf.nextIndex[n]: leader要发送给节点n的下一个日志索引
		// leader的日志长度大于leader所知道的follower n的日志长度
		if rf.nextIndex[n] < len(rf.Logs) {
			// 添加新日志
			args.Entries = append(args.Entries, rf.Logs[rf.nextIndex[n]:]...)
		} else {
			args.Entries = nil
		}
		rf.mu.Unlock()

		replyCh := make(chan AppendEntriesReply, 1)
		go func() {
			var reply AppendEntriesReply
			// 发起日志复制请求
			if ok := rf.sendAppenEntries(n, &args, &reply); ok {
				replyCh <- reply
			}
		}()

		// 获取响应
		select {
		case reply := <-replyCh:
			rf.mu.Lock()
			if !reply.Success {
				// 响应失败
				// 判断响应传回的term与当前节点(leader)的term值大
				if reply.CurrentTerm > rf.CurrentTerm {
					// 更新当前leader的任期
					rf.CurrentTerm = reply.CurrentTerm
					rf.Votedfor = -1
				}

				// leader接收的是每个节点的响应
				// 所以在进行当前判断的时候 可能角色状态已经变化
				if rf.isLeader {
					rf.isLeader = false
					// 变更角色转态 发起一致性检查
					rf.wakeupConsistencyCheck()
				}
				rf.mu.Unlock()

				// 重置选举超时
				rf.resetTimer <- struct{}{}
				return
			}

			// 响应成功
			rf.matchIndex[n] = reply.FirstIndex
			rf.nextIndex[n] = rf.matchIndex[n] + 1

			// 提交日志 更新已提交的日志索引
			rf.updateCommiteIndex()

			// 解决冲突相关问题
			if reply.ConflictTerm == 0 {
				// 如果响应中没有返回冲突的任期编号
				rf.nextIndex[n] = reply.FirstIndex
			} else {
				// know:当前leader能否找到冲突
				// lastIndex: 代表节点中最后一个包含冲突任期编号的日志索引
				know, lastIndex := false, 0
				// 查找最后一个包含冲突的日志索引
				logLen := len(rf.Logs)
				for i := logLen - 1; i > 0; i-- {
					// 找到产生冲突的任期号
					if rf.Logs[i].Term == reply.ConflictTerm {
						know = true
						lastIndex = i
						break
					}
				}

				// 如果找到了冲突任期号
				if know {
					// 判断当前获取到的冲突日志编号索引与响应中的第一个冲突日志索引的大小
					if lastIndex > reply.FirstIndex {
						// 满足当前条件的情况下，说明
						// 在最后一个产生前，已经有了一个冲突编号
						// 只允许存在一个
						lastIndex = reply.FirstIndex
					}
					rf.nextIndex[n] = lastIndex
				} else {
					rf.nextIndex[n] = reply.FirstIndex
				}
			}

			// 1 <= rf.nextIndex[n] <= len(rf.logs)
			rf.nextIndex[n] = min(max(1, rf.nextIndex[n]), len(rf.Logs))
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendAppenEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 更新已提交的日志索引
func (rf *Raft) updateCommiteIndex() {

}

// 接收到日志复制请求后的处理
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply)  {

}
