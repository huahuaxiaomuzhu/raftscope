/* jshint globalstrict: true */
/* jshint browser: true */
/* jshint devel: true */
/* jshint jquery: true */
/* global util */
'use strict';

var raft = {};
var RPC_TIMEOUT = 250000;
var MIN_RPC_LATENCY = 10000;
var MAX_RPC_LATENCY = 15000;
var ELECTION_TIMEOUT = 100000;
var NUM_SERVERS = 5;
//一次RPC能够传输的日志条数
var BATCH_SIZE = 3;
//model是全局状态，用来模拟时间
(function() {
//模拟发送消息,加入了随机延迟
  var sendMessage = function(model, message) {
    message.sendTime = model.time;
    message.recvTime = model.time +
        MIN_RPC_LATENCY +
        Math.random() * (MAX_RPC_LATENCY - MIN_RPC_LATENCY); //在min~max中随机
    //TODO 加入消息丢失的模拟
    if(Math.random()>0.05){
      model.messages.push(message);
      // model.debugLogs.unshift(`${JSON.stringify(message)} transferred successfully`)
      if (model.debugLogs.length>9){
        model.debugLogs.splice(-1,1);
      }
    }else{
      model.messages.push({});
      model.debugLogs.unshift(`${message.type} ${message.direction} from ${message.from} to ${message.to}  missed!`)
      if (model.debugLogs.length>9){
        model.debugLogs.splice(-1,1);
      }

    }
  };

  var sendRequest = function(model, request) {
    request.direction = 'request';
    sendMessage(model, request);
  };
//响应一次request
  var sendReply = function(model, request, reply) {
    reply.from = request.to;
    reply.to = request.from;
    reply.type = request.type;
    reply.direction = 'reply';
    sendMessage(model, reply);
  };

  var logTerm = function(log, index) {
    if (index < 1 || index > log.length) {
      return 0;
    } else {
      return log[index - 1].term;
    }
  };

  var rules = {};
  raft.rules = rules;
  //开始一轮从now开始，持续1～2个Election TIMEOUT的选举
  var makeElectionAlarm = function(now) {
    return now + (Math.random() + 1) * ELECTION_TIMEOUT;
  };

  raft.server = function(id, peers) {
    return {
      id: id,
      peers: peers, //除了我以外的其他节点
      state: 'follower',
      term: 0, //任期，从1开始
      votedFor: null, //当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空
      log: [], //日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1）
      commitIndex: 0, //已知已提交的最高的日志条目的索引（初始值为0，单调递增）
      electionAlarm: makeElectionAlarm(0), //选举计时器
      voteGranted:  util.makeMap(peers, false), //是否收到了m[peer]的投票
      matchIndex:   util.makeMap(peers, 0), //只有当我是领导人的时候才有效
      nextIndex:    util.makeMap(peers, 1), //只有当我是领导人的时候才有效
      rpcDue:       util.makeMap(peers, 0), //对每个节点的rpc超时计时器
      heartbeatDue: util.makeMap(peers, 0), //对每个节点的心跳超时计时器
      counter: 0, //用于实现increase and query
    };
  };
  //如果接收到的 RPC 请求或响应中，任期号T > currentTerm，则令 currentTerm = T，并切换为跟随者状态（5.1 节）
  var stepDown = function(model, server, term) {
    server.term = term;
    server.state = 'follower';
    server.votedFor = null;
    if (server.electionAlarm <= model.time || server.electionAlarm == util.Inf) {
      server.electionAlarm = makeElectionAlarm(model.time);
      model.debugLogs.unshift(`${server.id} step down`);
      /*
      （作为一个追随者）如果在超过选举超时时间的情况之前没有收到当前领导人（
      即该领导人的任期需与这个跟随者的当前任期相同）的心跳/附加日志，
      或者是给某个候选人投了票，就自己变成候选人
       */
    }
  };
  //开始一轮选举，不包括发送拉票请求
  rules.startNewElection = function(model, server) {
    if ((server.state == 'follower' || server.state == 'candidate') &&
        server.electionAlarm <= model.time) {
      model.debugLogs.unshift(`${server.id} started election at ${model.time}`);
      server.electionAlarm = makeElectionAlarm(model.time);
      server.term += 1;
      server.votedFor = server.id;
      server.state = 'candidate';
      server.voteGranted  = util.makeMap(server.peers, false); //作为候选人，不投给任何人
      server.matchIndex   = util.makeMap(server.peers, 0);//对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
      server.nextIndex    = util.makeMap(server.peers, 1);//初始值为领导人最后的日志条目的索引+1?这条满足了吗
      server.rpcDue       = util.makeMap(server.peers, 0);
      server.heartbeatDue = util.makeMap(server.peers, 0);
    }
  };
  //server向peer发送拉票请求
  rules.sendRequestVote = function(model, server, peer) {
    if (server.state == 'candidate' &&
        server.rpcDue[peer] <= model.time) {
      model.debugLogs.unshift(`${server.id} want ${peer} to vote for him`);
      server.rpcDue[peer] = model.time + RPC_TIMEOUT; //重置RPC超时计时器
      sendRequest(model, {
        from: server.id,
        to: peer,
        type: 'RequestVote',
        term: server.term,
        lastLogTerm: logTerm(server.log, server.log.length),
        lastLogIndex: server.log.length}); //发送拉票请求
    }
  };
  //server上台
  rules.becomeLeader = function(model, server) {
    if (server.state == 'candidate' &&
        util.countTrue(util.mapValues(server.voteGranted)) + 1 > Math.floor(NUM_SERVERS / 2)) {
      //拿到了超过半数的票
      //!!如果掉线节点大于全部节点的一半的话就永远无法选出leader，所以raft在极限情况下只能容忍半数节点宕机
      //console.log('server ' + server.id + ' is leader in term ' + server.term);
      model.debugLogs.unshift(`${server.id} become new leader`)
      server.state = 'leader';
      server.nextIndex    = util.makeMap(server.peers, server.log.length + 1);
      server.rpcDue       = util.makeMap(server.peers, util.Inf);
      server.heartbeatDue = util.makeMap(server.peers, 0);
      server.electionAlarm = util.Inf;
    }
  };
  //AppendEntries RPC 用于日志条目的复制，同时也被当做心跳使用 server:领导人 peer:追随者 model:全局模型
  rules.sendAppendEntries = function(model, server, peer) {
    if (server.state === 'leader' && //必须由领导人调用
        (server.heartbeatDue[peer] <= model.time || //server和peer的心跳连接已经断开
            (server.nextIndex[peer] <= server.log.length && //server有需要复制到peer的log
                server.rpcDue[peer] <= model.time))) {  //server和peer的RPC计时器没有超时

      //心跳还连着或者心跳断了RPC计时器没断，就还发送
      var prevIndex = server.nextIndex[peer] - 1;
      var lastIndex = Math.min(prevIndex + BATCH_SIZE,
          server.log.length);
      if (server.matchIndex[peer] + 1 < server.nextIndex[peer])
        lastIndex = prevIndex;
      sendRequest(model, {
        from: server.id,
        to: peer,
        type: 'AppendEntries',
        term: server.term,
        prevIndex: prevIndex,
        prevTerm: logTerm(server.log, prevIndex),
        entries: server.log.slice(prevIndex, lastIndex),
        commitIndex: Math.min(server.commitIndex, lastIndex)});
      server.rpcDue[peer] = model.time + RPC_TIMEOUT;//刷新RPC计时器
      server.heartbeatDue[peer] = model.time + ELECTION_TIMEOUT / 2; //刷新心跳计时器
    }
  };
  //
  rules.advanceCommitIndex = function(model, server) {
    var matchIndexes = util.mapValues(server.matchIndex).concat(server.log.length);
    matchIndexes.sort(util.numericCompare);
    var n = matchIndexes[Math.floor(NUM_SERVERS / 2)];
    if (server.state == 'leader' &&
        logTerm(server.log, n) == server.term) {
      server.commitIndex = Math.max(server.commitIndex, n);
    }
  };
  //处理拉票请求
  var handleRequestVoteRequest = function(model, server, request) {
    if (server.term < request.term)
      stepDown(model, server, request.term);
    var granted = false;
    if (server.term == request.term &&
        (server.votedFor === null ||
            server.votedFor == request.from) &&
        (request.lastLogTerm > logTerm(server.log, server.log.length) ||
            (request.lastLogTerm == logTerm(server.log, server.log.length) &&
                request.lastLogIndex >= server.log.length))) {
      granted = true;
      server.votedFor = request.from;
      server.electionAlarm = makeElectionAlarm(model.time);
    }
    sendReply(model, request, {
      term: server.term,
      granted: granted,
    });
  };
  //处理拉票响应
  var handleRequestVoteReply = function(model, server, reply) {
    if (server.term < reply.term)
      stepDown(model, server, reply.term);
    //如果接收到的 RPC 请求或响应中，任期号T > currentTerm，则令 currentTerm = T，并切换为跟随者状态（5.1 节）
    if (server.state == 'candidate' &&
        server.term == reply.term) {
      server.rpcDue[reply.from] = util.Inf; //收到reply代表的选票结果,暂时不允许RPC超时
      server.voteGranted[reply.from] = reply.granted;
    }
  };

  var handleAppendEntriesRequest = function(model, server, request) {
    var success = false;
    var matchIndex = 0;
    if (server.term < request.term)
      stepDown(model, server, request.term);
    if (server.term == request.term) {
      server.state = 'follower';
      server.electionAlarm = makeElectionAlarm(model.time);
      if (request.prevIndex === 0 ||
          (request.prevIndex <= server.log.length &&
              logTerm(server.log, request.prevIndex) == request.prevTerm)) {
        success = true;
        var index = request.prevIndex;
        for (var i = 0; i < request.entries.length; i += 1) {
          index += 1;
          if (logTerm(server.log, index) != request.entries[i].term) {
            while (server.log.length > index - 1)
              server.log.pop();
            server.log.push(request.entries[i]);
          }
        }
        matchIndex = index;
        server.commitIndex = Math.max(server.commitIndex,
            request.commitIndex);
      }
    }
    server.counter=server.log.reduce((previousValue, currentValue, currentIndex, array)=>{
      if(currentValue.value==='i'){
        return previousValue+1;
      }else {
        return previousValue;
      }
    },0);
    sendReply(model, request, {
      term: server.term,
      success: success,
      matchIndex: matchIndex,
    });
  };

  var handleAppendEntriesReply = function(model, server, reply) {
    if (server.term < reply.term)
      stepDown(model, server, reply.term);
    if (server.state == 'leader' &&
        server.term == reply.term) {
      if (reply.success) {
        server.matchIndex[reply.from] = Math.max(server.matchIndex[reply.from],
            reply.matchIndex);
        server.nextIndex[reply.from] = reply.matchIndex + 1;
      } else {
        server.nextIndex[reply.from] = Math.max(1, server.nextIndex[reply.from] - 1);
      }
      server.rpcDue[reply.from] = 0;
    }
  };

  var handleMessage = function(model, server, message) {
    if (server.state == 'stopped')
      return;
    if (message.type == 'RequestVote') {
      if (message.direction == 'request')
        handleRequestVoteRequest(model, server, message);
      else
        handleRequestVoteReply(model, server, message);
    } else if (message.type == 'AppendEntries') {
      if (message.direction == 'request')
        handleAppendEntriesRequest(model, server, message);
      else
        handleAppendEntriesReply(model, server, message);
    }
  };


  raft.update = function(model) {
    model.servers.forEach(function(server) {
      rules.startNewElection(model, server);
      rules.becomeLeader(model, server);
      rules.advanceCommitIndex(model, server);
      server.peers.forEach(function(peer) {
        rules.sendRequestVote(model, server, peer);
        rules.sendAppendEntries(model, server, peer);
      });
    });
    var deliver = [];
    var keep = [];
    model.messages.forEach(function(message) {
      if (message.recvTime <= model.time)
        deliver.push(message);
      else if (message.recvTime < util.Inf)
        keep.push(message);
    });
    model.messages = keep;
    deliver.forEach(function(message) {
      model.servers.forEach(function(server) {
        if (server.id == message.to) {
          handleMessage(model, server, message);
        }
      });
    });
  };

  raft.stop = function(model, server) {
    server.state = 'stopped';
    server.electionAlarm = 0;
  };

  raft.resume = function(model, server) {
    server.state = 'follower';
    server.electionAlarm = makeElectionAlarm(model.time);
  };

  raft.resumeAll = function(model) {
    model.servers.forEach(function(server) {
      raft.resume(model, server);
    });
  };

  raft.restart = function(model, server) {
    raft.stop(model, server);
    raft.resume(model, server);
  };

  raft.drop = function(model, message) {
    model.messages = model.messages.filter(function(m) {
      return m !== message;
    });
  };

  raft.timeout = function(model, server) {
    server.state = 'follower';
    server.electionAlarm = 0;
    rules.startNewElection(model, server);
  };

  raft.clientRequest = function(model, server) {
    if (server.state==='follower'){
      if (server.term===0){
        model.debugLogs.unshift(`system has not elected a leader yet,please wait`);
        return;
      }
    }
    if (server.state == 'leader') {
      model.debugLogs.unshift(`leader ${server.id} received client request`);
      server.log.push({term: server.term,
        value: 'v'});
    }else{
      model.debugLogs.unshift(`follower ${server.id} received client request,redirecting to leader ${server.votedFor} `);
      model.servers[server.votedFor-1].log.push({term: server.term,
        value: 'v'})
    }
  };
  raft.increase=function(model, server) {
    if (server.state==='follower'){
      if (server.term===0){
        model.debugLogs.unshift(`system has not elected a leader yet,please wait`);
        return;
      }
    }
    if (server.state == 'leader') {
      model.debugLogs.unshift(`leader ${server.id} received client request`);
      server.log.push({term: server.term,
        value: 'i'});
    }else{
      model.debugLogs.unshift(`follower ${server.id} received client request,redirecting to leader ${server.votedFor} `);
      model.servers[server.votedFor-1].log.push({term: server.term,
        value: 'i'})
    }
  };
  raft.query=function(model, server) {
    server.counter=server.log.reduce((previousValue, currentValue, currentIndex, array)=>{
      if(currentValue.value==='i'){
        return previousValue+1;
      }else{
        return previousValue;
      }
    },0);
    model.debugLogs.unshift(`${server.id}'s counter is ${server.counter}`);
  };
  raft.spreadTimers = function(model) {
    var timers = [];
    model.servers.forEach(function(server) {
      if (server.electionAlarm > model.time &&
          server.electionAlarm < util.Inf) {
        timers.push(server.electionAlarm);
      }
    });
    timers.sort(util.numericCompare);
    if (timers.length > 1 &&
        timers[1] - timers[0] < MAX_RPC_LATENCY) {
      if (timers[0] > model.time + MAX_RPC_LATENCY) {
        model.servers.forEach(function(server) {
          if (server.electionAlarm == timers[0]) {
            server.electionAlarm -= MAX_RPC_LATENCY;
            console.log('adjusted S' + server.id + ' timeout forward');
          }
        });
      } else {
        model.servers.forEach(function(server) {
          if (server.electionAlarm > timers[0] &&
              server.electionAlarm < timers[0] + MAX_RPC_LATENCY) {
            server.electionAlarm += MAX_RPC_LATENCY;
            console.log('adjusted S' + server.id + ' timeout backward');
          }
        });
      }
    }
  };

  raft.alignTimers = function(model) {
    raft.spreadTimers(model);
    var timers = [];
    model.servers.forEach(function(server) {
      if (server.electionAlarm > model.time &&
          server.electionAlarm < util.Inf) {
        timers.push(server.electionAlarm);
      }
    });
    timers.sort(util.numericCompare);
    model.servers.forEach(function(server) {
      if (server.electionAlarm == timers[1]) {
        server.electionAlarm = timers[0];
        console.log('adjusted S' + server.id + ' timeout forward');
      }
    });
  };

  raft.setupLogReplicationScenario = function(model) {
    var s1 = model.servers[0];
    raft.restart(model, model.servers[1]);
    raft.restart(model, model.servers[2]);
    raft.restart(model, model.servers[3]);
    raft.restart(model, model.servers[4]);
    raft.timeout(model, model.servers[0]);
    rules.startNewElection(model, s1);
    model.servers[1].term = 2;
    model.servers[2].term = 2;
    model.servers[3].term = 2;
    model.servers[4].term = 2;
    model.servers[1].votedFor = 1;
    model.servers[2].votedFor = 1;
    model.servers[3].votedFor = 1;
    model.servers[4].votedFor = 1;
    s1.voteGranted = util.makeMap(s1.peers, true);
    raft.stop(model, model.servers[2]);
    raft.stop(model, model.servers[3]);
    raft.stop(model, model.servers[4]);
    rules.becomeLeader(model, s1);
    raft.clientRequest(model, s1);
    raft.clientRequest(model, s1);
    raft.clientRequest(model, s1);
  };

})();
