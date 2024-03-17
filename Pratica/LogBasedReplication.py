#!/usr/bin/env python

# 'echo' workload in Python for Maelstrom
# with an addtional custom MyMsg message

import logging
from concurrent.futures import ThreadPoolExecutor
from ms import send, receiveAll, reply, exitOnError

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

isLeader, leader = False, None
log = []
commitIndex, lastApplied = 0, 0
data = {}

# leader state
nextIndex = {}
matchIndex = {}
sent = {}

def sendEntries(prevLogIndex, node):
    global node_id, log, nextIndex, waiting

    entries = log[prevLogIndex:nextIndex[node]]
    leaderCommit = commitIndex

    if len(entries) > 0:
        send(node_id, node, type='appendEntries', leaderId=node_id, prevLogIndex=prevLogIndex, entries=entries, leaderCommit=leaderCommit)

        sent[node] = True
    else:
        sent[node] = False

def apply(msg):
    global data

    if msg.body.type == 'write':
        key, value = msg.body.key, msg.body.value
        data[key] = value

        if isLeader:
            reply(msg, type='write_ok')

    elif msg.body.type == 'cas':
        key, fromVal, to = msg.body.key, getattr(msg.body, 'from'), msg.body.to

        if isLeader:
            if key not in data.keys():
                reply(msg, type='error', code=20, text=f"Key {key} not found...")
            elif data[key] != fromVal:
                reply(msg, type='error', code=22, text=f"Value of key is not equal to {fromVal}")
            else:
                data[key] = to
                reply(msg, type='cas_ok')
    elif msg.body.type == 'read' and isLeader:
        
        if msg.body.key in data.keys():
            reply(msg, type='read_ok', value=data[msg.body.key])
        else:
            reply(msg, type='error', code=20, text=f"Key {msg.body.key} not found..")

    logging.info(f"{data = }\n")
    
def handle(msg):
    # State
    global node_id, node_ids, isLeader, leader, log, nextIndex, matchIndex, commitIndex, lastApplied, waiting

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        if node_id == node_ids[0]:
            isLeader = True
        else:
            isLeader = False
            leader = node_ids[0]

        for n in node_ids:
            if n != node_id:
                nextIndex[n] = len(log) + 1
                matchIndex[n] = 0

        reply(msg, type='init_ok')
    elif msg.body.type in ['read', 'write', 'cas']:

        if not isLeader:
            reply(msg, type='error', code=11, text="Service Unavailable")
            return

        log.append(msg)
       
        if not any(sent.values()) :
            for node in node_ids:
                if node != node_id:
                    sendEntries(commitIndex, node)

    elif msg.body.type == 'appendEntries':

        prevLogIndex = msg.body.prevLogIndex

        if len(log) >= prevLogIndex:
            
            newEntries = msg.body.entries
            for entry in newEntries:
                if entry not in log:
                    log.append(entry)
                    apply(entry)
                    
                    lastApplied += 1

            if msg.body.leaderCommit > commitIndex:
                commitIndex = min(msg.body.leaderCommit, len(log))

            reply(msg, type='appendEntriesReply', success=True)
        else:
            reply(msg, type='appendEntriesReply', success=False)

    elif msg.body.type == 'appendEntriesReply':

        node = msg.src
        success = msg.body.success

        if success:
            matchIndex[node] = nextIndex[node]
            nextIndex[node] = len(log)
            
            sendEntries(matchIndex[node], node)
        else:
            nextIndex[node] -= 1

            sendEntries(matchIndex[node], node)

        applied = min(matchIndex.values())

        for i in range(lastApplied, applied):
            apply(log[i])
            
        
    else:
        logging.warning('unknown message type %s', msg.body.type)

# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

# schedule deferred work with:
# executor.submit(exitOnError, myTask, args...)

# schedule a timeout with:
# from threading import Timer
# Timer(seconds, lambda: executor.submit(exitOnError, myTimeout, args...)).start()

# exitOnError is always optional, but useful for debugging
