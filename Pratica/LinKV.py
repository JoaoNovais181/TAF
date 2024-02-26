#!/usr/bin/env python

import logging
from concurrent.futures import ThreadPoolExecutor
from ms import send, receiveAll, reply, exitOnError
from threading import Lock

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

data = {}
requests = []
waitingR, waitingW, waitingU = False, False, False

quorumSizeR, quorumSizeW = len(node_ids), 1

acks = []
timestamp = 0

def handle(msg):

    # State
    global node_id, node_ids, data, requests, waiting, timestamp, acks, quorumSizeR, quorumSizeW

    timestamp += 1

    def change(key, value):
        data[key] = value
    
    def read(key):
        value = (key in data.keys() and data[key]) or None

        return value

    def writeQuorum(key):
        waitingW = True
        acks.append(timestamp)
        for node in node_ids:
            if node != node_id:
                send(src=node_id, dest=node, type="quorumw", key=key)
    
    def readQuorum(key):
        waitingR = True
        val = (None, -1, node_id())
        if key in data.keys():
            val = (data[key][0], data[key][1], node_id())
        acks.append(val)
        for node in node_ids:
            if node != node_id:
                send(src=node_id, dest=node, type="quorumr", key=key)

    def update(key, value):
        for node in node_ids:
            if node != node_id:
                send(src=node_id, dest=node, type="update", key=key, value=value)
    
    def waitingHandler(msg):
        if waitingR and msg.body.type == "ackr":
            acks.append((msg.body.value, msg.body.timestamp, msg.src))
            acks.sort(key=lambda val: val[2])
            acks.sort(key=lambda val: val[1], reverse=True)

            if len(acks) > quorumSizeR:
                returnVal = acks[0]
                change(requests[0].body.key, (returnVal[0], returnVal[1]))

                if returnVal != -1:
                    reply(requests[0], type="read_ok", value=returnVal[0])
                else:
                    reply(requests[0], type="error", code=20, text=f"Key {requests[0].body.key} not found..")

                requests.pop(0)
                acks.clear()
                waitingR = False
    
        elif waitingW and msg.body.type == "ackw":
            acks.append(msg.body.timestamp)
            acks.sort()

            if len(acks) > quorumSizeW:
                tim = max(acks)
                timestamp = tim + 1
                change(requests[0].body.key, (requests[0].body.value, timestamp))



        else:
            requests.append(msg)

    def clientMsgHandler():
        msg = requests[0]

        # Message handlers
        if msg.body.type == 'init':
            node_id = msg.body.node_id
            node_ids = msg.body.node_ids
            logging.info('node %s initialized', node_id)

            reply(msg, type='init_ok')
        elif msg.body.type == 'read':
            readQuorum(msg.body.key)
        elif msg.body.type == 'write':

            change(key, value)
            reply(msg, type="write_ok")
            propagate(key, value)
        elif msg.body.type == 'cas':
            key = msg.body.key
            fromValue = getattr(msg.body, 'from')
            to = msg.body.to

            if key not in data.keys():
                reply(msg, type="error", code=20, text=f"Key {key} not found..")
            elif data[key] == fromValue:
                # data[key] = to
                change(key, to)
                reply(msg, type="cas_ok")
                propagate(key, to)
            else:
                reply(msg, type="error", code=22, text=f"Value of key {key} is not equal to {fromValue}..")
        else:
            logging.warning('unknown message type %s', msg.body.type)

    def serverMsgHandler(msg):

        if msg.body.type == 'update':

    
    if waitingW or waitingR:
        waitingHandler(msg)
    else:
        if msg.src in node_ids():
            serverMsgHandler(msg)
        else
            requests.append(msg)
            normalHandler()

# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

# schedule deferred work with:
# executor.submit(exitOnError, myTask, args...)

# schedule a timeout with:
# from threading import Timer
# Timer(seconds, lambda: executor.submit(exitOnError, myTimeout, args...)).start()

# exitOnError is always optional, but useful for debugging



### TODO
# 1- esperar pelo update
# 2- esperar por ackw
# 3- esperar por acku
# 4- lidar msg de servidor
# 5- enviar msg de update
