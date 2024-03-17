#!/usr/bin/env python

import logging
from concurrent.futures import ThreadPoolExecutor
from ms import send, receiveAll, reply, exitOnError
from threading import Lock

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

data = {}
requests = []
serverRequests = []
waitingR, waitingW, waitingU = False, False, False

quorumSizeR, quorumSizeW = 1, 0

acksR, acksW, acksU = [], [], []
timestamp = 0

def handle(msg):

    # State
    global node_id, node_ids, data, requests, waiting, timestamp, acksR, acksW, acksU, quorumSizeR, quorumSizeW, serverRequests
    logging.info(f"\n\n{data = }\n\n")

    timestamp += 1

    def change(key, value):
        data[key] = value
    
    def read(key):
        value = (key in data.keys() and data[key]) or None

        return value

    def writeQuorum(key):
        logging.info("EXECUTING WRITE QUORUM\n")
        acksW.append(timestamp)
        for node in node_ids:
            if node != node_id:
                send(src=node_id, dest=node, type="quorumw", key=key)
    
    def readQuorum(key):
        logging.info("EXECUTING READ QUORUM\n")
        val = ("None", -1, node_id)
        if key in data.keys():
            val = (data[key][0], data[key][1], node_id)
        acksR.append(val)
        for node in node_ids:
            if node != node_id:
                send(src=node_id, dest=node, type="quorumr", key=key)

    def update(key, value):
        logging.info("EXECUTING VALUE UPDATE\n")
        for node in node_ids:
            if node != node_id:
                send(src=node_id, dest=node, type="update", key=key, value=value, timestamp=timestamp)
    
    def waitingHandler(msg):
        global node_id, node_ids, data, requests, waitingW, waitingR, waitingU, timestamp, acksR, acksW, acksU, quorumSizeR, quorumSizeW
        logging.debug(f"\n\n{msg = }")
        if waitingR and msg.body.type == "ackr":
            acksR.append((msg.body.value, msg.body.timestamp, msg.src))
            logging.info(f"\n\n{acksR = }\n\n")
            acksR.sort(key=lambda val: val[2])
            acksR.sort(key=lambda val: val[1], reverse=True)

            if len(acksR) >= quorumSizeR:
                returnVal = acksR[0]

                if returnVal[0] != "None":
                    change(requests[0].body.key, (returnVal[0], returnVal[1]))
                    reply(requests[0], type="read_ok", value=returnVal[0])
                else:
                    reply(requests[0], type="error", code=20, text=f"Key {requests[0].body.key} not found..")

                acksR.clear()
                waitingR = False
                requests.pop(0)
                clientMsgHandler()
    
        elif waitingW and msg.body.type == "ackw":
            acksW.append(msg.body.timestamp)
            logging.debug(f"\n{acksW = } {quorumSizeW = }")
            acksW.sort()
            
            if len(acksW) >= quorumSizeW:
                tim = max(acksW)
                timestamp = tim + 1
                logging.info(f"{requests = } {serverRequests = }")
                body = requests[0].body
                key, value = body.key, body.value
                change(key, (value, timestamp))

                update(key, value)

                acksW.clear()
                acksU.append(node_id)
                waitingW = False
                waitingU = True

        elif waitingU and msg.body.type == "acku":
            acksU.append(msg.src)

            if len(acksU) >= len(node_ids):
                reply(requests[0], type="write_ok")

                requests.pop(0)
                acksU.clear()
                waitingU = False
                clientMsgHandler()
        else:
            if msg.src not in node_ids:
                requests.append(msg)
            else:
                serverRequests.append(msg)

    def clientMsgHandler():
        global node_id, node_ids, data, requests, waitingR, waitingW, timestamp, quorumSizeR, quorumSizeW

        while True:
            if len(requests) == 0:
                return       

            msg = requests[0]

            # Message handlers
            if msg.body.type == 'init':
                node_id = msg.body.node_id
                node_ids = msg.body.node_ids
                logging.info('node %s initialized', node_id)
                quorumSizeW = len(node_ids)

                reply(msg, type='init_ok')
                requests.pop(0)
            elif msg.body.type == 'read':
                readQuorum(msg.body.key)
                waitingR = True
                return
            elif msg.body.type == 'write':
                writeQuorum(msg.body.key)
                waitingW = True
                return
            elif msg.body.type == 'cas':
                reply(msg, type="undefined")
                requests.pop(0)
            else:
                logging.warning('unknown message type %s', msg.body.type)

    def serverMsgHandler():
        global node_id, node_ids, data, requests, waitingW, waitingR, waitingU, timestamp, quorumSizeR, quorumSizeW

        while True:
            if len(serverRequests) <= 0:
                return
            msg = serverRequests.pop(0)

            if msg.body.type == 'update':
                key, value, ts = msg.body.key, msg.body.value, msg.body.timestamp
                change(key, (value, ts))

                if timestamp < ts:
                    timestamp = ts
                reply(msg, type="acku")
            elif msg.body.type == 'quorumr':
                val = read(msg.body.key) or ("None", -1)
                reply(msg, type="ackr", value=val[0], timestamp=val[1])
            elif msg.body.type == 'quorumw':
                reply(msg, type="ackw", timestamp=timestamp)
    
    logging.info(f"{waitingR = } {waitingW = } {waitingU = } {msg = }")
    if waitingW or waitingR or waitingU:
        waitingHandler(msg)
    else:
        if msg.body.type == "init" or msg.src not in node_ids:
            requests.append(msg)
        else:
            serverRequests.append(msg)
        clientMsgHandler()
    serverMsgHandler()

# Main loop
executor.map(lambda msg: exitOnError(handle, msg), receiveAll())

# schedule deferred work with:
# executor.submit(exitOnError, myTask, args...)

# schedule a timeout with:
# from threading import Timer
# Timer(seconds, lambda: executor.submit(exitOnError, myTimeout, args...)).start()

# exitOnError is always optional, but useful for debugging



### TODO
# 1- Erro linha 91 
