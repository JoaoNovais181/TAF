#!/usr/bin/env python

import logging
from concurrent.futures import ThreadPoolExecutor
from ms import send, receiveAll, reply, exitOnError
from threading import Lock

logging.getLogger().setLevel(logging.DEBUG)
executor=ThreadPoolExecutor(max_workers=1)

data = {}
dataLock = Lock()

def handle(msg):

    # State
    global node_id, node_ids, data

    def change(key, value):
        # dataLock.acquire()
        try:
            data[key] = value
        finally:
            # dataLock.release()
            pass

    def read(key):
        value = None
        # dataLock.acquire()
        try:
            value = data[key]
        finally:
            # dataLock.release()
            pass
        
        return value

    def propagate(key, value):
        for node in node_ids:
            if node != node_id:
                send(src=node_id, dest=node, type="data_change", key=key, value=value)

    # Message handlers
    if msg.body.type == 'init':
        node_id = msg.body.node_id
        node_ids = msg.body.node_ids
        logging.info('node %s initialized', node_id)

        reply(msg, type='init_ok')
    elif msg.body.type == 'read':
        valid = msg.body.key in data.keys()

        if valid:
            reply(msg, type="read_ok", value=read(msg.body.key))
        else:
            reply(msg, type="error", code=20, text=f"Key {msg.body.key} not found..")
    elif msg.body.type == 'write':
        key, value = msg.body.key, msg.body.value

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
    elif msg.body.type == 'data_change':
        key = msg.body.key
        value = msg.body.value

        data[key] = value
        logging.info("Changed %d to %d", (key, value))
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
