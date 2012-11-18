ZPER
========

ZPER is ZeroMQ persistence broker. ZPER store ZMQ message frames to disk as is.

## Design
Inspired by Apache [Kafka](http://incubator.apache.org/kafka/)

ZPFront
* Listen on the ROUTER socket
* Handle write requests
* Multiplex worker thread based on the topic hash

ZPAck
* Listen on the PUB socket for public
* Connected SUB will get last sucessfully synced offset and the first frame of the message
 * So try to use the first frame as a message id
 * By subscribing, an application get more fined grained offset
* Listen on the inproc PULL socket for ack input from the ZPFront

ZPRear
* Listen on the ROUTER socket
* Handle fetch requests
* Handle operation requests

ZPMan
* Control above components
* Send heartbeats to components through a ipc 

## Configuration

    # common configuration
    base_dir = "/data/zper"         # data store base directory 
    segment_size = 536870912        # segment file size 512M
    flush_messages = 10000          # flush every N messages
    flush_interval = 1000           # flush every N milliseconds
    cleanup_interval = 604800000L   # cleanuy segement every 1 week

    # front configuration
    front.bind = tcp://*:5555       # front bind address
    front.workers = 3               # number of worker threads
    front.io_threads = 1            # number of io threads

    # rear configuration
    rear.bind = tcp://*:5556
    rear.workers = 3                # number of worker threads
    rear.io_threads = 1             # number of io threads

    # ack configuration
    ack.bind = tcp://*:5557

## Write messages to ZPER
* Create a DEALER or PUSH socket and set proper HWM 
* Set a identity as the following format (ZMQ 3.2.2 or above is required for PUSH)

    -------------------------------------------------------------------
    | 1 byte-topic-hash | 1 byte-topic-length | topic | 16 bytes uuid |
    -------------------------------------------------------------------

    Hash function is
    0xff & (s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1])

* Connect to ZPFront and send (socket.write) messages
* Recommend to send multi frames with the first frame as a message id which will be used at the ZPAck


## Fetch messages to ZPER
* Create a DEALER socket
* Connect to ZPRear
* Send a command (socket.write) and fetch (socket.read) messages


