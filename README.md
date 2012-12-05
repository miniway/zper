ZPER
========

ZPER is ZeroMQ persistence broker. ZPER store ZMQ message frames to disk as is.

## Design
Inspired by Apache [Kafka](http://incubator.apache.org/kafka/)

* Easier to use for 0MQ users
* 2x faster than Kafka

### ZPWriter
* Listen on the ROUTER socket
* Handle write requests
* Multiplex worker thread based on the topic hash
* Support replica to another ZPWriter

### ZPAck
* Listen on the PUB socket for public
* Connected SUB will get last sucessfully synced offset and the first frame of the message
 * So try to use the first frame as a message id
 * By subscribing, an application get more fined grained offset
* Listen on the inproc PULL socket for ack input from the ZPWriter
* Keep basic stats

### ZPReader
* Quorum between ZPReaders 
* Listen on the ROUTER socket
* Handle fetch requests
* Handle operation requests

## Configuration

    # common configuration
    base_dir = "/data/zper"         # data store base directory 
    segment_size = 536870912        # segment file size 512M
    flush_messages = 10000          # flush every N messages
    flush_interval = 1000           # flush every N milliseconds
    cleanup_interval = 604800000L   # cleanup segements older than 1 week

    # writer configuration
    writer.bind = tcp://*:5555       # writer bind address
    writer.workers = 3               # number of worker threads
    writer.io_threads = 1            # number of io threads
    writer.replica.1 = tcp://addr1:port  # target replica 1's address
    writer.replica.2 = tcp://addr2:port  # target replica 2's address

    # reader configuration
    reader.bind = tcp://*:5556        # reader bind address
    reader.workers = 3                # number of worker threads
    reader.io_threads = 1             # number of io threads

    # ack configuration
    ack.bind = tcp://*:5557

## Write messages to ZPER
* Create a DEALER, REQ or PUSH socket and set proper HWM 
* Set a identity as the following format (ZMQ 3.2.2 or above is required for PUSH)

     + ----------------- + ----------- + ------------------- + ----- + ------------- +
     | 1 byte-topic-hash | 1 byte-flag | 1 byte-topic-length | topic | 16 bytes uuid |
     + ----------------- + ----------- + ------------------- + ----- + ------------- +

     Hash function is
     0xff & (s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1])

     Flags is
     0 (Fastest) : No response (DEALER and PUSH)
     1           : Response at flush (DEALER)
     2 (Slowest) : Response at every write (REQ)

* Connect to ZPWriter and send (socket.write) messages
* Recommend to send multi frames with the first frame as a message id which will be used at the ZPAck


## Fetch messages to ZPER
* Create a DEALER or REQ socket
* Set a identity as the above format
* Connect to ZPReader
* Send a request (socket.write) and fetch (socket.read) messages

    # command consists of 1 command frame and variable argument frames 
     + ----------+
     | command   |       # 1 byte
     + ----------+
     | argument1 |       # variable bytes depends on command
     + ----------+
     | ....      |       
     + ----------+
     | argumentN |       
     + ----------+

    # commands
    1 : fetch ( 8_bytes_offset , 8_bytes_max_bytes )
    2 : offset ( 8_bytes_last_modified_millis )  # -1 : oldest , -2 : latest
    3 : master 

    # responses
     + ----------+
     | status    |       # 1 byte common first frame
     + ----------+

    for fetch request        for stream fetch request ( with -1 max bytes )
     + ---------------+      + ---------------+
     | Super Frame    |      |     frame 1    |
     |  + ----------+ |      + ---------------+
     |  | frame1    | |      |     frame 2    |
     |  + ----------+ |      + ---------------+
     |  | ...       | |      |     ...        |
     |  + ----------+ |      + ---------------+
     |  | frameN    | |      |     frane N    |
     |  + ----------+ |      + ---------------+
     + ---------------+

    for offset request
     + ----------+
     | offset1   |       
     + ----------+
     | ...       |       
     + ----------+
     | offsetN   |       
     + ----------+

    for master request
     + -------------------------+
     | master ZPWriter address  |       
     + -------------------------+
     | master ZPReader address  |       
     + -------------------------+

## Listen acks from ZPER
* Create a SUB socket
* Set subscribe topics which you want to subscribe
* Connect to ZPAck
* ZPAck will send you 3 frames every time ZPWriter flush to disks

    # ack consists of 3 frames
     + -----------+
     | topic      |    # topic string
     + -----------+
     | message-id |    # first frame which was sent to the ZPWriter
     + -----------+
     | offset     |    # 8 bytes offset of the message
     + -----------+

## Working on quorum
* When works on a cluster, send 'master' command to one of ZPReader(s) then work with the master ZPWriter and ZPReader
