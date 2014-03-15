ZPER
========

ZPER is ZeroMQ persistence broker. ZPER store ZMQ message frames to disk as is.

## Design
Inspired by Apache [Kafka](http://incubator.apache.org/kafka/)

* Easier to use for 0MQ developer
* 2x faster than Kafka at Producer (1.5x faster at Consumer)
* No ZooKeepr dependency 

### ZPWriter
* Listen on the ROUTER socket
* Handle write requests
* Multiplex worker thread based on the topic hash
* Support replica to another ZPWriter (at 0.2)

### ZPReader
* Listen on the ROUTER socket
* Handle fetch requests
* Handle operation requests

### ZPAck (at 1.0)
* Listen on the PUB socket for public
* Connected SUB will get last sucessfully synced offset and the first frame of the message
 * So try to use the first frame as a message id
 * By subscribing, an application get more fined grained offset
* Listen on the inproc PULL socket for ack input from the ZPWriter
* Keep basic stats

### ZPRelay (at 1.0)
* Listen on the ROUTER socket for public
* ZPRelay poll the data file and send new messages to client
* If the ZPER is restarted, ZPRelay starts to send messages from the topic's last offset.
* Offers a seamless experience

### ZPController (at 2.0)
* When ZPER works as a cluster, ZPER coordinates through ZPController
* Listen on the ROUTER socket 
* ZPController connects each other and shares informaion
* Client can get cluster information 
* Keep basic stats

## RAW format 
* follows ZMTP 2.0 frame

<pre>
    # if a message has more frame
    flag |= 0x01

    # when payload length is less than 255 
    + ----------- + ------------- + -------------------- +
    | 1 byte-flag | 1 byte-length | length-bytes-payload |
    + ----------- + ------------- + -------------------- +

    # when payload length exceeds 255
    flag |= 0x02
    + ----------- + -------------- + ---------------------+
    | 1 byte-flag | 8 bytes-length | length-bytes-payload |
    + ----------- + -------------- + ---------------------+

</pre>


## Configuration

    # common configuration
    io_threads = 2                  # number of io threads
    base_dir = "/data/zper"         # data store base directory 
    segment_size = 536870912        # segment file size 512M
    flush_messages = 10000          # flush every N messages
    flush_interval = 1000           # flush every N milliseconds
    retain_hours = 168              # cleanup segements older than 1 week

    # writer configuration
    writer.bind = tcp://*:5555       # writer bind address
    writer.workers = 3               # number of worker threads
    writer.replica.1 = tcp://addr1:port  # target replica 1's address
    writer.replica.2 = tcp://addr2:port  # target replica 2's address

    # reader configuration
    reader.bind = tcp://*:5556        # reader bind address
    reader.workers = 3                # number of worker threads

    # ack configuration
    ack.bind = tcp://*:5557

    # relay configuration
    relay.bind = tcp://*:5558
    relay.poll = 1000                   # poll every N milliseconds

    # controller configuration (at 0.2)
    controller.bind = tcp://*:5559
    controller.quorum.1 = tcp://addr1:port   # target quorum 1's address
    controller.quorum.2 = tcp://addr2:port   # target quorum 2's address

## How to run ZPER

* Start ZPER
    ./bin/zper-server-start.sh conf/server.properties

* Stop ZPER
    ./bin/zper-server-stop.sh 


## Write messages to ZPER
* Create a DEALER, REQ or PUSH socket and set proper HWM 
* Set a identity as the following format (ZMQ 3.2.2 or above is required for PUSH)
* Connect to ZPWriter. To load balance just connect to multiple ZPWriter

<pre>
     + ----------------- + ----------- + ------------------- + ----- + ------------- +
     | 1 byte-topic-hash | 1 byte-mode | 1 byte-topic-length | topic | 16 bytes uuid |
     + ----------------- + ----------- + ------------------- + ----- + ------------- +

     Hash function is
     0xff & (s[0]*31^(n-1) + s[1]*31^(n-2) + ... + s[n-1])

     Flags is
     0 (Fastest) : No response (DEALER and PUSH)
     1           : Response at flush (DEALER)
     2 (Slowest) : Response at every write (REQ)
</pre>

```java
    Context ctx = ZMQ.context (1);
    Socket sock = ctx.socket (ZMQ.DEALER);
    
    sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));  // mode = 0
    sock.connect ("tcp://127.0.0.1:5555");

    while (true) {
        String data = "hello";
        sock.send (data);
    }

    sock.close ();
    ctx.term ();
```

* Connect to ZPWriter and send (socket.write) messages
* Recommend to send multi frames with the first frame as a message id which will be used at the ZPAck


## Fetch messages from ZPER
* Create a DEALER or REQ socket
* Set a identity as the above format
* Connect to ZPReader. To sink messages just connect to multiple ZPReader
* Send a "OFFSET" request if you don't have the last offset
* Send a "FETCH" request 

<pre>
    # command consists of 1 command frame and variable argument frames 
     + ----------+
     | command   |       # command String
     + ----------+
     | argument1 |       # variable bytes depends on command
     + ----------+
     | ....      |       
     + ----------+
     | argumentN |       
     + ----------+

    # commands
    FETCH ( 8_bytes_offset , 8_bytes_max_bytes )
    OFFSET ( 8_bytes_last_modified_millis [, 4_bytes_max_entries ] )  # -1 : oldest , -2 : latest

    # responses
     + ----------+
     | status    |       # 1 byte common first frame
     + ----------+
        - OK : 100
        - INVALID_OFFSET : 101
        - INVALID_COMMAND : 102
        - INTERNAL_ERROR : -1

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

</pre>

```java
    Context ctx = ZMQ.context (1);
    Socket sock = ctx.socket (ZMQ.DEALER);
    
    sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));  // flag = 0
    sock.connect ("tcp://127.0.0.1:5556");

    // get offset
    sock.sendMore ("OFFSET");
    sock.sendMore (ZPUtils.getBytes (timestamp));
    sock.send (ZPUtils.getBytes (entry));
        
    status = ZPUtils.getInt (sock.recv ()); // check status is 100
        
    while (sock.hasReceiveMore ()) {
        // 8 byte offset
        long offset = ZPUtils.getLong (sock.recv ());
    }

    // fetch data
    sock.sendMore ("FETCH");
    sock.sendMore (ZPUtils.getBytes (offset));
    sock.send (ZPUtils.getBytes (size));

    status = ZPUtils.getInt (sock.recv ()); // check status is 100

    MsgIterator it = ZPUtils.iterator (sock.recvMsg ());
        
    while (it.hasNext ()) {
        Msg msg = it.next ();
        // Do your stuff ...
    }
    // store the last offset somewhere
    lastOffset = offset + it.validBytes ()

    sock.close ();
    ctx.term ();
```
```java
    // Streaming mode (at 0.1.1)
    Context ctx = ZMQ.context (1);
    Socket sock = ctx.socket (ZMQ.DEALER);
    
    sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));  // flag = 0
    sock.connect ("tcp://127.0.0.1:5556");

    // get offset, refer above

    // fetch data
    sock.sendMore ("FETCH");
    sock.sendMore (ZPUtils.getBytes (offset));
    sock.send (ZPUtils.getBytes (-1L));   // Streaming

    while (true) {
        Msg msg = sock.recvMsg (ZMQ.DONTWAIT);
        if (msg == null)
            break;  // no more message
        // Do your stuff ...

        // store the last offset somewhere
        lastOffset += ZPUtils.validBytes (msg);
    }

    sock.close ();
    ctx.term ();
```

## Fetch messages from ZPRelay (at 0.1.1)

* Create a DEALER or PULL socket
* Set a identity as the above format ((ZMQ 3.2.2 or above is required for PULL)
* Connect to ZPRelay. 
* Doesn't require offset. But you can loose messages if the broker restarted.

```java
    Context ctx = ZMQ.context (1);
    Socket sock = ctx.socket (ZMQ.DEALER);
    
    sock.setIdentity (ZPUtils.genTopicIdentity (topic, 0));  // flag = 0
    sock.connect ("tcp://127.0.0.1:5558");

    while (true) {
        Msg msg = sock.recvMsg ();
        if (msg == null)
            break;  // interrupted
        // Do your stuff ...
    }

    sock.close ();
    ctx.term ();
```

## Listen acks from ZPER (at 2.0)
* Create a SUB socket
* Set subscribe topics which you want to subscribe
* Connect to ZPAck
* ZPAck will send you 3 frames every time ZPWriter flush to disks

<pre>
    # ack consists of 3 frames
     + -----------+
     | topic      |    # topic string
     + -----------+
     | message-id |    # first frame which was sent to the ZPWriter
     + -----------+
     | offset     |    # 8 bytes offset of the message
     + -----------+
</pre>

## Working on Cluster (at 2.0)
* By connecting to one of ZPController, client can get node and peer information
