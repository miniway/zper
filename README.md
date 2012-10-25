ZPER
========

ZPER is ZeroMQ persistence broker. ZPER store ZMQ message frames to disk as is.

## Architecture
* Inspired by Apache [Kafka](http://incubator.apache.org/kafka/)
* ZPFront
 * Listen on a ROUTER socket
 * Handle write requests
* ZPRear
 * Listen on a ROUTER socket
 * Handle fetch requests
 * Handle operation requests
* ZPFront and ZPRear run on different port for seperation of concerns. 
 * They could be run on different process also
 * They share nothing but disk

## Write messages to ZPER
* Create a PUSH or DEALER socket and set proper HWM 
* Set a identity as the following format (ZMQ 3.2.2 or above is required for PUSH)

  | 1 byte-topic-hash | 1 byte-topic-length | topic | 16 bytes uuid |

* Connect to ZPFront and send (socket.write) messages


## Fetch messages to ZPER
* Create a DEALER socket
* Connect to ZPRear
* Send a command (socket.write) and fetch (socket.read) messages


