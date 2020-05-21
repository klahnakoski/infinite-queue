# infinite-queue
Implement an infinite replayable queue

## Problems

* Listening to a queue requires a reliable listener; queues have a limit and if messages are not recieved in a timely manner they will be lost.
* Replay old messages for debugging
* Replay old messages after fixing bugs
* Replay old messages for testing

## Solution

Infinite replayable queue

* The service is backed by S3; the only limit is cost.
* You can replay old messages: Request a subscriber to iterate over some subset of messages.
* Subscribers are permanent, so your listener can be offline for days or months without loosing messages.


## Nomenclature

* **Message** - A JSON-serializable object
* **Queue** - An unbounded, ordered list of messages 
* **Subscriber** - Tracks the current position in a queue. There can be many subscribers for a single queue. Subscribers are not obligated to start at the beginning of the list, nether are they obligated to consume all messages.
* **Listener** - Any number of threads or processes may `pop()` messages off a subscriber, each is called a listener.
* **Broker** - The overall application that is managing queues and their subscribers

### Gotchas

The queue service marks up JSON with an additional `etl` property; which is an array of ETL records; the last entry recording the addition to the queue. 


## Non-solutions

### Not a high speed queue

Despite the size of queues it handles, this implementation is a single-process Python application. It can only handle [40 messages per second](tests/test_speed.py).  


### Not to avoid back pressure

Large queues are sometimes a result of the reader falling behind the writer. If you use this infinite queue to solve this problem, you may find you have a much bigger problem later. The proper solution is to use/implement a queue that will provide back pressure to slow down the writer.