# Introduction 
This is a Kafka Connect source connector for Google Cloud Pubsub based on [async pull api](https://cloud.google.com/pubsub/docs/pull#asynchronous-pull)
  
# Features
Supports custom message key and timestamp attributes.

# Configuration
For configuration options checkout `PubsubSourceConnectorConfig`.

# Tuning
While consuming from Pubsub there are 2 major problems: duplicates and delays.

## Duplicates 
A message will be resend (to any consumer) if there is no acknowledgment received before its ack deadline expires. 
Amount of duplicates is increased if batches are used since a whole batch gets redelivered if a single message fails to meet its ack deadline.  
There is no guarantee though that the message won't get redelivered earlier.

## Delays     
Maximum time a message can sit wihout being delivered to any consumer is defined by ack deadline.
It is set on subscription level, but it can be extended dynamically up to `max.ack.extension.period.sec`. By default it's 5 minutes. 
Note that since there are not ordering guarantees there delays can even higher if there is a backlog in a subscription.

## Minimizing delays and duplicates
Given the above a message delivery should be attempted until ack deadline is expired, but no longer.
If connector holds message for a longer period there is a high chance that the message will get redelivered to some other worker and thus eventually written to kafka twice.
Thus it is recommended to set the following connector level settings:
- `max.ack.extension.period.sec` - max message ack deadline in pubsub
- `cache.expiration.deadline.sec` - time to keep a message in internal message buffer for acknowledgement after delivery to kafka.
Kafka producer settings (https://kafka.apache.org/documentation/#producerconfigs) in connect worker configs:
- `producer.delivery.timeout.ms` - a deadline for message delivery to kafka

The above settings should satisfy the following constraint: `producer.delivery.timeout.ms` < `cache.expiration.deadline.sec` < `max.ack.extension.period.sec`.  

## Back pressure 
If messages are not delivered to kafka fast enough each task will accumulate up to `flow.control.max.outstanding.elements` records.
Each record is attempted for delivery up to `producer.delivery.timeout.ms` and stays in internal message buffer for `cache.expiration.deadline.sec`.
Normally a message will be redelivered after `max.ack.extension.period.sec`.
