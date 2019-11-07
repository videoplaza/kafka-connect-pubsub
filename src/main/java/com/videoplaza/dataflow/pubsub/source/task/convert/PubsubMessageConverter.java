package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;

public interface PubsubMessageConverter {
   SourceMessage convert(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer);
}
