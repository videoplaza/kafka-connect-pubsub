package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public interface PubsubSourceTaskStrategy {

   void init();

   void onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey);

   void onDuplicateReceived(PubsubMessage pubsubMessage, String messageKey, Message current);

   List<SourceRecord> poll();

   void commitRecord(SourceRecord record);

   void commit() throws InterruptedException;

   void stop();

}
