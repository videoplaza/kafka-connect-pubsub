package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.List;

/**
 * Defines state dependant interactions of a {@link PubsubSourceTask} with pubsub and kafka connect frameworks.
 * Implementations correspond to specific task states (running, stopping, stopped).
 */
public interface PubsubSourceTaskStrategy {

   void init();

   /**
    * Callback for the case when a new <tt>pubsubMessage</tt> is received from pubsub and its message id is not present
    * in the task in flight message buffer.
    *
    * @see PubsubSourceTask#onPubsubMessageReceived(PubsubMessage, AckReplyConsumer)
    */
   void onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey);

   /**
    * Callback for the case when a new <tt>pubsubMessage</tt> is received, but it is already present
    * in the task in flight message buffer.
    *
    * @see PubsubSourceTask#onPubsubMessageReceived(PubsubMessage, AckReplyConsumer)
    */
   void onDuplicateReceived(PubsubMessage pubsubMessage, String messageKey, Message current);

   /**
    * Returns messages to Kafka connect framework for delivery to kafka
    *
    * @see SourceTask#poll()
    */
   List<SourceRecord> poll();

   /**
    * Called by Kafka connect framework, upon message delivery to kafka.
    *
    * @see SourceTask#commitRecord(SourceRecord)
    */
   void commitRecord(SourceRecord record);

   /**
    * A hook into the shutdown process. Not much to do during for the source task.
    *
    * @see SourceTask#commit()
    */
   void commit() throws InterruptedException;

   void stop();

}
