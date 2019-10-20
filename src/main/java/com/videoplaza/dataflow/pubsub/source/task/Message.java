package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.videoplaza.dataflow.pubsub.util.TaskMetrics;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Objects;

import static java.lang.System.currentTimeMillis;

/**
 * Encapsulates Pubsub message and corresponding <code>AckReplyConsumer</code> instance.
 */
public class Message {
   private final String messageId;
   private final AckReplyConsumer ackReplyConsumer;
   private final TaskMetrics metrics;

   private volatile boolean polled = false;
   private final long createdTimestamp;
   private final SourceRecord record;

   public Message(String messageId, SourceRecord record, AckReplyConsumer ackReplyConsumer, TaskMetrics metrics) {
      Objects.requireNonNull(messageId, "messageId cannot be null");
      Objects.requireNonNull(record, "record cannot be null");
      Objects.requireNonNull(ackReplyConsumer, "ackReplyConsumer cannot be null");
      Objects.requireNonNull(metrics, "metrics cannot be null");
      this.messageId = messageId;
      this.record = record;
      this.ackReplyConsumer = ackReplyConsumer;
      this.metrics = metrics;
      this.createdTimestamp = currentTimeMillis();
   }

   public String getMessageId() {
      return messageId;
   }

   public String getMessageKey() {
      return record.key().toString();
   }

   public SourceRecord getRecord() {
      return record;
   }

   public boolean isPolled() {
      return polled;
   }

   public void markAsPolled() {
      this.polled = true;
   }

   public void ack(boolean ack) {
      if (ack) {
         ackReplyConsumer.ack();
         metrics.onAck();
      } else {
         ackReplyConsumer.nack();
         metrics.onNack();
      }
   }

   @Override public String toString() {
      return "MessageInFlight[" + messageId + '/' + getMessageKey() + '/' + polled + '|' + (currentTimeMillis() - createdTimestamp) + "ms]";
   }
}