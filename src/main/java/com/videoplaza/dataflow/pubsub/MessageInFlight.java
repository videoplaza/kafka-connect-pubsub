package com.videoplaza.dataflow.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import org.slf4j.Logger;

import java.util.Objects;

import static java.lang.System.currentTimeMillis;

/**
 * Encapsulates Pubsub message and corresponding <code>AckReplyConsumer</code> instance.
 */
public class MessageInFlight {
   private final String messageId;
   private final String messageKey;
   private final AckReplyConsumer ackReplyConsumer;
   private final TaskMetrics metrics;
   private final Logger log;
   private volatile boolean polled = false;
   private final long createdTimestamp;

   public MessageInFlight(String messageId, String messageKey, AckReplyConsumer ackReplyConsumer, TaskMetrics metrics, Logger log) {
      Objects.requireNonNull(messageId, "messageId cannot be null");
      Objects.requireNonNull(ackReplyConsumer, "ackReplyConsumer cannot be null");
      Objects.requireNonNull(metrics, "metrics cannot be null");
      Objects.requireNonNull(log, "log cannot be null");
      this.messageId = messageId;
      this.messageKey = messageKey;
      this.ackReplyConsumer = ackReplyConsumer;
      this.metrics = metrics;
      this.log = log;
      this.createdTimestamp = currentTimeMillis();
   }

   public String getMessageId() {
      return messageId;
   }

   public String getMessageKey() {
      return messageKey;
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
         log.trace("Acked {}", getMessageId());
      } else {
         ackReplyConsumer.nack();
         metrics.onNack();
         log.trace("Nacked {}", getMessageId());
      }
   }

   @Override public String toString() {
      return "MessageInFlight[" + messageId + '/' + messageKey + '/' + polled + '|' + (currentTimeMillis() - createdTimestamp) + "ms]";
   }
}