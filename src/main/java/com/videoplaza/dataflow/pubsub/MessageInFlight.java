package com.videoplaza.dataflow.pubsub;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import org.slf4j.Logger;

import java.util.Objects;

/**
 * Encapsulates Pubsub message and corresponding <code>AckReplyConsumer</code> instance.
 */
public class MessageInFlight {
   private final String messageId;
   private final AckReplyConsumer ackReplyConsumer;
   private final TaskMetrics metrics;
   private final Logger log;
   private volatile boolean polled = false;

   public MessageInFlight(String messageId, AckReplyConsumer ackReplyConsumer, TaskMetrics metrics, Logger log) {
      Objects.requireNonNull(messageId, "messageId cannot be null");
      Objects.requireNonNull(ackReplyConsumer, "ackReplyConsumer cannot be null");
      Objects.requireNonNull(metrics, "metrics cannot be null");
      Objects.requireNonNull(log, "log cannot be null");
      this.messageId = messageId;
      this.ackReplyConsumer = ackReplyConsumer;
      this.metrics = metrics;
      this.log = log;
   }

   public String getMessageId() {
      return messageId;
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
      return "MessageInFlight[" + getMessageId() + '/' + polled + ']';
   }
}