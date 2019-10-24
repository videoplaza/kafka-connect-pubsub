package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.videoplaza.dataflow.pubsub.util.TimeUtils.msSince;
import static com.videoplaza.dataflow.pubsub.util.TimeUtils.msTo;

public class StoppingStrategy extends BaseStrategy {

   private final CountDownLatch complete = new CountDownLatch(1);
   private final boolean shouldNackMessagesDuringShutdown;
   private final long totalTerminationTimeoutMs;
   private final long subscriberTerminationTimeoutMs;
   private final long inflightAckTimeoutMs;

   public StoppingStrategy(PubsubSourceTaskState state) {
      super(state);
      shouldNackMessagesDuringShutdown = state.getConfig().shouldNackMessagesDuringShutdown();
      totalTerminationTimeoutMs = state.getConfig().getTotalTerminationTimeoutMs();
      subscriberTerminationTimeoutMs = state.getConfig().getSubscriberTerminationTimeoutMs();
      inflightAckTimeoutMs = state.getConfig().getInflightAckTimeoutMs();
   }

   /**
    * Kicks off async shutdown sequence. See {@link StoppingStrategy#shutdown()}
    */
   @Override public void init() {
      new Thread(this::shutdown, log.getName() + "-shutdown").start();
   }

   @Override
   public void onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey) {
      if (shouldNackMessagesDuringShutdown) {
         ackReplyConsumer.nack();
         if (isDebugEnabled(messageKey)) {
            log.debug("Received a message during shutdown. Nacking. {}/{}. {}", pubsubMessage.getMessageId(), messageKey, state);
         }
      }
      if (isDebugEnabled(messageKey)) {
         log.debug("Received a message during shutdown. Ignoring. {}/{}. {}", pubsubMessage.getMessageId(), messageKey, state);
      }
   }

   @Override public List<SourceRecord> poll() {
      log.info("No polling while stopping");
      return null;
   }

   @Override public void commit() throws InterruptedException {
      log.info("Waiting for async shutdown thread to complete. {}", state);
      complete.await(totalTerminationTimeoutMs, TimeUnit.MILLISECONDS);
      log.info("Task is shutdown. {}", state);
   }

   @Override public void stop() {
      log.error("Stopping already being stopped task {}", state);
   }

   /**
    * Attempts to do the following asynchronously:
    * <ul>
    * <li>Wait for all in flight messages to be delivered to kafka and acknowledge those in Cloud PubSub</li>
    * <li>Wait for Cloud PubSub subscriber to shutdown and 'nacks' all messages that have been received but not delivered to kafka</li>
    * </ul>
    * <p>
    * Should take no longer then sum of {@link PubsubSourceConnectorConfig#SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_CONFIG} and
    * {@link PubsubSourceConnectorConfig#SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_CONFIG}.
    */
   private void shutdown() {
      try {
         long start = System.nanoTime();

         waitForPolledMessagesToBeAcknowledged();
         terrminateSubscriber();

         if (!state.isClean()) {
            log.warn("Task is shutdown with unclean state in {}ms. {}", msSince(start), state);
         } else {
            log.info("Task is shutdown in {}ms. {}", msSince(start), state);
         }
      } finally {
         complete.countDown();
         state.moveTo(new StoppedStrategy(state));
      }
   }

   /**
    * Attempts to nack messages, terminates subscriber and aits at most  {@link PubsubSourceConnectorConfig#SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_CONFIG}.
    */
   private void terrminateSubscriber() {
      log.info("Nacking messages not delivered to kafka and stopping the subscriber. {}", state);
      nackReceivedMessages();
      long start = System.nanoTime();
      state.getSubscriber().stopAsync();
      try {
         state.getSubscriber().awaitTerminated(subscriberTerminationTimeoutMs, TimeUnit.MILLISECONDS);
         log.info("Subscriber is terminated in {} ms. {}", msSince(start), state);
      } catch (TimeoutException e) {
         log.info("Subscriber was not terminated in {} ms. {}", msSince(start), state);
      }
   }

   private void nackReceivedMessages() {
      List<SourceRecord> records = messages.poll();
      log.debug("Nacking {} records. {}", records.size(), state);
      records.forEach(r -> commitRecord(r, false));
   }

   private void waitForPolledMessagesToBeAcknowledged() {
      log.info("Waiting for {} inflight messages to be delivered to kafka. {}ms left. {}", messages.getPolledCount(), inflightAckTimeoutMs, state);
      long start = System.nanoTime();
      long deadline = start + TimeUnit.MILLISECONDS.toNanos(inflightAckTimeoutMs);
      try {
         while (msTo(deadline) > 0 && messages.getPolledCount() > 0) {
            TimeUnit.MILLISECONDS.sleep(100);
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         log.warn("Interrupted while waiting for inflight messages. {} messages can have duplicates.", messages.getPolledCount());
      }
      if (messages.getPolledCount() == 0) {
         log.info("All messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), state);
      } else {
         log.warn("Not all messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), state);
         messages.dump(log);
      }
   }
}