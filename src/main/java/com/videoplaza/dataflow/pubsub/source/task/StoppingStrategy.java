package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

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
      new Thread(this::shutdown, "shutdown-" + logger.getTaskUUID()).start();
   }

   @Override
   public SourceMessage onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      if (shouldNackMessagesDuringShutdown) {
         ackReplyConsumer.nack();
      }
      return null;
   }

   @Override public List<SourceRecord> poll() {
      logger.info("No polling while stopping.");
      return null;
   }

   @Override public void commit() throws InterruptedException {
      logger.info("Waiting for async shutdown thread to complete.");
      complete.await(totalTerminationTimeoutMs, TimeUnit.MILLISECONDS);
      logger.info("Task is shutdown.");
   }

   @Override public void stop() {
      logger.error("Stopping already being stopped task.");
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
         terminateSubscriber();

         if (!state.isClean()) {
            logger.warn("Task is shutdown with unclean state in {}ms.", msSince(start));
         } else {
            logger.info("Task is shutdown in {}ms.", msSince(start));
         }
      } finally {
         complete.countDown();
         state.moveTo(new StoppedStrategy(state));
      }
   }

   /**
    * Attempts to nack messages, terminates subscriber and aits at most  {@link PubsubSourceConnectorConfig#SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_CONFIG}.
    */
   private void terminateSubscriber() {
      logger.info("Nacking messages not delivered to kafka and stopping the subscriber.");
      nackReceivedMessages();
      long start = System.nanoTime();
      state.getSubscriber().stopAsync();
      try {
         state.getSubscriber().awaitTerminated(subscriberTerminationTimeoutMs, TimeUnit.MILLISECONDS);
         state.getPubsubEventLoopGroup().shutdownGracefully();
         state.getPubsubEventLoopGroup().terminationFuture().await(subscriberTerminationTimeoutMs, TimeUnit.MILLISECONDS);
         logger.info("Subscriber is terminated in {} ms.", msSince(start));
      } catch (TimeoutException e) {
         logger.info("Subscriber was not terminated in {} ms.", msSince(start));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         logger.info("Interrupted while waiting for event loop group to shutdown. {} ms.", msSince(start));
      }
   }

   private void nackReceivedMessages() {
      Set<SourceMessage> toNack = messages.pollMessages().collect(Collectors.toSet());
      logger.info("Nacking {} records.", toNack.size());
      toNack.forEach((m) -> {
         m.nack();
         logger.log("Nacked {}.", m);
         messages.remove(m.getMessageId());
         state.getMetrics().onMessageNack(m.getReceivedMs());
      });
   }

   private void waitForPolledMessagesToBeAcknowledged() {
      logger.info("Waiting for {} inflight messages to be delivered to kafka. {}ms left.", messages.getPolledCount(), inflightAckTimeoutMs);
      long start = System.nanoTime();
      long deadline = start + TimeUnit.MILLISECONDS.toNanos(inflightAckTimeoutMs);
      try {
         while (msTo(deadline) > 0 && messages.getPolledCount() > 0) {
            TimeUnit.MILLISECONDS.sleep(100);
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         logger.warn("Interrupted while waiting for inflight messages. {} messages can have duplicates.", messages.getPolledCount());
      }
      if (messages.getPolledCount() == 0) {
         logger.info("All messages delivered to kafka are acknowledged in {} ms.", msSince(start));
      } else {
         logger.warn("Not all messages delivered to kafka are acknowledged in {} ms. {}", msSince(start));
         messages.polled().filter(SourceMessage::hasTraceableRecords).forEach(sm -> logger.log("Polled but not acknowledged: {}.", sm));
      }
   }
}