package com.videoplaza.dataflow.pubsub;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.videoplaza.dataflow.pubsub.TimeUtils.msSince;
import static com.videoplaza.dataflow.pubsub.TimeUtils.msTo;
import static java.util.stream.Collectors.toList;

/**
 * TODO report metrics
 */
public class PubsubSourceTask extends SourceTask {

   private static final AtomicInteger TASK_COUNT = new AtomicInteger();

   private final int id = TASK_COUNT.incrementAndGet();
   private final Logger log = LoggerFactory.getLogger(PubsubSourceTask.class.getName() + "-" + id);

   private final TaskMetrics metrics = new TaskMetrics();
   private final Map<String, MessageInFlight> messages = new ConcurrentHashMap<>();
   private final Set<PubsubMessage> toBePolled = ConcurrentHashMap.newKeySet();

   private final AtomicBoolean stopping = new AtomicBoolean();
   private final Lock pollCommitLock = new ReentrantLock();
   private final Lock receiveLock = new ReentrantLock();
   private final Condition recordsReceived = receiveLock.newCondition();

   private volatile Subscriber subscriber;
   private volatile PubsubSourceConnectorConfig config;
   private volatile PubsubMessageConverter converter;
   private volatile Sleeper sleeper;

   @Override public String version() {
      return Version.getVersion();
   }

   @Override public void start(Map<String, String> props) {
      configure(props);
      configure(new SimpleSleeper());
      subscribe(newSubscriber());
      log.info("Started");
   }

   PubsubSourceTask configure(Sleeper sleeper) {
      this.sleeper = sleeper;
      return this;
   }

   private Subscriber newSubscriber() {
      Subscriber newSubscriber = Subscriber.newBuilder(config.getProjectSubscription(), this::onPubsubMessageReceived)
          .setFlowControlSettings(config.getFlowControlSettings())
          .setMaxAckExtensionPeriod(config.getMaxAckExtensionPeriod())
          .setParallelPullCount(config.getParallelPullCount())
          .build();

      newSubscriber.addListener(new LoggingSubscriberListener(), Executors.newSingleThreadExecutor());

      return newSubscriber;
   }

   PubsubSourceTask subscribe(Subscriber subscriber) {
      this.subscriber = subscriber;
      subscriber.startAsync();
      return this;
   }

   PubsubSourceTask configure(Map<String, String> props) {
      config = new PubsubSourceConnectorConfig(props);
      converter = new PubsubMessageConverter(
          config.getKeyAttribute(),
          config.getTimestampAttribute(),
          config.getSubscription(),
          config.getTopic()
      );
      return this;
   }

   /**
    * Attempts to discard some obvious duplicates.
    * Does not guarantee duplicate detection if message has been already processed on this on other nodes.
    */
   void onPubsubMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      if (!messages.containsKey(pubsubMessage.getMessageId())) {
         log.trace("Received {}", pubsubMessage.getMessageId());
         messages.put(
             pubsubMessage.getMessageId(),
             new MessageInFlight(pubsubMessage.getMessageId(), ackReplyConsumer, metrics, log)
         );
         toBePolled.add(pubsubMessage);
         metrics.onReceived();

         receiveLock.lock();
         try {
            recordsReceived.signalAll();
         } finally {
            receiveLock.unlock();
         }
      } else {
         metrics.onDuplicate();
         log.info("A duplicate received: [{}/{}/{}]. {}", pubsubMessage.getMessageId(), converter.getKey(pubsubMessage), converter.getTimestamp(pubsubMessage), metrics());
      }
   }

   @Override public List<SourceRecord> poll() {
      long timeWaited = waitForMessagesToPoll();
      pollCommitLock.lock();
      try {
         List<SourceRecord> records = doPoll();
         log.debug("Returning {} records after waiting for {}ms. {}", records.size(), timeWaited, metrics());
         return records.isEmpty() ? null : records;
      } finally {
         pollCommitLock.unlock();
      }
   }

   /**
    * Waits for new messages to minimize CPU consumption.
    */
   private long waitForMessagesToPoll() {
      long start = System.nanoTime();
      receiveLock.lock();
      try {
         boolean received = recordsReceived.await(config.gePollTimeoutMs(), TimeUnit.MILLISECONDS);
         log.trace("Received={}, in {}ms", received, msSince(start));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } finally {
         receiveLock.unlock();
      }
      return msSince(start);
   }

   private List<SourceRecord> doPoll() {
      return new HashSet<>(toBePolled).stream().map(this::convertAndPoll).collect(toList());
   }

   private SourceRecord convertAndPoll(PubsubMessage message) {
      toBePolled.remove(message);
      messages.get(message.getMessageId()).markAsPolled();
      return converter.convert(message);
   }

   /**
    * Does nothing if task is not being stopped. Otherwise attempts the following:
    * <ul>
    * <li>Waits for all in flight messages to be delivered to kafka and acknowledge those in Cloud PubSub</li>
    * <li>Waits for Cloud PubSub subscriber to shutdown and 'nacks' all messages that have been received but not delivered to kafka</li>
    * </ul>
    * <p>
    * In any way the method will complete within approximately {@link PubsubSourceConnectorConfig#GCPS_SHUTDOWN_TIMEOUT_MS_CONFIG}.
    */
   @Override public void commit() {
      pollCommitLock.lock();
      try {
         //Source offset committer is stopped before tasks stop method is invoked so this the final commit after all polling is done.
         if (stopping.get()) {
            log.info("Committing after stop request within {}ms. {}", config.getTerminationTimeoutMs(), metrics());
            long start = System.nanoTime();
            long deadline = start + TimeUnit.MILLISECONDS.toNanos(config.getTerminationTimeoutMs());

            waitForPolledMessagesToBeAcknowledged(deadline);

            terrminateSubscriber(deadline);

            if (!isClean()) {
               log.warn("Task ended up in unclean state after shutdown in {}ms. {}", msSince(start), metrics());
            } else {
               log.info("Task is shutdown in {}ms. {}", msSince(start), metrics());
            }
         }
      } finally {
         pollCommitLock.unlock();
      }
   }

   private void waitForPolledMessagesToBeAcknowledged(long deadline) {
      log.info("Waiting for {} inflight messages to be delivered to kafka. {}ms left. {}", getPolledCount(), msTo(deadline), metrics());
      long start = System.nanoTime();
      while (msTo(deadline) > 0 && getPolledCount() > 0) {
         sleeper.sleep(10);
      }
      if (getPolledCount() == 0) {
         log.info("All messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), metrics());
      } else {
         log.warn("Not all messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), metrics());
      }
   }

   /**
    * Waits until <code>deadline</> to let grpc/netty to shutdown itself gracefully. Attempts to nack messages while waiting.
    */
   private void terrminateSubscriber(long deadline) {
      log.info("Nacking messages not delivered to kafka and stopping the subscriber. {}", metrics());
      nackReceivedMessages();
      subscriber.stopAsync();
      long start = System.nanoTime();
      while (msTo(deadline) > 0) {
         nackReceivedMessages();
         sleeper.sleep(1000);
      }

      if (subscriber.state().equals(ApiService.State.TERMINATED)) {
         log.info("Subscriber is terminated in {} ms. {}", msSince(start), metrics());
      } else {
         log.info("Subscriber was not terminated in {} ms. {}", msSince(start), metrics());
      }
   }

   private boolean isClean() {
      return toBePolled.isEmpty() &&
          messages.isEmpty() &&
          metrics.getCountersMismatch() == 0 &&
          metrics.getLostCount() == 0;
   }

   private void nackReceivedMessages() {
      List<SourceRecord> records = doPoll();
      log.debug("Nacking {} records. {}", records.size(), metrics());
      records.forEach(r -> commitRecord(r, false));
   }

   @Override public void commitRecord(SourceRecord record) {
      commitRecord(record, true);
   }

   private void commitRecord(SourceRecord record, boolean ack) {
      String messageId = (String) record.sourceOffset().get(config.getSubscription());
      MessageInFlight m = messages.get(messageId);
      if (m != null) {
         m.ack(ack);
         messages.remove(messageId);
      } else {
         metrics.onLost();
         log.error("Nothing to ack[{}] for {}. So far: {}", ack, record.key(), metrics.getLostCount());
      }
   }

   /**
    * Triggers the shutdown process. All the necessary final actions happen in {@link #commit()}.
    */
   @Override public void stop() {
      log.info("Stopping the task. {}", metrics());
      stopping.set(true);
   }

   /**
    * Number of messages currently received from Cloud Pubsub but not yet picked up by connect framework for publishing to kafka
    */
   public int getToBePolledCount() {
      return toBePolled.size();
   }

   /**
    * Number of messages being delivered to kafka and not yet acknowledged in Cloud Pubsub
    */
   public long getPolledCount() {
      return messages.values().stream().filter(MessageInFlight::isPolled).count();
   }

   public TaskMetrics getMetrics() {
      return metrics;
   }

   private String metrics() {
      return metrics + " [" + getPolledCount() + "/" + getToBePolledCount() + "]";
   }

   /**
    * Introduced mostly for testing purposes.
    */
   interface Sleeper {
      void sleep(long ms);
   }

   class SimpleSleeper implements Sleeper {

      @Override public void sleep(long ms) {
         try {
            Thread.sleep(ms);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for inflight messages. {} messages can have duplicates.", getPolledCount());
         }
      }
   }

   class LoggingSubscriberListener extends Subscriber.Listener {
      @Override public void failed(Subscriber.State from, Throwable failure) {
         log.error("Failed {}.", from, failure);
      }

      @Override public void stopping(ApiService.State from) {
         log.info("Stopping at {}. {}", from, metrics());
      }

      @Override public void terminated(ApiService.State from) {
         log.info("Terminated at {}. {}", from, metrics());
      }
   }

}