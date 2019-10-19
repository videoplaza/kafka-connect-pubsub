package com.videoplaza.dataflow.pubsub;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.partition;
import static com.videoplaza.dataflow.pubsub.TimeUtils.msSince;
import static com.videoplaza.dataflow.pubsub.TimeUtils.msTo;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

/**
 * TODO report metrics
 */
public class PubsubSourceTask extends SourceTask {

   private static final AtomicInteger TASK_COUNT = new AtomicInteger();
   private static final int DUMP_MESSAGE_IN_FLIGHT_BATCH_SIZE = 500;

   private final int id = TASK_COUNT.incrementAndGet();
   private final Logger log = LoggerFactory.getLogger(taskId());

   private final TaskMetrics metrics = new TaskMetrics();
   private volatile Cache<String, MessageInFlight> messages;
   private final Set<PubsubMessage> toBePolled = ConcurrentHashMap.newKeySet();

   private final CountDownLatch state = new CountDownLatch(2);
   private final Lock pollStopLock = new ReentrantLock();
   private final Lock receiveLock = new ReentrantLock();
   private final Condition recordsReceived = receiveLock.newCondition();

   private volatile Subscriber subscriber;
   private volatile PubsubSourceConnectorConfig config;
   private volatile PubsubMessageConverter converter;
   private volatile Sleeper sleeper;

   private final boolean debugLoggingEnabled = log.isDebugEnabled();
   private volatile int debugLogSparsity = PubsubSourceConnectorConfig.DEBUG_LOG_SPARSITY_DEFAULT;

   private String taskId() {
      return PubsubSourceTask.class.getName() + "-" + id;
   }

   @Override public String version() {
      return Version.getVersion();
   }

   @Override public void start(Map<String, String> props) {
      configure(props);
      configure(new SimpleSleeper());
      configure(CacheBuilder.newBuilder()
          .expireAfterWrite(config.getCacheExpirationDeadlineSeconds(), TimeUnit.SECONDS)
          .removalListener(this::onMessageRemoval)
          .build()
      );
      debugLogSparsity = config.getDebugLogSparsity();
      subscribe(newSubscriber());
      log.info("Started");
   }

   PubsubSourceTask configure(Sleeper sleeper) {
      this.sleeper = sleeper;
      return this;
   }

   PubsubSourceTask configure(Cache<String, MessageInFlight> messages) {
      this.messages = messages;
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

   private boolean isDebugEnabled(String messageKey) {
      return debugLoggingEnabled && messageKey.hashCode() % debugLogSparsity == 0;
   }

   void onPubsubMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      String messageKey = converter.getKey(pubsubMessage);

      if (isStopped()) {
         log.error("Received a message after shutdown {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
         subscriber.stopAsync();
         return;
      }

      MessageInFlight mif = messages.asMap().get(pubsubMessage.getMessageId());
      if (mif != null) {
         metrics.onDuplicate();
         if (isDebugEnabled(messageKey)) {
            log.warn("A duplicate for {} received: [{}/{}]. {}. Debug enabled.", mif, converter.getKey(pubsubMessage), converter.getTimestamp(pubsubMessage), metrics());
         } else {
            log.warn("A duplicate for {} received: [{}/{}]. {}", mif, converter.getKey(pubsubMessage), converter.getTimestamp(pubsubMessage), metrics());
         }
         return;
      }

      if (isStopping()) {
         if (config.shouldNackMessagesDuringShutdown()) {
            ackReplyConsumer.nack();
            if (isDebugEnabled(messageKey)) {
               log.debug("Received a message during shutdown. Nacking. {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
            }
         }
         if (isDebugEnabled(messageKey)) {
            log.debug("Received a message during shutdown. Ignoring. {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
         }
         return;
      }

      acceptMessage(pubsubMessage, ackReplyConsumer, messageKey);
   }

   private void acceptMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey) {
      if (isDebugEnabled(messageKey)) {
         log.debug("Received {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
      }

      messages.put(
          pubsubMessage.getMessageId(),
          new MessageInFlight(pubsubMessage.getMessageId(), messageKey, ackReplyConsumer, metrics, log)
      );

      toBePolled.add(pubsubMessage);
      metrics.onReceived();

      receiveLock.lock();
      try {
         recordsReceived.signalAll();
      } finally {
         receiveLock.unlock();
      }
   }

   @Override public List<SourceRecord> poll() {
      messages.cleanUp();
      long timeWaited = waitForMessagesToPoll();
      if (isStopping()) {
         log.info("No poll during shutdown");
         return null;
      }
      try {
         if (pollStopLock.tryLock(config.gePollTimeoutMs(), TimeUnit.MILLISECONDS)) {
            try {
               List<SourceRecord> records = doPoll();
               log.trace("Returning {} records after waiting for {}ms. {}", records.size(), timeWaited, metrics());
               return records.isEmpty() ? null : records;
            } finally {
               pollStopLock.unlock();
            }
         } else {
            log.info("Could not obtain lock in {}ms. Will try later.", config.gePollTimeoutMs());
            return null;
         }
      } catch (InterruptedException e) {
         log.info("Poll was interrupted");
         Thread.currentThread().interrupt();
         return null;
      }
   }

   @Override public void commit() throws InterruptedException {
      if (isStopping()) {
         log.info("Waiting for async shutdown thread to complete. {}", metrics());
         state.await(config.getTotalTerminationTimeoutMs(), TimeUnit.MILLISECONDS);
         log.info("Task is shutdown: {}. {}", isStopped(), metrics());
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
      return new HashSet<>(toBePolled).stream().map(this::convertAndPoll).filter(Objects::nonNull).collect(toList());
   }

   private SourceRecord convertAndPoll(PubsubMessage message) {
      toBePolled.remove(message);
      MessageInFlight m = messages.getIfPresent(message.getMessageId());
      if (m == null) {
         log.warn("Message [{},{}] cannot be polled since it has already expired", message.getMessageId(), converter.getKey(message));
         return null;
      } else {
         m.markAsPolled();
         return converter.convert(message);
      }
   }

   private void waitForPolledMessagesToBeAcknowledged() {
      log.info("Waiting for {} inflight messages to be delivered to kafka. {}ms left. {}", getPolledCount(), config.getInflightAckTimeoutMs(), metrics());
      long start = System.nanoTime();
      long deadline = start + TimeUnit.MILLISECONDS.toNanos(config.getInflightAckTimeoutMs());
      while (msTo(deadline) > 0 && getPolledCount() > 0) {
         sleeper.sleep(100);
      }
      if (getPolledCount() == 0) {
         log.info("All messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), metrics());
      } else {
         log.warn("Not all messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), metrics());
         dumpPolledMessages();
      }
   }

   /**
    * Attempts to nack messages, terminates subscriber and aits at most  {@link PubsubSourceConnectorConfig#SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_CONFIG}.
    */
   private void terrminateSubscriber() {
      log.info("Nacking messages not delivered to kafka and stopping the subscriber. {}", metrics());
      nackReceivedMessages();
      long start = System.nanoTime();
      subscriber.stopAsync();
      try {
         subscriber.awaitTerminated(config.getSubscriberTerminationTimeoutMs(), TimeUnit.MILLISECONDS);
         log.info("Subscriber is terminated in {} ms. {}", msSince(start), metrics());
      } catch (TimeoutException e) {
         log.info("Subscriber was not terminated in {} ms. {}", msSince(start), metrics());
      }
   }

   private boolean isClean() {
      return toBePolled.isEmpty() &&
          messages.asMap().isEmpty() &&
          metrics.getCountersMismatch() == 0 &&
          metrics.getAckLostCount() == 0;
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
      MessageInFlight m = messages.getIfPresent(messageId);
      if (m != null) {
         m.ack(ack);
         if (isDebugEnabled(m.getMessageKey())) {
            log.debug("{} {}. {}", ack ? "Acked" : "Nacked", m, metrics());
         }
         messages.asMap().remove(messageId);
      } else {
         metrics.onAckLost();
         if (isDebugEnabled(record.key() == null ? messageId : record.key().toString())) {
            log.warn("Nothing to ack[{}] for {}/{}. So far: {}. Debug enabled.", ack, messageId, record.key(), metrics.getAckLostCount());
         } else {
            log.warn("Nothing to ack[{}] for {}/{}. So far: {}", ack, messageId, record.key(), metrics.getAckLostCount());
         }
      }
   }

   /**
    * Kicks off async shutdown sequence. See {@link PubsubSourceTask#shutdown()}
    */
   @Override public void stop() {
      log.info("Stopping the task. {}", metrics());
      if (isRunning()) {
         state.countDown();
         new Thread(this::shutdown, taskId() + "-shutdown").start();
      } else {
         log.error("Unexpected state: {}", state.getCount());
      }
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
      pollStopLock.lock();
      try {
         long start = System.nanoTime();

         waitForPolledMessagesToBeAcknowledged();
         terrminateSubscriber();

         if (!isClean()) {
            log.warn("Task is shutdown with unclean state in {}ms. {}", msSince(start), metrics());
         } else {
            log.info("Task is shutdown in {}ms. {}", msSince(start), metrics());
         }
      } finally {
         pollStopLock.unlock();
         state.countDown();
      }
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
      return messages.asMap().values().stream().filter(MessageInFlight::isPolled).count();
   }

   private void dumpPolledMessages() {
      if (debugLoggingEnabled) {
         final AtomicInteger count = new AtomicInteger();
         List<String> messageBatches = messages.asMap().values().stream().filter(MessageInFlight::isPolled).map(Object::toString).collect(Collectors.toList());
         stream(partition(messageBatches, DUMP_MESSAGE_IN_FLIGHT_BATCH_SIZE).spliterator(), false)
             .forEach(messageInFlights ->
                 log.debug("Polled messages batch {} of {}: {}", count.incrementAndGet(), messageBatches.size(), join(",", messageInFlights))
             );
      }
   }

   public TaskMetrics getMetrics() {
      return metrics;
   }

   private String metrics() {
      return metrics + " [" + getPolledCount() + "/" + getToBePolledCount() + "]";
   }

   void onMessageRemoval(RemovalNotification<String, MessageInFlight> removal) {
      if (removal.wasEvicted()) {
         metrics.onEvicted();
         if (isDebugEnabled(removal.getValue().getMessageKey())) {
            log.debug("Evicted {}. {}", removal.getValue(), metrics());
         }
      }
   }

   public boolean isStopped() {
      return state.getCount() == 0;
   }

   public boolean isStopping() {
      return state.getCount() < 2;
   }

   public boolean isRunning() {
      return state.getCount() == 2;
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