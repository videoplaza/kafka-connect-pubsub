package com.videoplaza.dataflow.pubsub.source.task;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.Version;
import com.videoplaza.dataflow.pubsub.util.TaskMetrics;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.videoplaza.dataflow.pubsub.util.TimeUtils.msSince;
import static com.videoplaza.dataflow.pubsub.util.TimeUtils.msTo;

/**
 * TODO report metrics
 */
public class PubsubSourceTask extends SourceTask {

   private static final AtomicInteger TASK_COUNT = new AtomicInteger();

   private final int id = TASK_COUNT.incrementAndGet();
   private final Logger log = LoggerFactory.getLogger(taskId());
   private final TaskMetrics metrics = new TaskMetrics();
   private final boolean debugLoggingEnabled = log.isDebugEnabled();
   private final AtomicReference<Strategy> strategy = new AtomicReference<>();
   private final ReentrantLock shutdownLock = new ReentrantLock();

   private volatile MessageMap messages;

   private volatile Subscriber subscriber;
   private volatile PubsubSourceConnectorConfig config;
   private volatile PubsubMessageConverter converter;

   private volatile int debugLogSparsity = PubsubSourceConnectorConfig.DEBUG_LOG_SPARSITY_DEFAULT;

   private String taskId() {
      return PubsubSourceTask.class.getName() + "-" + id;
   }

   @Override public String version() {
      return Version.getVersion();
   }

   @Override public void start(Map<String, String> props) {
      configure(props);
      configure(new MessageMap(
          CacheBuilder.newBuilder()
              .expireAfterWrite(config.getCacheExpirationDeadlineSeconds(), TimeUnit.SECONDS)
              .removalListener(this::onMessageRemoval)
              .build()
      ));
      subscribe(newSubscriber());
      log.info("Started");
   }

   PubsubSourceTask configure(MessageMap messages) {
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
      moveTo(new Running());
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
      debugLogSparsity = config.getDebugLogSparsity();
      return this;
   }

   private boolean isDebugEnabled(String messageKey) {
      return debugLoggingEnabled && messageKey.hashCode() % debugLogSparsity == 0;
   }

   void onPubsubMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      String messageKey = converter.getKey(pubsubMessage);
      Message current = messages.get(pubsubMessage.getMessageId());
      if (current != null) {
         strategy.get().onDuplicate(pubsubMessage, messageKey, current);
      } else {
         strategy.get().onMessage(pubsubMessage, ackReplyConsumer, messageKey);
      }
   }

   void onDuplicate(PubsubMessage pubsubMessage, String messageKey, Message mif) {
      metrics.onDuplicate();

      if (isDebugEnabled(messageKey)) {
         log.debug("A duplicate for {} received: [{}/{}]. {}.", mif, messageKey, converter.getTimestamp(pubsubMessage), metrics());
      }
   }

   @Override public List<SourceRecord> poll() {
      return strategy.get().poll();
   }

   @Override public void commit() throws InterruptedException {
      strategy.get().commit();
   }

   @Override public void commitRecord(SourceRecord record) {
      commitRecord(record, true);
   }

   private void commitRecord(SourceRecord record, boolean ack) {
      String messageId = (String) record.sourceOffset().get(config.getSubscription());
      Message message = messages.get(messageId);
      if (message != null) {
         message.ack(ack);
         messages.remove(messageId);

         if (isDebugEnabled(record.key().toString())) {
            log.debug("{} {}. {}", ack ? "Acked" : "Nacked", message, metrics());
         }
      } else {
         metrics.onAckLost();

         log.warn("Nothing to ack[{}] for {}/{}. So far: {}. Debug enabled: {}.",
             ack,
             messageId,
             record.key(),
             metrics.getAckLostCount(),
             (isDebugEnabled(record.key() == null ? messageId : record.key().toString()))
         );

      }
   }

   @Override public void stop() {
      log.info("Stopping the task. {}", metrics());
      strategy.get().stop();
   }

   public TaskMetrics getMetrics() {
      return metrics;
   }

   private String metrics() {
      return metrics + " [" + messages + "]";
   }

   void onMessageRemoval(RemovalNotification<String, Message> removal) {
      if (removal.wasEvicted()) {
         metrics.onEvicted();

         if (isDebugEnabled(removal.getValue().getMessageKey())) {
            log.debug("Evicted {}. {}", removal.getValue(), metrics());
         }
      }
   }

   void moveTo(Strategy s) {
      strategy.set(s);
      s.init();
   }

   boolean isStopped() {
      return strategy.get() instanceof PubsubSourceTask.Stopped;
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

   interface Strategy {

      void init();

      void onMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey);

      void onDuplicate(PubsubMessage pubsubMessage, String messageKey, Message current);

      List<SourceRecord> poll();

      void commit() throws InterruptedException;

      void stop();

   }

   class Running implements Strategy {

      private final Lock receiveLock = new ReentrantLock();
      private final Condition recordsReceived = receiveLock.newCondition();

      @Override public void init() {
      }

      @Override
      public void onMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey) {
         if (isDebugEnabled(messageKey)) {
            log.debug("Received {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
         }

         messages.put(new Message(pubsubMessage.getMessageId(), converter.convert(pubsubMessage), ackReplyConsumer, metrics));

         metrics.onReceived();

         receiveLock.lock();
         try {
            recordsReceived.signalAll();
         } finally {
            receiveLock.unlock();
         }
      }

      @Override public void onDuplicate(PubsubMessage pubsubMessage, String messageKey, Message current) {
         PubsubSourceTask.this.onDuplicate(pubsubMessage, messageKey, current);
      }

      @Override public List<SourceRecord> poll() {
         long timeWaited = waitForMessagesToPoll();
         try {
            if (shutdownLock.tryLock(config.gePollTimeoutMs(), TimeUnit.MILLISECONDS)) {
               try {
                  List<SourceRecord> records = messages.poll();
                  log.trace("Returning {} records after waiting for {}ms. {}", records.size(), timeWaited, metrics());
                  return records.isEmpty() ? null : records;
               } finally {
                  shutdownLock.unlock();
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


      @Override public void commit() {
      }

      @Override public void stop() {
         moveTo(new Stopping());
      }
   }

   class Stopping implements Strategy {

      private final CountDownLatch complete = new CountDownLatch(1);

      /**
       * Kicks off async shutdown sequence. See {@link PubsubSourceTask.Stopping#shutdown()}
       */
      @Override public void init() {
         new Thread(this::shutdown, taskId() + "-shutdown").start();
      }

      @Override
      public void onMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey) {
         if (config.shouldNackMessagesDuringShutdown()) {
            ackReplyConsumer.nack();
            if (isDebugEnabled(messageKey)) {
               log.debug("Received a message during shutdown. Nacking. {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
            }
         }
         if (isDebugEnabled(messageKey)) {
            log.debug("Received a message during shutdown. Ignoring. {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
         }
      }

      @Override public void onDuplicate(PubsubMessage pubsubMessage, String messageKey, Message current) {
         PubsubSourceTask.this.onDuplicate(pubsubMessage, messageKey, current);
      }

      @Override public List<SourceRecord> poll() {
         log.info("No poll while stopping");
         return null;
      }

      @Override public void commit() throws InterruptedException {
         log.info("Waiting for async shutdown thread to complete. {}", metrics());
         complete.await(config.getTotalTerminationTimeoutMs(), TimeUnit.MILLISECONDS);
         log.info("Task is shutdown. {}", metrics());
      }

      @Override public void stop() {
         log.error("Stopping already being stopped task {}", metrics());
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
         shutdownLock.lock();
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
            shutdownLock.unlock();
            complete.countDown();
            moveTo(new Stopped());
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
         return messages.isEmpty() &&
             metrics.getCountersMismatch() == 0 &&
             metrics.getAckLostCount() == 0;
      }

      private void nackReceivedMessages() {
         List<SourceRecord> records = messages.poll();
         log.debug("Nacking {} records. {}", records.size(), metrics());
         records.forEach(r -> commitRecord(r, false));
      }

      private void waitForPolledMessagesToBeAcknowledged() {
         log.info("Waiting for {} inflight messages to be delivered to kafka. {}ms left. {}", messages.getPolledCount(), config.getInflightAckTimeoutMs(), metrics());
         long start = System.nanoTime();
         long deadline = start + TimeUnit.MILLISECONDS.toNanos(config.getInflightAckTimeoutMs());
         try {
            while (msTo(deadline) > 0 && messages.getPolledCount() > 0) {
               TimeUnit.MILLISECONDS.sleep(100);
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for inflight messages. {} messages can have duplicates.", messages.getPolledCount());
         }
         if (messages.getPolledCount() == 0) {
            log.info("All messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), metrics());
         } else {
            log.warn("Not all messages delivered to kafka are acknowledged in {} ms. {}", msSince(start), metrics());
            messages.dump(log);
         }
      }
   }

   class Stopped implements Strategy {

      @Override public void init() {
      }

      @Override
      public void onMessage(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey) {
         log.error("Received a message after shutdown {}/{}. {}", pubsubMessage.getMessageId(), messageKey, metrics());
         subscriber.stopAsync();
      }

      @Override public void onDuplicate(PubsubMessage pubsubMessage, String messageKey, Message current) {
         onMessage(pubsubMessage, null, messageKey);
      }

      @Override public List<SourceRecord> poll() {
         log.error("Polling already stopped task {}", metrics());
         return null;
      }

      @Override public void commit() {
         log.error("Committing already stopped task {}", metrics());
      }

      @Override public void stop() {
         log.error("Stopping already stopped task {}", metrics());
      }
   }
}