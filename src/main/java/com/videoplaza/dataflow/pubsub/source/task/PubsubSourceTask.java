package com.videoplaza.dataflow.pubsub.source.task;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Asynchronously pulls Google Cloud Pub/Sub (https://cloud.google.com/pubsub/docs/pull#asynchronous-pull) subscription by utilizing {@link Subscriber} and implementing {@link SourceTask}.
 * The way task responds to multiple events differs depending on the state task in and is encapsulated in instances of {@link PubsubSourceTaskStrategy}.
 * A task starts with {@link RunningStrategy}, then upon {@link #stop()} request moves to {@link StoppingStrategy} and finally to {@link StoppedStrategy}
 * <p>
 * TODO add metrics
 */
public class PubsubSourceTask extends SourceTask implements PubsubSourceTaskState {

   private static final AtomicInteger TASK_COUNT = new AtomicInteger();

   private final int id = TASK_COUNT.incrementAndGet();
   private final Logger log = LoggerFactory.getLogger(PubsubSourceTask.class.getName() + "-" + id);
   private final TaskMetrics metrics = new TaskMetrics(id);
   private final boolean debugLoggingEnabled = log.isDebugEnabled();
   private final AtomicReference<PubsubSourceTaskStrategy> strategy = new AtomicReference<>();
   private final ReentrantLock stopLock = new ReentrantLock();

   private volatile MessageMap messages;

   private volatile Subscriber subscriber;
   private volatile PubsubSourceConnectorConfig config;
   private volatile PubsubMessageConverter converter;

   private volatile int debugLogSparsity = PubsubSourceConnectorConfig.DEBUG_LOG_SPARSITY_DEFAULT;

   @Override public void start(Map<String, String> props) {
      configure(props);
      configure(new MessageMap(config.getCacheExpirationDeadlineSeconds(), this::onEviction));
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
          .setEndpoint(config.getEndpoint())
          .build();

      newSubscriber.addListener(new LoggingSubscriberListener(), Executors.newSingleThreadExecutor());

      return newSubscriber;
   }

   void onPubsubMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      String messageKey = converter.getKey(pubsubMessage);
      Message current = messages.get(pubsubMessage.getMessageId());
      if (current != null) {
         strategy.get().onDuplicateReceived(pubsubMessage, messageKey, current);
      } else {
         strategy.get().onNewMessageReceived(pubsubMessage, ackReplyConsumer, messageKey);
      }
   }

   PubsubSourceTask subscribe(Subscriber subscriber) {
      this.subscriber = subscriber;
      moveTo(new RunningStrategy(this));
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

   @Override public PubsubSourceConnectorConfig getConfig() {
      return config;
   }

   @Override public String version() {
      return Version.getVersion();
   }

   @Override public boolean isDebugEnabled(String messageKey) {
      return debugLoggingEnabled && messageKey.hashCode() % debugLogSparsity == 0;
   }

   @Override public List<SourceRecord> poll() {
      try {
         if (stopLock.tryLock(config.getPollTimeoutMs(), TimeUnit.MILLISECONDS)) {
            try {
               return strategy.get().poll();
            } finally {
               stopLock.unlock();
            }
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }
      return null;
   }

   @Override public void commit() throws InterruptedException {
      strategy.get().commit();
   }

   @Override public void commitRecord(SourceRecord record) {
      strategy.get().commitRecord(record);
   }

   @Override public void stop() {
      log.info("Stopping the task. {}", this);
      stopLock.lock();
      try {
         strategy.get().stop();
      } finally {
         stopLock.unlock();
      }
      log.info("Stopped the task. {}", this);
   }

   @Override public Logger getLogger() {
      return log;
   }

   @Override public Subscriber getSubscriber() {
      return subscriber;
   }

   public TaskMetrics getMetrics() {
      return metrics;
   }

   public String toString() {
      return "PubsubTask-" + id + ":" + metrics + " [" + messages + "]";
   }

   @Override public PubsubMessageConverter getConverter() {
      return converter;
   }

   @Override public boolean isClean() {
      return messages.isEmpty() && metrics.getCountersMismatch() == 0 && metrics.getAckLostCount() == 0;
   }

   @Override public MessageMap getMessages() {
      return messages;
   }

   @Override public void moveTo(PubsubSourceTaskStrategy s) {
      log.info("Moving {} -> {}", strategy.get(), s);
      strategy.set(s);
      s.init();
   }

   boolean isStopped() {
      return strategy.get() instanceof StoppedStrategy;
   }

   void onEviction(Message m) {
      metrics.onEvicted();

      if (isDebugEnabled(m.getMessageKey())) {
         log.debug("Evicted {}. {}", m, this);
      }
   }

   class LoggingSubscriberListener extends Subscriber.Listener {
      @Override public void failed(Subscriber.State from, Throwable failure) {
         log.error("Failed {}.", from, failure);
      }

      @Override public void stopping(ApiService.State from) {
         log.info("Stopping at {}. {}", from, PubsubSourceTask.this);
      }

      @Override public void terminated(ApiService.State from) {
         log.info("Terminated at {}. {}", from, PubsubSourceTask.this);
      }
   }

}