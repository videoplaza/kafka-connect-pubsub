package com.videoplaza.dataflow.pubsub.source.task;

import com.codahale.metrics.jmx.JmxReporter;
import com.google.api.core.ApiService;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.Version;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetrics;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import com.videoplaza.dataflow.pubsub.source.task.convert.AvroBatchPubsubMessageConverter;
import com.videoplaza.dataflow.pubsub.source.task.convert.BatchTypePubsubMessageConverter;
import com.videoplaza.dataflow.pubsub.source.task.convert.PubsubAttributeExtractor;
import com.videoplaza.dataflow.pubsub.source.task.convert.PubsubMessageConverter;
import com.videoplaza.dataflow.pubsub.source.task.convert.SinglePubsubMessageConverter;
import com.videoplaza.dataflow.pubsub.source.task.convert.SourceRecordFactory;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.Channel;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.nio.NioEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.time.Clock;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

/**
 * Asynchronously pulls Google Cloud Pub/Sub (https://cloud.google.com/pubsub/docs/pull#asynchronous-pull) subscription by utilizing {@link Subscriber} and implementing {@link SourceTask}.
 * The way task responds to multiple events differs depending on the state task in and is encapsulated in instances of {@link PubsubSourceTaskStrategy}.
 * A task starts with {@link RunningStrategy}, then upon {@link #stop()} request moves to {@link StoppingStrategy} and finally to {@link StoppedStrategy}
 * <p>
 */
public class PubsubSourceTask extends SourceTask implements PubsubSourceTaskState {

   final static AtomicReference<TaskMetricsImpl> METRICS = new AtomicReference<>();
   private final String id = UUID.randomUUID().toString();
   private final AtomicReference<PubsubSourceTaskStrategy> strategy = new AtomicReference<>();
   private final ReentrantLock stopLock = new ReentrantLock();
   private final Class<? extends Channel> pubsubChannelType = Epoll.isAvailable() ? EpollSocketChannel.class : NioSocketChannel.class;

   private volatile SourceMessageMap messages;

   private volatile Subscriber subscriber;
   private volatile PubsubSourceConnectorConfig config;
   private volatile PubsubMessageConverter converter;
   private volatile JmxReporter reporter;

   private volatile PubsubSourceTaskLogger logger;
   private volatile EventLoopGroup pubsubEventLoopGroup;

   @Override public void start(Map<String, String> props) {
      configure(props);
      configure(new SourceMessageMap(config.getCacheExpirationDeadlineSeconds(), this::onEviction));
      configureEventLoopGroup(config.getNettyEventLoopCount());
      configure(newSubscriber());
      init();
      logger.info("Started.");
   }

   PubsubSourceTask init() {
      moveTo(new RunningStrategy(this));
      return this;
   }

   PubsubSourceTask configure(SourceMessageMap messages) {
      this.messages = messages;
      return this;
   }

   PubsubSourceTask configureEventLoopGroup(int nThreads) {
      this.pubsubEventLoopGroup = newEventLoopGroup(nThreads);
      return this;
   }

   static EventLoopGroup newEventLoopGroup(int nThreads) {
      return Epoll.isAvailable() ? new EpollEventLoopGroup(nThreads) : new NioEventLoopGroup(nThreads);
   }

   private Subscriber newSubscriber() {
      logger.info("Using netty channel type: {}", pubsubChannelType);
      if (pubsubEventLoopGroup == null) {
         throw new IllegalStateException("Cannot create a subscriber without Netty event loop group. Configure event loop group first!");
      }
      InstantiatingGrpcChannelProvider channelProvider = SubscriberStubSettings.defaultGrpcTransportProviderBuilder().setChannelConfigurator(input -> {
         NettyChannelBuilder nettyChannelBuilder = (NettyChannelBuilder) input;
         nettyChannelBuilder.eventLoopGroup(pubsubEventLoopGroup);
         nettyChannelBuilder.channelType(pubsubChannelType);
         return nettyChannelBuilder;
      }).build();

      Subscriber newSubscriber = Subscriber.newBuilder(config.getProjectSubscription(), this::onPubsubMessageReceived)
         .setFlowControlSettings(config.getFlowControlSettings())
         .setMaxAckExtensionPeriod(config.getMaxAckExtensionPeriod())
         .setParallelPullCount(config.getParallelPullCount())

         .setChannelProvider(channelProvider)
         .setEndpoint(config.getEndpoint())
         .build();

      newSubscriber.addListener(new LoggingSubscriberListener(), Executors.newSingleThreadExecutor());

      return newSubscriber;
   }

   void onPubsubMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      SourceMessage current = messages.get(pubsubMessage.getMessageId());
      if (current != null) {
         strategy.get().onDuplicateReceived(pubsubMessage, current);
         logger.log("Received a duplicate. {}/{}", current, pubsubMessage);
         getMetrics().onMessageDuplicate(current.getCreatedMs(), current.getReceivedMs());
      } else {
         SourceMessage m = strategy.get().onNewMessageReceived(pubsubMessage, ackReplyConsumer);

         logger.log("Received new message. {}/{}", m, pubsubMessage);
      }
   }

   PubsubSourceTask configure(Subscriber subscriber) {
      this.subscriber = subscriber;
      return this;
   }

   PubsubSourceTask configure(Map<String, String> props) {
      config = new PubsubSourceConnectorConfig(props);
      PubsubAttributeExtractor attributeExtractor = config.getPubsubAttributeExtractor();
      logger = new PubsubSourceTaskLogger(id, attributeExtractor, this, config.getDebugLogSparsity());
      boolean metricsSet = METRICS.compareAndSet(null, new TaskMetricsImpl(Clock.systemUTC(), config.getHistogramUpdateIntervalMs()));
      logger.info("Configure task. Metrics set: {}", metricsSet);
      SourceRecordFactory recordFactory = new SourceRecordFactory(config.getSubscription(), config.getTopic(), config.getPayloadVerifier());
      converter = new BatchTypePubsubMessageConverter(
         new SinglePubsubMessageConverter(recordFactory, attributeExtractor, getMetrics(), logger),
         new AvroBatchPubsubMessageConverter(recordFactory, attributeExtractor, getMetrics(), logger),
         config.getBatchAttribute()
      );

      reporter = JmxReporter.forRegistry(getMetrics().getMetricRegistry())
         .inDomain("kafka.connect.pubsub")
         .createsObjectNamesWith(this::metricName)
         .build();
      return this;
   }

   private ObjectName metricName(String type, String domain, String name) {
      Hashtable<String, String> properties = new Hashtable<>();
      properties.put("name", name);
      properties.put("type", type);

      try {
         return new ObjectName(domain, properties);
      } catch (MalformedObjectNameException e) {
         throw new RuntimeException(e);
      }
   }

   @Override public PubsubSourceConnectorConfig getConfig() {
      return config;
   }

   @Override public String version() {
      return Version.getVersion();
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
      logger.log("Commit", record);
      SourceMessage m = strategy.get().commitRecord(record);
      if (m == null) {
         logger.warn("Nothing to ack for {}.", record);
      } else if (m.allRecordsDelivered()) {
         logger.log("Acked {}.", m);
      }
   }

   @Override public void stop() {
      logger.info("Stopping the task.");
      stopLock.lock();
      try {
         strategy.get().stop();
      } finally {
         stopLock.unlock();
      }
      logger.info("Stopped the task.");
   }

   @Override public PubsubSourceTaskLogger getLogger() {
      return logger;
   }

   @Override public Subscriber getSubscriber() {
      return subscriber;
   }

   public TaskMetricsImpl getMetrics() {
      return METRICS.get();
   }

   public String toString() {
      return "task-" + id + "|" + strategy.get() + ":" + getMetrics() + " [" + messages + "]";
   }

   @Override public PubsubMessageConverter getConverter() {
      return converter;
   }

   @Override public boolean isClean() {
      return messages.isEmpty() && getMetrics().getMessageReceivedButNotAcked() == 0 && getMetrics().getRecordAckLostCount() == 0;
   }

   @Override public SourceMessageMap getMessages() {
      return messages;
   }

   @Override public void moveTo(PubsubSourceTaskStrategy s) {
      logger.info("Moving {} -> {}", strategy.get(), s);
      strategy.set(s);
      s.init();
   }

   @Override public JmxReporter getJmxReporter() {
      return reporter;
   }

   boolean isStopped() {
      return strategy.get() instanceof StoppedStrategy;
   }

   void onEviction(SourceMessage m) {
      getMetrics().onMessageEvicted(m.getReceivedMs());
      logger.log("Evicted {}.", m);
   }

   class LoggingSubscriberListener extends Subscriber.Listener {
      @Override public void failed(Subscriber.State from, Throwable failure) {
         logger.error(format("Failed %s.", from), failure);
      }

      @Override public void stopping(ApiService.State from) {
         logger.info("Stopping at {}. {}", from, PubsubSourceTask.this);
      }

      @Override public void terminated(ApiService.State from) {
         logger.info("Terminated at {}. {}", from, PubsubSourceTask.this);
      }
   }

   public static TaskMetrics metrics() {
      return METRICS.get();
   }

   public EventLoopGroup getPubsubEventLoopGroup() {
      return pubsubEventLoopGroup;
   }
}