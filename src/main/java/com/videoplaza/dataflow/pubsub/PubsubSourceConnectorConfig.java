package com.videoplaza.dataflow.pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.videoplaza.dataflow.pubsub.source.task.convert.PubsubAttributeExtractor;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.threeten.bp.Duration;

import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class PubsubSourceConnectorConfig extends AbstractConfig {
   public static final String KAFKA_TOPIC_CONFIG = "topic";
   private static final String KAFKA_TOPIC_DOC = "The topic in Kafka which will receive messages that were pulled from Cloud Pub/Sub.";

   public static final String GCPS_PROJECT_CONFIG = "project";
   private static final String GCPS_PROJECT_DOC = "The project containing the subscription from which to pull messages.";

   public static final String GCPS_SUBSCRIPTION_CONFIG = "subscription";
   private static final String GCPS_SUBSCRIPTION_DOC = "The name of the Cloud Pub/Sub subscription.";

   public static final String GCPS_KEY_ATTRIBUTE_CONFIG = "key.attribute";
   private static final String GCPS_KEY_ATTRIBUTE_DOC = "The Cloud Pub/Sub message attribute to use as a key for messages published to Kafka.";
   public static final String GCPS_KEY_ATTRIBUTE_DEFAULT = "";

   public static final String GCPS_TIMESTAMP_ATTRIBUTE_CONFIG = "timestamp.attribute";
   private static final String GCPS_TIMESTAMP_ATTRIBUTE_DOC = "The Cloud Pub/Sub message attribute to use as a timestamp for messages published to Kafka.";
   public static final String GCPS_TIMESTAMP_ATTRIBUTE_DEFAULT = "";

   public static final String GCPS_BATCH_TYPE_ATTRIBUTE_CONFIG = "batch.type.attribute";
   private static final String GCPS_BATCH_TYPE_ATTRIBUTE_DOC = "The Cloud Pub/Sub message attribute to use to indicate message batching mode. Batching currently supports only 1 value `avro`.";
   public static final String GCPS_BATCH_TYPE_ATTRIBUTE_DEFAULT = "batch.type";
   public static final String AVRO_BATCH_TYPE = "avro";

   public static final String MAX_NUMBER_CONVERSION_FAILURES_CONFIG = "max.conversion.failures";
   private static final String MAX_NUMBER_CONVERSION_FAILURES_DOC = "Maximum number of conversion failures to ignore. ";
   public static final long MAX_NUMBER_CONVERSION_FAILURES_DEFAULT = 0;

   /**
    * See {@link FlowControlSettings.Builder#getMaxOutstandingElementCount()}
    */
   public static final String GCPS_MAX_OUTSTANDING_ELEMENTS_CONFIG = "flow.control.max.outstanding.elements";
   private static final String GCPS_MAX_OUTSTANDING_ELEMENTS_DOC = "Maximum number of outstanding pubsub messages to keep in memory before enforcing flow control. See https://cloud.google.com/pubsub/docs/pull#message-flow-control";
   public static final Long GCPS_MAX_OUTSTANDING_ELEMENTS_DEFAULT = 10_000L;

   /**
    * See {@link FlowControlSettings.Builder#getMaxOutstandingRequestBytes()}
    */
   public static final String GCPS_MAX_OUTSTANDING_BYTES_CONFIG = "flow.control.max.outstanding.bytes";
   private static final String GCPS_MAX_OUTSTANDING_BYTES_DOC = "Maximum number of outstanding bytes to keep in memory before enforcing flow control. See https://cloud.google.com/pubsub/docs/pull#message-flow-control";
   public static final Long GCPS_MAX_OUTSTANDING_BYTES_DEFAULT = 1_000_000_000L;

   public static final String SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_CONFIG = "shutdown.inflight.ack.timeout.ms";
   private static final String SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_DOC = "Time in milliseconds to wait for all messages that have already been polled by kafka connect framework to be acknowledged during shutdown.";
   public static final Long SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_DEFAULT = 3_000L;

   public static final String SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_CONFIG = "shutdown.terminate.subscriber.timeout.ms";
   private static final String SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_DOC = "Time in milliseconds to wait for Cloud Pub/Sub subscriber to terminate during shutdown.";
   public static final Long SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_DEFAULT = 6_000L;

   public static final String POLL_TIMEOUT_MS_CONFIG = "poll.timeout.ms";
   private static final String POLL_TIMEOUT_MS_DOC = "Time in milliseconds to wait for messages to be read from pubsub before returning from poll method.";
   public static final Long POLL_TIMEOUT_MS_DEFAULT = 100L;

   /**
    * See {@link com.google.cloud.pubsub.v1.Subscriber.Builder#setMaxAckExtensionPeriod(Duration)}
    */
   public static final String GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_CONFIG = "max.ack.extension.period.sec";
   private static final String GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_DOC = "Maximum period in seconds a message ack deadline will be extended. Defaults 5 minutes instead of 1 hour default in Google Cloud Pubs/Sub. It is recommended to set this value to a reasonable upper bound of the subscriber time to process any message. A zero duration effectively disables auto deadline extensions. See https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/pubsub/v1/Subscriber.Builder.html#setMaxAckExtensionPeriod-org.threeten.bp.Duration-";
   public static final long GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_DEFAULT = 300L;


   public static final String CACHE_EXPIRATION_DEADLINE_SEC_CONFIG = "cache.expiration.deadline.sec";
   private static final String CACHE_EXPIRATION_DEADLINE_SEC_DOC = "Maximum period in seconds a message is kept in internal cache if not delivered to kafka. Messages with same ids are considered duplicates and discarded. This should be greater than `delivery.timeout.ms` setting for kafka producer and less than `max.ack.extension.period.sec` in Cloud Pub/Sub";
   public static final long CACHE_EXPIRATION_DEADLINE_SEC_DEFAULT = GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_DEFAULT - 10;

   /**
    * See {@link com.google.cloud.pubsub.v1.Subscriber.Builder#setParallelPullCount(int)}
    */
   public static final String GCPS_PARALLEL_PULL_COUNT_CONFIG = "parallel.pull.count";
   private static final String GCPS_PARALLEL_PULL_COUNT_DOC = "Number of pullers used to pull messages from the subscription. Defaults to one. See https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/pubsub/v1/Subscriber.Builder.html#setParallelPullCount-int-";
   public static final int GCPS_PARALLEL_PULL_COUNT_DEFAULT = 1;

   public static final String DEBUG_LOG_SPARSITY_CONFIG = "debug.log.sparsity";
   private static final String DEBUG_LOG_SPARSITY_DOC = "Number of messages to skip per single log line. Does have any effect if debug logging is disabled";
   public static final int DEBUG_LOG_SPARSITY_DEFAULT = 1;

   public static final String NACK_MESSAGES_DURING_SHUTDOWN_CONFIG = "nack.messages.during.shutdown";
   private static final String NACK_MESSAGES_DURING_SHUTDOWN_DOC = "If true a message received from pubsub during shutdown will be nacked immediately, with subsequent redelivery. Minimizes delays cause by stopping the connector, but increases redelivery rates during shutdown";
   public static final boolean NACK_MESSAGES_DURING_SHUTDOWN_DEFAULT = false;

   public static final String GCPS_ENDPOINT_CONFIG = "pubsub.endpoint";
   private static final String GCPS_ENDPOINT_DOC = "Cloud Pub/Sub endpoint";
   public static final String GCPS_ENDPOINT_DEFAULT = SubscriberStubSettings.getDefaultEndpoint();

   public static final String HISTOGRAM_UPDATE_INTERVAL_MS_CONFIG = "histogram.update.interval.ms";
   private static final String HISTOGRAM_UPDATE_INTERVAL_MS_DOC = "Histogram data collection interval in milliseconds.";
   public static final Long HISTOGRAM_UPDATE_INTERVAL_MS_DEFAULT = 10_000L;

   public static final ConfigDef CONFIG = configDef();

   public PubsubSourceConnectorConfig(Map<?, ?> originals) {
      super(CONFIG, originals);
   }

   private static ConfigDef configDef() {
      return new ConfigDef().define(
          KAFKA_TOPIC_CONFIG,
          STRING,
          Importance.HIGH,
          KAFKA_TOPIC_DOC
      ).define(
          GCPS_PROJECT_CONFIG,
          STRING,
          Importance.HIGH,
          GCPS_PROJECT_DOC
      ).define(
          GCPS_SUBSCRIPTION_CONFIG,
          STRING,
          Importance.HIGH,
          GCPS_SUBSCRIPTION_DOC
      ).define(
          GCPS_KEY_ATTRIBUTE_CONFIG,
          STRING,
          GCPS_KEY_ATTRIBUTE_DEFAULT,
          Importance.MEDIUM,
          GCPS_KEY_ATTRIBUTE_DOC
      ).define(
          GCPS_TIMESTAMP_ATTRIBUTE_CONFIG,
          STRING,
          GCPS_TIMESTAMP_ATTRIBUTE_DEFAULT,
          Importance.MEDIUM,
          GCPS_TIMESTAMP_ATTRIBUTE_DOC
      ).define(
          GCPS_MAX_OUTSTANDING_ELEMENTS_CONFIG,
          LONG,
          GCPS_MAX_OUTSTANDING_ELEMENTS_DEFAULT,
          Importance.MEDIUM,
          GCPS_MAX_OUTSTANDING_ELEMENTS_DOC
      ).define(
          GCPS_MAX_OUTSTANDING_BYTES_CONFIG,
          LONG,
          GCPS_MAX_OUTSTANDING_BYTES_DEFAULT,
          Importance.MEDIUM,
          GCPS_MAX_OUTSTANDING_BYTES_DOC
      ).define(
          SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_CONFIG,
          LONG,
          SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_DEFAULT,
          Importance.LOW,
          SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_DOC
      ).define(
          SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_CONFIG,
          LONG,
          SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_DEFAULT,
          Importance.LOW,
          SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_DOC
      ).define(
          POLL_TIMEOUT_MS_CONFIG,
          LONG,
          POLL_TIMEOUT_MS_DEFAULT,
          Importance.LOW,
          POLL_TIMEOUT_MS_DOC
      ).define(
          GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_CONFIG,
          LONG,
          GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_DEFAULT,
          Importance.HIGH,
          GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_DOC
      ).define(
          GCPS_PARALLEL_PULL_COUNT_CONFIG,
          INT,
          GCPS_PARALLEL_PULL_COUNT_DEFAULT,
          Importance.LOW,
          GCPS_PARALLEL_PULL_COUNT_DOC
      ).define(
          CACHE_EXPIRATION_DEADLINE_SEC_CONFIG,
          LONG,
          CACHE_EXPIRATION_DEADLINE_SEC_DEFAULT,
          Importance.HIGH,
          CACHE_EXPIRATION_DEADLINE_SEC_DOC
      ).define(
          DEBUG_LOG_SPARSITY_CONFIG,
          INT,
          DEBUG_LOG_SPARSITY_DEFAULT,
          Importance.LOW,
          DEBUG_LOG_SPARSITY_DOC
      ).define(
          NACK_MESSAGES_DURING_SHUTDOWN_CONFIG,
          BOOLEAN,
          NACK_MESSAGES_DURING_SHUTDOWN_DEFAULT,
          Importance.LOW,
          NACK_MESSAGES_DURING_SHUTDOWN_DOC
      ).define(
          GCPS_ENDPOINT_CONFIG,
          STRING,
          GCPS_ENDPOINT_DEFAULT,
          Importance.LOW,
          GCPS_ENDPOINT_DOC
      ).define(
          GCPS_BATCH_TYPE_ATTRIBUTE_CONFIG,
          STRING,
          GCPS_BATCH_TYPE_ATTRIBUTE_DEFAULT,
          Importance.LOW,
          GCPS_BATCH_TYPE_ATTRIBUTE_DOC
      ).define(
          MAX_NUMBER_CONVERSION_FAILURES_CONFIG,
          LONG,
          MAX_NUMBER_CONVERSION_FAILURES_DEFAULT,
          Importance.LOW,
          MAX_NUMBER_CONVERSION_FAILURES_DOC
      ).define(
          HISTOGRAM_UPDATE_INTERVAL_MS_CONFIG,
          LONG,
          HISTOGRAM_UPDATE_INTERVAL_MS_DEFAULT,
          Importance.HIGH,
          HISTOGRAM_UPDATE_INTERVAL_MS_DOC
      );
   }

   public String getProject() {
      return getString(GCPS_PROJECT_CONFIG);
   }

   public String getSubscription() {
      return getString(GCPS_SUBSCRIPTION_CONFIG);
   }

   public String getTopic() {
      return getString(KAFKA_TOPIC_CONFIG);
   }

   public String getKeyAttribute() {
      return getString(GCPS_KEY_ATTRIBUTE_CONFIG);
   }

   public String getTimestampAttribute() {
      return getString(GCPS_TIMESTAMP_ATTRIBUTE_CONFIG);
   }

   public long getMaxOutstandingElements() {
      return getLong(GCPS_MAX_OUTSTANDING_ELEMENTS_CONFIG);
   }

   public long getMaxOutstandingBytes() {
      return getLong(GCPS_MAX_OUTSTANDING_BYTES_CONFIG);
   }

   public long getSubscriberTerminationTimeoutMs() {
      return getLong(SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_CONFIG);
   }

   public long getInflightAckTimeoutMs() {
      return getLong(SHUTDOWN_INFLIGHT_ACK_TIMEOUT_MS_CONFIG);
   }

   public long getPollTimeoutMs() {
      return getLong(POLL_TIMEOUT_MS_CONFIG);
   }

   public ProjectSubscriptionName getProjectSubscription() {
      return ProjectSubscriptionName.of(getProject(), getSubscription());
   }

   public FlowControlSettings getFlowControlSettings() {
      return FlowControlSettings.newBuilder()
          .setMaxOutstandingElementCount(getMaxOutstandingElements())
          .setMaxOutstandingRequestBytes(getMaxOutstandingBytes())
          .setLimitExceededBehavior(FlowController.LimitExceededBehavior.Block)
          .build();
   }

   public Duration getMaxAckExtensionPeriod() {
      return Duration.ofSeconds(getLong(GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_CONFIG));
   }

   public long getCacheExpirationDeadlineSeconds() {
      return getLong(CACHE_EXPIRATION_DEADLINE_SEC_CONFIG);
   }

   public int getDebugLogSparsity() {
      return getInt(DEBUG_LOG_SPARSITY_CONFIG);
   }

   public int getParallelPullCount() {
      return getInt(GCPS_PARALLEL_PULL_COUNT_CONFIG);
   }

   public boolean shouldNackMessagesDuringShutdown() {
      return getBoolean(NACK_MESSAGES_DURING_SHUTDOWN_CONFIG);
   }

   public long getTotalTerminationTimeoutMs() {
      return getSubscriberTerminationTimeoutMs() + getInflightAckTimeoutMs() + 2000;
   }

   public String getEndpoint() {
      return getString(GCPS_ENDPOINT_CONFIG);
   }

   public String getBatchAttribute() {
      return getString(GCPS_BATCH_TYPE_ATTRIBUTE_CONFIG);
   }

   public PubsubAttributeExtractor getPubsubAttributeExtractor() {
      return new PubsubAttributeExtractor(getKeyAttribute(), getTimestampAttribute());
   }

   public long getHistogramUpdateIntervalMs() {
      return getLong(HISTOGRAM_UPDATE_INTERVAL_MS_CONFIG);
   }

   public long getMaxNumberOfConversionFailures() {
      return getLong(MAX_NUMBER_CONVERSION_FAILURES_CONFIG);
   }
}
