package com.videoplaza.dataflow.pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.pubsub.v1.ProjectSubscriptionName;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.threeten.bp.Duration;

import java.util.Map;

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
   private static final String GCPS_KEY_ATTRIBUTE_DEFAULT = "";

   public static final String GCPS_TIMESTAMP_ATTRIBUTE_CONFIG = "timestamp.attribute";
   private static final String GCPS_TIMESTAMP_ATTRIBUTE_DOC = "The Cloud Pub/Sub message attribute to use as a timestamp for messages published to Kafka.";
   private static final String GCPS_TIMESTAMP_ATTRIBUTE_DEFAULT = "";

   /**
    * See {@link FlowControlSettings.Builder#getMaxOutstandingElementCount()}
    */
   public static final String GCPS_MAX_OUTSTANDING_ELEMENTS_CONFIG = "flow.control.max.outstanding.elements";
   private static final String GCPS_MAX_OUTSTANDING_ELEMENTS_DOC = "Maximum number of outstanding pubsub messages to keep in memory before enforcing flow control. See https://cloud.google.com/pubsub/docs/pull#message-flow-control";
   private static final long GCPS_MAX_OUTSTANDING_ELEMENTS_DEFAULT = 1_000_000L;

   /**
    * See {@link FlowControlSettings.Builder#getMaxOutstandingRequestBytes()}
    */
   public static final String GCPS_MAX_OUTSTANDING_BYTES_CONFIG = "flow.control.max.outstanding.bytes";
   private static final String GCPS_MAX_OUTSTANDING_BYTES_DOC = "Maximum number of outstanding bytes to keep in memory before enforcing flow control. See https://cloud.google.com/pubsub/docs/pull#message-flow-control";
   private static final long GCPS_MAX_OUTSTANDING_BYTES_DEFAULT = 1_000_000_000L;

   public static final String GCPS_SHUTDOWN_TIMEOUT_MS_CONFIG = "shutdown.timeout.ms";
   private static final String GCPS_SHUTDOWN_TIMEOUT_MS_DOC = "Time in ms to wait for all read messages being acknowledged and terminate pubsub subscriber during shutdown.";
   private static final long GCPS_SHUTDOWN_TIMEOUT_MS_DEFAULT = 5_000L;

   public static final String POLL_TIMEOUT_MS_CONFIG = "poll.timeout.ms";
   private static final String POLL_TIMEOUT_MS_DOC = "Time in ms to wait for messages to be read from pubsub before returning from poll method.";
   private static final long POLL_TIMEOUT_MS_DEFAULT = 100;

   /**
    * See {@link com.google.cloud.pubsub.v1.Subscriber.Builder#setMaxAckExtensionPeriod(Duration)}
    */
   public static final String GCPS_MAX_ACK_EXTENSION_PERIOD_MS_CONFIG = "max.ack.extension.period.ms";
   public static final String GCPS_MAX_ACK_EXTENSION_PERIOD_MS_DOC = "Set the maximum period a message ack deadline will be extended. Defaults to one hour. It is recommended to set this value to a reasonable upper bound of the subscriber time to process any message. A zero duration effectively disables auto deadline extensions. See https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/pubsub/v1/Subscriber.Builder.html#setMaxAckExtensionPeriod-org.threeten.bp.Duration-";
   private static final long GCPS_MAX_ACK_EXTENSION_PERIOD_MS_DEFAULT = 3_600_000L;


   /**
    * See {@link com.google.cloud.pubsub.v1.Subscriber.Builder#setParallelPullCount(int)}
    */
   public static final String GCPS_PARALLEL_PULL_COUNT_CONFIG = "parallel.pull.count";
   public static final String GCPS_PARALLEL_PULL_COUNT_DOC = "Sets the number of pullers used to pull messages from the subscription. Defaults to one. See https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/pubsub/v1/Subscriber.Builder.html#setParallelPullCount-int-";
   private static final int GCPS_PARALLEL_PULL_COUNT_DEFAULT = 1;

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
          Importance.LOW,
          GCPS_MAX_OUTSTANDING_ELEMENTS_DOC
      ).define(
          GCPS_MAX_OUTSTANDING_BYTES_CONFIG,
          LONG,
          GCPS_MAX_OUTSTANDING_BYTES_DEFAULT,
          Importance.LOW,
          GCPS_MAX_OUTSTANDING_BYTES_DOC
      ).define(
          GCPS_SHUTDOWN_TIMEOUT_MS_CONFIG,
          LONG,
          GCPS_SHUTDOWN_TIMEOUT_MS_DEFAULT,
          Importance.LOW,
          GCPS_SHUTDOWN_TIMEOUT_MS_DOC
      ).define(
          POLL_TIMEOUT_MS_CONFIG,
          LONG,
          POLL_TIMEOUT_MS_DEFAULT,
          Importance.LOW,
          POLL_TIMEOUT_MS_DOC
      ).define(
          GCPS_MAX_ACK_EXTENSION_PERIOD_MS_CONFIG,
          LONG,
          GCPS_MAX_ACK_EXTENSION_PERIOD_MS_DEFAULT,
          Importance.MEDIUM,
          GCPS_MAX_ACK_EXTENSION_PERIOD_MS_DOC
      ).define(
          GCPS_PARALLEL_PULL_COUNT_CONFIG,
          INT,
          GCPS_PARALLEL_PULL_COUNT_DEFAULT,
          Importance.LOW,
          GCPS_PARALLEL_PULL_COUNT_DOC
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

   public long getTerminationTimeoutMs() {
      return getLong(GCPS_SHUTDOWN_TIMEOUT_MS_CONFIG);
   }

   public long gePollTimeoutMs() {
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
      return Duration.ofSeconds(getLong(GCPS_MAX_ACK_EXTENSION_PERIOD_MS_CONFIG));
   }

   public int getParallelPullCount() {
      return getInt(GCPS_PARALLEL_PULL_COUNT_CONFIG);
   }

}
