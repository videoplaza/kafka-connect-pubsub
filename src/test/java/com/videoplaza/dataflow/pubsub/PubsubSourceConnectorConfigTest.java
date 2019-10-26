package com.videoplaza.dataflow.pubsub;


import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.pubsub.v1.ProjectSubscriptionName;
import org.junit.Before;
import org.junit.Test;
import org.threeten.bp.Duration;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class PubsubSourceConnectorConfigTest {

   private final Map<String, String> requiredOnly = new HashMap<>();
   private PubsubSourceConnectorConfig requiredOnlyConfig;

   @Before public void setUp() {
      requiredOnly.put(PubsubSourceConnectorConfig.KAFKA_TOPIC_CONFIG, "t");
      requiredOnly.put(PubsubSourceConnectorConfig.GCPS_PROJECT_CONFIG, "p");
      requiredOnly.put(PubsubSourceConnectorConfig.GCPS_SUBSCRIPTION_CONFIG, "s");
      requiredOnlyConfig = new PubsubSourceConnectorConfig(requiredOnly);
   }

   @Test public void testMaxAckExtensionPeriod() {
      assertEquals("Default value",
          Duration.ofMinutes(5),
          requiredOnlyConfig.getMaxAckExtensionPeriod()
      );

      PubsubSourceConnectorConfig config = with(PubsubSourceConnectorConfig.GCPS_MAX_ACK_EXTENSION_PERIOD_SEC_CONFIG, "30");
      assertEquals("Value is set",
          Duration.ofSeconds(30),
          config.getMaxAckExtensionPeriod()
      );
   }

   @Test public void testParallelPullCount() {
      assertEquals("Default value",
          1,
          requiredOnlyConfig.getParallelPullCount()
      );

      PubsubSourceConnectorConfig config = with(PubsubSourceConnectorConfig.GCPS_PARALLEL_PULL_COUNT_CONFIG, "3");
      assertEquals("Value is set",
          3,
          config.getParallelPullCount()
      );
   }

   @Test public void testProjectSubscription() {
      assertEquals("Default value",
          ProjectSubscriptionName.of("p", "s"),
          requiredOnlyConfig.getProjectSubscription()
      );

      PubsubSourceConnectorConfig withProject = with(PubsubSourceConnectorConfig.GCPS_PROJECT_CONFIG, "pp");
      assertEquals("Project is set",
          ProjectSubscriptionName.of("pp", "s"),
          withProject.getProjectSubscription()
      );

      PubsubSourceConnectorConfig withSubscription = with(PubsubSourceConnectorConfig.GCPS_PROJECT_CONFIG, "pp");
      assertEquals("Subscription is set",
          ProjectSubscriptionName.of("pp", "s"),
          withSubscription.getProjectSubscription()
      );
   }

   @Test public void testMaxOutstandingElementCount() {
      FlowControlSettings defaultFlowControlSettings = requiredOnlyConfig.getFlowControlSettings();
      assertEquals("Default " + PubsubSourceConnectorConfig.GCPS_MAX_OUTSTANDING_ELEMENTS_CONFIG,
          PubsubSourceConnectorConfig.GCPS_MAX_OUTSTANDING_ELEMENTS_DEFAULT,
          defaultFlowControlSettings.getMaxOutstandingElementCount()
      );

      PubsubSourceConnectorConfig config = with(PubsubSourceConnectorConfig.GCPS_MAX_OUTSTANDING_ELEMENTS_CONFIG, "100");
      assertEquals("Value is set",
          100L,
          config.getFlowControlSettings().getMaxOutstandingElementCount().longValue()
      );
   }

   @Test public void testMaxOutstandingRequestBytes() {
      FlowControlSettings defaultFlowControlSettings = requiredOnlyConfig.getFlowControlSettings();
      assertEquals("Default " + PubsubSourceConnectorConfig.GCPS_MAX_OUTSTANDING_BYTES_CONFIG,
          PubsubSourceConnectorConfig.GCPS_MAX_OUTSTANDING_BYTES_DEFAULT,
          defaultFlowControlSettings.getMaxOutstandingRequestBytes()
      );

      PubsubSourceConnectorConfig config = with(PubsubSourceConnectorConfig.GCPS_MAX_OUTSTANDING_BYTES_CONFIG, "10000");
      assertEquals("Value is set",
          10000L,
          config.getFlowControlSettings().getMaxOutstandingRequestBytes().longValue()
      );
   }

   @Test public void testLimitExceededBehavior() {
      FlowControlSettings defaultFlowControlSettings = requiredOnlyConfig.getFlowControlSettings();
      assertEquals("Default limit exceeded behavior",
          FlowController.LimitExceededBehavior.Block,
          defaultFlowControlSettings.getLimitExceededBehavior()
      );
   }

   private PubsubSourceConnectorConfig with(String config, String value) {
      Map<String, String> configs = new HashMap<>(requiredOnly);
      configs.put(config, value);
      return new PubsubSourceConnectorConfig(configs);
   }

}
