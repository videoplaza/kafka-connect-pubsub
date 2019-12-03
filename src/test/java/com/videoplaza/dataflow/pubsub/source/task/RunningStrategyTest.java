package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import com.videoplaza.dataflow.pubsub.source.task.convert.PubsubMessageConverter;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;

import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.GCPS_PROJECT_CONFIG;
import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.GCPS_SUBSCRIPTION_CONFIG;
import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.KAFKA_TOPIC_CONFIG;
import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.MAX_NUMBER_CONVERSION_FAILURES_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RunningStrategyTest {
   private final static PubsubMessage MESSAGE = PubsubMessage.newBuilder()
       .setData(ByteString.copyFromUtf8("Boom"))
       .setMessageId("message-id")
       .setPublishTime(Timestamp.newBuilder().setSeconds(12345L).build())
       .build();

   private final PubsubSourceConnectorConfig requiredOnlyConfig = new PubsubSourceConnectorConfig(ImmutableMap.of(
       KAFKA_TOPIC_CONFIG, "t",
       GCPS_PROJECT_CONFIG, "p",
       GCPS_SUBSCRIPTION_CONFIG, "s"
   ));
   private final PubsubSourceConnectorConfig ignoreSingleFailureConfig = new PubsubSourceConnectorConfig(ImmutableMap.of(
       KAFKA_TOPIC_CONFIG, "t",
       GCPS_PROJECT_CONFIG, "p",
       GCPS_SUBSCRIPTION_CONFIG, "s",
       MAX_NUMBER_CONVERSION_FAILURES_CONFIG, "1"
   ));

   private final AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);


   private final PubsubMessageConverter converter = mock(PubsubMessageConverter.class);
   private final PubsubSourceTaskState state = mock(PubsubSourceTaskState.class);
   private final SourceMessageMap messages = new SourceMessageMap(Long.MAX_VALUE, null);
   private final SourceMessage sourceMessage = mock(SourceMessage.class);
   private final TaskMetricsImpl metrics = new TaskMetricsImpl(Clock.systemUTC(), 1000);

   private RunningStrategy strategy;

   @Before public void setUp() {
      when(sourceMessage.getMessageId()).thenReturn("1");
      when(state.getConfig()).thenReturn(requiredOnlyConfig);
      when(state.getConverter()).thenReturn(converter);
      when(state.getMetrics()).thenReturn(metrics);
      when(state.getLogger()).thenReturn(mock(PubsubSourceTaskLogger.class));
      when(state.getMessages()).thenReturn(messages);


      strategy = new RunningStrategy(state);
   }

   @Test public void newStrategy() {
      assertThat(strategy.getPollTimeoutMs()).isEqualTo(PubsubSourceConnectorConfig.POLL_TIMEOUT_MS_DEFAULT);
   }

   @Test public void onNewMessage() {
      when(converter.convert(MESSAGE, ackReplyConsumer)).thenReturn(sourceMessage);
      SourceMessage m = strategy.onNewMessageReceived(MESSAGE, ackReplyConsumer);
      assertThat(m).isSameAs(sourceMessage);
      assertThat(messages.contains(m)).isTrue();
   }


   @Test public void onNewMessageConversionFailed() {
      when(converter.convert(MESSAGE, ackReplyConsumer)).thenThrow(new GoingSouthException());
      assertThat(strategy.onNewMessageReceived(MESSAGE, ackReplyConsumer)).isNull();
      assertThat(metrics.getMessageConversionFailures()).isEqualTo(1);
   }

   @Test public void onNewMessageConversionFailedBeforeMaxFailuresReached() {
      when(state.getConfig()).thenReturn(ignoreSingleFailureConfig);
      when(converter.convert(MESSAGE, ackReplyConsumer)).thenThrow(new GoingSouthButToBeIgnoredException());
      PubsubMessage secondMessage = PubsubMessage.newBuilder(MESSAGE).setMessageId("2").build();
      when(converter.convert(secondMessage, ackReplyConsumer)).thenThrow(new GoingSouthException());

      SourceMessage m = strategy.onNewMessageReceived(MESSAGE, ackReplyConsumer);
      assertThat(m).isNull();
      assertThat(metrics.getMessageConversionFailures()).isEqualTo(1);

      assertThat(strategy.onNewMessageReceived(secondMessage, ackReplyConsumer)).isNull();
      assertThat(metrics.getMessageConversionFailures()).isEqualTo(2);
   }

   private static class GoingSouthException extends RuntimeException {
   }

   private static class GoingSouthButToBeIgnoredException extends RuntimeException {
   }
}
