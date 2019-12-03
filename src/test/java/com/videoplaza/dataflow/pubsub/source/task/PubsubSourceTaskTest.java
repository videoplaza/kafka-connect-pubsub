package com.videoplaza.dataflow.pubsub.source.task;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.GCPS_PROJECT_CONFIG;
import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.GCPS_SUBSCRIPTION_CONFIG;
import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.KAFKA_TOPIC_CONFIG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PubsubSourceTaskTest {

   private final Subscriber subscriber = mock(Subscriber.class);

   private final SourceMessageMap messages = new SourceMessageMap(10000, null);

   private final PubsubSourceTask pubsubSourceTask = new PubsubSourceTask()
       .configure(ImmutableMap.of(
           GCPS_PROJECT_CONFIG, "project",
           GCPS_SUBSCRIPTION_CONFIG, "subscription",
           KAFKA_TOPIC_CONFIG, "topic"
       ))
       .configure(messages)
       .configure(subscriber)
       .configureEventLoopGroup(1)
       .init();


   private final ExecutorService executor = Executors.newSingleThreadExecutor();
   private final CountDownLatch asyncCompletedLatch = new CountDownLatch(1);

   private final AckSupplier ackSupplier = new AckSupplier();

   private final static PubsubMessage MESSAGE = PubsubMessage.newBuilder()
       .setData(ByteString.copyFromUtf8("Boom"))
       .setMessageId("message-id")
       .setPublishTime(Timestamp.newBuilder().setSeconds(12345L).build())
       .build();

   @Before public void setUp() {
      PubsubSourceTask.METRICS.set(new TaskMetricsImpl(Clock.systemUTC(), PubsubSourceConnectorConfig.HISTOGRAM_UPDATE_INTERVAL_MS_DEFAULT));
      when(subscriber.state()).thenReturn(ApiService.State.TERMINATED);
      verify(subscriber).startAsync();
   }

   @Test public void test() {
      String id1 = "741967042247751";
      String id2 = "741967042247751";
      assertEquals(id1.hashCode(), id2.hashCode());

   }

   @Test public void testPoll() {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);
      assertEquals(1, pubsubSourceTask.poll().size());
      assertEquals(0, messages.getToBePolledCount());
      assertEquals(1, messages.getPolledCount());
   }

   @Test public void testMessageLifeCycle() throws TimeoutException {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);
      assertEquals(1, messages.getToBePolledCount());

      List<SourceRecord> records = pubsubSourceTask.poll();
      assertEquals(1, records.size());
      assertEquals(0, messages.getToBePolledCount());
      assertEquals(1, messages.getPolledCount());

      pubsubSourceTask.commitRecord(records.get(0));
      assertEquals(0, messages.getPolledCount());
      assertTrue(ackSupplier.acked);
      assertFalse(ackSupplier.nacked);
      assertEquals(1, pubsubSourceTask.getMetrics().getMessageAckCount());
      assertEquals(0, pubsubSourceTask.getMetrics().getMessageNackCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getMessageReceivedCount());

      stopTask();
      verify(subscriber).stopAsync();
      verify(subscriber).awaitTerminated(PubsubSourceConnectorConfig.SHUTDOWN_TERMINATE_SUBSCRIBER_TIMEOUT_MS_DEFAULT, TimeUnit.MILLISECONDS);
      verifyNoMoreInteractions(subscriber);
   }

   @Test public void testStopAfterMessagePolled() throws InterruptedException {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);
      assertEquals(1, messages.getToBePolledCount());
      SourceRecord record = pubsubSourceTask.poll().get(0);

      assertEquals(0, messages.getToBePolledCount());

      stopTaskAsync();


      assertEquals(1, messages.getPolledCount());
      pubsubSourceTask.commitRecord(record);
      asyncCompletedLatch.await(2000, TimeUnit.MILLISECONDS);
      verify(subscriber).stopAsync();
      assertEquals(0, messages.getPolledCount());
      assertTrue(ackSupplier.acked);
      assertFalse(ackSupplier.nacked);
      assertEquals(1, pubsubSourceTask.getMetrics().getMessageAckCount());
      assertEquals(0, pubsubSourceTask.getMetrics().getMessageNackCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getMessageReceivedCount());
   }

   private void stopTask() {
      pubsubSourceTask.stop();
      while (!pubsubSourceTask.isStopped()) {
         try {
            Thread.sleep(100);
         } catch (InterruptedException e) {
         }
      }
   }

   private void stopTaskAsync() {
      executor.execute(() -> {
         stopTask();
         asyncCompletedLatch.countDown();
      });
   }

   @Test public void testStopBeforeMessagePolled() throws InterruptedException {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);

      assertEquals(1, messages.getToBePolledCount());
      assertEquals(0, messages.getPolledCount());

      stopTaskAsync();

      asyncCompletedLatch.await(6000, TimeUnit.MILLISECONDS);

      verify(subscriber).stopAsync();

      assertEquals(0, messages.getPolledCount());
      assertEquals(0, messages.getToBePolledCount());
      assertFalse(ackSupplier.acked);
      assertTrue(ackSupplier.nacked);
      assertEquals(0, pubsubSourceTask.getMetrics().getMessageAckCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getMessageNackCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getMessageReceivedCount());
   }

   private static class AckSupplier implements AckReplyConsumer {

      private boolean acked;
      private boolean nacked;

      @Override public void ack() {
         acked = true;
      }

      @Override public void nack() {
         nacked = true;
      }
   }

}
