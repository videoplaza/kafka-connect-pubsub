package com.videoplaza.dataflow.pubsub;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.GCPS_PROJECT_CONFIG;
import static com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig.GCPS_SHUTDOWN_TIMEOUT_MS_CONFIG;
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
   private final TestSleeper sleeper = new TestSleeper();

   private final PubsubSourceTask pubsubSourceTask = new PubsubSourceTask()
       .configure(Map.of(
           GCPS_PROJECT_CONFIG, "project",
           GCPS_SUBSCRIPTION_CONFIG, "subscription",
           KAFKA_TOPIC_CONFIG, "topic",
           GCPS_SHUTDOWN_TIMEOUT_MS_CONFIG, "1000"
       ))
       .subscribe(subscriber)
       .configure(sleeper);

   private final ExecutorService executor = Executors.newSingleThreadExecutor();
   private final CountDownLatch asyncCompletedLatch = new CountDownLatch(1);

   private final AckSupplier ackSupplier = new AckSupplier();

   private final static PubsubMessage MESSAGE = PubsubMessage.newBuilder()
       .setData(ByteString.copyFromUtf8("Boom"))
       .setMessageId("message-id")
       .setPublishTime(Timestamp.newBuilder().setSeconds(12345L).build())
       .build();

   @Before public void setUp() {
      when(subscriber.state()).thenReturn(ApiService.State.TERMINATED);
      verify(subscriber).startAsync();

   }

   @Test public void testPoll() {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);
      assertEquals(1, pubsubSourceTask.poll().size());
      assertEquals(0, pubsubSourceTask.getToBePolledCount());
      assertEquals(1, pubsubSourceTask.getPolledCount());
   }

   @Test public void testMessageLifeCycle() {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);
      assertEquals(1, pubsubSourceTask.getToBePolledCount());

      List<SourceRecord> records = pubsubSourceTask.poll();
      assertEquals(1, records.size());
      assertEquals(0, pubsubSourceTask.getToBePolledCount());
      assertEquals(1, pubsubSourceTask.getPolledCount());

      pubsubSourceTask.commitRecord(records.get(0));
      assertEquals(0, pubsubSourceTask.getPolledCount());
      assertTrue(ackSupplier.acked);
      assertFalse(ackSupplier.nacked);
      assertEquals(1, pubsubSourceTask.getMetrics().getAckCount());
      assertEquals(0, pubsubSourceTask.getMetrics().getNackCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getReceivedCount());

      pubsubSourceTask.commit();
      verifyNoMoreInteractions(subscriber);
   }

   @Test public void testStopAfterMessagePolled() throws InterruptedException {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);
      assertEquals(1, pubsubSourceTask.getToBePolledCount());
      SourceRecord record = pubsubSourceTask.poll().get(0);

      assertEquals(0, pubsubSourceTask.getToBePolledCount());
      pubsubSourceTask.stop();

      executor.execute(() -> {
         pubsubSourceTask.commit();
         asyncCompletedLatch.countDown();
      });

      assertTrue(sleeper.awaitSleep(2000));
      assertEquals(1, pubsubSourceTask.getPolledCount());
      pubsubSourceTask.commitRecord(record);
      asyncCompletedLatch.await(2000, TimeUnit.MILLISECONDS);
      verify(subscriber).stopAsync();
      assertEquals(0, pubsubSourceTask.getPolledCount());
      assertTrue(ackSupplier.acked);
      assertFalse(ackSupplier.nacked);
      assertEquals(1, pubsubSourceTask.getMetrics().getAckCount());
      assertEquals(0, pubsubSourceTask.getMetrics().getNackCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getReceivedCount());
   }

   @Test public void testStopBeforeMessagePolled() throws InterruptedException {
      pubsubSourceTask.onPubsubMessageReceived(MESSAGE, ackSupplier);
      pubsubSourceTask.stop();

      assertEquals(1, pubsubSourceTask.getToBePolledCount());
      assertEquals(0, pubsubSourceTask.getPolledCount());

      executor.execute(() -> {
         pubsubSourceTask.commit();
         asyncCompletedLatch.countDown();
      });

      asyncCompletedLatch.await(2000, TimeUnit.MILLISECONDS);

      assertEquals(0, sleeper.sleptLatch.getCount());
      verify(subscriber).stopAsync();

      assertEquals(0, pubsubSourceTask.getPolledCount());
      assertEquals(0, pubsubSourceTask.getToBePolledCount());
      assertFalse(ackSupplier.acked);
      assertTrue(ackSupplier.nacked);
      assertEquals(0, pubsubSourceTask.getMetrics().getAckCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getNackCount());
      assertEquals(1, pubsubSourceTask.getMetrics().getReceivedCount());
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

   private static class TestSleeper implements PubsubSourceTask.Sleeper {
      private final CountDownLatch sleptLatch = new CountDownLatch(1);

      @Override public void sleep(long ms) {
         sleptLatch.countDown();
      }

      public boolean awaitSleep(long ms) {
         try {
            return sleptLatch.await(ms, TimeUnit.MILLISECONDS);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }
   }
}
