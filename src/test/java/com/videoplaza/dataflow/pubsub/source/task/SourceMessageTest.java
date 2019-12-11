package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SourceMessageTest {
   private final static Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());
   private final AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);
   private final TaskMetricsImpl metrics = new TaskMetricsImpl(CLOCK, 1000);
   private final SourceMessage empty = new SourceMessage(
       "1",
       CLOCK.millis() - 1000,
       Collections.emptyMap(),
       ackReplyConsumer,
       null
   );

   private final Map<Object, SourceRecord> records = records("k1", "k2");
   private final SourceMessage with2Records = new SourceMessage(
       "1",
       CLOCK.millis() - 2000,
       records,
       ackReplyConsumer,
      null
   );

   private final SourceRecord aRecord = aRecord("1");


   @Test public void emptyHasNoRecords() {
      assertThat(empty.getRecordsStream()).hasSize(0);
      assertThat(empty.size()).isEqualTo(0);
   }

   @Test public void notEmptyHasSomeRecords() {
      assertThat(with2Records.getRecordsStream()).hasSize(2);
      assertThat(with2Records.size()).isEqualTo(2);
   }

   @Test(expected = IllegalStateException.class)
   public void ackFailsWhenEmpty() {
      empty.ack(aRecord);
   }

   @Test(expected = IllegalStateException.class)
   public void extraAckFails() {
      with2Records.ack(aRecord("k3"));
   }

   @Test public void nack() {
      with2Records.nack();
      verify(ackReplyConsumer).nack();
      verifyNoMoreInteractions(ackReplyConsumer);
   }

   @Test public void ack() {
      boolean allRecordsDeliveredAfter1stAck = with2Records.ack(records.get("k1"));
      assertThat(allRecordsDeliveredAfter1stAck).as("All records are delivered after 1st ack").isFalse();

      verifyZeroInteractions(ackReplyConsumer);
      assertThat(with2Records.size()).as("Size after 1st ack").isEqualTo(1);

      boolean allRecordsDeliveredAfter2ndAck = with2Records.ack(records.get("k2"));
      assertThat(allRecordsDeliveredAfter2ndAck).as("All records are delivered after 2nd ack").isTrue();

      assertThat(with2Records.size()).as("Size after 2nd ack").isEqualTo(0);
      verify(ackReplyConsumer).ack();
      verifyNoMoreInteractions(ackReplyConsumer);
   }

   private static SourceRecord aRecord(String key) {
      SourceRecord record = mock(SourceRecord.class);
      when(record.key()).thenReturn(key);
      return record;
   }

   private static Map<Object, SourceRecord> records(String... keys) {
      return stream(keys).collect(toMap(identity(), SourceMessageTest::aRecord));
   }
}
