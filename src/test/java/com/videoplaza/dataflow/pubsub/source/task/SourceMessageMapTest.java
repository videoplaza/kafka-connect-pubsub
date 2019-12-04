package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SourceMessageMapTest {
   private final static Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());
   private final Consumer<SourceMessage> evictionHandler = mock(Consumer.class);
   private final SourceMessageMap neverExpireMap = new SourceMessageMap(Integer.MAX_VALUE, null);
   private final SourceMessageMap mapWithExpiration = new SourceMessageMap(1, evictionHandler);
   private final SourceRecord record = mock(SourceRecord.class);
   private final SourceMessage sourceMessage = new SourceMessage(
       "1",
       CLOCK.millis() - 1000,
       ImmutableMap.of("1", record),
       mock(AckReplyConsumer.class),
       false
   );
   private final SourceMessage anotherSourceMessage = new SourceMessage(
       "2",
       CLOCK.millis() - 1000,
       ImmutableMap.of("1", mock(SourceRecord.class)),
       mock(AckReplyConsumer.class),
       false
   );

   @Test public void pollEmpty() {
      assertTrue(neverExpireMap.pollRecords().isEmpty());
   }

   @Test public void poll() {
      neverExpireMap.put(sourceMessage);
      List<SourceRecord> records = neverExpireMap.pollRecords();
      assertEquals(1, records.size());
      assertSame(record, records.get(0));
      assertTrue(sourceMessage.isPolled());
      assertFalse(neverExpireMap.isEmpty());
      assertTrue(neverExpireMap.pollRecords().isEmpty());
      assertEquals(0, neverExpireMap.getToBePolledCount());
      assertEquals(1, neverExpireMap.getPolledCount());
      neverExpireMap.put(anotherSourceMessage);
      assertEquals(1, neverExpireMap.getToBePolledCount());
      assertEquals(1, neverExpireMap.getPolledCount());
      List<SourceMessage> polled = neverExpireMap.polled().collect(Collectors.toList());
      assertEquals(1, polled.size());
      assertSame(sourceMessage, polled.get(0));
   }

   @Test public void pollAfterExpiration() throws InterruptedException {
      mapWithExpiration.put(sourceMessage);
      Thread.sleep(1500);
      assertTrue(mapWithExpiration.pollRecords().isEmpty());
      assertTrue(mapWithExpiration.isEmpty());
      verify(evictionHandler).accept(sourceMessage);
   }

   @Test public void remove() {
      neverExpireMap.put(sourceMessage);
      neverExpireMap.remove(sourceMessage.getMessageId());
      assertFalse(neverExpireMap.isEmpty());
      assertTrue(neverExpireMap.pollRecords().isEmpty());
   }

   @Test public void get() {
      neverExpireMap.put(sourceMessage);
      assertEquals(1, neverExpireMap.getToBePolledCount());
      assertEquals(0, neverExpireMap.getPolledCount());
      assertFalse(sourceMessage.isPolled());
      assertSame(sourceMessage, neverExpireMap.get(sourceMessage.getMessageId()));
      assertTrue(neverExpireMap.contains(sourceMessage));
   }
}
