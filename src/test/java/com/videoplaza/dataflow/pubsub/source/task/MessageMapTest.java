package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.videoplaza.dataflow.pubsub.util.TaskMetrics;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessageMapTest {
   private final Consumer<Message> evictionHandler = mock(Consumer.class);
   private final MessageMap neverExpireMap = new MessageMap(Integer.MAX_VALUE, null);
   private final MessageMap mapWithExpiration = new MessageMap(1, evictionHandler);
   private final SourceRecord record = mock(SourceRecord.class);
   private final Message message = new Message("1", record, mock(AckReplyConsumer.class), new TaskMetrics(1));
   private final Message anotherMessage = new Message("2", mock(SourceRecord.class), mock(AckReplyConsumer.class), new TaskMetrics(1));

   @Test public void pollEmpty() {
      assertTrue(neverExpireMap.poll().isEmpty());
   }

   @Test public void poll() {
      neverExpireMap.put(message);
      List<SourceRecord> records = neverExpireMap.poll();
      assertEquals(1, records.size());
      assertSame(record, records.get(0));
      assertTrue(message.isPolled());
      assertFalse(neverExpireMap.isEmpty());
      assertTrue(neverExpireMap.poll().isEmpty());
      assertEquals(0, neverExpireMap.getToBePolledCount());
      assertEquals(1, neverExpireMap.getPolledCount());
      neverExpireMap.put(anotherMessage);
      assertEquals(1, neverExpireMap.getToBePolledCount());
      assertEquals(1, neverExpireMap.getPolledCount());
      List<Message> polled = neverExpireMap.polled().collect(Collectors.toList());
      assertEquals(1, polled.size());
      assertSame(message, polled.get(0));
   }

   @Test public void pollAfterExpiration() throws InterruptedException {
      mapWithExpiration.put(message);
      Thread.sleep(1500);
      assertTrue(mapWithExpiration.poll().isEmpty());
      assertTrue(mapWithExpiration.isEmpty());
      verify(evictionHandler).accept(message);
   }

   @Test public void remove() {
      neverExpireMap.put(message);
      neverExpireMap.remove(message.getMessageId());
      assertFalse(neverExpireMap.isEmpty());
      assertTrue(neverExpireMap.poll().isEmpty());
   }

   @Test public void get() {
      neverExpireMap.put(message);
      assertEquals(1, neverExpireMap.getToBePolledCount());
      assertEquals(0, neverExpireMap.getPolledCount());
      assertFalse(message.isPolled());
      assertSame(message, neverExpireMap.get(message.getMessageId()));
      assertTrue(neverExpireMap.contains(message));
   }
}
