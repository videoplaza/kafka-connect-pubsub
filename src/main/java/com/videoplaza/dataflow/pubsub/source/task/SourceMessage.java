package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;

/**
 * Encapsulates Pubsub message and corresponding <code>AckReplyConsumer</code> instance.
 */
public class SourceMessage {
   private final static Logger LOG = LoggerFactory.getLogger(SourceMessage.class);

   private final String messageId;
   private final AckReplyConsumer ackReplyConsumer;
   private final long receivedMs;
   private final Map<Object, SourceRecord> records;
   private final AtomicInteger count = new AtomicInteger();
   private final long createdMs;
   private final List<String> traceableKeys;

   private volatile boolean polled = false;

   public SourceMessage(String messageId, long createdMs, Map<Object, SourceRecord> records, AckReplyConsumer ackReplyConsumer, List<String> traceableKeys) {
      requireNonNull(messageId, "messageId cannot be null");
      requireNonNull(records, "record cannot be null");
      requireNonNull(ackReplyConsumer, "ackReplyConsumer cannot be null");
      this.messageId = messageId;
      this.createdMs = createdMs;
      this.records = new ConcurrentHashMap<>(records);
      this.ackReplyConsumer = ackReplyConsumer;
      this.receivedMs = Clock.systemUTC().millis();
      this.traceableKeys = traceableKeys;
      count.set(records.size());
   }

   public String getMessageId() {
      return messageId;
   }

   public Collection<SourceRecord> getRecords() {
      return records.values();
   }

   public Stream<SourceRecord> getRecordsStream() {
      return records.values().stream();
   }

   public boolean isPolled() {
      return polled;
   }

   public void markAsPolled() {
      this.polled = true;
   }

   public boolean ack(SourceRecord record) {
      if (records.remove(record.key()) == null) {
         throw new IllegalStateException(format("There is no record for %s in %s", record.key(), this));
      }

      count.decrementAndGet();

      int numberOfRecords = count.get();
      if (numberOfRecords < 0) {
         throw new IllegalStateException(format("Number of records is negative: %s", numberOfRecords));
      }

      boolean allRecordsDelivered = numberOfRecords == 0;
      if (allRecordsDelivered) {
         ackReplyConsumer.ack();
      }

      if (LOG.isDebugEnabled() && allRecordsDelivered && records.size() > 0) {
         LOG.debug("No records left according to counter, but there are {} according to records.size(). " +
            "This can happen since records size is an approximation.", records.size());
      }

      return allRecordsDelivered;
   }

   public void nack() {
      ackReplyConsumer.nack();
   }

   @Override public String toString() {
      return "SourceMessage[" + messageId + traceableKeys + '/' + polled + '|' + (currentTimeMillis() - receivedMs) + "ms]";
   }

   public boolean hasTraceableRecords() {
      return traceableKeys !=null && !traceableKeys.isEmpty();
   }

   public int size() {
      return count.get();
   }

   public boolean allRecordsDelivered() {
      return count.get() == 0;
   }

   public long getCreatedMs() {
      return createdMs;
   }

   public long getReceivedMs() {
      return receivedMs;
   }
}