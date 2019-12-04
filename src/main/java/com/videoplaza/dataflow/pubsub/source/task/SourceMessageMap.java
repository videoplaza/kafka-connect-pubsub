package com.videoplaza.dataflow.pubsub.source.task;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class SourceMessageMap {

   private final Cache<String, SourceMessage> cache;
   private final Set<SourceMessage> toBePolled = ConcurrentHashMap.newKeySet();
   private final Consumer<SourceMessage> onEviction;

   public SourceMessageMap(long expireAfterWriteSec, Consumer<SourceMessage> onEviction) {
      this.cache = CacheBuilder.newBuilder()
          .expireAfterWrite(expireAfterWriteSec, TimeUnit.SECONDS)
          .removalListener(this::onMessageRemoval)
          .build();
      this.onEviction = onEviction;
   }

   private void onMessageRemoval(RemovalNotification<String, SourceMessage> removal) {
      if (removal.wasEvicted() && onEviction != null) {
         onEviction.accept(removal.getValue());
      }
   }

   public SourceMessage get(String key) {
      return cache.getIfPresent(key);
   }

   public boolean contains(SourceMessage m) {
      return get(m.getMessageId()) != null;
   }

   public List<SourceRecord> pollRecords() {
      return pollMessages().flatMap(SourceMessage::getRecordsStream).collect(toList());
   }

   public Stream<SourceMessage> pollMessages() {
      cache.cleanUp();

      Set<SourceMessage> batch = new HashSet<>(toBePolled);
      toBePolled.removeAll(batch);
      batch.forEach(SourceMessage::markAsPolled);
      return batch.stream().filter(this::contains);
   }

   public Stream<SourceMessage> polled() {
      return cache.asMap().values().stream().filter(SourceMessage::isPolled);
   }

   public boolean isEmpty() {
      return cache.size() == 0 && toBePolled.isEmpty();
   }

   public SourceMessage remove(String messageId) {
      return cache.asMap().remove(messageId);
   }

   public void put(SourceMessage sourceMessage) {
      cache.put(
          sourceMessage.getMessageId(),
          sourceMessage
      );

      toBePolled.add(sourceMessage);
   }

   /**
    * Number of messages currently received from Cloud Pubsub but not yet picked up by connect framework for publishing to kafka
    */
   public int getToBePolledCount() {
      return toBePolled.size();
   }

   /**
    * Number of messages being delivered to kafka and not yet acknowledged in Cloud Pubsub
    */
   public long getPolledCount() {
      return cache.asMap().values().stream().filter(SourceMessage::isPolled).count();
   }

   @Override public String toString() {
      return "messages[" + getPolledCount() + "/" + getToBePolledCount() + "]";
   }

}
