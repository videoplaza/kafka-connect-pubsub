package com.videoplaza.dataflow.pubsub.source.task;

import com.google.common.cache.Cache;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.partition;
import static java.lang.String.join;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

public class MessageMap {

   private static final int DUMP_MESSAGE_IN_FLIGHT_BATCH_SIZE = 500;
   private final Cache<String, Message> cache;
   private final Set<Message> toBePolled = ConcurrentHashMap.newKeySet();


   public MessageMap(Cache<String, Message> cache) {
      this.cache = cache;
   }

   public Message get(String key) {
      return cache.getIfPresent(key);
   }

   public boolean contains(Message m) {
      return get(m.getMessageId()) != null;
   }

   public List<SourceRecord> poll() {
      cache.cleanUp();

      Set<Message> batch = new HashSet<>(toBePolled);
      toBePolled.removeAll(batch);
      batch.forEach(Message::markAsPolled);
      return batch.stream().filter(this::contains).map(Message::getRecord).collect(toList());
   }

   public Stream<Message> polled() {
      return cache.asMap().values().stream().filter(Message::isPolled);
   }

   public boolean isEmpty() {
      return cache.size() == 0 && toBePolled.isEmpty();
   }

   public void dump(Logger log) {
      if (log.isDebugEnabled()) {
         final AtomicInteger count = new AtomicInteger();
         List<String> messageBatches = polled().map(Object::toString).collect(Collectors.toList());
         stream(partition(messageBatches, DUMP_MESSAGE_IN_FLIGHT_BATCH_SIZE).spliterator(), false)
             .forEach(messageInFlights ->
                 log.debug("Polled messages batch {} of {}: {}", count.incrementAndGet(), messageBatches.size(), join(",", messageInFlights))
             );
      }
   }

   public Message remove(String messageId) {
      return cache.asMap().remove(messageId);
   }

   public void put(Message message) {
      cache.put(
          message.getMessageId(),
          message
      );

      toBePolled.add(message);
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
      return cache.asMap().values().stream().filter(Message::isPolled).count();
   }

   @Override public String toString() {
      return "messages[" + getPolledCount() + "/" + getToBePolledCount() + "]";
   }

}
