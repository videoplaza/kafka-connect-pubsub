package com.videoplaza.dataflow.pubsub.source.task.convert;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

public class SourceRecordFactory {

   private final String subscription;
   private final String topic;

   public SourceRecordFactory(String subscription, String topic) {
      this.subscription = requireNonNull(subscription, "Subscription is required");
      this.topic = requireNonNull(topic, "Topic is required");
   }

   //TODO pass extra message attributes as headers
   //TODO make key type/schema configurable
   public SourceRecord create(String messageId, String key, byte[] data, long timestamp) {
      return new SourceRecord(
          null,
          singletonMap(subscription, messageId),
          topic,
          null,
          Schema.OPTIONAL_STRING_SCHEMA,
          key,
          Schema.BYTES_SCHEMA,
          data,
          timestamp
      );
   }
}
