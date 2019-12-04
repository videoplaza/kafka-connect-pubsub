package com.videoplaza.dataflow.pubsub.source.task.convert;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static java.util.Objects.requireNonNull;

public class SourceRecordFactory {

   private final String subscription;
   private final String topic;
   private final PayloadVerifier payloadVerifier;

   public SourceRecordFactory(String subscription, String topic, PayloadVerifier payloadVerifier) {
      this.subscription = requireNonNull(subscription, "Subscription is required");
      this.topic = requireNonNull(topic, "Topic is required");
      this.payloadVerifier = payloadVerifier;
   }

   public SourceRecordFactory(String subscription, String topic) {
      this(subscription, topic, null);
   }

   public SourceRecord create(String messageId, String key, byte[] data, long timestamp) {
      if (payloadVerifier != null) {
          payloadVerifier.verify(topic, key, data);
      }
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
