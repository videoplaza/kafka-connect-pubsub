package com.videoplaza.dataflow.pubsub;

import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static java.util.Collections.singletonMap;


/**
 * Converts Cloud Pubsub message into {@link SourceRecord} instance.
 *
 * <ul>
 * <li>Kafka message key is either set to <code>keyAttribute</code> or, if its not defined, {@link PubsubMessage#getMessageId()}</li>
 * <li>Kafka message timestamp is either set to <code>timestampAttribute</code> or, if its not defined, {@link PubsubMessage#getPublishTime()}</li>
 * </ul>
 */
public class PubsubMessageConverter {

   private static final Logger LOG = LoggerFactory.getLogger(PubsubMessageConverter.class);

   private final String keyAttribute;

   private final String timestampAttribute;

   private final String subscription;

   private final String topic;


   //TODO make a builder
   public PubsubMessageConverter(String keyAttribute, String timestampAttribute, String subscription, String topic) {
      this.keyAttribute = keyAttribute;
      this.timestampAttribute = timestampAttribute;
      this.subscription = subscription;
      this.topic = topic;
   }

   //TODO pass extra message attributes as headers
   public SourceRecord convert(PubsubMessage message) {
      return new SourceRecord(
          null,
          singletonMap(subscription, message.getMessageId()),
          topic,
          null,
          Schema.OPTIONAL_STRING_SCHEMA,
          getKey(message),
          Schema.BYTES_SCHEMA,
          message.getData().toByteArray(),
          getTimestamp(message)
      );
   }

   String getKey(PubsubMessage message) {
      return Optional
          .ofNullable(message.getAttributesMap().get(keyAttribute))
          .orElse(message.getMessageId());
   }

   Long getTimestamp(PubsubMessage message) {
      String t = message.getAttributesMap().get(timestampAttribute);
      long timestamp = Timestamps.toMillis(message.getPublishTime());
      if (t != null) {
         try {
            timestamp = Long.parseLong(t);
         } catch (NumberFormatException e) {
            LOG.warn("Could not parse timestamp attribute {}. ", t);
         }
      }
      return timestamp;
   }

}
