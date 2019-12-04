package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.protobuf.util.Timestamps;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

public class PubsubAttributeExtractor {

   private static final Logger LOG = LoggerFactory.getLogger(PubsubAttributeExtractor.class);

   private final String keyAttribute;
   private final String timestampAttribute;

   public PubsubAttributeExtractor(String keyAttribute, String timestampAttribute) {
      this.keyAttribute = keyAttribute;
      this.timestampAttribute = timestampAttribute;
   }

   public String getKey(Map<String, String> attributes, String defaultValue) {
      return Optional
          .ofNullable(getKey(attributes))
          .orElse(defaultValue);
   }

   public String getKey(Map<String, String> attributes) {
      return attributes.get(keyAttribute);
   }

   public Optional<Long> getTimestamp(Map<String, String> attributes) {
      String timestamp = attributes.get(timestampAttribute);
      if (timestamp != null) {
         try {
            return Optional.of(Long.parseLong(timestamp));
         } catch (NumberFormatException e) {
            LOG.warn("Could not parse timestamp attribute {}. ", timestamp);
         }

      }
      return Optional.empty();
   }

   public Long getTimestamp(PubsubMessage message) {
      return getTimestamp(message.getAttributesMap()).orElse(Timestamps.toMillis(message.getPublishTime()));
   }

   public String getKey(PubsubMessage pubsubMessage) {
      return getKey(pubsubMessage.getAttributesMap(), pubsubMessage.getMessageId());
   }

   public String debugInfo(PubsubMessage m) {
      return m == null ? "" : m.getMessageId() + "/" + getKey(m.getAttributesMap()) + "/" + getTimestamp(m);
   }
}
