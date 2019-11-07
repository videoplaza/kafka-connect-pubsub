package com.videoplaza.dataflow.pubsub.source.task.convert;


import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import org.junit.Test;

import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class PubsubAttributeExtractorTest {

   private final static String MESSAGE_ID = "message-id";
   private final static long TIMESTAMP_SEC = 12345L;
   private final static String KEY_ATTRIBUTE = "ka";
   private final static String TIMESTAMP_ATTRIBUTE = "ta";
   private final static String KEY = "key";
   private final static long TIMESTAMP = System.currentTimeMillis();

   private final PubsubAttributeExtractor noAttributesExtractor = new PubsubAttributeExtractor(null, null);
   private final PubsubAttributeExtractor extractor = new PubsubAttributeExtractor(KEY_ATTRIBUTE, TIMESTAMP_ATTRIBUTE);
   private final Map<String, String> someAttributes = ImmutableMap.of("k", "v");
   private final Map<String, String> attributes = ImmutableMap.of(KEY_ATTRIBUTE, KEY, TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP), "k", "v");

   private final PubsubMessage aMessageWithNoAttributes = PubsubMessage.newBuilder()
       .setData(ByteString.copyFromUtf8("Boom"))
       .setMessageId(MESSAGE_ID)
       .setPublishTime(Timestamp.newBuilder().setSeconds(TIMESTAMP_SEC).build())
       .build();

   private final PubsubMessage aMessage = PubsubMessage.newBuilder(aMessageWithNoAttributes)
       .putAttributes(KEY_ATTRIBUTE, KEY)
       .putAttributes(TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP))
       .build();

   @Test public void getKeyWithNoAttributesExtractor() {
      assertThat(noAttributesExtractor.getKey(someAttributes)).isNull();
      assertThat(noAttributesExtractor.getKey(someAttributes, "d")).as("default value").isEqualTo("d");
      assertThat(noAttributesExtractor.getKey(aMessageWithNoAttributes)).as("default pubsub message key").isEqualTo(MESSAGE_ID);
   }

   @Test public void getTimestampWithNoAttributesExtractor() {
      assertThat(noAttributesExtractor.getTimestamp(someAttributes).isPresent()).isFalse();
      assertThat(noAttributesExtractor.getTimestamp(aMessageWithNoAttributes)).as("default pubsub message timestamp").isEqualTo(SECONDS.toMillis(TIMESTAMP_SEC));
   }

   @Test public void getKeyWithAttributes() {
      assertThat(extractor.getKey(attributes)).isEqualTo(KEY);
      assertThat(extractor.getKey(attributes, "d")).isEqualTo(KEY);
      assertThat(extractor.getKey(aMessage)).as("pubsub message key").isEqualTo(KEY);
   }

   @Test public void getKeyWithNoAttributesInTheMessage() {
      assertThat(extractor.getKey(someAttributes)).isNull();
      assertThat(extractor.getKey(someAttributes, "d")).as("default value").isEqualTo("d");
      assertThat(extractor.getKey(aMessageWithNoAttributes)).as("default pubsub message key").isEqualTo(MESSAGE_ID);
   }

   @Test public void getTimestampWithNoAttributesInTheMessage() {
      assertThat(extractor.getTimestamp(someAttributes).isPresent()).isFalse();
      assertThat(extractor.getTimestamp(aMessageWithNoAttributes)).as("default pubsub message timestamp").isEqualTo(SECONDS.toMillis(TIMESTAMP_SEC));
   }

   @Test public void getTimestampWithAttributes() {
      assertThat(extractor.getTimestamp(attributes).get()).isEqualTo(TIMESTAMP);
      assertThat(extractor.getTimestamp(aMessage)).as("pubsub message timestamp").isEqualTo(TIMESTAMP);
   }
}
