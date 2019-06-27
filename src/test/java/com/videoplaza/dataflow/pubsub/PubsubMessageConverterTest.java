package com.videoplaza.dataflow.pubsub;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import static com.google.protobuf.util.Timestamps.toMillis;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class PubsubMessageConverterTest {

   private static final String TOPIC = "t";
   private static final String SUBSCRIPTION = "s";
   private static final String KEY_ATTRIBUTE = "key";
   private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
   private static final long TIMESTAMP = 999999L;

   private final static PubsubMessage MESSAGE_WITH_NO_ATTRIBUTES = PubsubMessage.newBuilder()
       .setData(ByteString.copyFromUtf8("Boom"))
       .setMessageId("message-id")
       .setPublishTime(Timestamp.newBuilder().setSeconds(12345L).build())
       .build();

   private final static PubsubMessage MESSAGE_WITH_KEY_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(KEY_ATTRIBUTE, "custom-key")
       .build();

   private final static PubsubMessage MESSAGE_WITH_TIMESTAMP_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP))
       .build();

   private final static PubsubMessage MESSAGE_WITH_INCORRECT_TIMESTAMP_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(TIMESTAMP_ATTRIBUTE, "not a timestamp")
       .build();

   private final static PubsubMessage MESSAGE_WITH_KEY_AND_TIMESTAMP_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(KEY_ATTRIBUTE, "custom-key")
       .putAttributes(TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP))
       .build();

   private final PubsubMessageConverter converterWithNoAttributes = new PubsubMessageConverter(
       "",
       "",
       SUBSCRIPTION,
       TOPIC
   );

   private final PubsubMessageConverter converterWithKeyAttribute = new PubsubMessageConverter(
       KEY_ATTRIBUTE,
       "",
       SUBSCRIPTION,
       TOPIC
   );

   private final PubsubMessageConverter converterWithTimestampAttribute = new PubsubMessageConverter(
       "",
       TIMESTAMP_ATTRIBUTE,
       SUBSCRIPTION,
       TOPIC
   );

   private final PubsubMessageConverter converterWithKeyAndTimestampAttributes = new PubsubMessageConverter(
       KEY_ATTRIBUTE,
       TIMESTAMP_ATTRIBUTE,
       SUBSCRIPTION,
       TOPIC
   );

   @Test
   public void convertMessageWithNoAttributes() {
      assertSourceRecord(
          MESSAGE_WITH_NO_ATTRIBUTES,
          MESSAGE_WITH_NO_ATTRIBUTES.getMessageId(),
          toMillis(MESSAGE_WITH_NO_ATTRIBUTES.getPublishTime()),
          TOPIC,
          converterWithNoAttributes.convert(MESSAGE_WITH_NO_ATTRIBUTES)
      );
   }

   @Test
   public void convertMessageWithKeyAttribute() {
      assertSourceRecord(
          MESSAGE_WITH_KEY_ATTRIBUTE,
          MESSAGE_WITH_KEY_ATTRIBUTE.getAttributesMap().get(KEY_ATTRIBUTE),
          toMillis(MESSAGE_WITH_KEY_ATTRIBUTE.getPublishTime()),
          TOPIC,
          converterWithKeyAttribute.convert(MESSAGE_WITH_KEY_ATTRIBUTE)
      );
   }

   @Test
   public void convertMessageWithTimestampAttribute() {
      assertSourceRecord(
          MESSAGE_WITH_TIMESTAMP_ATTRIBUTE,
          MESSAGE_WITH_TIMESTAMP_ATTRIBUTE.getMessageId(),
          TIMESTAMP,
          TOPIC,
          converterWithTimestampAttribute.convert(MESSAGE_WITH_TIMESTAMP_ATTRIBUTE)
      );
   }

   @Test
   public void convertMessageWithIncorrectTimestampAttribute() {
      assertSourceRecord(
          MESSAGE_WITH_INCORRECT_TIMESTAMP_ATTRIBUTE,
          MESSAGE_WITH_INCORRECT_TIMESTAMP_ATTRIBUTE.getMessageId(),
          toMillis(MESSAGE_WITH_NO_ATTRIBUTES.getPublishTime()),
          TOPIC,
          converterWithTimestampAttribute.convert(MESSAGE_WITH_INCORRECT_TIMESTAMP_ATTRIBUTE)
      );
   }

   @Test
   public void convertMessageWithKeyAndTimestampAttribute() {
      assertSourceRecord(
          MESSAGE_WITH_KEY_AND_TIMESTAMP_ATTRIBUTE,
          MESSAGE_WITH_KEY_AND_TIMESTAMP_ATTRIBUTE.getAttributesMap().get(KEY_ATTRIBUTE),
          TIMESTAMP,
          TOPIC,
          converterWithKeyAndTimestampAttributes.convert(MESSAGE_WITH_KEY_AND_TIMESTAMP_ATTRIBUTE)
      );
   }

   private static void assertSourceRecord(PubsubMessage message, String key, long timestamp, String topic, SourceRecord record) {
      assertArrayEquals("data mismatch", message.getData().toByteArray(), (byte[]) record.value());
      assertEquals("message key mismatch", key, record.key());
      assertEquals("message timestamp mismatch", timestamp, record.timestamp().longValue());
      assertEquals("topic mismatch", topic, record.topic());
      assertEquals("source offset mismatch", message.getMessageId(), record.sourceOffset().get(SUBSCRIPTION));
   }

}
