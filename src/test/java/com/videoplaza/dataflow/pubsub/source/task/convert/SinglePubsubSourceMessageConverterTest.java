package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static com.videoplaza.dataflow.pubsub.source.task.convert.AssertUtil.assertThatRecordsAreEqual;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class SinglePubsubSourceMessageConverterTest {

   private static final String TOPIC = "t";
   private static final String SUBSCRIPTION = "s";
   private static final String KEY_ATTRIBUTE = "key";
   private static final String KEY = "k";
   private static final String MESSAGE_ID = "id";
   private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
   private static final long TIMESTAMP = 999999L;
   private static final long PUBLISH_TIMESTAMP = 12345000;
   private static final ByteString DATA = ByteString.copyFromUtf8("Boom");

   public final static PubsubMessage MESSAGE_WITH_NO_ATTRIBUTES = PubsubMessage.newBuilder()
       .setData(DATA)
       .setMessageId(MESSAGE_ID)
       .setPublishTime(Timestamp.newBuilder().setSeconds(TimeUnit.MILLISECONDS.toSeconds(PUBLISH_TIMESTAMP)).build())
       .build();

   public final static PubsubMessage MESSAGE_WITH_KEY_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(KEY_ATTRIBUTE, KEY)
       .build();

   public final static PubsubMessage MESSAGE_WITH_TIMESTAMP_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP))
       .build();

   public final static PubsubMessage MESSAGE_WITH_INCORRECT_TIMESTAMP_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(TIMESTAMP_ATTRIBUTE, "not a timestamp")
       .build();

   public final static PubsubMessage MESSAGE_WITH_KEY_AND_TIMESTAMP_ATTRIBUTE = PubsubMessage.newBuilder(MESSAGE_WITH_NO_ATTRIBUTES)
       .putAttributes(KEY_ATTRIBUTE, KEY)
       .putAttributes(TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP))
       .build();

   private final TaskMetricsImpl metrics = new TaskMetricsImpl(Clock.systemUTC(), 1000);
   private final SourceRecordFactory recordFactory = new SourceRecordFactory(SUBSCRIPTION, TOPIC);
   private final SinglePubsubMessageConverter converterWithNoAttributes = new SinglePubsubMessageConverter(
       recordFactory,
       new PubsubAttributeExtractor("", ""),
       metrics,
       mock(PubsubSourceTaskLogger.class)
   );

   private final SinglePubsubMessageConverter converterWithKeyAttribute = new SinglePubsubMessageConverter(
       recordFactory,
       new PubsubAttributeExtractor(KEY_ATTRIBUTE, ""),
       metrics,
       mock(PubsubSourceTaskLogger.class)
   );

   private final SinglePubsubMessageConverter converterWithTimestampAttribute = new SinglePubsubMessageConverter(
       recordFactory,
       new PubsubAttributeExtractor("", TIMESTAMP_ATTRIBUTE),
       metrics,
       mock(PubsubSourceTaskLogger.class)
   );

   private final SinglePubsubMessageConverter converterWithKeyAndTimestampAttributes = new SinglePubsubMessageConverter(
       recordFactory,
       new PubsubAttributeExtractor(KEY_ATTRIBUTE, TIMESTAMP_ATTRIBUTE),
       metrics,
       mock(PubsubSourceTaskLogger.class)
   );

   private final AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);

   @Test
   public void convertMessageWithNoAttributes() {
      assertSourceMessage(
          converterWithNoAttributes.convert(MESSAGE_WITH_NO_ATTRIBUTES, ackReplyConsumer),
          recordFactory.create(MESSAGE_ID, MESSAGE_ID, DATA.toByteArray(), PUBLISH_TIMESTAMP)
      );
   }

   @Test
   public void convertMessageWithKeyAttribute() {
      assertSourceMessage(
          converterWithKeyAttribute.convert(MESSAGE_WITH_KEY_ATTRIBUTE, ackReplyConsumer),
          recordFactory.create(MESSAGE_ID, KEY, DATA.toByteArray(), PUBLISH_TIMESTAMP)
      );
   }

   @Test
   public void convertMessageWithTimestampAttribute() {
      assertSourceMessage(
          converterWithTimestampAttribute.convert(MESSAGE_WITH_TIMESTAMP_ATTRIBUTE, ackReplyConsumer),
          recordFactory.create(MESSAGE_ID, MESSAGE_ID, DATA.toByteArray(), TIMESTAMP)
      );
   }

   @Test
   public void convertMessageWithIncorrectTimestampAttribute() {
      assertSourceMessage(
          converterWithTimestampAttribute.convert(MESSAGE_WITH_INCORRECT_TIMESTAMP_ATTRIBUTE, ackReplyConsumer),
          recordFactory.create(MESSAGE_ID, MESSAGE_ID, DATA.toByteArray(), PUBLISH_TIMESTAMP)
      );
   }

   @Test
   public void convertMessageWithKeyAndTimestampAttribute() {
      assertSourceMessage(
          converterWithKeyAndTimestampAttributes.convert(MESSAGE_WITH_KEY_AND_TIMESTAMP_ATTRIBUTE, ackReplyConsumer),
          recordFactory.create(MESSAGE_ID, KEY, DATA.toByteArray(), TIMESTAMP)
      );
   }

   private static void assertSourceMessage(SourceMessage sourceMessage, SourceRecord expected) {
      assertThat(sourceMessage.getRecords()).as("number of records").hasSize(1);
      SourceRecord record = sourceMessage.getRecords().iterator().next();
      assertThatRecordsAreEqual(record, expected);
   }

}
