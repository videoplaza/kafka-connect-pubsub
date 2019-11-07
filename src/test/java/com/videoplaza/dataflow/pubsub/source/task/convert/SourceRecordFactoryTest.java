package com.videoplaza.dataflow.pubsub.source.task.convert;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class SourceRecordFactoryTest {
   private static final String SUBSCRIPTION = "s";
   private static final String TOPIC = "t";
   private static final String MESSAGE_ID = "id";
   private static final String KEY = "k";
   private static final long TIMESTAMP = 1234567890;
   private static final byte[] DATA = "data".getBytes();

   private final SourceRecordFactory factory = new SourceRecordFactory(SUBSCRIPTION, TOPIC);

   @Test(expected = NullPointerException.class)
   public void subscriptionRequired() {
      new SourceRecordFactory(null, "t");
   }

   @Test(expected = NullPointerException.class)
   public void topicRequired() {
      new SourceRecordFactory("s", null);
   }

   @Test public void create() {
      SourceRecord record = factory.create(MESSAGE_ID, KEY, DATA, TIMESTAMP);
      assertThat(record.sourcePartition()).as("source path").isNull();
      assertThat(record.sourceOffset().get(SUBSCRIPTION)).as("source offset map subscription key").isEqualTo(MESSAGE_ID);
      assertThat(record.topic()).as("topic").isEqualTo(TOPIC);
      assertThat(record.kafkaPartition()).as("kafka partition").isNull();
      assertThat(record.keySchema()).as("key schema").isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
      assertThat(record.key()).as("key").isEqualTo(KEY);
      assertThat(record.valueSchema()).as("value schema").isEqualTo(Schema.BYTES_SCHEMA);
      assertThat(record.value()).as("value").isEqualTo(DATA);
      assertThat(record.timestamp()).as("timestamp").isEqualTo(TIMESTAMP);
   }

}
