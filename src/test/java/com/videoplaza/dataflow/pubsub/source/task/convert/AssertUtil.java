package com.videoplaza.dataflow.pubsub.source.task.convert;

import org.apache.kafka.connect.source.SourceRecord;

import static org.assertj.core.api.Assertions.assertThat;

public class AssertUtil {

   public static void assertThatRecordsAreEqual(SourceRecord record, SourceRecord expected) {
      assertThat(record.timestamp()).as("record timestamp").isEqualTo(expected.timestamp());
      assertThat(record.key()).as("record key").isEqualTo(expected.key());
      assertThat(record.topic()).as("record topic").isEqualTo(expected.topic());
      assertThat(record.sourceOffset()).as("record source offset").isEqualTo(expected.sourceOffset());
      assertThat((byte[]) record.value()).as("record value").isEqualTo((byte[]) expected.value());
   }
}
