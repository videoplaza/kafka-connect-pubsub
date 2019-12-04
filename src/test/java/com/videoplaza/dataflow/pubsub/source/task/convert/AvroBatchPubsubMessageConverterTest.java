package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.avro.schema.Record;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.videoplaza.dataflow.pubsub.source.task.convert.AssertUtil.assertThatRecordsAreEqual;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class AvroBatchPubsubMessageConverterTest {

   private static final String TOPIC = "t";
   private static final String SUBSCRIPTION = "s";
   private static final String KEY_ATTRIBUTE = "key";
   private static final String MESSAGE_ID = "id";
   private static final String TIMESTAMP_ATTRIBUTE = "timestamp";
   private final static Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());
   private final TaskMetricsImpl metrics = new TaskMetricsImpl(CLOCK, 1000);
   private final SourceRecordFactory recordFactory = new SourceRecordFactory(SUBSCRIPTION, TOPIC);
   private final static long TIMESTAMP = 12345000;

   private static final DatumWriter<Record> datumWriter = new SpecificDatumWriter<>(Record.class);

   private final static Record aRecord = Record.newBuilder()
       .setData(StandardCharsets.UTF_8.encode("boom!"))
       .setAttributes(ImmutableMap.of(KEY_ATTRIBUTE, "k1", TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP)))
       .build();

   private final static Record anotherRecord = Record.newBuilder()
       .setData(StandardCharsets.UTF_8.encode("bam!"))
       .setAttributes(ImmutableMap.of(KEY_ATTRIBUTE, "k2", TIMESTAMP_ATTRIBUTE, Long.toString(TIMESTAMP + 1)))
       .build();

   private final static PubsubMessage PUBSUB_MESSAGE_WITH_NO_DATA = PubsubMessage.newBuilder()
       .setMessageId(MESSAGE_ID)
       .setPublishTime(Timestamp.newBuilder().setSeconds(TimeUnit.MILLISECONDS.toSeconds(TIMESTAMP)).build())
       .build();

   private final static PubsubMessage PUBSUB_MESSAGE_WITH_BATCH_OF_2 = PubsubMessage.newBuilder(PUBSUB_MESSAGE_WITH_NO_DATA)
       .setData(batch(aRecord, anotherRecord))
       .build();

   private final static PubsubMessage PUBSUB_MESSAGE_WITH_EMPTY_BATCH = PubsubMessage.newBuilder(PUBSUB_MESSAGE_WITH_NO_DATA)
       .setData(batch())
       .build();

   private final AvroBatchPubsubMessageConverter converterWithKeyAndTimestamp = new AvroBatchPubsubMessageConverter(
       recordFactory,
       new PubsubAttributeExtractor(KEY_ATTRIBUTE, TIMESTAMP_ATTRIBUTE),
       metrics,
       mock(PubsubSourceTaskLogger.class)
   );

   private final AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);

   @Test public void convert() {
      SourceMessage sm = converterWithKeyAndTimestamp.convert(PUBSUB_MESSAGE_WITH_BATCH_OF_2, ackReplyConsumer);
      assertThat(sm.size()).isEqualTo(2);
      Map<Object, SourceRecord> expected = records(aRecord, anotherRecord);
      for (SourceRecord r : sm.getRecords()) {
         assertThatRecordsAreEqual(r, expected.get(r.key()));
      }
      assertAttributes(sm);
   }

   @Test public void convertEmpty() {
      SourceMessage sm = converterWithKeyAndTimestamp.convert(PUBSUB_MESSAGE_WITH_EMPTY_BATCH, ackReplyConsumer);
      assertThat(sm.size()).isEqualTo(0);
      assertAttributes(sm);
   }

   private Map<Object, SourceRecord> records(Record... records) {
      return Arrays.stream(records).map(r -> recordFactory.create(
          MESSAGE_ID,
          r.getAttributes().get(KEY_ATTRIBUTE),
          r.getData().array(),
          Long.parseLong(r.getAttributes().get(TIMESTAMP_ATTRIBUTE)))).collect(Collectors.toMap(SourceRecord::key, identity()));
   }

   private static void assertAttributes(SourceMessage sm) {
      assertThat(sm.getMessageId()).isEqualTo(MESSAGE_ID);
      assertThat(sm.getCreatedMs()).isEqualTo(TIMESTAMP);
   }

   private static ByteString batch(Record... records) {
      final ByteString.Output byteStringOut = ByteString.newOutput();
      try (DataFileWriter<Record> dataFileWriter = new DataFileWriter<>(datumWriter)) {
         dataFileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
         dataFileWriter.create(Record.SCHEMA$, byteStringOut);
         for (Record r : records) {
            dataFileWriter.append(r);
         }
      } catch (IOException e) {
         throw new RuntimeException(e);
      }
      return byteStringOut.toByteString();
   }
}