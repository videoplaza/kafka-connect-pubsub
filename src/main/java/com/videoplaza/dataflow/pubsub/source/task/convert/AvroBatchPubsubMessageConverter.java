package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.avro.schema.Record;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class AvroBatchPubsubMessageConverter implements PubsubMessageConverter {

   private final SpecificDatumReader<Record> datumReader = new SpecificDatumReader<>(Record.class);

   private final SourceRecordFactory recordFactory;

   private final PubsubAttributeExtractor attributeExtractor;

   private final TaskMetricsImpl metrics;
   private final PubsubSourceTaskLogger logger;

   public AvroBatchPubsubMessageConverter(
       SourceRecordFactory recordFactory,
       PubsubAttributeExtractor attributeExtractor,
       TaskMetricsImpl metrics,
       PubsubSourceTaskLogger logger
   ) {
      this.recordFactory = requireNonNull(recordFactory);
      this.attributeExtractor = requireNonNull(attributeExtractor);
      this.metrics = requireNonNull(metrics);
      this.logger = requireNonNull(logger);
   }

   @Override public SourceMessage convert(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      metrics.onAvroBatchMessageReceived();
      String messageId = pubsubMessage.getMessageId();
      long createdMs = attributeExtractor.getTimestamp(pubsubMessage);
      try (DataFileReader<Record> reader = new DataFileReader<>(new SeekableByteArrayInput(pubsubMessage.getData().toByteArray()), datumReader)) {
         List<String> traceableKeys = new LinkedList<>();
         Map<Object, SourceRecord> records = StreamSupport.stream(reader.spliterator(), false)
             .map((m) -> from(m, messageId, createdMs))
             .peek(r -> {
                if (logger.isTraceable(r.key())) {
                   traceableKeys.add(r.key().toString());
                }
             }).collect(toMap(SourceRecord::key, identity()));

         return new SourceMessage(messageId, createdMs, records, ackReplyConsumer, traceableKeys);
      } catch (IOException e) {
         throw new RuntimeException("Fail to read the data for " + messageId, e);
      }
   }

   private SourceRecord from(Record record, String messageId, long timestamp) {
      Optional<Long> ts = attributeExtractor.getTimestamp(record.getAttributes());
      ts.ifPresent(metrics::onRecordReceived);
      return recordFactory.create(
          messageId,
          attributeExtractor.getKey(record.getAttributes()),
          record.getData().array(),
          ts.orElse(timestamp)
      );
   }

}
