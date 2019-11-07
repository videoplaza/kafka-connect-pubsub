package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;


/**
 * Converts Cloud Pubsub message into {@link SourceRecord} instance.
 *
 * <ul>
 * <li>Kafka message key is either set to <code>keyAttribute</code> or, if its not defined, {@link PubsubMessage#getMessageId()}</li>
 * <li>Kafka message timestamp is either set to <code>timestampAttribute</code> or, if its not defined, {@link PubsubMessage#getPublishTime()}</li>
 * </ul>
 */
public class SinglePubsubMessageConverter implements PubsubMessageConverter {

   private final SourceRecordFactory recordFactory;
   private final PubsubAttributeExtractor attributeExtractor;
   private final TaskMetricsImpl metrics;
   private final PubsubSourceTaskLogger logger;

   public SinglePubsubMessageConverter(
       SourceRecordFactory recordFactory,
       PubsubAttributeExtractor attributeExtractor,
       TaskMetricsImpl metrics,
       PubsubSourceTaskLogger logger
   ) {
      this.attributeExtractor = requireNonNull(attributeExtractor);
      this.recordFactory = requireNonNull(recordFactory);
      this.metrics = requireNonNull(metrics);
      this.logger = requireNonNull(logger);
   }

   @Override public SourceMessage convert(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
      long createdMs = attributeExtractor.getTimestamp(message);
      metrics.onSingleMessageReceived(createdMs);
      Map<Object, SourceRecord> records = new HashMap<>();
      String key = attributeExtractor.getKey(message);
      records.put(
          attributeExtractor.getKey(message),
          recordFactory.create(message.getMessageId(), key, message.getData().toByteArray(), createdMs)
      );

      SourceMessage sm = new SourceMessage(message.getMessageId(), createdMs, records, ackReplyConsumer, logger.isTraceable(key));
      logger.log("Converted single message. {}/{}", sm, message);
      return sm;
   }
}
