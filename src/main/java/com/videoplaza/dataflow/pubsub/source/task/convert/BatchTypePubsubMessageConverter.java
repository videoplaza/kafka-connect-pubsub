package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;

import static java.util.Objects.requireNonNull;

public class BatchTypePubsubMessageConverter implements PubsubMessageConverter {

   private final PubsubMessageConverter singlePubsubMessageConverter;
   private final PubsubMessageConverter avroBatchPubsubMessageConverter;
   private final String batchTypeAttribute;

   public BatchTypePubsubMessageConverter(
       PubsubMessageConverter singlePubsubMessageConverter,
       PubsubMessageConverter avroBatchPubsubMessageConverter,
       String batchTypeAttribute
   ) {
      this.singlePubsubMessageConverter = requireNonNull(singlePubsubMessageConverter, "single message converter is required");
      this.avroBatchPubsubMessageConverter = requireNonNull(avroBatchPubsubMessageConverter, "avro batch converter is required");
      this.batchTypeAttribute = requireNonNull(batchTypeAttribute, "batch type attribute is required");
   }

   @Override public SourceMessage convert(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      String batchType = getBatchType(pubsubMessage);

      if (batchType == null) {
         return singlePubsubMessageConverter.convert(pubsubMessage, ackReplyConsumer);
      } else if (PubsubSourceConnectorConfig.AVRO_BATCH_TYPE.equals(batchType)) {
         return avroBatchPubsubMessageConverter.convert(pubsubMessage, ackReplyConsumer);
      }

      throw new UnsupportedOperationException("Unknown batch type" + batchType);
   }

   String getBatchType(PubsubMessage message) {
      return message.getAttributesMap().get(batchTypeAttribute);
   }
}
