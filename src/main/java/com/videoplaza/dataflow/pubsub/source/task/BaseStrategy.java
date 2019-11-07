package com.videoplaza.dataflow.pubsub.source.task;

import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.source.task.convert.PubsubAttributeExtractor;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import org.apache.kafka.connect.source.SourceRecord;

public abstract class BaseStrategy implements PubsubSourceTaskStrategy {

   private final String subscription;
   final PubsubSourceTaskState state;
   final SourceMessageMap messages;
   final PubsubAttributeExtractor attributeExtractor;
   final PubsubSourceTaskLogger logger;

   public BaseStrategy(PubsubSourceTaskState state) {
      this.state = state;
      logger = state.getLogger();
      messages = state.getMessages();
      subscription = state.getConfig().getSubscription();
      attributeExtractor = state.getConfig().getPubsubAttributeExtractor();
   }

   @Override public void init() {
   }

   @Override public void onDuplicateReceived(PubsubMessage pubsubMessage, SourceMessage current) {
   }

   @Override public SourceMessage commitRecord(SourceRecord record) {
      String messageId = (String) record.sourceOffset().get(subscription);
      SourceMessage sourceMessage = messages.get(messageId);
      if (sourceMessage != null) {
         boolean allRecordsAcked = sourceMessage.ack(record);
         state.getMetrics().onRecordAck(sourceMessage.getReceivedMs()); //TODO move to the task?
         if (allRecordsAcked) {
            messages.remove(messageId);
            state.getMetrics().onMessageAck(sourceMessage.getReceivedMs());
            return sourceMessage;
         }
      } else {
         state.getMetrics().onRecordAckLost();
      }
      return null;
   }

   @Override public String toString() {
      return getClass().getSimpleName();
   }
}
