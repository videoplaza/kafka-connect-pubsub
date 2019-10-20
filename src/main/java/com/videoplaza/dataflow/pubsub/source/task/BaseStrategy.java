package com.videoplaza.dataflow.pubsub.source.task;

import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.util.TaskMetrics;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;

public abstract class BaseStrategy implements PubsubSourceTaskStrategy {

   private final String subscription;
   final PubsubSourceTaskState state;
   final Logger log;
   final MessageMap messages;
   final TaskMetrics metrics;

   public BaseStrategy(PubsubSourceTaskState state) {
      this.state = state;
      log = state.getLogger();
      messages = state.getMessages();
      metrics = state.getMetrics();
      subscription  = state.getConfig().getSubscription();
   }

   @Override public void init() {}

   @Override public void onDuplicateReceived(PubsubMessage pubsubMessage, String messageKey, Message current) {
      state.getMetrics().onDuplicate();

      if (isDebugEnabled(messageKey)) {
         log.debug("A duplicate for {} received: [{}/{}]. {}.", current, messageKey, state.getConverter().getTimestamp(pubsubMessage), state);
      }
   }

   boolean isDebugEnabled(String messageKey) {
      return state.isDebugEnabled(messageKey);
   }

   @Override public void commitRecord(SourceRecord record) {
      commitRecord(record, true);
   }

   void commitRecord(SourceRecord record, boolean ack) {
      String messageId = (String) record.sourceOffset().get(subscription);
      Message message = messages.get(messageId);
      if (message != null) {
         message.ack(ack);
         messages.remove(messageId);

         if (isDebugEnabled(record.key().toString())) {
            log.debug("{} {}. {}", ack ? "Acked" : "Nacked", message, state);
         }
      } else {
         metrics.onAckLost();

         log.warn("Nothing to ack[{}] for {}/{}. So far: {}. Debug enabled: {}.",
             ack,
             messageId,
             record.key(),
             metrics.getAckLostCount(),
             (isDebugEnabled(record.key() == null ? messageId : record.key().toString()))
         );

      }
   }
}
