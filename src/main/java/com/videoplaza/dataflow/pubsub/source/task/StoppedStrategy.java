package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class StoppedStrategy extends BaseStrategy {

   public StoppedStrategy(PubsubSourceTaskState state) {
      super(state);
   }

   @Override
   public void onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey) {
      log.error("Received a message after shutdown {}/{}. {}", pubsubMessage.getMessageId(), messageKey, state);
      state.getSubscriber().stopAsync();
   }

   @Override public void onDuplicateReceived(PubsubMessage pubsubMessage, String messageKey, Message current) {
      onNewMessageReceived(pubsubMessage, null, messageKey);
   }

   @Override public List<SourceRecord> poll() {
      log.error("Polling already stopped task {}", state);
      return null;
   }

   @Override public void commit() {
      log.error("Committing already stopped task {}", state);
   }

   @Override public void stop() {
      log.error("Stopping already stopped task {}", state);
   }
}
