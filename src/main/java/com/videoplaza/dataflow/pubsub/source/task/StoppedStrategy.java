package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class StoppedStrategy extends BaseStrategy {

   public StoppedStrategy(PubsubSourceTaskState state) {
      super(state);
   }

   @Override public void init() {
      state.getJmxReporter().stop();
   }

   @Override
   public SourceMessage onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {
      logger.error("Received a message after shutdown: {}.", pubsubMessage);
      state.getSubscriber().stopAsync();
      return null;
   }

   @Override public void onDuplicateReceived(PubsubMessage pubsubMessage, SourceMessage current) {
      onNewMessageReceived(pubsubMessage, null);
   }

   @Override public List<SourceRecord> poll() {
      logger.error("Polling already stopped task.");
      return null;
   }

   @Override public void commit() {
      logger.error("Committing already stopped task.");
   }

   @Override public void stop() {
      logger.error("Stopping already stopped task.");
   }
}
