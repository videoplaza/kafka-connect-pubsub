package com.videoplaza.dataflow.pubsub.source.task;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.videoplaza.dataflow.pubsub.util.TimeUtils.msSince;

public class RunningStrategy extends BaseStrategy {

   private final Lock receiveLock = new ReentrantLock();
   private final Condition recordsReceived = receiveLock.newCondition();
   private final long pollTimeoutMs;

   public RunningStrategy(PubsubSourceTaskState state) {
      super(state);
      pollTimeoutMs = state.getConfig().getPollTimeoutMs();
   }

   @Override public void init() {
      state.getSubscriber().startAsync();
      state.getJmxReporter().start();
   }

   @Override
   public SourceMessage onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer) {

      try {
         SourceMessage m = state.getConverter().convert(pubsubMessage, ackReplyConsumer);
         messages.put(m);
         state.getMetrics().onMessageReceived(m.getCreatedMs(), m.size());
         receiveLock.lock();
         try {
            recordsReceived.signalAll();
         } finally {
            receiveLock.unlock();
         }
         return m;
      } catch (RuntimeException e) {
         state.getMetrics().onMessageConversionFailure();
         if (shouldAckAndIgnoreFailedConversion()) {
            ackReplyConsumer.ack();
            logger.warn("Failed to convert a message. Acked and ignored. "+ logger.traceInfo(pubsubMessage), e);
         } else {
            logger.error("Failed to convert a message. " + logger.traceInfo(pubsubMessage), e);
         }
      }
      return null;
   }

   boolean shouldAckAndIgnoreFailedConversion() {
      return state.getMetrics().getMessageConversionFailures() <= state.getConfig().getMaxNumberOfConversionFailures();
   }

   @Override public List<SourceRecord> poll() {
      long start = System.nanoTime();
      try {
         waitForMessagesToPoll(start);
         List<SourceRecord> records = messages.pollRecords();
         logger.trace("Returning {} records in {}ms.", records.size(), msSince(start));
         return records.isEmpty() ? null : records;
      } catch (InterruptedException e) {
         logger.warn("Poll was interrupted.");
         Thread.currentThread().interrupt();
      }
      return null;
   }

   /**
    * Waits for new messages to minimize CPU consumption.
    */
   private void waitForMessagesToPoll(long start) throws InterruptedException {
      receiveLock.lock();
      try {
         boolean received = recordsReceived.await(pollTimeoutMs, TimeUnit.MILLISECONDS);
         logger.trace("Received={}, in {}ms.", received, msSince(start));
      } finally {
         receiveLock.unlock();
      }
   }

   @Override public void commit() {
   }

   @Override public void stop() {
      state.moveTo(new StoppingStrategy(state));
   }

   long getPollTimeoutMs() {
      return pollTimeoutMs;
   }
}
