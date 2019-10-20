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
      pollTimeoutMs = state.getConfig().gePollTimeoutMs();
   }

   @Override
   public void onNewMessageReceived(PubsubMessage pubsubMessage, AckReplyConsumer ackReplyConsumer, String messageKey) {
      if (isDebugEnabled(messageKey)) {
         log.debug("Received {}/{}. {}", pubsubMessage.getMessageId(), messageKey, state);
      }

      messages.put(new Message(pubsubMessage.getMessageId(), state.getConverter().convert(pubsubMessage), ackReplyConsumer, metrics));

      metrics.onReceived();

      receiveLock.lock();
      try {
         recordsReceived.signalAll();
      } finally {
         receiveLock.unlock();
      }
   }

   @Override public List<SourceRecord> poll() {
      long timeWaited = waitForMessagesToPoll();
      try {
         if (state.getShutdownLock().tryLock(pollTimeoutMs, TimeUnit.MILLISECONDS)) {
            try {
               List<SourceRecord> records = messages.poll();
               log.trace("Returning {} records after waiting for {}ms. {}", records.size(), timeWaited, state);
               return records.isEmpty() ? null : records;
            } finally {
               state.getShutdownLock().unlock();
            }
         } else {
            log.info("Could not obtain lock in {}ms. Will try later.", pollTimeoutMs);
            return null;
         }
      } catch (InterruptedException e) {
         log.info("Poll was interrupted");
         Thread.currentThread().interrupt();
         return null;
      }
   }

   /**
    * Waits for new messages to minimize CPU consumption.
    */
   private long waitForMessagesToPoll() {
      long start = System.nanoTime();
      receiveLock.lock();
      try {
         boolean received = recordsReceived.await(pollTimeoutMs, TimeUnit.MILLISECONDS);
         log.trace("Received={}, in {}ms", received, msSince(start));
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      } finally {
         receiveLock.unlock();
      }
      return msSince(start);
   }

   @Override public void commit() {
   }

   @Override public void stop() {
      state.moveTo(new StoppingStrategy(state));
   }
}
