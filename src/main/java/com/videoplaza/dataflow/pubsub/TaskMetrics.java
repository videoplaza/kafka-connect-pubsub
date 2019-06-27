package com.videoplaza.dataflow.pubsub;

import java.util.concurrent.atomic.AtomicLong;

public class TaskMetrics {
   private final AtomicLong received = new AtomicLong();
   private final AtomicLong duplicates = new AtomicLong();
   private final AtomicLong acks = new AtomicLong();
   private final AtomicLong nacks = new AtomicLong();
   private final AtomicLong lost = new AtomicLong();

   /**
    * Total number of acknowledgments sent to Cloud Pubsub
    */
   public long getAckCount() {
      return acks.get();
   }

   /**
    * Total number of negative acknowledgments sent to Cloud Pubsub.
    * Might occur during connector shutdown.
    */
   public long getNackCount() {
      return nacks.get();
   }

   /**
    * Total number of messages received from Cloud Pubsub
    */
   public long getReceivedCount() {
      return received.get();
   }

   public long getLostCount() {
      return lost.get();
   }

   public long getDuplicatesCount() {
      return duplicates.get();
   }

   public long getCountersMismatch() {
      return received.get() - acks.get() - nacks.get();
   }

   public void onLost() {
      lost.incrementAndGet();
   }

   public void onAck() {
      acks.incrementAndGet();
   }

   public void onNack() {
      nacks.incrementAndGet();
   }

   public void onDuplicate() {
      duplicates.incrementAndGet();
   }

   public void onReceived() {
      received.incrementAndGet();
   }

   @Override public String toString() {
      return "TaskMetrics[" +
          getReceivedCount() + "/" +
          getAckCount() + "/" +
          getNackCount() + "|" +
          getDuplicatesCount() + "][" +
          getCountersMismatch() + "/" +
          getLostCount() + "]";
   }


}
