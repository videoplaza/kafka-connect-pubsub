package com.videoplaza.dataflow.pubsub.util;

public interface TaskMetricsMXBean {

   long getAckCount();

   long getNackCount();

   long getReceivedCount();

   long getAckLostCount();

   long getEvictedCount();

   long getDuplicatesCount();

   long getCountersMismatch();
}
