package com.videoplaza.dataflow.pubsub.util;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

public class TaskMetrics implements TaskMetricsMXBean {

   private final ObjectName name;
   private final AtomicLong received = new AtomicLong();
   private final AtomicLong duplicates = new AtomicLong();
   private final AtomicLong acks = new AtomicLong();
   private final AtomicLong nacks = new AtomicLong();
   private final AtomicLong ackLost = new AtomicLong();
   private final AtomicLong evicted = new AtomicLong();

   public TaskMetrics(int taskId) {
      name = metricName(taskId);
   }

   /**
    * Total number of acknowledgments sent to Cloud Pubsub
    */
   @Override public long getAckCount() {
      return acks.get();
   }

   /**
    * Total number of negative acknowledgments sent to Cloud Pubsub.
    * Might occur during connector shutdown.
    */
   @Override public long getNackCount() {
      return nacks.get();
   }

   /**
    * Total number of messages received from Cloud Pubsub
    */
   @Override public long getReceivedCount() {
      return received.get();
   }

   @Override public long getAckLostCount() {
      return ackLost.get();
   }

   @Override public long getEvictedCount() {
      return evicted.get();
   }

   @Override public long getDuplicatesCount() {
      return duplicates.get();
   }

   @Override public long getCountersMismatch() {
      return received.get() - acks.get() - nacks.get();
   }

   public void onAckLost() {
      ackLost.incrementAndGet();
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
      return "TaskMetrics[r:" +
          getReceivedCount() + "/a:" +
          getAckCount() + "/n:" +
          getNackCount() + "|e:" +
          getEvictedCount() + "|d:" +
          getDuplicatesCount() + "][m:" +
          getCountersMismatch() + "/l:" +
          getAckLostCount() + "]";
   }


   public void onEvicted() {
      evicted.incrementAndGet();
   }

   public synchronized void registerMBean(Logger logger) {
      try {
         final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
         mBeanServer.registerMBean(this, name);
         logger.info("Registered task metrics '{}'", name);
      } catch (JMException e) {
         logger.warn("Error while register the MBean '{}': {}", name, e.getMessage());
      }
   }

   public final void unregisterMBean(Logger logger) {
      if (this.name != null) {
         try {
            final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            mBeanServer.unregisterMBean(name);
            logger.info("Unregistered task metrics '{}'", name);
         } catch (JMException e) {
            logger.error("Unable to unregister the MBean '{}'", name);
         }
      }
   }

   public ObjectName metricName(int taskId) {
      final String metricName = "com.videoplaza.pubsub:type=connector-metrics,taskid=" + taskId;
      try {
         return new ObjectName(metricName);
      } catch (MalformedObjectNameException e) {
         throw new ConnectException("Invalid metric name '" + metricName + "'");
      }
   }

}
