package com.videoplaza.dataflow.pubsub.metrics;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.videoplaza.dataflow.pubsub.util.SnapshotMemoizingHistogram;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramResetOnSnapshotReservoir;

import java.time.Clock;

import static java.util.Objects.requireNonNull;

public class TaskMetricsImpl implements TaskMetrics {

   public static Histogram histogram(long memoizeHistogramSnapshotMs) {
      return new SnapshotMemoizingHistogram(new HdrHistogramResetOnSnapshotReservoir(), memoizeHistogramSnapshotMs);
   }

   private final Clock clock;
   private final MetricRegistry metricRegistry = new MetricRegistry();

   private final MessageMetrics messageMetrics;
   private final RecordMetrics recordMetrics;

   public TaskMetricsImpl(Clock clock, long histogramUpdateIntervalMs) {
      this.clock = requireNonNull(clock);
      messageMetrics = new MessageMetrics(histogramUpdateIntervalMs);
      recordMetrics = new RecordMetrics(histogramUpdateIntervalMs);
   }

   /**
    * Total number of acknowledgments sent to Cloud Pubsub
    */
   public long getMessageAckCount() {
      return messageMetrics.acks.get();
   }

   public long getRecordAckCount() {
      return recordMetrics.acks.get();
   }

   /**
    * Total number of negative acknowledgments sent to Cloud Pubsub.
    * Might occur during connector shutdown.
    */
   public long getMessageNackCount() {
      return messageMetrics.nacks.get();
   }

   /**
    * Total number of messages received from Cloud Pubsub
    */
   public long getMessageReceivedCount() {
      return messageMetrics.received.get();
   }

   public long getRecordAckLostCount() {
      return recordMetrics.acksLost.get();
   }

   public long getMessageEvictedCount() {
      return messageMetrics.evicted.get();
   }

   public long getMessageDuplicatesCount() {
      return messageMetrics.duplicates.get();
   }

   public long getMessageReceivedButNotAcked() {
      return messageMetrics.getReceivedButNotAcked();
   }

   public long getMessageConversionFailures() {
      return messageMetrics.failedConversions.get();
   }

   public void onRecordAckLost() {
      recordMetrics.acksLost.incrementAndGet();
   }

   public void onMessageAck(long receivedMs) {
      messageMetrics.acks.incrementAndGet();
      messageMetrics.ackLag.update(lag(receivedMs));
   }

   public void onRecordAck(long receivedMs) {
      recordMetrics.acks.incrementAndGet();
      recordMetrics.ackLag.update(lag(receivedMs));
   }

   public void onMessageNack(long receivedMs) {
      messageMetrics.nacks.incrementAndGet();
      messageMetrics.nackLag.update(lag(receivedMs));
   }

   public void onMessageDuplicate(long createdMs, long receivedMs) {
      messageMetrics.duplicates.incrementAndGet();
      messageMetrics.duplicateCreateLag.update(lag(createdMs));
      messageMetrics.duplicateLag.update(lag(receivedMs));
   }

   public void onMessageReceived(long createdMs, int records) {
      messageMetrics.received.incrementAndGet();
      recordMetrics.received.addAndGet(records);
      messageMetrics.receivedLag.update(lag(createdMs));
      messageMetrics.recordsPerMessage.update(records);
   }

   public void onMessageEvicted(long receivedMs) {
      messageMetrics.evicted.incrementAndGet();
      messageMetrics.evictedLag.update(lag(receivedMs));
   }

   public void onRecordReceived(long createdMs) {
      recordMetrics.received.incrementAndGet();
      recordMetrics.receivedLag.update(lag(createdMs));
   }

   public void onMessageConversionFailure() {
      messageMetrics.failedConversions.incrementAndGet();
   }

   private long lag(long ts) {
      return Math.max(0, clock.millis() - ts);
   }

   public Clock getClock() {
      return clock;
   }

   public MetricRegistry getMetricRegistry() {
      return metricRegistry;
   }

   @Override public String toString() {
      return "TaskMetrics[r:" +
          getMessageReceivedCount() + "/a:" +
          getMessageAckCount() + "/n:" +
          getMessageNackCount() + "|e:" +
          getMessageEvictedCount() + "|d:" +
          getMessageDuplicatesCount() + "][m:" +
          getMessageReceivedButNotAcked() + "/l:" +
          getRecordAckLostCount() + "/f:" +
          getMessageConversionFailures() + "]";
   }

   @Override public MessageMetrics getMessageMetrics() {
      return messageMetrics;
   }

   @Override public RecordMetrics getRecordMetrics() {
      return recordMetrics;
   }

   @Override public void register(String prefix, MetricRegistry registry) {
      messageMetrics.register(prefix, registry);
      recordMetrics.register(prefix, registry);
   }

   public void onAvroBatchMessageReceived() {
      messageMetrics.receivedAvroBatch.incrementAndGet();
   }

   public void onSingleMessageReceived(long createdMs) {
      messageMetrics.receivedSingle.incrementAndGet();
      onRecordReceived(createdMs);
   }
}
