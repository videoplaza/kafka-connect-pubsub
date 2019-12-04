package com.videoplaza.dataflow.pubsub.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

import static com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl.histogram;

public class RecordMetrics {
   private final static String PREFIX = ".record.";

   final AtomicLong received = new AtomicLong();
   final AtomicLong acks = new AtomicLong();
   final AtomicLong acksLost = new AtomicLong();
   final Histogram ackLag;
   final Histogram receivedLag;

   public RecordMetrics(long memoizeHistogramSnapshotMs) {
      ackLag = histogram(memoizeHistogramSnapshotMs);
      receivedLag = histogram(memoizeHistogramSnapshotMs);
   }

   public void register(String prefix, MetricRegistry registry) {
      registry.register(name(prefix, "received"), (Gauge<Long>) received::longValue);
      registry.register(name(prefix, "acks"), (Gauge<Long>) acks::longValue);
      registry.register(name(prefix, "acks.lost"), (Gauge<Long>) acksLost::longValue);
      registry.register(name(prefix, "lag.ack"), ackLag);
      registry.register(name(prefix, "lag.received"), receivedLag);
   }

   private String name(String prefix, String name) {
      return prefix + PREFIX + name;
   }

   public long getReceived() {
      return received.get();
   }

   public long getAcks() {
      return acks.get();
   }

   public long getAcksLost() {
      return acksLost.get();
   }

   public Histogram getAckLag() {
      return ackLag;
   }

   public Histogram getReceivedLag() {
      return receivedLag;
   }
}
