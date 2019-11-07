package com.videoplaza.dataflow.pubsub.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicLong;

import static com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl.histogram;

public class MessageMetrics {
   private final static String PREFIX = ".message.";

   final AtomicLong received = new AtomicLong();
   final AtomicLong receivedAvroBatch = new AtomicLong();
   final AtomicLong receivedSingle = new AtomicLong();
   final AtomicLong duplicates = new AtomicLong();
   final AtomicLong acks = new AtomicLong();
   final AtomicLong nacks = new AtomicLong();
   final AtomicLong evicted = new AtomicLong();
   final AtomicLong failedConversions = new AtomicLong();
   final Histogram ackLag;
   final Histogram nackLag;
   final Histogram duplicateCreateLag;
   final Histogram duplicateLag;
   final Histogram receivedLag;
   final Histogram recordsPerMessage;
   final Histogram evictedLag;

   public MessageMetrics(long memoizeHistogramSnapshotMs) {
      ackLag = histogram(memoizeHistogramSnapshotMs);
      nackLag = histogram(memoizeHistogramSnapshotMs);
      duplicateCreateLag = histogram(memoizeHistogramSnapshotMs);
      duplicateLag = histogram(memoizeHistogramSnapshotMs);
      receivedLag = histogram(memoizeHistogramSnapshotMs);
      recordsPerMessage = histogram(memoizeHistogramSnapshotMs);
      evictedLag = histogram(memoizeHistogramSnapshotMs);
   }

   public long getReceivedButNotAcked() {
      return received.get() - acks.get() - nacks.get();
   }

   public void register(String prefix, MetricRegistry registry) {
      registry.register(name(prefix, "received"), (Gauge<Long>) received::longValue);
      registry.register(name(prefix, "received.avrobatch"), (Gauge<Long>) receivedAvroBatch::longValue);
      registry.register(name(prefix, "received.single"), (Gauge<Long>) receivedSingle::longValue);
      registry.register(name(prefix, "duplicates"), (Gauge<Long>) duplicates::longValue);
      registry.register(name(prefix, "acks"), (Gauge<Long>) acks::longValue);
      registry.register(name(prefix, "nacks"), (Gauge<Long>) nacks::longValue);
      registry.register(name(prefix, "evicted"), (Gauge<Long>) evicted::longValue);
      registry.register(name(prefix, "received.notacked"), (Gauge<Long>) this::getReceivedButNotAcked);
      registry.register(name(prefix, "failed.convert"), (Gauge<Long>) failedConversions::longValue);
      registry.register(name(prefix, "lag.ack"), ackLag);
      registry.register(name(prefix, "lag.nack"), nackLag);
      registry.register(name(prefix, "lag.duplicate.create"), duplicateCreateLag);
      registry.register(name(prefix, "lag.duplicate"), duplicateLag);
      registry.register(name(prefix, "lag.received"), receivedLag);
      registry.register(name(prefix, "lag.evicted"), evictedLag);
      registry.register(name(prefix, "rpm"), recordsPerMessage);
   }

   private String name(String prefix, String name) {
      return prefix + PREFIX + name;
   }

   public long getReceived() {
      return received.get();
   }

   public long getDuplicates() {
      return duplicates.get();
   }

   public long getAcks() {
      return acks.get();
   }

   public long getNacks() {
      return nacks.get();
   }

   public long getEvicted() {
      return evicted.get();
   }

   public long getFailedConversions() {
      return failedConversions.get();
   }

   public Histogram getAckLag() {
      return ackLag;
   }

   public Histogram getNackLag() {
      return nackLag;
   }

   public Histogram getDuplicateCreateLag() {
      return duplicateCreateLag;
   }

   public Histogram getDuplicateLag() {
      return duplicateLag;
   }

   public Histogram getReceivedLag() {
      return receivedLag;
   }

   public Histogram getRecordsPerMessage() {
      return recordsPerMessage;
   }

   public Histogram getEvictedLag() {
      return evictedLag;
   }

}