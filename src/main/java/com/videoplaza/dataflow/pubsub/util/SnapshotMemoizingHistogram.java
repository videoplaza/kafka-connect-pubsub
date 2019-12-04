package com.videoplaza.dataflow.pubsub.util;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;

public class SnapshotMemoizingHistogram extends Histogram {

   public SnapshotMemoizingHistogram(Reservoir reservoir, long durationMs) {
      super(new SnapshotMemoizingReservoir(reservoir, durationMs));
   }

   @Override public long getCount() {
      return getSnapshot().size();
   }
}
