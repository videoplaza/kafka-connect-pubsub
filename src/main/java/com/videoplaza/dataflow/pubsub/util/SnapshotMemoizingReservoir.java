package com.videoplaza.dataflow.pubsub.util;

import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.google.common.base.Supplier;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static java.util.Objects.requireNonNull;

public class SnapshotMemoizingReservoir implements Reservoir {
   private final Reservoir target;
   private final Supplier<Snapshot> snapshotSupplier;

   public SnapshotMemoizingReservoir(Reservoir target, long durationMs) {
      this.target = requireNonNull(target);
      snapshotSupplier = memoizeWithExpiration(target::getSnapshot, durationMs, TimeUnit.MILLISECONDS);
   }

   @Override public int size() {
      return snapshotSupplier.get().size();
   }

   @Override public void update(long value) {
      target.update(value);
   }

   @Override public Snapshot getSnapshot() {
      return snapshotSupplier.get();
   }
}


