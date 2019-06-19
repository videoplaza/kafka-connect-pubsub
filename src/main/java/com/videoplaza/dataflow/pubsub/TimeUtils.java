package com.videoplaza.dataflow.pubsub;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TimeUtils {

   public static long msSince(long startNanos) {
      return Math.max(0, NANOSECONDS.toMillis(System.nanoTime() - startNanos));
   }

   public static long msTo(long deadline) {
      return msTo(deadline, 0);
   }

   public static long msTo(long deadline, long minValue) {
      return Math.max(minValue, NANOSECONDS.toMillis(deadline - System.nanoTime()));
   }
}
