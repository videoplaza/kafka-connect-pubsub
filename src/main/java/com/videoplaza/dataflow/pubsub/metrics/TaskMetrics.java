package com.videoplaza.dataflow.pubsub.metrics;

import com.codahale.metrics.MetricRegistry;

public interface TaskMetrics {

   MessageMetrics getMessageMetrics();

   RecordMetrics getRecordMetrics();

   void register(String prefix, MetricRegistry registry);

}
