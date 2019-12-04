package com.videoplaza.dataflow.pubsub.source.task;


import com.codahale.metrics.jmx.JmxReporter;
import com.google.cloud.pubsub.v1.Subscriber;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.metrics.TaskMetricsImpl;
import com.videoplaza.dataflow.pubsub.source.task.convert.PubsubMessageConverter;
import com.videoplaza.dataflow.pubsub.util.PubsubSourceTaskLogger;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;

/**
 * A state shared by {@link PubsubSourceTaskStrategy} instances.
 */
public interface PubsubSourceTaskState {

   PubsubSourceTaskLogger getLogger();

   Subscriber getSubscriber();

   TaskMetricsImpl getMetrics();

   PubsubMessageConverter getConverter();

   PubsubSourceConnectorConfig getConfig();

   void moveTo(PubsubSourceTaskStrategy strategy);

   boolean isClean();

   /**
    * Collection of messages in flight.
    */
   SourceMessageMap getMessages();

   JmxReporter getJmxReporter();

   EventLoopGroup getPubsubEventLoopGroup();
}
