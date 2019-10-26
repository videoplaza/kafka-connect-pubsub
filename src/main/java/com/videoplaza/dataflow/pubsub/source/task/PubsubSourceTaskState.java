package com.videoplaza.dataflow.pubsub.source.task;


import com.google.cloud.pubsub.v1.Subscriber;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.util.TaskMetrics;
import org.slf4j.Logger;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A state shared by {@link PubsubSourceTaskStrategy} instances.
 */
public interface PubsubSourceTaskState {

   Logger getLogger();

   Subscriber getSubscriber();

   TaskMetrics getMetrics();

   PubsubMessageConverter getConverter();

   PubsubSourceConnectorConfig getConfig();

   boolean isDebugEnabled(String messageKey);

   void moveTo(PubsubSourceTaskStrategy strategy);

   boolean isClean();

   /**
    * Collection of messages in flight.
    */
   MessageMap getMessages();

   ReentrantLock getShutdownLock();
}
