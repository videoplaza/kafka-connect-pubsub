package com.videoplaza.dataflow.pubsub.util;

import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.source.task.PubsubSourceTask;
import com.videoplaza.dataflow.pubsub.source.task.PubsubSourceTaskState;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;
import com.videoplaza.dataflow.pubsub.source.task.convert.PubsubAttributeExtractor;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Base64;

import static java.util.Objects.requireNonNull;

public class PubsubSourceTaskLogger {

   private final String taskUUID;
   private final PubsubAttributeExtractor attributeExtractor;
   private final PubsubSourceTaskState state;
   private final Logger log;
   private final int debugLogSparsity;

   public PubsubSourceTaskLogger(String taskUUID, PubsubAttributeExtractor attributeExtractor, PubsubSourceTaskState state, int debugLogSparsity) {
      this.taskUUID = taskUUID;
      this.attributeExtractor = requireNonNull(attributeExtractor);
      this.state = requireNonNull(state);
      log = LoggerFactory.getLogger(PubsubSourceTask.class);
      this.debugLogSparsity = debugLogSparsity;
   }

   public boolean isTraceable(Object messageKey) {
      return messageKey != null && messageKey.hashCode() % debugLogSparsity == 0;
   }

   public String debugInfo(PubsubMessage message) {
      return attributeExtractor.getKey(message.getAttributesMap()) + "/" + attributeExtractor.getTimestamp(message);
   }

   public String traceInfo(PubsubMessage message) {
      return attributeExtractor.getKey(message.getAttributesMap()) + "/" + attributeExtractor.getTimestamp(message)+" data:[ "+ Base64.getEncoder().encodeToString(message.toByteArray()) +"]";
   }

   public String getTaskUUID() {
      return taskUUID;
   }

   public void error(String message) {
      if (log.isErrorEnabled()) {
         log.error(message + " [{}]", state);
      }
   }

   public void error(String message, Object arg1) {
      if (log.isErrorEnabled()) {
         log.error(message + " [{}]", arg1, state);
      }
   }

   public void error(String message, Throwable e) {
      if (log.isErrorEnabled()) {
         log.error(message + " [" + state + "]", e);
      }
   }


   public void error(String message, PubsubMessage pubsubMessage) {
      if (log.isErrorEnabled()) {
         log.error(message + " [{}]", debugInfo(pubsubMessage), state);
      }
   }

   public void warn(String message) {
      if (log.isWarnEnabled()) {
         log.warn(message + " [{}]", state);
      }
   }

   public void warn(String message, Object arg1) {
      if (log.isWarnEnabled()) {
         log.warn(message + " [{}]", arg1, state);
      }
   }

   public void warn(String message, PubsubMessage pubsubMessage) {
      if (log.isWarnEnabled()) {
         log.warn(message + " [{}]", debugInfo(pubsubMessage), state);
      }
   }

   public void warn(String message, Throwable e) {
      if (log.isWarnEnabled()) {
         log.warn(message + " [{" + state + "}]", e);
      }
   }

   public void warn(String message, SourceRecord sourceRecord) {
      if (log.isWarnEnabled() && sourceRecord != null) {
         log.warn(message + (isTraceable(sourceRecord.key()) ? "traceable" : "") + " [{}]", sourceRecord, state);
      }
   }

   public void info(String message) {
      if (log.isInfoEnabled()) {
         log.info(message + " [{}]", state);
      }
   }

   public void info(String message, Object arg1) {
      if (log.isInfoEnabled()) {
         log.info(message + " [{}]", arg1, state);
      }
   }

   public void info(String message, Object arg1, Object arg2) {
      if (log.isInfoEnabled()) {
         log.info(message + " [{}]", arg1, arg2, state);
      }
   }

   public void trace(String message, Object arg1) {
      if (log.isTraceEnabled()) {
         log.trace(message + " [{}]", arg1, state);
      }
   }

   public void trace(String message, Object arg1, Object arg2) {
      if (log.isTraceEnabled()) {
         log.trace(message + " [{}]", arg1, arg2, state);
      }
   }

   public void log(String message, SourceMessage sourceMessage) {
      if (log.isDebugEnabled() && sourceMessage != null && sourceMessage.hasTraceableRecords()) {
         log.debug(message + " [{}]", sourceMessage, state);
      } else if (log.isTraceEnabled()) {
         log.trace(message + " [{}]", sourceMessage, state);
      }
   }

   public void log(String message, SourceMessage sourceMessage, PubsubMessage pubsubMessage) {
      if (log.isDebugEnabled() && sourceMessage != null && sourceMessage.hasTraceableRecords()) {
         log.debug(message + " [{}]", sourceMessage, attributeExtractor.debugInfo(pubsubMessage), state);
      } else if (log.isTraceEnabled()) {
         log.trace(message + " [{}]", sourceMessage, attributeExtractor.debugInfo(pubsubMessage), state);
      }
   }

   public void log(String message, SourceRecord sourceRecord) {
      if (log.isDebugEnabled() && isTraceable(sourceRecord.key())) {
         log.debug(message + " [{}]", sourceRecord, state);
      } else if (log.isTraceEnabled()) {
         log.trace(message + " [{}]", sourceRecord, state);
      }
   }

}
