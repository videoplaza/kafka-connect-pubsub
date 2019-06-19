package com.videoplaza.dataflow.pubsub;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PubsubSourceConnector extends SourceConnector {

   private static final Logger LOG = LoggerFactory.getLogger(PubsubSourceConnector.class);

   private Map<String, String> props;

   @Override public void start(Map<String, String> props) {
      this.props = props;
      LOG.info("Started with {}", props);
   }

   @Override public Class<? extends Task> taskClass() {
      return PubsubSourceTask.class;
   }

   @Override public List<Map<String, String>> taskConfigs(int maxTasks) {
      ArrayList<Map<String, String>> configs = new ArrayList<>(maxTasks);
      for (int i = 0; i < maxTasks; i++) {
         configs.add(new HashMap<>(props));
      }
      return configs;
   }

   @Override public void stop() {
      LOG.info("Stopping.");
   }

   @Override public ConfigDef config() {
      return PubsubSourceConnectorConfig.CONFIG;
   }

   @Override public String version() {
      return Version.getVersion();
   }
}
