package com.videoplaza.dataflow.pubsub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class Version {
   private static final Logger LOG = LoggerFactory.getLogger(Version.class);
   private static final String VERSION;

   private Version() {}

   static {
      Properties props = new Properties();
      try {
         props.load(Version.class.getResourceAsStream("/kafka-connect-pubsub-version.properties"));
      } catch (Exception e) {
         LOG.warn("Error while loading version.", e);
      }

      VERSION = props.getProperty("version", "unknown").trim();
   }

   public static String getVersion() {
      return VERSION;
   }
}
