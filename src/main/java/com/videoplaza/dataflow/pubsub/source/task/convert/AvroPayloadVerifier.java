package com.videoplaza.dataflow.pubsub.source.task.convert;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.AvroSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AvroPayloadVerifier implements PayloadVerifier {
   private final static String SUBJECT_CONFIG = "avro.payload.verifier.subject";
   private final static Logger LOG = LoggerFactory.getLogger(AvroPayloadVerifier.class);

   private volatile SchemaRegistryClient schemaRegistryClient;
   private volatile KafkaAvroDeserializer kafkaAvroDeserializer;
   private volatile String subject;


   public AvroPayloadVerifier() {
   }

   public AvroPayloadVerifier(SchemaRegistryClient schemaRegistryClient) {
      this.schemaRegistryClient = schemaRegistryClient;
      kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
   }

   @Override public PayloadVerifier configure(Map<String, Object> originals) {
      subject = originals.get(SUBJECT_CONFIG) == null ? null : originals.get(SUBJECT_CONFIG).toString();
      KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(originals);
      this.schemaRegistryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrls(), config.getMaxSchemasPerSubject());
      kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient, originals);
      return this;
   }

   private boolean isSubjectSpecified() {
      return subject != null && !subject.isEmpty();
   }

   @Override public void verify(String topic, String key, byte[] data) {
      Object r = requireNonNull(kafkaAvroDeserializer.deserialize(topic, data));
      if (!isSubjectSpecified()) {
         LOG.trace("Verified {}", r);
         return;
      }
      try {
         int version = schemaRegistryClient.getVersion(subject, AvroSchemaUtils.getSchema(r));
         LOG.trace("Verified {}, version = {}", r, version);
      } catch (Exception e) {
         throw new PayloadVerificationException(format("Failed to get version for subject: %s, topic: %s, deserialized: %s", subject, topic, r), e);
      }
   }


   void setSubject(String subject) {
      this.subject = subject;
   }
}
