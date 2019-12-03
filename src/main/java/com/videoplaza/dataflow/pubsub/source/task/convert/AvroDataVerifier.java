package com.videoplaza.dataflow.pubsub.source.task.convert;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.GenericContainerWithVersion;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class AvroDataVerifier implements PayloadVerifier {
   private final static Logger LOG = LoggerFactory.getLogger(AvroDataVerifier.class);

   private final AtomicReference<VerifySubjectDeserializer> deserializer = new AtomicReference<>();

   public AvroDataVerifier() {
   }

   public AvroDataVerifier(SchemaRegistryClient schemaRegistryClient) {
      deserializer.set(new VerifySubjectDeserializer(schemaRegistryClient));
   }

   @Override public PayloadVerifier configure(Map<String, Object> originals) {
      KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(originals);
      SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrls(), config.getMaxSchemasPerSubject());
      if (!deserializer.compareAndSet(null, new VerifySubjectDeserializer(schemaRegistryClient, originals))) {
         throw new IllegalStateException("Deserializer has been already set. Properties: " + config);
      }
      return this;
   }

   @Override public void verify(String topic, String key, byte[] data) {
      GenericContainerWithVersion r = deserializer.get().deserialize(topic, data);
      requireNonNull(r);
      requireNonNull(r.container());
      LOG.trace("Verified {}", r);
   }

   private static class VerifySubjectDeserializer extends KafkaAvroDeserializer {

      public VerifySubjectDeserializer(SchemaRegistryClient client) {
         super(client);
      }

      public VerifySubjectDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
         super(client, props);
      }

      public GenericContainerWithVersion deserialize(String topic, byte[] payload) {
         return (GenericContainerWithVersion) deserialize(true, topic, false, payload, null);
      }
   }

}
