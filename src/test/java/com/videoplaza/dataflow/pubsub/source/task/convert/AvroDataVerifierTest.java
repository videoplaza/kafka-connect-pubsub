package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.videoplaza.avro.schema.Record;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AvroDataVerifierTest {

   private final static String TOPIC = "t";
   private final static String ANOTHER_TOPIC = "at";
   private final static String KEY = "k";

   private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
   private final AvroDataVerifier verifier = new AvroDataVerifier(schemaRegistryClient);
   private final KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient);
   private final Record record = Record.newBuilder().setData(ByteBuffer.wrap("Boom".getBytes())).build();

   @Before public void setUp() throws IOException, RestClientException {
      String subjectName = new TopicNameStrategy().subjectName(TOPIC, false, null);
      schemaRegistryClient.register(subjectName, Record.SCHEMA$);
   }

   @Test public void messageCompatible() {
      verifier.verify(TOPIC, KEY, serializer.serialize(TOPIC, record));
   }

   @Test(expected = RuntimeException.class)
   public void messageNotRegistered() {
      verifier.verify(ANOTHER_TOPIC, KEY, serializer.serialize(TOPIC, record));
   }

   @Test(expected = RuntimeException.class)
   public void notAvroData() {
      verifier.verify(TOPIC, KEY, "Boom".getBytes());
   }
}
