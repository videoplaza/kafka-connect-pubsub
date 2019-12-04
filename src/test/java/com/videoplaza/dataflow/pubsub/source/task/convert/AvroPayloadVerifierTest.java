package com.videoplaza.dataflow.pubsub.source.task.convert;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class AvroPayloadVerifierTest {

   private final static String SUBJECT = "t-value";
   private final static String TOPIC = "t";
   private final static String ANOTHER_TOPIC = "at";
   private final static String KEY = "k";

   private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
   private final AvroPayloadVerifier verifierWithSubject = new AvroPayloadVerifier(schemaRegistryClient);
   private final AvroPayloadVerifier verifier = new AvroPayloadVerifier(schemaRegistryClient);
   private final KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient, Collections.singletonMap("schema.registry.url",""));
   private final SchemaRegistryClient someOtherRegistryClient = new MockSchemaRegistryClient();
   private final KafkaAvroSerializer someOtherSerializer = new KafkaAvroSerializer(someOtherRegistryClient);
   private final Schema aSchema = SchemaBuilder
      .record("Stuff").namespace("test.dev")
      .fields()
      .name("name").type().nullable().stringType().noDefault()
      .endRecord();

   private final Schema anotherSchema = SchemaBuilder
      .record("SomeOtherStuff").namespace("test.dev")
      .fields()
      .name("name").type().nullable().stringType().noDefault()
      .endRecord();


   private final Schema unregisteredSchema = SchemaBuilder
      .record("SomeUnregistredStuff").namespace("test.dev")
      .fields()
      .name("name").type().nullable().stringType().noDefault()
      .endRecord();

   private final GenericRecord aRecord = new GenericData.Record(aSchema);
   private final GenericRecord anotherRecord = new GenericData.Record(anotherSchema);
   private final GenericRecord anUnregisteredRecord = new GenericData.Record(unregisteredSchema);
   private final byte[] aRecordBytes = serializer.serialize(TOPIC, aRecord);
   private final byte[] anotherRecordBytes = serializer.serialize(ANOTHER_TOPIC, anotherRecord);
   private byte[] anUnregisteredRecordBytes;

   @Before public void setUp() throws IOException, RestClientException {
      verifierWithSubject.setSubject(SUBJECT);
      someOtherRegistryClient.register("ut-value", unregisteredSchema, 1, 100);
      anUnregisteredRecordBytes = someOtherSerializer.serialize("ut", anUnregisteredRecord);
   }

   @Test public void messageCompatibleWithSubjectVerified() {
      verifierWithSubject.verify(TOPIC, KEY, aRecordBytes);
   }

   @Test public void messageCompatible() {
      verifier.verify(TOPIC, KEY, aRecordBytes);
      verifier.verify(TOPIC, KEY, anotherRecordBytes);

   }

   @Test(expected = PayloadVerificationException.class)
   public void messageWithWrongSubject() {
      verifierWithSubject.verify(TOPIC, KEY, anotherRecordBytes);
   }

   @Test(expected = RuntimeException.class)
   public void messageNotRegisteredVerifySubject() {
      verifierWithSubject.verify(TOPIC, KEY, anUnregisteredRecordBytes);
   }

   @Test(expected = RuntimeException.class)
   public void messageNotRegistered() {
      verifier.verify(TOPIC, KEY, anUnregisteredRecordBytes);
   }

   @Test(expected = RuntimeException.class)
   public void notAvroData() {
      verifierWithSubject.verify(TOPIC, KEY, "Boom".getBytes());
   }
}
