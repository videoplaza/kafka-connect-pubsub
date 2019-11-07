package com.videoplaza.dataflow.pubsub.source.task.convert;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.videoplaza.dataflow.pubsub.PubsubSourceConnectorConfig;
import com.videoplaza.dataflow.pubsub.source.task.SourceMessage;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class BatchTypePubsubMessageConverterTest {

   private static final String BATCH_ATTRIBUTE = "ba";
   private final AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);
   private final PubsubMessageConverter singleConverter = mock(PubsubMessageConverter.class);
   private final PubsubMessageConverter batchConverter = mock(PubsubMessageConverter.class);
   private final SourceMessage singleMessage = mock(SourceMessage.class);
   private final SourceMessage batchMessage = mock(SourceMessage.class);


   private final BatchTypePubsubMessageConverter converter = new BatchTypePubsubMessageConverter(singleConverter, batchConverter, BATCH_ATTRIBUTE);

   private final PubsubMessage aMessageWithNoAttributes = PubsubMessage.newBuilder().build();
   private final PubsubMessage aMessageWithAvroBatchAttribute = PubsubMessage.newBuilder()
       .putAttributes(BATCH_ATTRIBUTE, PubsubSourceConnectorConfig.AVRO_BATCH_TYPE)
       .build();

   private final PubsubMessage aMessageWithUnknownBatchAttribute = PubsubMessage.newBuilder()
       .putAttributes(BATCH_ATTRIBUTE, "unsupported")
       .build();

   @Before public void setUp() {
      when(singleConverter.convert(any(PubsubMessage.class), any(AckReplyConsumer.class))).thenReturn(singleMessage);
      when(batchConverter.convert(any(PubsubMessage.class), any(AckReplyConsumer.class))).thenReturn(batchMessage);
   }

   @Test public void getBatchType() {
      assertThat(converter.getBatchType(aMessageWithNoAttributes)).isNull();
      assertThat(converter.getBatchType(aMessageWithAvroBatchAttribute)).isEqualTo(PubsubSourceConnectorConfig.AVRO_BATCH_TYPE);
   }

   @Test public void convertNoBatch() {
      assertThat(converter.convert(aMessageWithNoAttributes, ackReplyConsumer)).isEqualTo(singleMessage);
      verify(singleConverter).convert(aMessageWithNoAttributes, ackReplyConsumer);
      verifyNoMoreInteractions(singleConverter, batchConverter);
   }

   @Test public void convertBatch() {
      assertThat(converter.convert(aMessageWithAvroBatchAttribute, ackReplyConsumer)).isEqualTo(batchMessage);
      verify(batchConverter).convert(aMessageWithAvroBatchAttribute, ackReplyConsumer);
      verifyNoMoreInteractions(singleConverter, batchConverter);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void convertIncorrectBatchType() {
      converter.convert(aMessageWithUnknownBatchAttribute, ackReplyConsumer);
   }

   @Test(expected = SomeException.class)
   public void convertBatchFails() {
      when(converter.convert(aMessageWithAvroBatchAttribute, ackReplyConsumer)).thenThrow(new SomeException());
      converter.convert(aMessageWithAvroBatchAttribute, ackReplyConsumer);
   }

   @Test(expected = SomeException.class)
   public void convertSingleFails() {
      when(converter.convert(aMessageWithNoAttributes, ackReplyConsumer)).thenThrow(new SomeException());
      converter.convert(aMessageWithNoAttributes, ackReplyConsumer);
   }

   private static class SomeException extends RuntimeException {
   }
}
