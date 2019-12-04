package com.videoplaza.dataflow.pubsub.source.task.convert;

public class PayloadVerificationException extends RuntimeException {

   public PayloadVerificationException(String message, Throwable cause) {
      super(message, cause);
   }
}
