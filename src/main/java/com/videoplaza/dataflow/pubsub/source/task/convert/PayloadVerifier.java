package com.videoplaza.dataflow.pubsub.source.task.convert;

import java.util.Map;

public interface PayloadVerifier {

   PayloadVerifier configure(Map<String, Object> originals);

   void verify(String topic, String key, byte[] data);
}
