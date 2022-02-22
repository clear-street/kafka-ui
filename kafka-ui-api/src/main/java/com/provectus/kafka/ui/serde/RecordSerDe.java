package com.provectus.kafka.ui.serde;

import com.provectus.kafka.ui.model.TopicMessageSchemaDTO;
import com.provectus.kafka.ui.serde.schemaregistry.MessageFormat;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;

public interface RecordSerDe {

  // This is the interface our clst-proto serde class will fulfill


  @Value
  @Builder
  class DeserializedKeyValue {
    @Nullable String key;
    @Nullable String value;
    @Nullable MessageFormat keyFormat;
    @Nullable MessageFormat valueFormat;
    @Nullable String keySchemaId;
    @Nullable String valueSchemaId;
  }

  // we must implement this method & reach out to Buf
  DeserializedKeyValue deserialize(ConsumerRecord<Bytes, Bytes> msg);

  // I think this can be an empty implementation, this is probably just for publishing in the UI
  ProducerRecord<byte[], byte[]> serialize(String topic,
                                           @Nullable String key,
                                           @Nullable String data,
                                           @Nullable Integer partition);

  TopicMessageSchemaDTO getTopicSchema(String topic);
}
