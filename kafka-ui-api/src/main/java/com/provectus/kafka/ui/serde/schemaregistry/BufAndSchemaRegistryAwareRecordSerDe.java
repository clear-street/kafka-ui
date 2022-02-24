package com.provectus.kafka.ui.serde.schemaregistry;


import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;

import com.google.common.annotations.VisibleForTesting;
import com.provectus.kafka.ui.client.BufSchemaRegistryClient;
import com.provectus.kafka.ui.exception.ValidationException;
import com.provectus.kafka.ui.model.KafkaCluster;
import com.provectus.kafka.ui.model.MessageSchemaDTO;
import com.provectus.kafka.ui.model.TopicMessageSchemaDTO;
import com.provectus.kafka.ui.serde.RecordSerDe;
import com.provectus.kafka.ui.serde.RecordSerDe.DeserializedKeyValue.DeserializedKeyValueBuilder;
import com.provectus.kafka.ui.util.jsonschema.AvroJsonSchemaConverter;
import com.provectus.kafka.ui.util.jsonschema.JsonSchema;
import com.provectus.kafka.ui.util.jsonschema.ProtobufSchemaConverter;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;

@Slf4j
public class BufAndSchemaRegistryAwareRecordSerDe implements RecordSerDe {

  class ProtoSchema {
    String fullyQualifiedTypeName;
    String schemaID;
  }

  private final SchemaRegistryAwareRecordSerDe schemaRegistryAwareRecordSerDe;
  private final KafkaCluster cluster;
  private final BufSchemaRegistryClient bufClient;

  public BufAndSchemaRegistryAwareRecordSerDe(KafkaCluster cluster) {
    this(cluster, createBufRegistryClient(cluster));
  }

  @VisibleForTesting
  public BufAndSchemaRegistryAwareRecordSerDe(KafkaCluster cluster, BufSchemaRegistryClient bufClient) {
    this.schemaRegistryAwareRecordSerDe = new SchemaRegistryAwareRecordSerDe(cluster);
    this.cluster = cluster;
    this.bufClient = bufClient;
  }

  private static BufSchemaRegistryClient createBufRegistryClient(KafkaCluster cluster) {
    // TODO: pass args from cluster to buf constructor
    return new BufSchemaRegistryClient();
  }

  public DeserializedKeyValue deserialize(ConsumerRecord<Bytes, Bytes> msg) {
    ProtoSchema protoSchema = protoSchemaFromHeaders(msg.headers());
    if (protoSchema != null) {
      // TODO: parse message
      // return
    }

    protoSchema = protoSchemaFromTopic(msg.topic());

    if (protoSchema != null) {
      // TODO: parse message
      // return
    }

    return this.schemaRegistryAwareRecordSerDe.deserialize(msg);
  }

  private @Nullable ProtoSchema protoSchemaFromHeaders(Headers headers) {
    // extract PROTOBUF_TYPE header
    String fullyQualifiedTypeName = null;
    for (Header header : headers.headers("PROTOBUF_TYPE")) {
      fullyQualifiedTypeName = new String(header.value());
    }

    if (fullyQualifiedTypeName == null) {
      return null;
    }

    // extract PROTOBUF_SCHEMA_ID header
    String schemaID = null;
    for (Header header : headers.headers("PROTOBUF_SCHEMA_ID")) {
      schemaID = new String(header.value());
    }

    ProtoSchema ret = new ProtoSchema();
    ret.fullyQualifiedTypeName = fullyQualifiedTypeName;
    ret.schemaID = schemaID;
    return ret;
  }

  private @Nullable ProtoSchema protoSchemaFromTopic(String topic) {
    if (!topic.contains(".proto.")) {
      return null;
    }

    // extract fqtn
    String[] parts = topic.split("\\.proto\\.");
    ProtoSchema ret = new ProtoSchema();
    ret.fullyQualifiedTypeName = parts[parts.length - 1];
    return ret;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(String topic,
                                                  @Nullable String key,
                                                  @Nullable String data,
                                                  @Nullable Integer partition) {
    // TODO: skip?
    return new ProducerRecord<byte[], byte[]>("", new byte[0]);
  }

  @Override
  public TopicMessageSchemaDTO getTopicSchema(String topic) {
    // TODO
    return new TopicMessageSchemaDTO();
  }
}
