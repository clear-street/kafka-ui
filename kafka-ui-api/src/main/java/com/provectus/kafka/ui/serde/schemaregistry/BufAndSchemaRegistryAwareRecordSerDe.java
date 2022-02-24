package com.provectus.kafka.ui.serde.schemaregistry;


import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG;

import com.google.common.annotations.VisibleForTesting;
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
import org.apache.kafka.common.utils.Bytes;

@Slf4j
public class SchemaRegistryAwareRecordSerDe implements RecordSerDe {

  private static final StringMessageFormatter FALLBACK_FORMATTER = new StringMessageFormatter();

  private static final ProtobufSchemaConverter protoSchemaConverter = new ProtobufSchemaConverter();
  private static final AvroJsonSchemaConverter avroSchemaConverter = new AvroJsonSchemaConverter();

  private final KafkaCluster cluster;
  private final SchemaRegistryClient schemaRegistryClient;

  private final Map<MessageFormat, MessageFormatter> schemaRegistryFormatters;

  private static SchemaRegistryClient createSchemaRegistryClient(KafkaCluster cluster) {
  }

  public SchemaRegistryAwareRecordSerDe(KafkaCluster cluster) {
    this(cluster, createSchemaRegistryClient(cluster));
  }

  @VisibleForTesting
  SchemaRegistryAwareRecordSerDe(KafkaCluster cluster, SchemaRegistryClient schemaRegistryClient) {
    this.cluster = cluster;
    this.schemaRegistryClient = schemaRegistryClient;
    this.schemaRegistryFormatters = Map.of(
        MessageFormat.AVRO, new AvroMessageFormatter(schemaRegistryClient),
        MessageFormat.JSON, new JsonSchemaMessageFormatter(schemaRegistryClient),
        MessageFormat.PROTOBUF, new ProtobufMessageFormatter(schemaRegistryClient)
    );
  }

  public DeserializedKeyValue deserialize(ConsumerRecord<Bytes, Bytes> msg) {
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(String topic,
                                                  @Nullable String key,
                                                  @Nullable String data,
                                                  @Nullable Integer partition) {
    // skip
  }

  @Override
  public TopicMessageSchemaDTO getTopicSchema(String topic) {
  }
}
