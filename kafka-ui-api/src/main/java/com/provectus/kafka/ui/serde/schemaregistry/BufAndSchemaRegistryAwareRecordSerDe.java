package com.provectus.kafka.ui.serde.schemaregistry;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.provectus.kafka.ui.client.BufSchemaRegistryClient;
import com.provectus.kafka.ui.model.KafkaCluster;
import com.provectus.kafka.ui.model.MessageSchemaDTO;
import com.provectus.kafka.ui.model.ProtoSchema;
import com.provectus.kafka.ui.model.TopicMessageSchemaDTO;
import com.provectus.kafka.ui.serde.RecordSerDe;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;
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

  private final SchemaRegistryAwareRecordSerDe schemaRegistryAwareRecordSerDe;
  private final BufSchemaRegistryClient bufClient;

  private final String bufDefaultOwner;
  private final Map<String, String> bufOwnerRepoByProtobufMessageName;
  private final Map<String, String> protobufMessageNameByTopic;

  private final Map<String, Descriptor> messageDescriptorMap;

  public BufAndSchemaRegistryAwareRecordSerDe(KafkaCluster cluster) {
    this(cluster, null, createBufRegistryClient(cluster));
  }

  @VisibleForTesting
  public BufAndSchemaRegistryAwareRecordSerDe(
      KafkaCluster cluster,
      SchemaRegistryClient schemaRegistryClient,
      BufSchemaRegistryClient bufClient) {
    if (schemaRegistryClient == null) {
      this.schemaRegistryAwareRecordSerDe = new SchemaRegistryAwareRecordSerDe(cluster);
    } else {
      // used for testing
      this.schemaRegistryAwareRecordSerDe = new SchemaRegistryAwareRecordSerDe(cluster, schemaRegistryClient);
    }
    this.bufClient = bufClient;

    this.bufDefaultOwner = cluster.getBufDefaultOwner();
    this.bufOwnerRepoByProtobufMessageName = cluster.getBufOwnerRepoByProtobufMessageName();
    this.protobufMessageNameByTopic = cluster.getProtobufMessageNameByTopic();

    this.messageDescriptorMap = new HashMap<>();
  }

  private static BufSchemaRegistryClient createBufRegistryClient(KafkaCluster cluster) {
    return new BufSchemaRegistryClient(cluster.getBufRegistry(),
        Integer.parseInt(cluster.getBufPort()),
        cluster.getBufApiToken());
  }

  @Override
  public DeserializedKeyValue deserialize(ConsumerRecord<Bytes, Bytes> msg) {
    String fullyQualifiedTypeName = protobufMessageNameByTopic.get(msg.topic());
    if (fullyQualifiedTypeName != null) {
      return deserializeProto(msg, fullyQualifiedTypeName);
    }

    ProtoSchema protoSchema = protoSchemaFromHeaders(msg.headers());
    if (protoSchema != null) {
      return deserializeProto(msg, protoSchema.getFullyQualifiedTypeName());
    }

    protoSchema = protoSchemaFromTopic(msg.topic());

    if (protoSchema != null) {
      return deserializeProto(msg, protoSchema.getFullyQualifiedTypeName());
    }

    return this.schemaRegistryAwareRecordSerDe.deserialize(msg);
  }

  private DeserializedKeyValue deserializeProto(ConsumerRecord<Bytes, Bytes> msg, String fullyQualifiedTypeName) {
    Descriptor descriptor = getDescriptor(fullyQualifiedTypeName);

    if (descriptor != null) {
      return this.deserializeProtobuf(msg, descriptor);
    }

    log.warn("Skipping buf for topic {} with schema {}", msg.topic(), fullyQualifiedTypeName);
    return this.schemaRegistryAwareRecordSerDe.deserialize(msg);
  }

  @Nullable
  ProtoSchema protoSchemaFromHeaders(Headers headers) {
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
    ret.setFullyQualifiedTypeName(fullyQualifiedTypeName);
    ret.setSchemaID(schemaID);
    return ret;
  }

  @Nullable
  ProtoSchema protoSchemaFromTopic(String topic) {
    if (!topic.contains(".proto.")) {
      return null;
    }

    // extract fqtn
    String[] parts = topic.split("\\.proto\\.");
    ProtoSchema ret = new ProtoSchema();
    ret.setFullyQualifiedTypeName(parts[parts.length - 1]);
    return ret;
  }

  private DeserializedKeyValue deserializeProtobuf(ConsumerRecord<Bytes, Bytes> msg, Descriptor descriptor) {
    try {
      var builder = DeserializedKeyValue.builder();
      if (msg.key() != null) {
        builder.key(new String(msg.key().get()));
        builder.keyFormat(MessageFormat.UNKNOWN);
      }
      if (msg.value() != null) {
        builder.value(parse(msg.value().get(), descriptor));
        builder.valueFormat(MessageFormat.PROTOBUF);
      }
      return builder.build();
    } catch (Throwable e) {
      throw new RuntimeException("Failed to parse record from topic " + msg.topic(), e);
    }
  }

  // TODO: Get descriptor from cache or Buf
  @Nullable
  private Descriptor getDescriptor(String fullyQualifiedTypeName) {
    String bufOwner = bufDefaultOwner;
    String bufRepo = "";

    String bufOwnerRepoInfo = bufOwnerRepoByProtobufMessageName.get(fullyQualifiedTypeName);

    if (bufOwnerRepoInfo != null) {
      String[] parts = bufOwnerRepoInfo.split("/");
      if (parts.length != 2) {
        log.error("Cannot parse Buf owner and repo info from {}, make sure it is in the 'owner/repo format'",
            bufOwnerRepoInfo);
      } else {
        bufOwner = parts[0];
        bufRepo = parts[1];
      }
    } else {
      String[] parts = fullyQualifiedTypeName.split("\\.");

      if (parts.length == 0) {
        log.warn("Cannot infer Buf repo name from type {}", fullyQualifiedTypeName);
      } else {
        bufRepo = parts[0];
      }
    }

    log.info("Get descriptor from Buf {}/{}@{}", bufOwner, bufRepo, fullyQualifiedTypeName);

    return bufClient.getDescriptor(bufOwner, bufRepo, fullyQualifiedTypeName);
  }

  // TODO: Shared code
  @SneakyThrows
  private String parse(byte[] value, Descriptor descriptor) {
    DynamicMessage protoMsg = DynamicMessage.parseFrom(
        descriptor,
        new ByteArrayInputStream(value));
    byte[] jsonFromProto = ProtobufSchemaUtils.toJson(protoMsg);
    return new String(jsonFromProto);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(String topic,
      @Nullable String key,
      @Nullable String data,
      @Nullable Integer partition) {
    return this.schemaRegistryAwareRecordSerDe.serialize(topic, key, data, partition);
  }

  @Override
  public TopicMessageSchemaDTO getTopicSchema(String topic) {
    // TODO: what is the importance of this?
    final MessageSchemaDTO schema = new MessageSchemaDTO()
        .name("unknown")
        .source(MessageSchemaDTO.SourceEnum.UNKNOWN)
        .schema("CLST_PROTOBUF");
    return new TopicMessageSchemaDTO()
        .key(schema)
        .value(schema);
  }
}
