package com.provectus.kafka.ui.serde.schemaregistry;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.provectus.kafka.ui.client.BufSchemaRegistryClient;
import com.provectus.kafka.ui.model.KafkaCluster;
import com.provectus.kafka.ui.model.MessageSchemaDTO;
import com.provectus.kafka.ui.model.ProtoSchema;
import com.provectus.kafka.ui.model.TopicMessageSchemaDTO;
import com.provectus.kafka.ui.serde.RecordSerDe;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Bytes;

@Slf4j
public class BufAndSchemaRegistryAwareRecordSerDe implements RecordSerDe {

  private final Charset utf8Charset = Charset.forName("UTF-8");

  @Data
  private class CachedDescriptor {
    final Date timeCached;
    final Descriptor descriptor;
  }

  private final SchemaRegistryAwareRecordSerDe schemaRegistryAwareRecordSerDe;
  private final BufSchemaRegistryClient bufClient;

  private final String bufDefaultOwner;
  private final Map<String, String> bufOwnerRepoByProtobufMessageName;
  private final Map<String, String> protobufMessageNameByTopic;
  private final Map<String, String> protobufKeyMessageNameByTopic;

  private final Map<String, CachedDescriptor> cachedMessageDescriptorMap;
  private final int cachedMessageDescriptorRetentionSeconds;

  public BufAndSchemaRegistryAwareRecordSerDe(KafkaCluster cluster) {
    this(cluster, null, createBufRegistryClient(cluster));
  }

  public BufAndSchemaRegistryAwareRecordSerDe(
      KafkaCluster cluster,
      SchemaRegistryClient schemaRegistryClient,
      BufSchemaRegistryClient bufClient) {
    if (schemaRegistryClient == null) {
      this.schemaRegistryAwareRecordSerDe = new SchemaRegistryAwareRecordSerDe(cluster);
    } else {
      // Used for testing.
      this.schemaRegistryAwareRecordSerDe = new SchemaRegistryAwareRecordSerDe(cluster, schemaRegistryClient);
    }

    this.bufClient = bufClient;

    if (cluster.getBufRegistryCacheDurationSeconds() != null) {
      this.cachedMessageDescriptorRetentionSeconds = cluster.getBufRegistryCacheDurationSeconds();
    } else {
      this.cachedMessageDescriptorRetentionSeconds = 300;
    }
    if (cluster.getBufDefaultOwner() != null) {
      this.bufDefaultOwner = cluster.getBufDefaultOwner();
    } else {
      this.bufDefaultOwner = "";
    }
    if (cluster.getBufOwnerRepoByProtobufMessageName() != null) {
      this.bufOwnerRepoByProtobufMessageName = cluster.getBufOwnerRepoByProtobufMessageName();
    } else {
      this.bufOwnerRepoByProtobufMessageName = new HashMap<String, String>();
    }
    if (cluster.getProtobufMessageNameByTopic() != null) {
      this.protobufMessageNameByTopic = cluster.getProtobufMessageNameByTopic();
    } else {
      this.protobufMessageNameByTopic = new HashMap<String, String>();
    }
    if (cluster.getProtobufKeyMessageNameByTopic() != null) {
      this.protobufKeyMessageNameByTopic = cluster.getProtobufKeyMessageNameByTopic();
    } else {
      this.protobufKeyMessageNameByTopic = new HashMap<String, String>();
    }

    this.cachedMessageDescriptorMap = new HashMap<>();

    log.info("Will cache descriptors from buf for {} seconds", this.cachedMessageDescriptorRetentionSeconds);
  }

  private static BufSchemaRegistryClient createBufRegistryClient(KafkaCluster cluster) {
    return new BufSchemaRegistryClient(cluster.getBufRegistry(),
        cluster.getBufPort(),
        cluster.getBufApiToken());
  }

  @Override
  public DeserializedKeyValue deserialize(ConsumerRecord<Bytes, Bytes> msg) {
    String keyType = null;
    String valueType = null;

    ProtoSchema protoSchemaFromTopic = protoSchemaFromTopic(msg.topic());

    if (protoSchemaFromTopic != null) {
      keyType = protoSchemaFromTopic.getFullyQualifiedTypeName();
      valueType = protoSchemaFromTopic.getFullyQualifiedTypeName();
    }

    ProtoSchema protoSchemaForKeyFromHeader = protoValueSchemaFromHeaders(msg.headers());
    if (protoSchemaForKeyFromHeader != null) {
      keyType = protoSchemaForKeyFromHeader.getFullyQualifiedTypeName();
    }

    ProtoSchema protoSchemaFromHeader = protoKeySchemaFromHeaders(msg.headers());
    if (protoSchemaFromHeader != null) {
      valueType = protoSchemaFromHeader.getFullyQualifiedTypeName();
    }

    String keyTypeFromConfig = protobufKeyMessageNameByTopic.get(msg.topic());
    if (keyTypeFromConfig != null) {
      keyType = keyTypeFromConfig;
    }

    String valueTypeFromConfig = protobufMessageNameByTopic.get(msg.topic());
    if (valueTypeFromConfig != null) {
      valueType = valueTypeFromConfig;
    }

    if (valueType != null) {
      return deserializeProto(msg, keyType, valueType);
    }

    log.debug("No proto schema found, skipping buf for topic {}", msg.topic());

    return this.schemaRegistryAwareRecordSerDe.deserialize(msg);
  }

  private DeserializedKeyValue deserializeProto(ConsumerRecord<Bytes, Bytes> msg,
        String keyFullyQualifiedType,
        String valueFullyQualifiedType) {
    Descriptor keyDescriptor = null;
    if (keyFullyQualifiedType != null) {
      keyDescriptor = getDescriptor(keyFullyQualifiedType);
      if (keyDescriptor == null) {
        log.warn("No key descriptor found for topic {} with schema {}", msg.topic(), keyFullyQualifiedType);
      }
    }

    Descriptor valueDescriptor = null;
    if (valueFullyQualifiedType != null) {
      valueDescriptor = getDescriptor(valueFullyQualifiedType);
      if (valueDescriptor == null) {
        log.warn("No value descriptor found for topic {} with schema {}", msg.topic(), valueFullyQualifiedType);
      }
    }

    return this.deserializeProtobuf(msg, keyDescriptor, valueDescriptor);
  }

  @Nullable
  ProtoSchema protoKeySchemaFromHeaders(Headers headers) {
    // Get PROTOBUF_TYPE_KEY header.
    String fullyQualifiedTypeName = null;
    for (Header header : headers.headers("PROTOBUF_TYPE_KEY")) {
      fullyQualifiedTypeName = new String(header.value());
    }

    if (fullyQualifiedTypeName == null) {
      return null;
    }

    ProtoSchema ret = new ProtoSchema();
    ret.setFullyQualifiedTypeName(fullyQualifiedTypeName);
    return ret;
  }

  @Nullable
  ProtoSchema protoValueSchemaFromHeaders(Headers headers) {
    // Get PROTOBUF_TYPE_VALUE header.
    String fullyQualifiedTypeName = null;
    for (Header header : headers.headers("PROTOBUF_TYPE_VALUE")) {
      fullyQualifiedTypeName = new String(header.value());
    }

    if (fullyQualifiedTypeName == null) {
      return null;
    }

    ProtoSchema ret = new ProtoSchema();
    ret.setFullyQualifiedTypeName(fullyQualifiedTypeName);
    return ret;
  }

  @Nullable
  ProtoSchema protoSchemaFromTopic(String topic) {
    if (!topic.contains(".proto.")) {
      return null;
    }

    // Extract the fully qualified type name from the topic.
    // E.g., my-topic.proto.service.v1.User -> service.v1.User.
    String[] parts = topic.split("\\.proto\\.");
    ProtoSchema ret = new ProtoSchema();
    ret.setFullyQualifiedTypeName(parts[parts.length - 1]);
    return ret;
  }

  private DeserializedKeyValue deserializeProtobuf(ConsumerRecord<Bytes, Bytes> msg,
        Descriptor keyDescriptor,
        Descriptor valueDescriptor) {
    try {
      var builder = DeserializedKeyValue.builder();
      if (msg.key() != null) {
        if (keyDescriptor != null) {
          builder.key(parse(msg.key().get(), keyDescriptor));
          builder.keyFormat(MessageFormat.PROTOBUF);
        } else {
          builder.key(new String(msg.key().get()));
          builder.keyFormat(MessageFormat.UNKNOWN);
        }
      }
      if (msg.value() != null) {
        if (valueDescriptor != null) {
          builder.value(parse(msg.value().get(), valueDescriptor));
          builder.valueFormat(MessageFormat.PROTOBUF);
        } else {
          builder.value(new String(msg.value().get()));
          builder.valueFormat(MessageFormat.UNKNOWN);
        }
      }
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse record from topic " + msg.topic(), e);
    }
  }

  private static long getDateDiffMinutes(Date date1, Date date2, TimeUnit timeUnit) {
    long diffInMillies = date2.getTime() - date1.getTime();
    return timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
  }

  @Nullable
  private Descriptor getDescriptor(String fullyQualifiedTypeName) {
    Date currentDate = new Date();
    CachedDescriptor cachedDescriptor = cachedMessageDescriptorMap.get(fullyQualifiedTypeName);
    if (cachedDescriptor != null) {
      if (getDateDiffMinutes(cachedDescriptor.getTimeCached(), currentDate, TimeUnit.SECONDS)
          < cachedMessageDescriptorRetentionSeconds) {
        return cachedDescriptor.getDescriptor();
      }
    }

    String bufOwner = bufDefaultOwner;
    String bufRepo = "";

    String bufOwnerRepoInfo = bufOwnerRepoByProtobufMessageName.get(fullyQualifiedTypeName);

    if (bufOwnerRepoInfo != null) {
      String[] parts = bufOwnerRepoInfo.split("/");
      if (parts.length != 2) {
        log.error("Cannot parse Buf owner and repo info from {}, make sure it is in the 'owner/repo' format",
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

    Descriptor descriptor = bufClient.getDescriptor(bufOwner, bufRepo, fullyQualifiedTypeName);

    cachedDescriptor = new CachedDescriptor(currentDate, descriptor);
    cachedMessageDescriptorMap.put(fullyQualifiedTypeName, cachedDescriptor);

    return descriptor;
  }

  private String parse(byte[] value, Descriptor descriptor) throws IOException {
    DynamicMessage protoMsg = DynamicMessage.parseFrom(
        descriptor,
        new ByteArrayInputStream(value));
    byte[] jsonFromProto = ProtobufSchemaUtils.toJson(protoMsg);
    return new String(jsonFromProto, utf8Charset);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(String topic,
      @Nullable String key,
      @Nullable String data,
      @Nullable Integer partition) {
    if (data == null) {
      return new ProducerRecord<>(topic, partition, Objects.requireNonNull(key).getBytes(), null);
    }

    ProtoSchema protoSchema = protoSchemaFromTopic(topic);
    if (protoSchema == null) {
      throw new RuntimeException("Could not infer proto schema from topic " + topic);
    }

    Descriptor descriptor = getDescriptor(protoSchema.getFullyQualifiedTypeName());
    if (descriptor == null) {
      throw new RuntimeException("Could not get descriptor for " + protoSchema.getFullyQualifiedTypeName());
    }

    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor);

    DynamicMessage message;
    try {
      JsonFormat.parser().merge(data, builder);
      message = builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to merge record for topic " + topic, e);
    }

    return new ProducerRecord<>(
        topic,
        partition,
        Optional.ofNullable(key).map(String::getBytes).orElse(null),
        message.toByteArray());
  }

  @Override
  public TopicMessageSchemaDTO getTopicSchema(String topic) {
    final MessageSchemaDTO schema = new MessageSchemaDTO()
        .name("Unknown")
        .source(MessageSchemaDTO.SourceEnum.UNKNOWN)
        .schema("");
    return new TopicMessageSchemaDTO()
        .key(schema)
        .value(schema);
  }
}
