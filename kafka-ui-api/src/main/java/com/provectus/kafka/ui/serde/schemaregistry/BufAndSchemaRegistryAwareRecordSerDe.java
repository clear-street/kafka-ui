package com.provectus.kafka.ui.serde.schemaregistry;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Any;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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

  @Data
  private class CachedDescriptor {
    final Date timeCached;
    final Optional<Descriptor> descriptor;
  }

  @Data
  private class CachedTypeRegistry {
    final Date timeCached;
    final Optional<JsonFormat.TypeRegistry> typeRegistry;
  }

  @Data
  private class BufRepoInfo {
    final String owner;
    final String repo;
  }

  private final SchemaRegistryAwareRecordSerDe schemaRegistryAwareRecordSerDe;
  private final BufSchemaRegistryClient bufClient;

  private final String bufDefaultOwner;
  private final Map<String, String> bufOwnerRepoByProtobufMessageName;
  private final Map<String, String> protobufMessageNameByTopic;
  private final Map<String, String> protobufKeyMessageNameByTopic;

  private final Map<String, CachedDescriptor> cachedMessageDescriptorMap;
  private final Map<String, CachedTypeRegistry> cachedTypeRegistryMap;
  private final int cachedMessageDescriptorRetentionSeconds;

  private final String googleProtobufAnyType = "google.protobuf.Any";

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
      int cacheSeconds = cluster.getBufRegistryCacheDurationSeconds();
      if (cacheSeconds == 0) {
        log.warn("Buf registry cache duration seconds must be greater than 0, setting to 300");
        this.cachedMessageDescriptorRetentionSeconds = 300;
      } else {
        this.cachedMessageDescriptorRetentionSeconds = cacheSeconds;
      }
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
      this.bufOwnerRepoByProtobufMessageName = new HashMap<>();
    }
    if (cluster.getProtobufMessageNameByTopic() != null) {
      this.protobufMessageNameByTopic = cluster.getProtobufMessageNameByTopic();
    } else {
      this.protobufMessageNameByTopic = new HashMap<>();
    }
    if (cluster.getProtobufKeyMessageNameByTopic() != null) {
      this.protobufKeyMessageNameByTopic = cluster.getProtobufKeyMessageNameByTopic();
    } else {
      this.protobufKeyMessageNameByTopic = new HashMap<>();
    }

    this.cachedMessageDescriptorMap = new HashMap<>();
    this.cachedTypeRegistryMap = new HashMap<>();

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

    Optional<ProtoSchema> protoSchemaFromTopic = protoSchemaFromTopic(msg.topic());

    if (protoSchemaFromTopic.isPresent()) {
      keyType = protoSchemaFromTopic.get().getFullyQualifiedTypeName();
      valueType = protoSchemaFromTopic.get().getFullyQualifiedTypeName();
    }

    Optional<ProtoSchema> protoSchemaForKeyFromHeader = protoKeySchemaFromHeaders(msg.headers());
    if (protoSchemaForKeyFromHeader.isPresent()) {
      keyType = protoSchemaForKeyFromHeader.get().getFullyQualifiedTypeName();
    }

    Optional<ProtoSchema> protoSchemaForValueFromHeader = protoValueSchemaFromHeaders(msg.headers());
    if (protoSchemaForValueFromHeader.isPresent()) {
      valueType = protoSchemaForValueFromHeader.get().getFullyQualifiedTypeName();
    }

    String keyTypeFromConfig = protobufKeyMessageNameByTopic.get(msg.topic());
    if (keyTypeFromConfig != null) {
      keyType = keyTypeFromConfig;
    }

    String valueTypeFromConfig = protobufMessageNameByTopic.get(msg.topic());
    if (valueTypeFromConfig != null) {
      valueType = valueTypeFromConfig;
    }

    if (keyType != null || valueType != null) {
      return deserializeProto(msg, keyType, valueType);
    }

    log.debug("No proto schema found, skipping buf for topic {}", msg.topic());

    return this.schemaRegistryAwareRecordSerDe.deserialize(msg);
  }

  private DeserializedKeyValue deserializeProto(ConsumerRecord<Bytes, Bytes> msg,
      String keyFullyQualifiedType,
      String valueFullyQualifiedType) {
    Optional<Descriptor> keyDescriptor = Optional.empty();
    if (keyFullyQualifiedType != null) {
      keyDescriptor = getDescriptor(keyFullyQualifiedType);
      if (!keyDescriptor.isPresent()) {
        log.warn("No key descriptor found for topic {} with schema {}", msg.topic(), keyFullyQualifiedType);
      }
    }

    Optional<Descriptor> valueDescriptor = Optional.empty();
    if (valueFullyQualifiedType != null) {
      valueDescriptor = getDescriptor(valueFullyQualifiedType);
      if (!valueDescriptor.isPresent()) {
        log.warn("No value descriptor found for topic {} with schema {}", msg.topic(), valueFullyQualifiedType);
      }
    }

    return this.deserializeProtobuf(msg, keyDescriptor, valueDescriptor);
  }

  Optional<ProtoSchema> protoKeySchemaFromHeaders(Headers headers) {
    // Get protobuf.type.key header.
    String fullyQualifiedTypeName = null;
    for (Header header : headers) {
      if (header.key().toLowerCase().equals("protobuf.type.key")) {
        fullyQualifiedTypeName = new String(header.value(), StandardCharsets.UTF_8);
      }
    }

    if (fullyQualifiedTypeName == null) {
      return Optional.empty();
    }

    ProtoSchema ret = new ProtoSchema();
    ret.setFullyQualifiedTypeName(fullyQualifiedTypeName);
    return Optional.of(ret);
  }

  Optional<ProtoSchema> protoValueSchemaFromHeaders(Headers headers) {
    // Get protobuf.type.value header.
    String fullyQualifiedTypeName = null;
    for (Header header : headers) {
      if (header.key().toLowerCase().equals("protobuf.type.value")) {
        fullyQualifiedTypeName = new String(header.value(), StandardCharsets.UTF_8);
      }
    }

    if (fullyQualifiedTypeName == null) {
      return Optional.empty();
    }

    ProtoSchema ret = new ProtoSchema();
    ret.setFullyQualifiedTypeName(fullyQualifiedTypeName);
    return Optional.of(ret);
  }

  Optional<ProtoSchema> protoSchemaFromTopic(String topic) {
    if (!topic.contains(".proto.")) {
      return Optional.empty();
    }

    // Extract the fully qualified type name from the topic.
    // E.g., my-topic.proto.service.v1.User -> service.v1.User.
    String[] parts = topic.split("\\.proto\\.");
    ProtoSchema ret = new ProtoSchema();
    ret.setFullyQualifiedTypeName(parts[parts.length - 1]);
    return Optional.of(ret);
  }

  private DeserializedKeyValue deserializeProtobuf(ConsumerRecord<Bytes, Bytes> msg,
      Optional<Descriptor> keyDescriptor,
      Optional<Descriptor> valueDescriptor) {
    var builder = DeserializedKeyValue.builder();
    if (msg.key() != null) {
      if (keyDescriptor.isPresent()) {
        builder.key(parse(msg.key().get(), keyDescriptor.get()));
        builder.keyFormat(MessageFormat.PROTOBUF);
      } else {
        builder.key(new String(msg.key().get(), StandardCharsets.UTF_8));
        builder.keyFormat(MessageFormat.UNKNOWN);
      }
    }
    if (msg.value() != null) {
      if (valueDescriptor.isPresent()) {
        builder.value(parse(msg.value().get(), valueDescriptor.get()));
        builder.valueFormat(MessageFormat.PROTOBUF);
      } else {
        builder.value(new String(msg.value().get(), StandardCharsets.UTF_8));
        builder.valueFormat(MessageFormat.UNKNOWN);
      }
    }
    return builder.build();
  }

  private static long getDateDiffMinutes(Date date1, Date date2, TimeUnit timeUnit) {
    long diffInMillis = date2.getTime() - date1.getTime();
    return timeUnit.convert(diffInMillis, TimeUnit.MILLISECONDS);
  }

  private Optional<BufRepoInfo> getBufRepoInfo(String fullyQualifiedTypeName) {
    String bufOwner = bufDefaultOwner;
    String bufRepo = "";

    String bufOwnerRepoInfo = bufOwnerRepoByProtobufMessageName.get(fullyQualifiedTypeName);

    if (bufOwnerRepoInfo != null) {
      String[] parts = bufOwnerRepoInfo.split("/");
      if (parts.length != 2) {
        log.error("Cannot parse Buf owner and repo info from {}, make sure it is in the 'owner/repo' format",
            bufOwnerRepoInfo);
      } else {
        return Optional.of(new BufRepoInfo(parts[0], parts[1]));
      }
    } else {
      String[] parts = fullyQualifiedTypeName.split("\\.");

      if (parts.length == 0) {
        log.warn("Cannot infer Buf repo name from type {}", fullyQualifiedTypeName);
      } else {
        return Optional.of(new BufRepoInfo(bufDefaultOwner, parts[0]));
      }
    }

    return Optional.empty();
  }

  private Optional<Descriptor> getDescriptor(String fullyQualifiedTypeName) {
    if (fullyQualifiedTypeName.equals(googleProtobufAnyType)) {
      return Optional.of(Any.getDescriptor());
    }

    Date currentDate = new Date();
    CachedDescriptor cachedDescriptor = cachedMessageDescriptorMap.get(fullyQualifiedTypeName);
    if (cachedDescriptor != null) {
      if (getDateDiffMinutes(cachedDescriptor.getTimeCached(), currentDate,
          TimeUnit.SECONDS) < cachedMessageDescriptorRetentionSeconds) {
        return cachedDescriptor.getDescriptor();
      }
    }

    Optional<BufRepoInfo> bufRepoInfo = getBufRepoInfo(fullyQualifiedTypeName);
    if (bufRepoInfo.isEmpty()) {
      log.error("could not get Buf repo info for {}", fullyQualifiedTypeName);
      return Optional.empty();
    }

    log.info("Get descriptor from Buf {}/{}@{}", bufRepoInfo.get().getOwner(), bufRepoInfo.get().getRepo(),
        fullyQualifiedTypeName);

    Optional<Descriptor> descriptor = bufClient.getDescriptor(bufRepoInfo.get().getOwner(), bufRepoInfo.get().getRepo(),
        fullyQualifiedTypeName);

    cachedDescriptor = new CachedDescriptor(currentDate, descriptor);
    cachedMessageDescriptorMap.put(fullyQualifiedTypeName, cachedDescriptor);

    return descriptor;
  }

  private Optional<JsonFormat.TypeRegistry> getTypeRegistry(BufRepoInfo bufRepoInfo) {
    String cacheKey = String.format("%s/%s", bufRepoInfo.getOwner(), bufRepoInfo.getRepo());

    Date currentDate = new Date();
    CachedTypeRegistry cachedTypeRegistry = cachedTypeRegistryMap.get(cacheKey);
    if (cachedTypeRegistry != null) {
      if (getDateDiffMinutes(cachedTypeRegistry.getTimeCached(), currentDate,
          TimeUnit.SECONDS) < cachedMessageDescriptorRetentionSeconds) {
        return cachedTypeRegistry.getTypeRegistry();
      }
    }

    log.info("Get file descriptors from Buf {}/{}", bufRepoInfo.getOwner(), bufRepoInfo.getRepo());

    Optional<List<FileDescriptor>> fileDescriptors = bufClient.getFileDescriptors(bufRepoInfo.getOwner(),
        bufRepoInfo.getRepo());

    Optional<JsonFormat.TypeRegistry> typeRegistry = Optional.empty();

    if (fileDescriptors.isPresent()) {
      JsonFormat.TypeRegistry.Builder builder = JsonFormat.TypeRegistry.newBuilder();

      for (FileDescriptor fileDescriptor : fileDescriptors.get()) {
        for (Descriptor descriptor : fileDescriptor.getMessageTypes()) {
          builder.add(descriptor);
        }
      }

      typeRegistry = Optional.of(builder.build());
    }

    cachedTypeRegistry = new CachedTypeRegistry(currentDate, typeRegistry);
    cachedTypeRegistryMap.put(cacheKey, cachedTypeRegistry);

    return typeRegistry;
  }

  private String parse(byte[] value, Descriptor descriptor) {
    try {
      DynamicMessage protoMsg = DynamicMessage.parseFrom(
          descriptor,
          new ByteArrayInputStream(value));

      if (descriptor.getFullName().equals(googleProtobufAnyType)) {
        Any anyMsg = Any.parseFrom(value);

        // Get the fully qualified type name from a URL like
        // type.googleapis.com/google.protobuf.Duration.
        String type = anyMsg.getTypeUrl();
        String[] parts = anyMsg.getTypeUrl().split("/");
        if (parts.length == 2) {
          type = parts[1];
        }
        Optional<Descriptor> valueDescriptor = getDescriptor(type);

        if (valueDescriptor.isPresent()) {
          JsonFormat.TypeRegistry typeRegistry = JsonFormat.TypeRegistry.newBuilder()
              .add(valueDescriptor.get())
              .build();

          JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(typeRegistry);

          return printer.print(protoMsg);
        }
      } else {
        Optional<BufRepoInfo> bufRepoInfo = getBufRepoInfo(descriptor.getFullName());
        if (bufRepoInfo.isPresent()) {
          Optional<JsonFormat.TypeRegistry> typeRegistry = getTypeRegistry(bufRepoInfo.get());
          if (typeRegistry.isPresent()) {
            JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(typeRegistry.get());

            return printer.print(protoMsg);
          }
        }
      }

      return JsonFormat.printer().print(protoMsg);
    } catch (

    IOException e) {
      log.error("failed protobuf derserialization: {}", e);
    }

    return new String(value, StandardCharsets.UTF_8);
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(String topic,
      @Nullable String key,
      @Nullable String data,
      @Nullable Integer partition) {
    if (data == null) {
      return new ProducerRecord<>(topic, partition, Objects.requireNonNull(key).getBytes(), null);
    }

    Optional<ProtoSchema> protoSchema = protoSchemaFromTopic(topic);
    if (!protoSchema.isPresent()) {
      throw new IllegalStateException("Could not infer proto schema from topic " + topic);
    }

    Optional<Descriptor> descriptor = getDescriptor(protoSchema.get().getFullyQualifiedTypeName());
    if (!descriptor.isPresent()) {
      throw new IllegalArgumentException("Could not get descriptor for "
          + protoSchema.get().getFullyQualifiedTypeName());
    }

    DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor.get());

    DynamicMessage message;
    try {
      JsonFormat.parser().merge(data, builder);
      message = builder.build();
    } catch (InvalidProtocolBufferException e) {
      throw new UncheckedIOException("Failed to merge record for topic " + topic, e);
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
