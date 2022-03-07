package com.provectus.kafka.ui.client;

import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.provectus.kafka.ui.proto.gen.buf.alpha.image.v1.Image;
import com.provectus.kafka.ui.proto.gen.buf.alpha.registry.v1alpha1.GetImageRequest;
import com.provectus.kafka.ui.proto.gen.buf.alpha.registry.v1alpha1.GetImageResponse;
import com.provectus.kafka.ui.proto.gen.buf.alpha.registry.v1alpha1.ImageServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BufSchemaRegistryClient {
  private ImageServiceGrpc.ImageServiceBlockingStub bufClient;

  public BufSchemaRegistryClient(String host, int port, String apiKey) {
    this(ManagedChannelBuilder.forAddress(String.format("api.%s", host), port).useTransportSecurity(), apiKey);
  }

  public BufSchemaRegistryClient(ManagedChannelBuilder<?> channelBuilder, String apiKey) {
    Channel channel = channelBuilder.build();
    bufClient = ImageServiceGrpc.newBlockingStub(channel);

    Metadata headers = new Metadata();
    Metadata.Key<String> bearerKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(bearerKey, String.format("Bearer %s", apiKey));

    bufClient = bufClient.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  @Nullable
  public Descriptor getDescriptor(String owner, String repo, String fullyQualifiedTypeName) {
    List<String> parts = Arrays.asList(fullyQualifiedTypeName.split("\\."));

    if (parts.isEmpty()) {
      log.warn("Cannot get package name and type name from {}", fullyQualifiedTypeName);
      return null;
    }

    String packageName = String.join(".", parts.subList(0, parts.size() - 1));
    String typeName = parts.get(parts.size() - 1);

    log.info("Looking for type {} in package {}", typeName, packageName);

    Image image;

    try {
      image = getImage(owner, repo);
    } catch (StatusRuntimeException e) {
      log.error("Failed to get image {}", e);
      return null;
    }

    FileDescriptorSet fileDescriptorSet;
    try {
      fileDescriptorSet = FileDescriptorSet.parseFrom(image.toByteArray());
    } catch (InvalidProtocolBufferException e) {
      log.error("Failed to parse Image into FileDescriptorSet {}", e);
      return null;
    }

    Map<String, FileDescriptorProto> descriptorProtoIndexBuilder = new HashMap<>();

    for (int i = 0; i < fileDescriptorSet.getFileCount(); i++) {
      FileDescriptorProto p = fileDescriptorSet.getFile(i);
      descriptorProtoIndexBuilder.put(p.getName(), p);
    }

    Map<String, FileDescriptorProto> descriptorProtoIndex = new HashMap<>();
    final Map<String, FileDescriptor> descriptorCache = new HashMap<>();

    final Map<String, FileDescriptor> allFileDescriptors = new HashMap<>();
    try {
      for (int i = 0; i < fileDescriptorSet.getFileCount(); i++) {
        FileDescriptor desc = descriptorFromProto(fileDescriptorSet.getFile(i), descriptorProtoIndex, descriptorCache);
        allFileDescriptors.put(desc.getName(), desc);
      }
    } catch (DescriptorValidationException e) {
      log.error("Failed to create dependencies map {}", e);
    }

    return allFileDescriptors.values()
        .stream()
        .filter(f -> f.getPackage().equals(packageName))
        .map(f -> f.findMessageTypeByName(typeName))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(null);
  }

  private Image getImage(String owner, String repo) throws StatusRuntimeException {
    return bufClient.getImage(GetImageRequest.newBuilder()
        .setOwner(owner)
        .setRepository(repo)
        .setReference("main")
        .build())
        .getImage();
  }

  // From
  // https://github.com/grpc-swagger/grpc-swagger/blob/master/grpc-swagger-core/src/main/java/io/grpc/grpcswagger/grpc/ServiceResolver.java#L118.
  private FileDescriptor descriptorFromProto(
      FileDescriptorProto descriptorProto,
      Map<String, FileDescriptorProto> descriptorProtoIndex,
      Map<String, FileDescriptor> descriptorCache) throws DescriptorValidationException {
    // First, check the cache.
    String descriptorName = descriptorProto.getName();
    if (descriptorCache.containsKey(descriptorName)) {
      return descriptorCache.get(descriptorName);
    }

    // Then, fetch all the required dependencies recursively.
    List<FileDescriptor> dependencies = new ArrayList<>();
    for (String dependencyName : descriptorProto.getDependencyList()) {
      if (!descriptorProtoIndex.containsKey(dependencyName)) {
        throw new IllegalArgumentException("Could not find dependency: " + dependencyName);
      }
      FileDescriptorProto dependencyProto = descriptorProtoIndex.get(dependencyName);
      dependencies.add(descriptorFromProto(dependencyProto, descriptorProtoIndex, descriptorCache));
    }

    // Finally, construct the actual descriptor.
    FileDescriptor[] empty = new FileDescriptor[0];
    return FileDescriptor.buildFrom(descriptorProto, dependencies.toArray(empty));
  }
}