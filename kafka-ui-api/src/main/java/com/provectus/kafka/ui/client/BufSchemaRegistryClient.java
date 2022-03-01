package com.provectus.kafka.ui.client;

import com.provectus.kafka.ui.proto.gen.buf.alpha.registry.v1alpha1.ImageServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

public class BufSchemaRegistryClient {
  public BufSchemaRegistryClient() {
  }

  public BufSchemaRegistryClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
  }

  public BufSchemaRegistryClient(ManagedChannelBuilder<?> channelBuilder) {
    Channel channel = channelBuilder.build();
    Object blockingStub = ImageServiceGrpc.newBlockingStub(channel);
  }

  // TODO make this do something
}