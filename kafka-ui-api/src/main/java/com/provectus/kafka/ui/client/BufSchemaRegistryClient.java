package com.provectus.kafka.ui.client;

import com.provectus.kafka.ui.proto.gen.buf.alpha.registry.v1alpha1.GetImageRequest;
import com.provectus.kafka.ui.proto.gen.buf.alpha.registry.v1alpha1.GetImageResponse;
import com.provectus.kafka.ui.proto.gen.buf.alpha.registry.v1alpha1.ImageServiceGrpc;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;

public class BufSchemaRegistryClient {
  private ImageServiceGrpc.ImageServiceBlockingStub bufClient;

  public BufSchemaRegistryClient() {
  }

  public BufSchemaRegistryClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).useTransportSecurity());
  }

  public BufSchemaRegistryClient(ManagedChannelBuilder<?> channelBuilder) {
    Channel channel = channelBuilder.build();
    bufClient = ImageServiceGrpc.newBlockingStub(channel);

    Metadata headers = new Metadata();
    Metadata.Key<String> bearerKey = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    headers.put(bearerKey, "Bearer BUF_API_KEY");

    bufClient.withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  private void getImage(String owner, String repo) {
    GetImageRequest request = GetImageRequest.newBuilder().setOwner(owner).setRepository(repo).build();
    GetImageResponse response = bufClient.getImage(request);
  }
  // TODO make this do something
}