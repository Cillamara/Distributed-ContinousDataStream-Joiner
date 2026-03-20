package com.photon.proto;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.63.0)",
    comments = "Source: photon.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class JoinerServiceGrpc {

  private JoinerServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "photon.JoinerService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.photon.proto.JoinRequest,
      com.photon.proto.JoinResponse> getJoinMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Join",
      requestType = com.photon.proto.JoinRequest.class,
      responseType = com.photon.proto.JoinResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.photon.proto.JoinRequest,
      com.photon.proto.JoinResponse> getJoinMethod() {
    io.grpc.MethodDescriptor<com.photon.proto.JoinRequest, com.photon.proto.JoinResponse> getJoinMethod;
    if ((getJoinMethod = JoinerServiceGrpc.getJoinMethod) == null) {
      synchronized (JoinerServiceGrpc.class) {
        if ((getJoinMethod = JoinerServiceGrpc.getJoinMethod) == null) {
          JoinerServiceGrpc.getJoinMethod = getJoinMethod =
              io.grpc.MethodDescriptor.<com.photon.proto.JoinRequest, com.photon.proto.JoinResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Join"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.photon.proto.JoinRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.photon.proto.JoinResponse.getDefaultInstance()))
              .setSchemaDescriptor(new JoinerServiceMethodDescriptorSupplier("Join"))
              .build();
        }
      }
    }
    return getJoinMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static JoinerServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JoinerServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JoinerServiceStub>() {
        @java.lang.Override
        public JoinerServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JoinerServiceStub(channel, callOptions);
        }
      };
    return JoinerServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static JoinerServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JoinerServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JoinerServiceBlockingStub>() {
        @java.lang.Override
        public JoinerServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JoinerServiceBlockingStub(channel, callOptions);
        }
      };
    return JoinerServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static JoinerServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<JoinerServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<JoinerServiceFutureStub>() {
        @java.lang.Override
        public JoinerServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new JoinerServiceFutureStub(channel, callOptions);
        }
      };
    return JoinerServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void join(com.photon.proto.JoinRequest request,
        io.grpc.stub.StreamObserver<com.photon.proto.JoinResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getJoinMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service JoinerService.
   */
  public static abstract class JoinerServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return JoinerServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service JoinerService.
   */
  public static final class JoinerServiceStub
      extends io.grpc.stub.AbstractAsyncStub<JoinerServiceStub> {
    private JoinerServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JoinerServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JoinerServiceStub(channel, callOptions);
    }

    /**
     */
    public void join(com.photon.proto.JoinRequest request,
        io.grpc.stub.StreamObserver<com.photon.proto.JoinResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getJoinMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service JoinerService.
   */
  public static final class JoinerServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<JoinerServiceBlockingStub> {
    private JoinerServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JoinerServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JoinerServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.photon.proto.JoinResponse join(com.photon.proto.JoinRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getJoinMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service JoinerService.
   */
  public static final class JoinerServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<JoinerServiceFutureStub> {
    private JoinerServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected JoinerServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new JoinerServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.photon.proto.JoinResponse> join(
        com.photon.proto.JoinRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getJoinMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_JOIN = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_JOIN:
          serviceImpl.join((com.photon.proto.JoinRequest) request,
              (io.grpc.stub.StreamObserver<com.photon.proto.JoinResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getJoinMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              com.photon.proto.JoinRequest,
              com.photon.proto.JoinResponse>(
                service, METHODID_JOIN)))
        .build();
  }

  private static abstract class JoinerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    JoinerServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return com.photon.proto.Photon.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("JoinerService");
    }
  }

  private static final class JoinerServiceFileDescriptorSupplier
      extends JoinerServiceBaseDescriptorSupplier {
    JoinerServiceFileDescriptorSupplier() {}
  }

  private static final class JoinerServiceMethodDescriptorSupplier
      extends JoinerServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    JoinerServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (JoinerServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new JoinerServiceFileDescriptorSupplier())
              .addMethod(getJoinMethod())
              .build();
        }
      }
    }
    return result;
  }
}
