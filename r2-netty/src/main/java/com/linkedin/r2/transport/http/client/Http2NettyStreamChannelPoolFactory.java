package com.linkedin.r2.transport.http.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.net.SocketAddress;
import java.util.concurrent.ScheduledExecutorService;

class Http2NettyStreamChannelPoolFactory implements ChannelPoolFactory
{
  private final Bootstrap _bootstrap;
  private final long _idleTimeout;
  private final int _maxPoolWaiterSize;
  private final boolean _tcpNoDelay;
  private final ChannelGroup _allChannels;
  private final ScheduledExecutorService _scheduler;

  Http2NettyStreamChannelPoolFactory(
    long idleTimeout,
    int maxPoolWaiterSize,
    boolean tcpNoDelay,
    ScheduledExecutorService scheduler,
    SSLContext sslContext,
    SSLParameters sslParameters,
    long gracefulShutdownTimeout,
    int maxHeaderSize,
    int maxChunkSize,
    long maxResponseSize,
    EventLoopGroup eventLoopGroup)
  {
    ChannelInitializer<NioSocketChannel> initializer = new Http2ClientPipelineInitializer(
      sslContext, sslParameters, scheduler, maxHeaderSize, maxChunkSize, maxResponseSize, gracefulShutdownTimeout);
    Bootstrap bootstrap = new Bootstrap().group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(initializer);

    _bootstrap = bootstrap;
    _idleTimeout = idleTimeout;
    _maxPoolWaiterSize = maxPoolWaiterSize;
    _tcpNoDelay = tcpNoDelay;
    _allChannels = new DefaultChannelGroup("R2 client channels", eventLoopGroup.next());

    _scheduler = scheduler;
  }

  @Override
  public AsyncPool<Channel> getPool(SocketAddress address)
  {
    return new AsyncSharedPoolImpl<>(
      address.toString() + " HTTP connection pool",
      new ChannelPoolLifecycle(
        address,
        _bootstrap,
        _allChannels,
        _tcpNoDelay),
      _scheduler,
      new NoopRateLimiter(),
      _idleTimeout,
      _maxPoolWaiterSize);
  }
}
