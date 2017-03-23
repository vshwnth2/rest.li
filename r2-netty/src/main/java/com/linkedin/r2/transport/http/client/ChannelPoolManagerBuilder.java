/*
   Copyright (c) 2015 LinkedIn Corp.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package com.linkedin.r2.transport.http.client;

import io.netty.channel.nio.NioEventLoopGroup;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;


/**
 * Convenient class for building {@link HttpNettyStreamClient} with reasonable default configs.
 *
 * @author Ang Xu
 * @version $Revision: $
 */
class ChannelPoolManagerBuilder
{
  private final NioEventLoopGroup _eventLoopGroup;
  private final ScheduledExecutorService _scheduler;

  private ExecutorService _callbackExecutors = null;
  private SSLContext _sslContext = null;
  private SSLParameters _sslParameters = null;
  private long _requestTimeout = 10000;
  private long _shutdownTimeout = 5000;
  private long _idleTimeout = 25000;
  private int _maxHeaderSize = 8192;
  private int _maxChunkSize = 8192;
  private long _maxResponseSize = 1024 * 1024 * 2;
  private String _name = "noNameSpecifiedClient";
  private int _maxPoolSize = 200;
  private int _minPoolSize = 0;
  private int _maxConcurrentConnections = Integer.MAX_VALUE;
  private int _poolWaiterSize = Integer.MAX_VALUE;
  private AsyncPoolImpl.Strategy _strategy = AsyncPoolImpl.Strategy.MRU;
  private AbstractJmxManager _jmxManager = AbstractJmxManager.NULL_JMX_MANAGER;
  private boolean _tcpNoDelay = true;


  public ChannelPoolManagerBuilder(NioEventLoopGroup eventLoopGroup, ScheduledExecutorService scheduler)
  {
    _eventLoopGroup = eventLoopGroup;
    _scheduler = scheduler;
  }

  public ChannelPoolManagerBuilder setCallbackExecutors(ExecutorService callbackExecutors)
  {
    _callbackExecutors = callbackExecutors;
    return this;
  }

  public ChannelPoolManagerBuilder setSSLContext(SSLContext sslContext)
  {
    _sslContext = sslContext;
    return this;
  }

  public ChannelPoolManagerBuilder setSSLParameters(SSLParameters sslParameters)
  {
    _sslParameters = sslParameters;
    return this;
  }

  public ChannelPoolManagerBuilder setRequestTimeout(long requestTimeout)
  {
    _requestTimeout = requestTimeout;
    return this;
  }

  public ChannelPoolManagerBuilder setShutdownTimeout(long shutdownTimeout)
  {
    _shutdownTimeout = shutdownTimeout;
    return this;
  }

  public ChannelPoolManagerBuilder setIdleTimeout(long idleTimeout)
  {
    _idleTimeout = idleTimeout;
    return this;
  }

  public ChannelPoolManagerBuilder setMaxHeaderSize(int maxHeaderSize)
  {
    _maxHeaderSize = maxHeaderSize;
    return this;
  }

  public ChannelPoolManagerBuilder setMaxChunkSize(int maxChunkSize)
  {
    _maxChunkSize = maxChunkSize;
    return this;
  }

  public ChannelPoolManagerBuilder setMaxResponseSize(long maxResponseSize)
  {
    _maxResponseSize = maxResponseSize;
    return this;
  }

  public ChannelPoolManagerBuilder setClientName(String name)
  {
    _name = name;
    return this;
  }

  public ChannelPoolManagerBuilder setMaxPoolSize(int maxPoolSize)
  {
    _maxPoolSize = maxPoolSize;
    return this;
  }

  public ChannelPoolManagerBuilder setMinPoolSize(int minPoolSize)
  {
    _minPoolSize = minPoolSize;
    return this;
  }

  public ChannelPoolManagerBuilder setMaxConcurrentConnections(int maxConcurrentConnections) {
    _maxConcurrentConnections = maxConcurrentConnections;
    return this;
  }

  public ChannelPoolManagerBuilder setPoolWaiterSize(int poolWaiterSize)
  {
    _poolWaiterSize = poolWaiterSize;
    return this;
  }

  public ChannelPoolManagerBuilder setStrategy(AsyncPoolImpl.Strategy strategy)
  {
    _strategy = strategy;
    return this;
  }

  public ChannelPoolManagerBuilder setJmxManager(AbstractJmxManager jmxManager)
  {
    _jmxManager = jmxManager;
    return this;
  }

  public ChannelPoolManagerBuilder setTcpNoDelay(boolean tcpNoDelay)
  {
    _tcpNoDelay = tcpNoDelay;
    return this;
  }

  public ChannelPoolManager buildStream()
  {
    return new ChannelPoolManager(
      new HttpNettyStreamChannelPoolFactoryImpl(
        _maxPoolSize,
        _idleTimeout,
        _poolWaiterSize,
        _strategy,
        _minPoolSize,
        _tcpNoDelay,
        _scheduler,
        _requestTimeout,
        _maxConcurrentConnections,
        _sslContext,
        _sslParameters,
        _maxHeaderSize,
        _maxChunkSize,
        _maxResponseSize,
        _eventLoopGroup),
      "R2 Stream Http1 " + ChannelPoolManager.BASE_NAME);
  }

  public ChannelPoolManager buildRest()
  {
    return new ChannelPoolManager(
      new HttpNettyChannelPoolFactoryImpl(
        _maxPoolSize,
        _idleTimeout,
        _poolWaiterSize,
        _strategy,
        _minPoolSize,
        _eventLoopGroup,
        _sslContext,
        _sslParameters,
        _maxHeaderSize,
        _maxChunkSize,
        (int) _maxResponseSize,
        _scheduler,
        _requestTimeout,
        _maxConcurrentConnections),
      "R2 Stream Http2" + ChannelPoolManager.BASE_NAME);
  }

  public ChannelPoolManager buildHttp2Stream()
  {
    return new ChannelPoolManager(
      new Http2NettyStreamChannelPoolFactory(
        _idleTimeout,
        _poolWaiterSize,
        _tcpNoDelay,
        _scheduler,
        _sslContext,
        _sslParameters,
        _requestTimeout,
        _maxHeaderSize,
        _maxChunkSize,
        _maxResponseSize,
        _eventLoopGroup),
      "R2 Stream Http2" + ChannelPoolManager.BASE_NAME);
  }
}
