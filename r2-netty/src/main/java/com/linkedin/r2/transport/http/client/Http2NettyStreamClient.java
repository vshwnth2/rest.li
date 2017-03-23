/*
   Copyright (c) 2016 LinkedIn Corp.

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

/**
 * $Id: $
 */

package com.linkedin.r2.transport.http.client;

import com.google.common.collect.Sets;
import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;
import com.linkedin.r2.filter.R2Constants;
import com.linkedin.r2.message.Request;
import com.linkedin.r2.message.RequestContext;
import com.linkedin.r2.message.stream.StreamResponse;
import com.linkedin.r2.transport.common.bridge.common.RequestWithCallback;
import com.linkedin.r2.transport.common.bridge.common.TransportResponseImpl;
import com.linkedin.r2.transport.http.common.HttpProtocolVersion;
import com.linkedin.r2.util.Cancellable;
import com.linkedin.r2.util.TimeoutRunnable;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * @author Steven Ihde
 * @author Ang Xu
 * @author Zhenkai Zhu
 * @author Sean Sheng
 * @version $Revision: $
 */

/* package private */ class Http2NettyStreamClient extends AbstractNettyStreamClient
{
  static final Logger LOG = LoggerFactory.getLogger(Http2NettyStreamClient.class);

  private final ChannelPoolManager _channelPoolManager;
  private final ScheduledExecutorService _scheduler;
  private final long _requestTimeout;

  /**
   * Creates a new Http2NettyStreamClient
   *
   * @param eventLoopGroup            The NioEventLoopGroup; it is the caller's responsibility to
   *                                  shut it down
   * @param scheduler                  An executor; it is the caller's responsibility to shut it down
   * @param requestTimeout            Timeout, in ms, to get a connection from the pool or create one
   * @param idleTimeout               Interval after which idle connections will be automatically closed
   * @param shutdownTimeout           Timeout, in ms, the client should wait after shutdown is
   *                                  initiated before terminating outstanding requests
   * @param maxResponseSize           Maximum size of a HTTP response
   * @param sslContext                {@link SSLContext}
   * @param sslParameters             {@link SSLParameters}with overloaded construct
   * @param callbackExecutors         An optional EventExecutorGroup to invoke user callback
   * @param poolWaiterSize            Maximum waiters waiting on the HTTP connection pool
   * @param name                      Name of the {@link HttpNettyStreamClient}
   * @param jmxManager                A management class that is aware of the creation/shutdown event
   *                                  of the underlying {@link ChannelPoolManager}
   * @param maxHeaderSize             Maximum size of all HTTP headers
   * @param maxChunkSize              Maximum size of a HTTP chunk
   * @param maxConcurrentConnections  Maximum number of concurrent connection attempts the HTTP
   *                                  connection pool can make.
   */
  public Http2NettyStreamClient(NioEventLoopGroup eventLoopGroup, ScheduledExecutorService scheduler,
      long requestTimeout, long idleTimeout, long shutdownTimeout, long maxResponseSize, SSLContext sslContext,
      SSLParameters sslParameters, ExecutorService callbackExecutors, int poolWaiterSize, String name,
      AbstractJmxManager jmxManager, int maxHeaderSize,
      int maxChunkSize, int maxConcurrentConnections, boolean tcpNoDelay)
  {
    super(eventLoopGroup, scheduler, requestTimeout, shutdownTimeout, maxResponseSize, callbackExecutors,
        jmxManager, maxConcurrentConnections);

    _channelPoolManager = new ChannelPoolManager(
      new Http2NettyStreamChannelPoolFactory(
        idleTimeout,
        poolWaiterSize,
        tcpNoDelay,
        scheduler,
        sslContext,
        sslParameters,
        requestTimeout,
        maxHeaderSize,
        maxChunkSize,
        maxResponseSize,
        eventLoopGroup),
      name + ChannelPoolManager.BASE_NAME);
    _scheduler = scheduler;
    _requestTimeout = requestTimeout;

    _jmxManager.onProviderCreate(_channelPoolManager);
  }

  public Http2NettyStreamClient(NioEventLoopGroup eventLoopGroup, ScheduledExecutorService scheduler,
                                long requestTimeout, long shutdownTimeout, long maxResponseSize,
                                ExecutorService callbackExecutors,
                                AbstractJmxManager jmxManager,
                                int maxConcurrentConnections, ChannelPoolManager channelPoolManager)
  {
    super(eventLoopGroup, scheduler, requestTimeout, shutdownTimeout, maxResponseSize, callbackExecutors,
      jmxManager, maxConcurrentConnections);
    _channelPoolManager = channelPoolManager;
    _scheduler = scheduler;
    _requestTimeout = requestTimeout;

    _jmxManager.onProviderCreate(_channelPoolManager);
  }


  @Override
  public Map<String, PoolStats> getPoolStats()
  {
    return _channelPoolManager.getPoolStats();
  }

  @Override
  protected void doShutdown(Callback<None> callback)
  {
    final long deadline = System.currentTimeMillis() + _shutdownTimeout;
    TimeoutCallback<None> closeChannelsCallback = new ChannelPoolShutdownCallback(
      _scheduler, _shutdownTimeout, TimeUnit.MILLISECONDS, deadline, callback);
    _channelPoolManager.shutdown(closeChannelsCallback);
    _jmxManager.onProviderShutdown(_channelPoolManager);
  }

  class TimeoutTransportCallbackConnectionAwareHttp2 extends TimeoutTransportCallbackConnectionAware<StreamResponse, Map.Entry<Channel, Http2Stream>>
  {
    TimeoutTransportCallbackConnectionAwareHttp2(TimeoutTransportCallback<StreamResponse> callback,
                                                 Set<TimeoutTransportCallbackConnectionAware<StreamResponse, Map.Entry<Channel, Http2Stream>>> timeoutTransportCallbacks,
                                                 Consumer<Map.Entry<Channel, Http2Stream>> closeConnection)
    {
      super(callback, timeoutTransportCallbacks, closeConnection);
    }
  }

  @Override
  protected void doWriteRequest(Request request, final RequestContext context, SocketAddress address,
      TimeoutTransportCallback<StreamResponse> callback)
  {
    final AsyncPool<Channel> pool;
    try
    {
      pool = _channelPoolManager.getPoolForAddress(address);
    }
    catch (IllegalStateException e)
    {
      errorResponse(callback, e);
      return;
    }

    context.putLocalAttr(R2Constants.HTTP_PROTOCOL_VERSION, HttpProtocolVersion.HTTP_2);


    TimeoutTransportCallbackConnectionAwareHttp2 newCallback
      = new TimeoutTransportCallbackConnectionAwareHttp2(callback, callbacks, channelStreamEntry ->
    {
      Channel channel = channelStreamEntry.getKey();
      Http2Stream stream = channelStreamEntry.getValue();

      stream.close();
      Http2Connection.PropertyKey handleKey =
        channel.attr(Http2ClientPipelineInitializer.CHANNEL_POOL_HANDLE_ATTR_KEY).get();
      AsyncPoolHandle<Channel> handle = stream.getProperty(handleKey);
      if (handle != null)
      {
        handle.release();
      }
    });

    Callback<Channel> getCallback = new ChannelPoolGetCallback(pool, request, newCallback);
    final Cancellable pendingGet = pool.get(getCallback);
    if (pendingGet != null)
    {
      newCallback.addTimeoutTask(() -> pendingGet.cancel());
    }
  }

  Set<TimeoutTransportCallbackConnectionAware<StreamResponse, Map.Entry<Channel, Http2Stream>>> callbacks = Sets.newConcurrentHashSet();

  private class ChannelPoolGetCallback implements Callback<Channel>
  {
    private final AsyncPool<Channel> _pool;
    private final Request _request;
    private final TimeoutTransportCallbackConnectionAwareHttp2 _callback;

    ChannelPoolGetCallback(AsyncPool<Channel> pool, Request request, TimeoutTransportCallbackConnectionAwareHttp2 callback)
    {
      _pool = pool;
      _request = request;
      _callback = callback;
    }

    @Override
    public void onSuccess(Channel channel)
    {
      State state = _state.get();
      if (state == AbstractNettyStreamClient.State.REQUESTS_STOPPING || state == AbstractNettyStreamClient.State.SHUTDOWN)
      {
        // In this case, we acquired a channel from the pool as request processing is halting.
        // The shutdown task might not timeout this callback, since it may already have scanned
        // all the channels for pending requests before we set the callback as the channel
        // attachment.  The TimeoutTransportCallback ensures the user callback in never
        // invoked more than once, so it is safe to invoke it unconditionally.
        _callback.onResponse(TransportResponseImpl
            .<StreamResponse>error(new TimeoutException("Operation did not complete before shutdown")));
        return;
      }

      // By wrapping the channel and the pool in a timeout handle we can guarantee the following
      // 1. using the handle is the only mean to return a channel back to the pool because the reference to the
      //    channel pool is not otherwise passed along
      // 2. the channel can be returned back to the pool at most once through the handle
      // 3. the channel will eventually be returned to the pool due to timeout of handle
      TimeoutAsyncPoolHandle<Channel> handle = new TimeoutAsyncPoolHandle<>(
          _pool, _scheduler, _requestTimeout, TimeUnit.MILLISECONDS, channel);

      RequestWithCallback<Request, TimeoutTransportCallback<StreamResponse>, TimeoutAsyncPoolHandle<Channel>> request =
          new RequestWithCallback<>(_request, _callback, handle);

      // here we want the exception in outbound operations to be passed back through pipeline so that
      // the user callback would be invoked with the exception and the channel can be put back into the pool
      channel.writeAndFlush(request).addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);
    }

    @Override
    public void onError(Throwable e)
    {
      _callback.onResponse(TransportResponseImpl.<StreamResponse>error(e));
    }
  }

  private class ChannelPoolShutdownCallback extends TimeoutCallback<None>
  {
    public ChannelPoolShutdownCallback(ScheduledExecutorService scheduler, long timeout, TimeUnit timeoutUnit,
        long deadline, Callback<None> callback)
    {
      super(scheduler, timeout, timeoutUnit, new Callback<None>()
      {
        @Override
        public void onSuccess(None result)
        {
          LOG.info("All connection pools shut down, closing all channels");
          finishShutdown();
        }

        @Override
        public void onError(Throwable e)
        {
          LOG.warn("Error shutting down HTTP connection pools, ignoring and continuing shutdown", e);
          finishShutdown();
        }

        private void finishShutdown()
        {
          _state.set(AbstractNettyStreamClient.State.REQUESTS_STOPPING);
          callbacks.forEach(callback -> errorResponse(callback, new TimeoutException("Operation did not complete before shutdown")));
          callbacks.forEach(TimeoutTransportCallbackConnectionAware::close);

          // Close all active and idle Channels
          new TimeoutRunnable(scheduler, deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS, () ->
          {
            _state.set(State.SHUTDOWN);
            LOG.info("Shutdown complete");
            callback.onSuccess(None.none());
          }, "Timed out waiting for channels to close, continuing shutdown").run();
        }
      }, "Connection pool shutdown timeout exceeded (" + _shutdownTimeout + "ms)");
    }
  }
}
