package com.linkedin.r2.transport.http.client;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.util.AttributeKey;


/**
 * Util for setting, retrieving and removing properties in Http2 streams
 */
public final class PipelineHttp2PropertyUtil {
  private PipelineHttp2PropertyUtil() {
  }

  public static <T> T set(ChannelHandlerContext ctx, Http2Connection http2Connection, int streamId,
      AttributeKey<Http2Connection.PropertyKey> key, T value) {
    return http2Connection.stream(streamId).setProperty(getKey(ctx, key), value);
  }

  public static <T> T remove(ChannelHandlerContext ctx, Http2Connection http2Connection, int streamId,
      AttributeKey<Http2Connection.PropertyKey> key) {
    return http2Connection.stream(streamId).removeProperty(getKey(ctx, key));
  }

  public static <T> T get(ChannelHandlerContext ctx, Http2Connection http2Connection, int streamId,
      AttributeKey<Http2Connection.PropertyKey> key) {
    return http2Connection.stream(streamId).getProperty(getKey(ctx, key));
  }

  private static <T> T getKey(ChannelHandlerContext ctx, AttributeKey<T> key) {
    return ctx.channel().attr(key).get();
  }
}
