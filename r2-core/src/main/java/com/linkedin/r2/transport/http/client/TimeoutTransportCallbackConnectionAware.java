package com.linkedin.r2.transport.http.client;

import com.linkedin.r2.transport.common.bridge.common.TransportResponse;

import java.util.Set;
import java.util.function.Consumer;

public class TimeoutTransportCallbackConnectionAware<T, C> implements TimeoutTransportCallback<T>
{

  private TimeoutTransportCallback<T> _callback;
  private Set<TimeoutTransportCallbackConnectionAware<T, C>> _callbacks;
  private Consumer<C> _closeConnection;
  private C _connection;

  /**
   * Boolean if the close function has already been invoked
   */
  private boolean _closed = false;

  TimeoutTransportCallbackConnectionAware(TimeoutTransportCallback<T> callback,
                                          Set<TimeoutTransportCallbackConnectionAware<T, C>> callbacks,
                                          Consumer<C> closeConnection)
  {
    this._callback = callback;
    this._callbacks = callbacks;
    this._closeConnection = closeConnection;

    callbacks.add(this);
  }

  @Override
  public void onResponse(TransportResponse<T> response)
  {
    // once we have a response, the _callback doesn't have to be called anymore from anywhere else
    _callbacks.remove(this);
    _callback.onResponse(response);
  }

  @Override
  public void addTimeoutTask(Runnable task)
  {
    _callback.addTimeoutTask(task);
  }

  public void setConnection(C _connection)
  {
    this._connection = _connection;
    if (_closed)
    {
      close();
    }
  }

  public void close()
  {
    _closed = true;
    // if the _callback is set yet it means that the _callback is not in the pipeline yet.
    // the close will be called again as soon as the _connection is set
    if (_connection == null)
    {
      return;
    }
    _closeConnection.accept(_connection);
  }
}