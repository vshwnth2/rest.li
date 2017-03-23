package com.linkedin.r2.transport.http.client;

import com.linkedin.common.callback.Callback;
import com.linkedin.common.util.None;

public class ChannelPoolManagerShared extends ChannelPoolManager
{
  private final Object _mutex = new Object();

  private int _checkedOutClients = 0;

  private void initializeClient()
  {
    synchronized (_mutex)
    {
      ++_checkedOutClients;
    }
  }

  public ChannelPoolManagerShared(ChannelPoolFactory channelPoolFactory)
  {
    super(channelPoolFactory);
  }

  @Override
  public void shutdown(Callback<None> callback)
  {
    int checkedOutClients;
    synchronized (_mutex)
    {
      checkedOutClients = --_checkedOutClients;
    }
    // If there are still clients of this pool, we want to still accept connection and not shutdown the pools
    if (checkedOutClients > 0)
    {
      callback.onSuccess(None.none());
      return;
    }
    super.shutdown(callback);
  }
}
