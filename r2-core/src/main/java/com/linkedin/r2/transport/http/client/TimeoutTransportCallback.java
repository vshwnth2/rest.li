package com.linkedin.r2.transport.http.client;

import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.util.TimeoutExecutor;

/**
 * A TransportCallback wrapper with associated timeout.  If the TimeoutTransportCallback's
 * onResponse method is invoked before the timeout expires, the timeout is cancelled.
 * Otherwise, when the timeout expires, the wrapped TransportCallback's onResponse method is
 * invoked with a {@link java.util.concurrent.TimeoutException}.
 *
 * @author Steven Ihde
 * @version $Revision: $
 */
public interface TimeoutTransportCallback<T> extends TransportCallback<T>, TimeoutExecutor
{
}
