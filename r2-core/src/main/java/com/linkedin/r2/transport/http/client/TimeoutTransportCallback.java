/*
   Copyright (c) 2012 LinkedIn Corp.

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

import com.linkedin.r2.transport.common.bridge.common.TransportCallback;
import com.linkedin.r2.util.TimeoutExecutor;

/**
 * Interface to a TransportCallback wrapper with associated timeout.
 * The expected behavior is: if the TimeoutTransportCallback's onResponse method is invoked before the timeout expires,
 * the timeout is cancelled. Otherwise, when the timeout expires, the wrapped TransportCallback's onResponse method is
 * invoked with a {@link java.util.concurrent.TimeoutException}.
 *
 * @author Francesco Capponi
 */
public interface TimeoutTransportCallback<T> extends TransportCallback<T>, TimeoutExecutor
{
}
