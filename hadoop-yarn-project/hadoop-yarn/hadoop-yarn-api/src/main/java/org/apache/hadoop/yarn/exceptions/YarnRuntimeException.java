/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.exceptions;

import org.apache.hadoop.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/** Base YARN Exception.
 *
 * NOTE: All derivatives of this exception, which may be thrown by a remote
 * service, must include a String only constructor for the exception to be
 * unwrapped on the client.
 */
@LimitedPrivate({ "MapReduce", "YARN" })
@Unstable
public class YarnRuntimeException extends RuntimeException {

  private static final long serialVersionUID = -7153142425412203936L;
  public YarnRuntimeException(Throwable cause) { super(cause); }
  public YarnRuntimeException(String message) { super(message); }
  public YarnRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
