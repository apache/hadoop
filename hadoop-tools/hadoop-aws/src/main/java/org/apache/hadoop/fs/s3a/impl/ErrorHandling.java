/*
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

package org.apache.hadoop.fs.s3a.impl;

import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.UnknownHostException;

/**
 * Error handling and exception translation.
 */
public class ErrorHandling {

  /**
   * Translate an exception if it or its inner exception is an
   * IOException.
   * If this condition is not met, null is returned.
   * @param path path of operation.
   * @param thrown exception
   * @return a translated exception or null.
   */
  public static IOException maybeTranslateNetworkException(String path, Throwable thrown) {

    if (thrown == null) {
      return null;
    }

    // look inside
    Throwable cause = thrown.getCause();
    while(cause != null && cause.getCause() != null) {
      cause = cause.getCause();
    }
    if (cause == null || !(cause instanceof IOException)) {
      return null;
    }

    // the cause can be extracted to an IOE.
    // rather than just return it, we try to preserve the stack trace
    // of the outer exception.
    IOException ioe = (IOException) cause;
    // now examine any IOE
    if (ioe instanceof UnknownHostException) {
      return (IOException) new UnknownHostException(thrown + ": " + ioe.getMessage())
          .initCause(thrown);
    }
    if (ioe instanceof NoRouteToHostException) {
      return (IOException) new NoRouteToHostException(thrown + ": " + ioe.getMessage())
          .initCause(thrown);
    }
    if (ioe instanceof ConnectException) {
      return (IOException) new ConnectException(thrown + ": " + ioe.getMessage())
          .initCause(thrown);
    }

    // currently not attempting to translate any other exception types.
    return null;

  }
}
