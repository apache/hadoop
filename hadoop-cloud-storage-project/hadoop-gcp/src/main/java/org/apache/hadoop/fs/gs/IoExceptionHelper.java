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

package org.apache.hadoop.fs.gs;

import java.io.IOError;
import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import javax.net.ssl.SSLException;

/**
 * Translates exceptions from API calls into higher-level meaning, while allowing injectability for
 * testing how API errors are handled.
 */
public final class IoExceptionHelper {

  private IoExceptionHelper() {}

  /**
   * Determines if a given {@link Throwable} is caused by an IO error.
   *
   * <p>Recursively checks {@code getCause()} if outer exception isn't an instance of the correct
   * class.
   *
   * @param throwable The {@link Throwable} to check.
   * @return True if the {@link Throwable} is a result of an IO error.
   */
  public static boolean isIoError(Throwable throwable) {
    if (throwable instanceof IOException || throwable instanceof IOError) {
      return true;
    }
    Throwable cause = throwable.getCause();
    return cause != null && isIoError(cause);
  }

  /**
   * Determines if a given {@link Throwable} is caused by a socket error.
   *
   * <p>Recursively checks {@code getCause()} if outer exception isn't an instance of the correct
   * class.
   *
   * @param throwable The {@link Throwable} to check.
   * @return True if the {@link Throwable} is a result of a socket error.
   */
  public static boolean isSocketError(Throwable throwable) {
    if (throwable instanceof SocketException || throwable instanceof SocketTimeoutException) {
      return true;
    }
    Throwable cause = throwable.getCause();
    // Subset of SSL exceptions that are caused by IO errors (e.g. SSLHandshakeException due to
    // unexpected connection closure) is also a socket error.
    if (throwable instanceof SSLException && cause != null && isIoError(cause)) {
      return true;
    }
    return cause != null && isSocketError(cause);
  }

  /**
   * Determines if a given {@link IOException} is caused by a timed out read.
   *
   * @param e The {@link IOException} to check.
   * @return True if the {@link IOException} is a result of a read timeout.
   */
  public static boolean isReadTimedOut(IOException e) {
    return e instanceof SocketTimeoutException && e.getMessage().equalsIgnoreCase("Read timed out");
  }
}
