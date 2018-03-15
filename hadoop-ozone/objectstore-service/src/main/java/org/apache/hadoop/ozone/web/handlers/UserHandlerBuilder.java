/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.web.handlers;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.ozone.web.interfaces.UserAuth;
import org.apache.hadoop.ozone.web.userauth.Simple;

/**
 * This class is responsible for providing a
 * {@link org.apache.hadoop.ozone.web.interfaces.UserAuth}
 * implementation to object store web handlers.
 */
@InterfaceAudience.Private
public final class UserHandlerBuilder {

  private static final ThreadLocal<UserAuth> USER_AUTH_THREAD_LOCAL =
      new ThreadLocal<UserAuth>();

  /**
   * Returns the configured UserAuth from thread-local storage for this
   * thread.
   *
   * @return UserAuth from thread-local storage
   */
  public static UserAuth getAuthHandler() {
    UserAuth authHandler = USER_AUTH_THREAD_LOCAL.get();
    if (authHandler != null) {
      return authHandler;
    } else {
      // This only happens while using mvn jetty:run for testing.
      return new Simple();
    }
  }

  /**
   * Removes the configured UserAuth from thread-local storage for this
   * thread.
   */
  public static void removeAuthHandler() {
    USER_AUTH_THREAD_LOCAL.remove();
  }

  /**
   * Sets the configured UserAuthHandler in thread-local storage for this
   * thread.
   *
   * @param authHandler authHandler to set in thread-local storage
   */
  public static void setAuthHandler(UserAuth authHandler) {
    USER_AUTH_THREAD_LOCAL.set(authHandler);
  }

  /**
   * There is no reason to instantiate this class.
   */
  private UserHandlerBuilder() {
  }
}
