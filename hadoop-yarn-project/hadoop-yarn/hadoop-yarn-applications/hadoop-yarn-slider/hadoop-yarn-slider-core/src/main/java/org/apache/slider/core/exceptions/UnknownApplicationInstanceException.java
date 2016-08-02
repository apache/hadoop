/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.core.exceptions;

public class UnknownApplicationInstanceException extends SliderException {
  public UnknownApplicationInstanceException(String s) {
    super(EXIT_UNKNOWN_INSTANCE, s);
  }

  public UnknownApplicationInstanceException(String s, Throwable throwable) {
    super(EXIT_UNKNOWN_INSTANCE, throwable, s);
  }

  public UnknownApplicationInstanceException(String message,
      Object... args) {
    super(EXIT_UNKNOWN_INSTANCE, message, args);
  }

  /**
   * Create an instance with the standard exception name
   * @param name name
   * @return an instance to throw
   */
  public static UnknownApplicationInstanceException unknownInstance(String name) {
    return new UnknownApplicationInstanceException(ErrorStrings.E_UNKNOWN_INSTANCE
                                   + ": " + name);
  }
  public static UnknownApplicationInstanceException unknownInstance(String name,
      Throwable throwable) {
    UnknownApplicationInstanceException exception =
      unknownInstance(name);
    exception.initCause(throwable);
    return exception;
  }
}
