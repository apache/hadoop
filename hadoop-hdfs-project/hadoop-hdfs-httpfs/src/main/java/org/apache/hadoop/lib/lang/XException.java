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

package org.apache.hadoop.lib.lang;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.util.Check;

import java.text.MessageFormat;

/**
 * Generic exception that requires error codes and uses the a message
 * template from the error code.
 */
@InterfaceAudience.Private
public class XException extends Exception {

  /**
   * Interface to define error codes.
   */
  public static interface ERROR {

    /**
     * Returns the template for the error.
     *
     * @return the template for the error, the template must be in JDK
     *         <code>MessageFormat</code> syntax (using {#} positional parameters).
     */
    public String getTemplate();

  }

  private ERROR error;

  /**
   * Private constructor used by the public constructors.
   *
   * @param error error code.
   * @param message error message.
   * @param cause exception cause if any.
   */
  private XException(ERROR error, String message, Throwable cause) {
    super(message, cause);
    this.error = error;
  }

  /**
   * Creates an XException using another XException as cause.
   * <p>
   * The error code and error message are extracted from the cause.
   *
   * @param cause exception cause.
   */
  public XException(XException cause) {
    this(cause.getError(), cause.getMessage(), cause);
  }

  /**
   * Creates an XException using the specified error code. The exception
   * message is resolved using the error code template and the passed
   * parameters.
   *
   * @param error error code for the XException.
   * @param params parameters to use when creating the error message
   * with the error code template.
   */
  @SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
  public XException(ERROR error, Object... params) {
    this(Check.notNull(error, "error"), format(error, params), getCause(params));
  }

  /**
   * Returns the error code of the exception.
   *
   * @return the error code of the exception.
   */
  public ERROR getError() {
    return error;
  }

  /**
   * Creates a message using a error message template and arguments.
   * <p>
   * The template must be in JDK <code>MessageFormat</code> syntax
   * (using {#} positional parameters).
   *
   * @param error error code, to get the template from.
   * @param args arguments to use for creating the message.
   *
   * @return the resolved error message.
   */
  private static String format(ERROR error, Object... args) {
    String template = error.getTemplate();
    if (template == null) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < args.length; i++) {
        sb.append(" {").append(i).append("}");
      }
      template = sb.deleteCharAt(0).toString();
    }
    return error + ": " + MessageFormat.format(template, args);
  }

  /**
   * Returns the last parameter if it is an instance of <code>Throwable</code>
   * returns it else it returns NULL.
   *
   * @param params parameters to look for a cause.
   *
   * @return the last parameter if it is an instance of <code>Throwable</code>
   *         returns it else it returns NULL.
   */
  private static Throwable getCause(Object... params) {
    Throwable throwable = null;
    if (params != null && params.length > 0 && params[params.length - 1] instanceof Throwable) {
      throwable = (Throwable) params[params.length - 1];
    }
    return throwable;
  }

}
