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

package org.apache.hadoop.fs.swift.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * Variant of Hadoop NetUtils exception wrapping with URI awareness and
 * available in branch-1 too.
 */
public class ExceptionDiags {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExceptionDiags.class);

  /** text to point users elsewhere: {@value} */
  private static final String FOR_MORE_DETAILS_SEE
    = " For more details see:  ";
  /** text included in wrapped exceptions if the host is null: {@value} */
  public static final String UNKNOWN_HOST = "(unknown)";
  /** Base URL of the Hadoop Wiki: {@value} */
  public static final String HADOOP_WIKI = "http://wiki.apache.org/hadoop/";

  /**
   * Take an IOException and a URI, wrap it where possible with
   * something that includes the URI
   *
   * @param dest target URI
   * @param operation operation
   * @param exception the caught exception.
   * @return an exception to throw
   */
  public static IOException wrapException(final String dest,
                                          final String operation,
                                          final IOException exception) {
    String action = operation + " " + dest;
    String xref = null;

    if (exception instanceof ConnectException) {
      xref = "ConnectionRefused";
    } else if (exception instanceof UnknownHostException) {
      xref = "UnknownHost";
    } else if (exception instanceof SocketTimeoutException) {
      xref = "SocketTimeout";
    } else if (exception instanceof NoRouteToHostException) {
      xref = "NoRouteToHost";
    }
    String msg = action
                 + " failed on exception: "
                 + exception;
    if (xref != null) {
       msg = msg + ";" + see(xref);
    }
    return wrapWithMessage(exception, msg);
  }

  private static String see(final String entry) {
    return FOR_MORE_DETAILS_SEE + HADOOP_WIKI + entry;
  }

  @SuppressWarnings("unchecked")
  private static <T extends IOException> T wrapWithMessage(
    T exception, String msg) {
    Class<? extends Throwable> clazz = exception.getClass();
    try {
      Constructor<? extends Throwable> ctor =
        clazz.getConstructor(String.class);
      Throwable t = ctor.newInstance(msg);
      return (T) (t.initCause(exception));
    } catch (Throwable e) {
      return exception;
    }
  }

}
