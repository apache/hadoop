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

package org.apache.hadoop.http;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Contains utility methods and constants relating to Jetty.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class JettyUtils {
  public static final String UTF_8 = "charset=utf-8";
  public static final int HEADER_SIZE = 1024 * 64;

  /**
   * Clears the content type a response is carrying, charset included.
   * <p>
   * Every server built by {@link HttpServer2} runs
   * {@code QuotingInputFilter}, which sets {@code text/plain; charset=utf-8}
   * before the request reaches the resource. A JAX-RS resource that picks its
   * own content type has to undo that first, and {@code setContentType(null)}
   * alone is not enough on Jetty 12: it drops the charset but remembers that
   * one had been set explicitly, so the next content type gets that memory
   * appended to it - literally {@code ;charset=null} for a type that carries
   * no charset of its own, such as {@code application/octet-stream} or
   * {@code application/xml}. Clearing the encoding as well resets that state.
   *
   * @param response the response to clear
   */
  public static void clearContentType(HttpServletResponse response) {
    response.setContentType(null);
    response.setCharacterEncoding(null);
  }

  private JettyUtils() {
  }
}
