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
package org.apache.hadoop.security.token.delegation.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Servlet utility methods.
 */
@InterfaceAudience.Private
class ServletUtils {
  private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");

  /**
   * Extract a query string parameter without triggering http parameters
   * processing by the servlet container.
   *
   * @param request the request
   * @param name the parameter to get the value.
   * @return the parameter value, or <code>NULL</code> if the parameter is not
   * defined.
   * @throws IOException thrown if there was an error parsing the query string.
   */
  public static String getParameter(HttpServletRequest request, String name)
      throws IOException {
    List<NameValuePair> list = URLEncodedUtils.parse(request.getQueryString(),
        UTF8_CHARSET);
    if (list != null) {
      for (NameValuePair nv : list) {
        if (name.equals(nv.getName())) {
          return nv.getValue();
        }
      }
    }
    return null;
  }
}
