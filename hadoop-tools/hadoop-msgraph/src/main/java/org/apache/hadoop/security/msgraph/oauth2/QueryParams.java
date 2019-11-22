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

package org.apache.hadoop.security.msgraph.oauth2;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


/**
 * Utility class for constructing parameterized requests.
 */
public class QueryParams {

  /** Map with the parameters. */
  private Map<String, String> params = new HashMap<>();
  /** Separator among parameters. At the beginning is empty. */
  private String separator = "";
  /** Cached string containing all the parameters. */
  private String serializedString = null;

  public QueryParams() {
    // Empty constructor
  }

  /**
   * Add parameter to the query parameters.
   * @param name name of the parameter to add.
   * @param value value of the parameter to add.
   */
  public void add(String name, String value) {
    this.params.put(name, value);
    this.serializedString = null;
  }

  /**
   * Serialize the added parameters into a URL encoded String.
   */
  public String serialize() {
    if (this.serializedString == null) {
      StringBuilder sb = new StringBuilder();
      String encoding = StandardCharsets.UTF_8.name();

      for (Entry<String, String> entry : this.params.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        try {
          sb.append(this.separator)
              .append(URLEncoder.encode(name, encoding))
              .append('=')
              .append(URLEncoder.encode(value, encoding));
          this.separator = "&";
        } catch (UnsupportedEncodingException ignored) {
        }
      }

      this.serializedString = sb.toString();
    }
    return this.serializedString;
  }
}