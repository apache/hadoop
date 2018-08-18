/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

/**
 * Utilities class http query parameters.
 */
public class QueryParams {
  private Map<String, String> params = new HashMap<>();
  private String apiVersion = null;
  private String separator = "";
  private String serializedString = null;

  public void add(String name, String value) {
    params.put(name, value);
    serializedString = null;
  }

  public void setApiVersion(String apiVersion) {
    this.apiVersion = apiVersion;
    serializedString = null;
  }

  public String serialize() {
    if (serializedString == null) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, String> entry : params.entrySet()) {
        String name = entry.getKey();
        try {
          sb.append(separator);
          sb.append(URLEncoder.encode(name, "UTF-8"));
          sb.append('=');
          sb.append(URLEncoder.encode(entry.getValue(), "UTF-8"));
          separator = "&";
        } catch (UnsupportedEncodingException ex) {
        }
      }

      if (apiVersion != null) {
        sb.append(separator);
        sb.append("api-version=");
        sb.append(apiVersion);
        separator = "&";
      }
      serializedString = sb.toString();
    }
    return serializedString;
  }
}
