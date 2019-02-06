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

package org.apache.hadoop.fs.azurebfs.services;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;

/**
 * The UrlQueryBuilder for Rest AbfsClient.
 */
public class AbfsUriQueryBuilder {
  private Map<String, String> parameters;

  public AbfsUriQueryBuilder() {
    this.parameters = new HashMap<>();
  }

  public void addQuery(final String name, final String value) {
    if (value != null && !value.isEmpty()) {
      this.parameters.put(name, value);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    boolean first = true;

    for (Map.Entry<String, String> entry : parameters.entrySet()) {
      if (first) {
        sb.append(AbfsHttpConstants.QUESTION_MARK);
        first = false;
      } else {
        sb.append(AbfsHttpConstants.AND_MARK);
      }
      try {
        sb.append(entry.getKey()).append(AbfsHttpConstants.EQUAL).append(AbfsClient.urlEncode(entry.getValue()));
      }
      catch (AzureBlobFileSystemException ex) {
        throw new IllegalArgumentException("Query string param is not encode-able: " + entry.getKey() + "=" + entry.getValue());
      }
    }
    return sb.toString();
  }
}
