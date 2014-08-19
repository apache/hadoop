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

package org.apache.hadoop.security;

import java.net.URI;

import org.apache.hadoop.fs.Path;

public class ProviderUtils {
  /**
   * Convert a nested URI to decode the underlying path. The translation takes
   * the authority and parses it into the underlying scheme and authority.
   * For example, "myscheme://hdfs@nn/my/path" is converted to
   * "hdfs://nn/my/path".
   * @param nestedUri the URI from the nested URI
   * @return the unnested path
   */
  public static Path unnestUri(URI nestedUri) {
    String[] parts = nestedUri.getAuthority().split("@", 2);
    StringBuilder result = new StringBuilder(parts[0]);
    result.append("://");
    if (parts.length == 2) {
      result.append(parts[1]);
    }
    result.append(nestedUri.getPath());
    if (nestedUri.getQuery() != null) {
      result.append("?");
      result.append(nestedUri.getQuery());
    }
    if (nestedUri.getFragment() != null) {
      result.append("#");
      result.append(nestedUri.getFragment());
    }
    return new Path(result.toString());
  }
}
