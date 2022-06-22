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

package org.apache.hadoop.runc.docker;

import java.net.MalformedURLException;
import java.net.URL;

public class DockerContext {

  private final URL baseUrl;

  public DockerContext(URL baseUrl) throws MalformedURLException {
    this.baseUrl = normalize(baseUrl);

  }

  private static URL normalize(URL url) throws MalformedURLException {
    if (!url.getPath().endsWith("/")) {
      url = new URL(
          url.getProtocol(),
          url.getHost(),
          url.getPort(),
          url.getPath() + "/" + (url.getQuery() == null ? "" : url.getQuery()));
    }
    return url;
  }

  public URL getBaseUrl() {
    return baseUrl;
  }

  @Override
  public String toString() {
    return String.format("docker-context { baseUrl=%s }", baseUrl);
  }
}
