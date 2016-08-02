/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web.rest.management.resources;

import org.apache.slider.core.conf.ConfTree;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.ws.rs.core.UriBuilder;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class ConfTreeResource {

  private final String href;
  private final Map<String, Object> metadata;
  private final Map<String, String> global;
  private final Map<String, Map<String, String>> components;

  public ConfTreeResource() {
    this(null, null);
  }

  public ConfTreeResource(ConfTree confTree,
                          UriBuilder uriBuilder) {
    if (uriBuilder != null && confTree != null) {
      metadata = confTree.metadata;
      global = confTree.global;
      components = confTree.components;
      this.href = uriBuilder.build().toASCIIString();
    } else {
      this.href = null;
      this.metadata = null;
      this.global = null;
      this.components = null;
    }
  }

  public Map<String, Object> getMetadata() {
    return metadata;
  }

  public Map<String, String> getGlobal() {
    return global;
  }

  public Map<String, Map<String, String>> getComponents() {
    return components;
  }

  public String getHref() {
    return href;
  }
}
