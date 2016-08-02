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

import org.apache.slider.core.conf.AggregateConf;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.ws.rs.core.UriBuilder;
import java.util.HashMap;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class AggregateConfResource {
  private String href;
  private final ConfTreeResource resources;
  private final ConfTreeResource internal;
  private final ConfTreeResource appConf;
  @JsonIgnore
  private Map<String, ConfTreeResource> confMap;

  public AggregateConfResource(AggregateConf conf, UriBuilder uriBuilder) {
    if (uriBuilder != null) {
      this.href = uriBuilder.build().toASCIIString();
      resources = ResourceFactory.createConfTreeResource(conf.getResources(),
                   uriBuilder.clone().path("configurations").path("resources"));
      internal = ResourceFactory.createConfTreeResource(conf.getInternal(),
                   uriBuilder.clone().path("configurations").path("internal"));
      appConf = ResourceFactory.createConfTreeResource(conf.getAppConf(),
                   uriBuilder.clone().path("configurations").path("appConf"));
      initConfMap();
    } else {
      resources = null;
      internal = null;
      appConf = null;
    }
  }

  private void initConfMap() {
    confMap = new HashMap<>();
    confMap.put("internal", internal);
    confMap.put("resources", resources);
    confMap.put("appConf", appConf);
  }

  public AggregateConfResource() {
    this(null, null);
  }

  public ConfTreeResource getConfTree(String name) {
    return confMap.get(name);
  }

  public String getHref() {
    return href;
  }

  public void setHref(String href) {
    this.href = href;
  }

  public ConfTreeResource getResources() {
    return resources;
  }

  public ConfTreeResource getInternal() {
    return internal;
  }

  public ConfTreeResource getAppConf() {
    return appConf;
  }

}
