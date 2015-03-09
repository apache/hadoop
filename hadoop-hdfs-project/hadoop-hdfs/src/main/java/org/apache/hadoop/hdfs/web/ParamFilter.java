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
package org.apache.hadoop.hdfs.web;

import java.net.URI;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponseFilter;
import com.sun.jersey.spi.container.ResourceFilter;
import org.apache.hadoop.util.StringUtils;

/**
 * A filter to change parameter names to lower cases
 * so that parameter names are considered as case insensitive.
 */
public class ParamFilter implements ResourceFilter {
  private static final ContainerRequestFilter LOWER_CASE
      = new ContainerRequestFilter() {
    @Override
    public ContainerRequest filter(final ContainerRequest request) {
      final MultivaluedMap<String, String> parameters = request.getQueryParameters();
      if (containsUpperCase(parameters.keySet())) {
        //rebuild URI
        final URI lower = rebuildQuery(request.getRequestUri(), parameters);
        request.setUris(request.getBaseUri(), lower);
      }
      return request;
    }
  };

  @Override
  public ContainerRequestFilter getRequestFilter() {
    return LOWER_CASE;
  }

  @Override
  public ContainerResponseFilter getResponseFilter() {
    return null;
  }

  /** Do the strings contain upper case letters? */
  static boolean containsUpperCase(final Iterable<String> strings) {
    for(String s : strings) {
      for(int i = 0; i < s.length(); i++) {
        if (Character.isUpperCase(s.charAt(i))) {
          return true;
        }
      }
    }
    return false;
  }

  /** Rebuild the URI query with lower case parameter names. */
  private static URI rebuildQuery(final URI uri,
      final MultivaluedMap<String, String> parameters) {
    UriBuilder b = UriBuilder.fromUri(uri).replaceQuery("");
    for(Map.Entry<String, List<String>> e : parameters.entrySet()) {
      final String key = StringUtils.toLowerCase(e.getKey());
      for(String v : e.getValue()) {
        b = b.queryParam(key, v);
      }
    }
    return b.build();
  }
}