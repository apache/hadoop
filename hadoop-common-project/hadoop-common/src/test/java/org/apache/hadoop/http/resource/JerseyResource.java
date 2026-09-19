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
package org.apache.hadoop.http.resource;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Jersey resource class TestHttpServer.
 * The servlet simply puts the path and the op parameter in a map
 * and return it in JSON format in the response.
 */
@Path("")
public class JerseyResource {
  static final Logger LOG = LoggerFactory.getLogger(JerseyResource.class);

  public static final String PATH = "path";
  public static final String OP = "op";

  @GET
  @Path("{" + PATH + ":.*}")
  @Produces({MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8})
  public Response get(
      @PathParam(PATH) @DefaultValue("UNKNOWN_" + PATH) final String path,
      @QueryParam(OP) @DefaultValue("UNKNOWN_" + OP) final String op
      ) throws IOException {
    LOG.info("get: " + PATH + "=" + path + ", " + OP + "=" + op);

    final Map<String, Object> m = new TreeMap<String, Object>();
    m.put(PATH, path);
    m.put(OP, op);
    final String js = JsonUtils.toString(m);
    return Response.ok(js).type(MediaType.APPLICATION_JSON).build();
  }
}
