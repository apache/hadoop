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
package org.apache.hadoop.ozone.recon.api;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

/**
 * Endpoint for querying keys that belong to a container.
 */
@Path("/containers")
public class ContainerKeyService {

  /**
   * Return @{@link org.apache.hadoop.ozone.recon.api.types.KeyMetadata} for
   * all keys that belong to the container identified by the id param.
   *
   * @param containerId Container Id
   * @return {@link Response}
   */
  @GET
  @Path("{id}")
  public Response getKeysForContainer(@PathParam("id") String containerId) {
    return Response.ok().build();
  }
}
