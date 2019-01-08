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
package org.apache.hadoop.ozone.s3;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * Filter to adjust request headers for compatible reasons.
 */

@Provider
@PreMatching
public class HeaderPreprocessor implements ContainerRequestFilter {

  @Override
  public void filter(ContainerRequestContext requestContext) throws
      IOException {
    if (requestContext.getUriInfo().getQueryParameters()
        .containsKey("delete")) {
      //aws cli doesn't send proper Content-Type and by default POST requests
      //processed as form-url-encoded. Here we can fix this.
      requestContext.getHeaders()
          .putSingle("Content-Type", MediaType.APPLICATION_XML);
    }

    if (requestContext.getUriInfo().getQueryParameters()
        .containsKey("uploadId")) {
      //aws cli doesn't send proper Content-Type and by default POST requests
      //processed as form-url-encoded. Here we can fix this.
      requestContext.getHeaders()
          .putSingle("Content-Type", MediaType.APPLICATION_XML);
    }
  }

}
