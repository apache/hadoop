/** * Licensed to the Apache Software Foundation (ASF) under one
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
package org.apache.hadoop.yarn.server.globalpolicygenerator.webapp;

import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.GpgInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Singleton
@Path("/ws/v1/gpg")
public class GPGWebServices {

  private static final Logger LOG = LoggerFactory.getLogger(GPGWebServices.class);

  private GlobalPolicyGenerator gpgGenerator;

  @Inject
  public GPGWebServices(final @Named("gpg") GlobalPolicyGenerator gpg) {
    this.gpgGenerator = gpg;
  }

  @GET
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
      MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public GpgInfo get() {
    return new GpgInfo(this.gpgGenerator.getGPGContext());
  }

  @GET
  @Path("/info")
  @Produces({ MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
       MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8 })
  public GpgInfo getGPGInfo() {
    return new GpgInfo(this.gpgGenerator.getGPGContext());
  }
}
