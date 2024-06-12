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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.yarn.server.globalpolicygenerator.GlobalPolicyGenerator;
import org.apache.hadoop.yarn.server.globalpolicygenerator.webapp.dao.GpgInfo;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Singleton
@Path("/ws/v1/gpg")
public class GPGWebServices {

  private static final Logger LOG = LoggerFactory.getLogger(GPGWebServices.class);

  private GlobalPolicyGenerator gpgGenerator;
  private WebApp webapp;

  @Inject
  public GPGWebServices(final GlobalPolicyGenerator gpg, final WebApp webapp) {
    this.gpgGenerator = gpg;
    this.webapp = webapp;
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
